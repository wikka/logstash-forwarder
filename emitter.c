#include <sys/types.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h> /* C99 for int64_t */
#include <string.h>
#include <unistd.h>
#include "zmq.h"
#include "emitter.h"
#include "insist.h"
#include "backoff.h"
#include "clock_gettime.h"
#include <unistd.h>
#include <hiredis/hiredis.h>

#include <sys/resource.h>

#include "sleepdefs.h"

static void run_stunnel(struct emitter_config *config, const char *socketpath) {
  pid_t child;

  /* Generate stunnel config */
  int rc;
  int in[2];
  rc = pipe(in);
  if (rc == -1) {
    fprintf(stderr, "pipe(2) failed: %s\n", strerror(errno));
    exit(1);
  }

  child = fork();
  if (child < 0) {
    fprintf(stderr, "Failed to fork: %s\n", strerror(errno));
    exit(1);
  }

  if (child > 0) {
    FILE *configfile = fdopen(in[1], "w");
    fprintf(configfile, "compression = zlib\n");
    fprintf(configfile, "foreground = yes\n");
    fprintf(configfile, "pid = %s.pid\n", socketpath);
    fprintf(configfile, "CAfile = %s\n", config->ssl_ca_path);

    if (config->ssl_certificate != NULL) {
      fprintf(configfile, "cert = %s\n", config->ssl_certificate);
    }

    if (config->ssl_key != NULL) {
      fprintf(configfile, "key = %s\n", config->ssl_key);
    }
    fprintf(configfile, "options = NO_SSLv2\n");
    //fprintf(configfile, "output = /dev/stdout\n");
    fprintf(configfile, "client = yes\n");
    fprintf(configfile, "syslog = no\n");
    fprintf(configfile, "[redis-ssl]\n");
    fprintf(configfile, "accept = %s\n", socketpath);
    fprintf(configfile, "connect = %s\n", config->redis_address);
    fclose(configfile);
    close(in[0]);
  } else {
    close(in[1]);
    dup2(in[0], 0); /* redirect stdin */
    for (int i = 3; i < 10000; i++) { close(i); }
    //execlp("cat", "cat", (char *)NULL);
    execlp(config->stunnel_path, config->stunnel_path, "-fd", "0", (char *)NULL);
    fprintf(stderr, "exec(%s) failed: %s\n", config->stunnel_path,
            strerror(errno));
    exit(1);
  }
} /* run_stunnel */

static redisContext *redis_connect(const char *address) {
  struct backoff sleeper;
  backoff_init(&sleeper, &MIN_SLEEP, &MAX_SLEEP);
  redisContext *redis = NULL;

  struct timeval timeout = { 3, 0 };
  while (redis == NULL) {
    /* Assume unix socket if the address has a slash "/" in it */
    if (strchr(address, '/') != NULL) {
      redis = redisConnectUnixWithTimeout(address, timeout);
    } else {
      int port = 6379;
      /* TODO(sissel): parse port from address */
      redis = redisConnectWithTimeout(address, port, timeout);
    }
    if (redis == NULL) {
      printf("Failed to create a new redisContext\n");
      backoff(&sleeper);
    } else if (redis->err) {
      printf("connection to redis failed (%s): %s\n", address, redis->errstr);
      redisFree(redis);
      redis = NULL;
      backoff(&sleeper);
    }
  } /* while (redis != NULL) */

  return redis;
} /* redis_connect */

void *emitter(void *arg) {
  struct emitter_config *config = arg;
  int rc;
  redisContext *redis;
  struct backoff sleeper;
  backoff_init(&sleeper, &MIN_SLEEP, &MAX_SLEEP);

  void *socket = zmq_socket(config->zmq, ZMQ_PULL);
  insist(socket != NULL, "zmq_socket() failed: %s", strerror(errno));
  int64_t hwm = 100;
  zmq_setsockopt(socket, ZMQ_HWM, &hwm, sizeof(hwm));
  rc = zmq_bind(socket, config->zmq_endpoint);
  insist(rc != -1, "zmq_bind(%s) failed: %s", config->zmq_endpoint,
         zmq_strerror(errno));

  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);

  /* TODO(sissel): if ssl settings are given, run stunnel
   *   - generate an stunnel config
   *   - run stunnel as a child process
   */
  if (config->ssl_ca_path) {
    /* We'll have stunnel talk to the redis address. 
     * Create a temporary file that we'll use as a unix socket
     * to have redis client talk through stunnel. */
    char *tmpdir = getenv("TMP");
    if (tmpdir == NULL) {
      tmpdir = "/tmp";
    }
    char localsocket[100];
    snprintf(localsocket, 100, "%s/redis-stunnel.%d.sock", tmpdir, getpid());
    unlink(localsocket);
    run_stunnel(config, localsocket);
    sleep(30);
    config->redis_address = localsocket;
  }

  redis = redis_connect(config->redis_address);
  long count = 0, bytes = 0;
  for (;;) {
    /* Receive an event from a harvester and put it in the queue */
    zmq_msg_t message;

    rc = zmq_msg_init(&message);
    insist(rc == 0, "zmq_msg_init failed");
    rc = zmq_recv(socket, &message, 0);
    insist(rc == 0, "zmq_recv(%s) failed (returned %d): %s",
           config->zmq_endpoint, rc, zmq_strerror(errno));

    redisReply *reply = NULL;
    while (reply == NULL) {
      reply = redisCommand(redis, "RPUSH %s %b", config->redis_key,
                           zmq_msg_data(&message), zmq_msg_size(&message));
      if (reply == NULL) {
        /* Error sending the redis request, reconnect. */
        printf("Error sending command to redis: %s\n", redis->errstr);
        redisFree(redis);
        redis = redis_connect(config->redis_address);
      } else {
        freeReplyObject(reply);
      }
    }

    count++;
    bytes += zmq_msg_size(&message);
    zmq_msg_close(&message);

    if (count == 100000) {
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);
      double s = (start.tv_sec + 0.0) + (start.tv_nsec / 1000000000.0);
      double n = (now.tv_sec + 0.0) + (now.tv_nsec / 1000000000.0);
      fprintf(stderr, "Rate: %f (bytes: %f)\n", (count + 0.0) / (n - s), (bytes + 0.0) / (n - s));
      struct rusage rusage;
      rc = getrusage(RUSAGE_SELF, &rusage);
      insist(rc == 0, "getrusage failed: %s\n", strerror(errno));
      printf("cpu user/system: %d.%06d / %d.%06d\n",
             (int)rusage.ru_utime.tv_sec, (int)rusage.ru_utime.tv_usec,
             (int)rusage.ru_stime.tv_sec, (int)rusage.ru_stime.tv_usec);
      clock_gettime(CLOCK_MONOTONIC, &start);
      bytes = 0;
      count = 0;
    }
  } /* forever */
} /* emitter */
