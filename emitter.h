#ifndef _EMITTER_H_
#define _EMITTER_H_

struct emitter_config {
  void *zmq; /* zmq context */
  char *zmq_endpoint; /* inproc://whatever */

  /* The address to talk to redis on. Can be a host:port or /path/to/socket.
   * If the address has a "/" in it, it is assumed to be a unix socket path.
   * Otherwise, it is assumed to be an internet address. */
  //int redis_db;
  char *redis_key;
  char *redis_address; 

  char *ssl_ca_path;
  char *ssl_certificate;
  char *ssl_key;
  char *stunnel_path;
};

void *emitter(void *arg);
#endif /* _EMITTER_H_ */
