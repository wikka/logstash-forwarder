#ifndef _PROTO_H_
#define _PROTO_H_
#include <sys/types.h>
#include "str.h"

struct kv {
  char *key;
  size_t key_len;
  char *value;
  size_t value_len;
}; /* struct kv */

/* Pack a key-value list according to the lumberjack protocol */
struct str *lumberjack_kv_pack(struct kv *kv_list, size_t kv_count);

#endif /* _PROTO_H_ */
