/*
 * Copyright (C) 2010. See COPYRIGHT in top-level directory.
 */

#ifndef __SHR_RING_H__
#define __SHR_RING_H__

#include <sys/types.h>
#include <mutex.h>
#include <tc.h>

struct shrb_s {
  int             head;      // index of head element (between 0 and rb_size-1)
  int             tail;      // index of tail element (between 0 and rb_size-1)
  int             size;      // Number of elements in the buffer

  synch_mutex_t   lock;      // Lock for accessing the metadata

  int             procid;  // Local-only portion of the queue
  int             nproc;

  ptl_pt_index_t  ptindex;               // portal table index
  ptl_handle_me_t rb_me;     // matchlist entry for ring buffer
  ptl_handle_md_t rb_md;     // memory descriptor for local ring buffer comm ops
  ptl_handle_ct_t rb_ct;     // counter to watch completion of local ring buffer ops
  ptl_handle_eq_t rb_eq;     // event queue for failures

  unsigned long   nrelease;  // Unused
  unsigned long   nreacquire;

  int             max_size;  // Max size in number of elements
  int             elem_size; // Size of an element in bytes
  struct shrb_s **rbs;       // (private) array of base addrs for all rbs
  u_int8_t        q[0];      // (shared)  ring buffer data.  This will be allocated
                             // contiguous with the rb_s so allocating an rb_s will
                             // require "sizeof(struct rb_s) + elem_size*rb_size"
};

typedef struct shrb_s shrb_t;

shrb_t      *shrb_create(int elem_size, int max_size);
void         shrb_destroy(shrb_t *rb);
void         shrb_reset(shrb_t *rb);

void         shrb_lock(shrb_t *rb, int proc);
void         shrb_unlock(shrb_t *rb, int proc);

void         shrb_push_head(shrb_t *rb, int proc, void *e, int size);
void         shrb_push_n_head(shrb_t *rb, int proc, void *e, int n);
void         shrb_push_tail(shrb_t *rb, int proc, void *e, int size);

int          shrb_pop_head(shrb_t *rb, int proc, void *buf);
int          shrb_pop_tail(shrb_t *rb, int proc, void *buf);
int          shrb_pop_n_tail(shrb_t *rb, int proc, int n, void *buf, int steal_vol);

int          shrb_size(shrb_t *rb);
int          shrb_full(shrb_t *rb);
int          shrb_empty(shrb_t *rb);

void         shrb_print(shrb_t *rb);

#define shrb_elem_addr(MYRB, PROC, IDX) ((MYRB)->rbs[PROC]->q + (IDX)*(MYRB)->elem_size)
#define shrb_buff_elem_addr(RB, E, IDX) ((u_int8_t*)(E) + (IDX)*(RB)->elem_size)

#define shrb_malloc malloc
#define shrb_free   free

#endif /* __SHR_RING_H__ */
