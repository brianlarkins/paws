/*
 * Copyright (C) 2010. See COPYRIGHT in top-level directory.
 */

#ifndef __SDC_SHR_RING_H__
#define __SDC_SHR_RING_H__

#include <sys/types.h>
#include <mutex.h>
#include <tc.h>

struct sdc_shrb_s {
  int64_t         itail;     // Index of the intermediate tail (between vtail and tail)
  int64_t         tail;      // Index of tail element (between 0 and rb_size-1)

  int64_t         nlocal;    // Number of elements in the local portion of the queue
  int64_t         vtail;     // Index of the virtual tail
  int64_t         split;     // index of split between local-only and local-shared elements

  synch_mutex_t   lock;      // lock for shared portion of this queue
  unsigned        rbcheck;   // magic check value to test for memory corruption
  int             waiting;   // Am I currently waiting for transactions to complete?

  int             procid;
  int             nproc;
  int             max_size;  // Max size in number of elements
  int             elem_size; // Size of an element in bytes

  ptl_pt_index_t  ptindex;   // portal table index
  ptl_handle_me_t rb_me;     // matchlist entry for ring buffer
  ptl_handle_md_t rb_md;     // memory descriptor for local ring buffer comm ops
  ptl_handle_ct_t rb_ct;     // counter to watch completion of local ring buffer ops
  ptl_handle_eq_t rb_eq;     // event queue for failures

  tc_counter_t    nwaited;   // How many times did I have to wait
  tc_counter_t    nreclaimed;// How many times did I reclaim space from the public portion of the queue
  tc_counter_t    nreccalls; // How many times did I even try to reclaim
  tc_counter_t    nrelease;  // Number of times work was released from local->public
  tc_counter_t    nprogress; // Number of otimes we called the progress routine
  tc_counter_t    nreacquire;// Number of times work was reacquired from public->local
  tc_counter_t    ngets;     // Number of times we attempted a steal
  tc_counter_t    nensure;   // Number of times we call reclaim space
  tc_counter_t    nxfer;     // xferred bytes
  tc_counter_t    nsteals;   // number of successful steals
  tc_counter_t    nmeta;     // number of successful steals

  u_int8_t        q[0];      // (shared)  ring buffer data.  This will be allocated
                             // contiguous with the rb_s so allocating an rb_s will
                             // require "sizeof(struct rb_s) + elem_size*rb_size"
};

typedef struct sdc_shrb_s sdc_shrb_t;

sdc_shrb_t *sdc_shrb_create(int elem_size, int max_size);
void        sdc_shrb_destroy(sdc_shrb_t *rb);
void        sdc_shrb_reset(sdc_shrb_t *rb);

void        sdc_shrb_lock(sdc_shrb_t *rb, int proc);
void        sdc_shrb_unlock(sdc_shrb_t *rb, int proc);

int         sdc_shrb_head(sdc_shrb_t *rb);
int         sdc_shrb_local_isempty(sdc_shrb_t *rb);
int         sdc_shrb_shared_isempty(sdc_shrb_t *rb);
int         sdc_shrb_local_size(sdc_shrb_t *rb);
int         sdc_shrb_shared_size(sdc_shrb_t *rb);
int         sdc_shrb_reserved_size(sdc_shrb_t *rb);
int         sdc_shrb_public_size(sdc_shrb_t *rb);

void        sdc_shrb_fetch_remote_trb(sdc_shrb_t *myrb, sdc_shrb_t *trb, int proc);

void        sdc_shrb_release(sdc_shrb_t *rb);
void        sdc_shrb_release_all(sdc_shrb_t *rb);
int         sdc_shrb_reacquire(sdc_shrb_t *rb);
int         sdc_shrb_reclaim_space(sdc_shrb_t *rb);

void        sdc_shrb_push_head(sdc_shrb_t *rb, int proc, void *e, int size);
void        sdc_shrb_push_n_head(void *b, int proc, void *e, int size);
void       *sdc_shrb_alloc_head(sdc_shrb_t *rb);

int         sdc_shrb_pop_head(void *rb, int proc, void *buf);
int         sdc_shrb_pop_tail(sdc_shrb_t *rb, int proc, void *buf);
int         sdc_shrb_pop_n_tail(void *b, int proc, int n, void *buf, int steal_vol);
int         sdc_shrb_try_pop_n_tail(void *b, int proc, int n, void *buf, int steal_vol);

int         sdc_shrb_size(void *b);
int         sdc_shrb_full(sdc_shrb_t *rb);
int         sdc_shrb_empty(sdc_shrb_t *rb);

void        sdc_shrb_print(sdc_shrb_t *rb);

uint64_t    sdc_get_nrelease(void *rb);
uint64_t    sdc_get_nreacquire(void *rb);
uint64_t    sdc_get_nprogress(void *rb);

#define sdc_shrb_elem_addr(MYRB, IDX) ((MYRB)->q + (IDX)*(MYRB)->elem_size)
//#define sdc_shrb_elem_addr(MYRB, PROC, IDX) ((MYRB)->rbs[PROC]->q + (IDX)*(MYRB)->elem_size)
#define sdc_shrb_buff_elem_addr(RB, E, IDX) ((u_int8_t*)(E) + (IDX)*(RB)->elem_size)

// ARMCI allocated buffers should be faster/pinned
//#define sdc_shrb_malloc ARMCI_Malloc_local
//#define sdc_shrb_free   ARMCI_Free_local

#endif /* __SDC_SHR_RING_H__ */
