/*
 * Copyright (C) 2010. See COPYRIGHT in top-level directory.
 */

#pragma once
#ifndef __SN_RING_H__
#define __SN_SHR_RING_H__

#include <sys/types.h>
#include <mutex.h>
#include <tc.h>

enum sn_releasetype_e {
  SNReleaseHalf,
  SNReleaseAll,
  SNReleaseAllChunk,
  SNReacquire
};
typedef enum sn_releasetype_e sn_releasetype_t;


/************************************/
/*        matchpair handling        */
/************************************/
struct sn_matchpair_s {
  ptl_handle_me_t        me;       // portals matchlist entry for this block of N tasks
  int64_t                index;    // index into ring buffer corresponding to this ME
  int                    tasks;    // # of tasks in ME
  int                    unlinked; // is ME still linked?
  ptl_iovec_t           *iov;      // pointer to dynamically allocated iovec for ME
};
typedef struct sn_matchpair_s sn_matchpair_t;


struct sn_mplist_s {
  sn_matchpair_t      mp;
  struct sn_mplist_s *prev;
  struct sn_mplist_s *next;
};
typedef struct sn_mplist_s sn_mplist_t;



/************************************/
/*        steal-N queue             */
/************************************/
struct sn_ring_s {
  int64_t         tail;      // Index of tail element (between 0 and rb_size-1)
  int64_t         nlocal;    // Number of elements in the local portion of the queue
  int64_t         split;     // index of split between local-only and local-shared elements

  synch_mutex_t   lock;      // lock for shared portion of this queue
  unsigned        rbcheck;   // magic check value to test for memory corruption

  int             procid;
  int             nproc;
  int             max_size;  // Max size in number of elements
  int             elem_size; // Size of an element in bytes
  int             chunk_size;// number of tasks in each block of N tasks
  int             max_chunks;// max number of stealable chunks
  int             max_steal; // max number of tasks that can be stolen (steal-half)
  int             rthresh;   // reclaim threshold - when to start walking EQ
  uint64_t        eoq_sentinel; // magic cookie for validating EOQ message
  int             reclaimfreq; // reclaim dampening frequency
  sn_mplist_t    *matchpairs;// head of list of ME/index pairs
  sn_mplist_t    *matchtail; // head of list of ME/index pairs
  sn_mplist_t    *freepairs; // freelist of matchpair entries
  sn_mplist_t    *mpcursor;  // cursor for iterating of mp list
  int             mpiterdir; // direction of iteration

  ptl_pt_index_t  ptindex;   // portal table index
  ptl_handle_md_t rb_md;     // memory descriptor for local ring buffer comm ops
  ptl_handle_ct_t rb_ct;     // counter to watch completion of local ring buffer ops
  ptl_handle_eq_t rb_eq;     // event queue for failures
  ptl_handle_eq_t rb_sheq;   // event queue for shared queue link/unlink ops
  ptl_handle_ct_t rb_shct;   // counter for shared queue GET events
  ptl_size_t      rb_lastcount; // counter value last time we reclaimed 
  ptl_handle_me_t rb_eoq_h;  // end-of-queue ME handle
  ptl_me_t        rb_eoq_me; // end-of-queue ME

  tc_counter_t    nwaited;   // How many times did I have to wait
  tc_counter_t    nreclaimed;// How many times did I reclaim space from the public portion of the queue
  tc_counter_t    nreccalls; // How many times did I even try to reclaim
  tc_counter_t    nrelease;  // Number of times work was released from local->public
  tc_counter_t    nprogress; // Number of times we called the progress routine
  tc_counter_t    nreacquire;// Number of times work was reacquired from public->local
  tc_counter_t    nensure;   // Number of times we call reclaim space
  tc_counter_t    neoq;      // Number of EOQ matches
  tc_counter_t    ngets;     // Number of PtlGet calls
  tc_counter_t    nsteals;   // Number of successful steals
  tc_counter_t    ndisabled; // Number of disabled PTE attempts
  tc_counter_t    ndropped;  // Number of dropped gets
  tc_counter_t    nmpseek;   // MP entries searched for
  tc_counter_t    nmptrav;   // MP entries traversed (traversed/searched == avg. search depth)

  tc_timer_t      acqtime;   // timer for acquire operations
  tc_timer_t      reltime;   // timer for release operations

  u_int8_t        q[0];      // (shared)  ring buffer data.  This will be allocated
                             // contiguous with the rb_s so allocating an rb_s will
                             // require "sizeof(struct rb_s) + elem_size*rb_size"
};
typedef struct sn_ring_s sn_ring_t;

// sn_common.c
sn_ring_t  *sn_create(int elem_size, int max_size, int chunk_size);
void        sn_destroy(sn_ring_t *rb);
void        sn_reset(sn_ring_t *rb);
void        sn_print(sn_ring_t *rb);
int         sn_head(sn_ring_t *rb);
int         sn_local_isempty(sn_ring_t *rb);
int         sn_shared_isempty(sn_ring_t *rb);
int         sn_isempty(sn_ring_t *rb);
int         sn_local_size(sn_ring_t *rb);
int         sn_shared_size(sn_ring_t *rb);
int         sn_size(void *b);
int         sn_reclaim_space(sn_ring_t *rb);
int         sn_process_event(sn_ring_t *rb, ptl_event_t *ev);
void        sn_ensure_space(sn_ring_t *rb, int n);
int         sn_pop_head(void *rb, int proc, void *buf);
void        sn_push_head(sn_ring_t *rb, int proc, void *e, int size);
void        sn_push_n_head(void *b, int proc, void *e, int size);
void       *sn_alloc_head(sn_ring_t *rb);

// sn_ring.c
void        sn_release(sn_ring_t *rb);
void        sn_release_all(sn_ring_t *rb);
int         sn_reacquire(sn_ring_t *rb);
int         sn_pop_tail(sn_ring_t *rb, int proc, void *buf);
int         sn_pop_n_tail(void *b, int proc, int n, void *buf, int steal_vol);
int         sn_ring_try_pop_n_tail(void *b, int proc, int n, void *buf, int steal_vol);

// sh_ring.c
void        sh_release(sn_ring_t *rb);
void        sh_release_all(sn_ring_t *rb);
void        sh_release_all_chunks(sn_ring_t *rb, int chunksize);
int         sh_reacquire(sn_ring_t *rb);
int         sh_pop_head(void *rb, int proc, void *buf);
int         sh_pop_tail(sn_ring_t *rb, int proc, void *buf);
int         sh_pop_n_tail(void *b, int proc, int n, void *buf, int steal_vol);

void            mp_init(sn_ring_t *rb);
void            mp_print_matchpairs(sn_ring_t *rb, char *msg);
int             mp_isempty(sn_ring_t *rb);
sn_matchpair_t *mp_head(sn_ring_t *rb);
sn_matchpair_t *mp_tail(sn_ring_t *rb);
void            mp_push_head(sn_ring_t *rb, sn_matchpair_t *n);
sn_matchpair_t  mp_pop_head(sn_ring_t *rb);
void            mp_push_tail(sn_ring_t *rb, sn_matchpair_t *n);
sn_matchpair_t  mp_pop_tail(sn_ring_t *rb);
int             mp_update_tasks(sn_ring_t *rb, sn_matchpair_t *mpp, int dec);
int             mp_unlink(sn_ring_t *rb, sn_matchpair_t *mpp);
sn_matchpair_t  mp_remove_entry(sn_ring_t *rb, sn_matchpair_t *mpp);

void            mp_start_head(sn_ring_t *rb);
void            mp_start_tail(sn_ring_t *rb);
int             mp_has_next(sn_ring_t *rb);
int             mp_has_prev(sn_ring_t *rb);
sn_matchpair_t  mp_next(sn_ring_t *rb);
sn_matchpair_t *mp_cur_evptr(sn_ring_t *rb);




#define sn_elem_addr(MYRB, IDX) ((MYRB)->q + (IDX)*(MYRB)->elem_size)
//#define sn_elem_addr(MYRB, PROC, IDX) ((MYRB)->rbs[PROC]->q + (IDX)*(MYRB)->elem_size)
#define sn_buff_elem_addr(RB, E, IDX) ((u_int8_t*)(E) + (IDX)*(RB)->elem_size)

#endif /* __SN_RING_H__ */
