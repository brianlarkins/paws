/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <mutex.h>

#include "sn_ring.h"
#include "tc.h"

/**
 * sn_create - create a new Portals Steal-N task queue
 *
 * @param elem_size    size of each task structure
 * @param max_size     max queue size
 * @param chunk_size   the value of N
 * @return a new task queue
 */
sn_ring_t *sn_create(int elem_size, int max_size, int chunk_size) {
  sn_ring_t  *rb;
  ptl_md_t md;
  char *rec;
  int err;

  // allocate memory for our ring buffer
  err = posix_memalign((void **)&rb, sizeof(int64_t), sizeof(sn_ring_t) + elem_size*max_size);
  if (err != 0) {
    gtc_lprintf(DBGSHRB, "sn_create: unable to get aligned memory for atomic ops - %s\n", strerror(errno));
    exit(1);
  }
  memset(rb, 0, sizeof(sn_ring_t) + elem_size*max_size);

  rb->procid  = _c->rank;
  rb->nproc   = _c->size;
  rb->elem_size = elem_size;
  rb->max_size  = max_size;
  rb->chunk_size = chunk_size;
  rb->max_chunks = (max_size / chunk_size) + 1;
  rb->max_steal  = (max_size / 2) + 1;
  rb->reclaimfreq = __GTC_RECLAIM_POLLFREQ;
  rec = getenv("GTC_RECLAIM_FREQ");
  if (rec)
    rb->reclaimfreq = atoi(rec);

  mp_init(rb);

  TC_INIT_ATIMER(rb->acqtime);
  TC_INIT_ATIMER(rb->reltime);


  /* setup Portals structures for queue management */

  // counter for ops our shared queue (ME)
  err = PtlCTAlloc(_c->lni, &rb->rb_shct);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_create: CTAlloc error\n");
    exit(1);
  }
  rb->rb_lastcount = 0; // we've never reclaimed shared queue space

  // counter for local gets (MD)
  err = PtlCTAlloc(_c->lni, &rb->rb_ct);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_create: CTAlloc error\n");
    exit(1);
  }

  err = PtlEQAlloc(_c->lni, 1024, &rb->rb_eq);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_create: EQAlloc error\n");
    exit(1);
  }

  md.start     = NULL;
  md.length    = PTL_SIZE_MAX;
  //md.options   = PTL_MD_EVENT_CT_ACK | PTL_MD_EVENT_CT_REPLY | PTL_MD_EVENT_SEND_DISABLE;
  md.options   = PTL_MD_EVENT_CT_ACK | PTL_MD_EVENT_SEND_DISABLE | PTL_MD_EVENT_CT_REPLY | PTL_MD_EVENT_SUCCESS_DISABLE;
  md.eq_handle = rb->rb_eq;
  md.ct_handle = rb->rb_ct;

  err = PtlMDBind(_c->lni, &md, &rb->rb_md);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_create: PtlMDBind error\n");
    exit(1);
  }

  err = PtlEQAlloc(_c->lni, 1024, &rb->rb_sheq);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_create: EQAlloc error\n");
    exit(1);
  }

  // don't use unordered matching, want to always get first entry
  err = PtlPTAlloc(_c->lni, 0, rb->rb_sheq, PTL_PT_ANY, &rb->ptindex);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_create: PTAlloc error: %s\n", gtc_ptl_error(err));
    exit(1);
  }

  // always post an EOQ marker entry at the end of the matchlist
  rb->eoq_sentinel            = 0xdeadb33f; // because deadME3t isn't hex-friendly
  rb->rb_eoq_me.start         = &rb->eoq_sentinel;
  rb->rb_eoq_me.length        = sizeof(rb->eoq_sentinel);
  rb->rb_eoq_me.ct_handle     = PTL_CT_NONE;
  rb->rb_eoq_me.uid           = PTL_UID_ANY;
  rb->rb_eoq_me.options       = PTL_ME_OP_PUT | PTL_ME_OP_GET | PTL_ME_EVENT_LINK_DISABLE |
                                PTL_ME_EVENT_OVER_DISABLE | PTL_ME_EVENT_COMM_DISABLE | PTL_ME_UNEXPECTED_HDR_DISABLE;
  rb->rb_eoq_me.match_id.rank = PTL_RANK_ANY;
  rb->rb_eoq_me.match_bits    = __GTC_SN_QUEUE_MATCH;
  rb->rb_eoq_me.ignore_bits   = 0;

  err = PtlMEAppend(_c->lni, rb->ptindex, &rb->rb_eoq_me, PTL_OVERFLOW_LIST, NULL, &(rb->rb_eoq_h));
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_create: PtlMEAppend error (%s)\n", gtc_ptl_error(err));
    exit(1);
  }
  sn_reset(rb);

  gtc_barrier();

  return rb;
}


/**
 * sn_reset - reset a steal-N task queue
 *
 * @param rb the task queue to reset
 */
void sn_reset(sn_ring_t *rb) {
  int ret;
  sn_matchpair_t mp;

  // disable PTE to block remote steals temporarily
  ret = PtlPTDisable(_c->lni, rb->ptindex);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB, "sn_reset: PtlPTDisable error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // clean up matchpair list of stolen tasks
  sn_reclaim_space(rb);

  while (mp_head(rb)) {
    mp = mp_pop_head(rb);
    if (mp.me != PTL_INVALID_HANDLE)  {
      ret = PtlMEUnlink(mp.me);
      assert(ret == PTL_OK);
    }
    if (mp.iov) free(mp.iov);
  }
  assert(mp_isempty(rb));

  // Reset state to empty
  rb->nlocal = 0;
  rb->tail   = 0;
  rb->split  = 0;

  // enable PTE
  ret = PtlPTEnable(_c->lni, rb->ptindex);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB, "sn_reset: PtlPTEnable error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // Reset queue statistics
  rb->nrelease   = 0;
  rb->nprogress  = 0;
  rb->nreacquire = 0;
  rb->nwaited    = 0;
  rb->nreclaimed = 0;
}



/**
 * sn_destroy - destroy a steal-N task queue
 *
 * @param rb the task queue to teardown
 */
void sn_destroy(sn_ring_t *rb) {
  int ret;

  if (rb) {
    sn_matchpair_t mp;
    gtc_barrier();

    // clean up matchpair list of stolen tasks
    sn_reclaim_space(rb);

    while (mp_head(rb)) {
      mp = mp_pop_head(rb);
      if (mp.me != PTL_INVALID_HANDLE)  {
        ret = PtlMEUnlink(mp.me);
        assert(ret == PTL_OK);
      }
      if (mp.iov) free(mp.iov);
    }
    assert(mp_isempty(rb));

    if (rb->rb_eoq_h != PTL_INVALID_HANDLE) PtlMEUnlink(rb->rb_eoq_h);
    if (rb->rb_ct    != PTL_INVALID_HANDLE) PtlCTFree(rb->rb_ct);
    if (rb->rb_eq    != PTL_INVALID_HANDLE) PtlEQFree(rb->rb_eq);
    if (rb->rb_md    != PTL_INVALID_HANDLE) PtlMDRelease(rb->rb_md);
    PtlPTFree(_c->lni, rb->ptindex);

    free(rb);
  }
}



/**
 * sn_print - print a Steal-N task queue
 *
 * @param rb the task queue to print
 */
void sn_print(sn_ring_t *rb) {
  printf("rb: %p {\n", rb);
  printf("   procid  = %d\n", rb->procid);
  printf("   nproc  = %d\n", rb->nproc);
  printf("   nlocal    = %"PRId64"\n", rb->nlocal);
  printf("   head      = %d\n", sn_head(rb));
  printf("   split     = %"PRId64"\n", rb->split);
  printf("   tail      = %"PRId64"\n", rb->tail);
  printf("   max_size  = %d\n", rb->max_size);
  printf("   elem_size = %d\n", rb->elem_size);
  printf("   chunk_size = %d\n", rb->chunk_size);
  printf("   local_size = %d\n", sn_local_size(rb));
  printf("   shared_size= %d\n", sn_shared_size(rb));
  printf("   size       = %d\n", sn_size(rb));
  mp_print_matchpairs(rb, NULL);
  printf("}\n");
}



/*==================== STATE QUERIES ====================*/


int sn_head(sn_ring_t *rb) {
  return (rb->split + rb->nlocal - 1) % rb->max_size;
}


int sn_local_isempty(sn_ring_t *rb) {
  return rb->nlocal == 0;
}


int sn_shared_isempty(sn_ring_t *rb) {
  return rb->tail == rb->split;
}


int sn_isempty(sn_ring_t *rb) {
  return sn_local_isempty(rb) && sn_shared_isempty(rb);
}


int sn_local_size(sn_ring_t *rb) {
  return rb->nlocal;
}


int sn_shared_size(sn_ring_t *rb) {
  if (sn_shared_isempty(rb)) // Shared is empty
    return 0;
  else if (rb->tail < rb->split)   // No wrap-around
    return rb->split - rb->tail;
  else                             // Wrap-around
    return rb->split + rb->max_size - rb->tail;
}

int sn_size(void *b) {
  sn_ring_t *rb = (sn_ring_t *)b;
  return sn_local_size(rb) + sn_shared_size(rb);
}


/*==================== SPLIT MOVEMENT ====================*/

/**
 * sn_process_event - update queue state relative to a
 *   steal or unlink event on the Portals event queue
 *
 *    @param rb the task queue to synchronize
 *    @param ev the current event to process
 */
int sn_process_event(sn_ring_t *rb, ptl_event_t *ev) {
  int nstolen = 0;
  int reclaimed = 0;
  sn_matchpair_t *mpp;

  // sanity check the matchpair queue ordering
  mpp = (sn_matchpair_t *)(ev->user_ptr);

  // handle get and unlink events separately
  switch (ev->type) {

    // get updates matchpairs task count
    case PTL_EVENT_GET:
      nstolen = (ev->mlength / rb->elem_size);

      assert((ev->mlength % rb->elem_size) == 0);      // no partial tasks

      mp_update_tasks(rb, mpp, nstolen);
      reclaimed = nstolen;
      rb->tail = (rb->tail + nstolen) % rb->max_size;
      gtc_lprintf(DBGSHRB,"reclaimed %5d tasks : local: %d shared: %d split: %d\n", nstolen, sn_local_size(rb), sn_shared_size(rb), rb->split);
      break;

      // remove entries from the matchpair list
    case PTL_EVENT_AUTO_UNLINK:
      mp_unlink(rb, mpp);
      break;

    default:
      gtc_lprintf(DBGSHRB,"sn_process_event: Found unexpected event\n");
      gtc_dump_event(ev);
  }

  return reclaimed;
}



/**
 * sn_reclaim_space - there are no deferred copies with this implementation, however
 *    the bookkeeping between consumed matchlists entries and the queue tail can
 *    become out of date. this routine synchs the Portals data with the queue metadata
 *
 *    @param rb the task queue to synchronize
 */
int sn_reclaim_space(sn_ring_t *rb) {
  ptl_ct_event_t ctevent;
  ptl_event_t ev;
  int ret, reclaimed = 0;
  TC_START_TSCTIMER(reclaim);
  rb->nreccalls++;

  PtlCTGet(rb->rb_shct, &ctevent);

  if ((ctevent.success <= rb->rb_lastcount) || mp_isempty(rb)) {
    TC_STOP_TSCTIMER(reclaim);
    return 0;
  }

  rb->rb_lastcount = ctevent.success; // update reclaim counter

  // walk through the event queue
  while ((ret = PtlEQGet(rb->rb_sheq, &ev)) != PTL_EQ_EMPTY) {
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sn_reclaim_space: PtlEQGet error (%s)\n", gtc_ptl_error(ret));
      break;
    }
    // process steal or unlink event - modifies MP list
    reclaimed += sn_process_event(rb, &ev);
    rb->nreclaimed++;
  }
  //sn_print(rb);

  TC_STOP_TSCTIMER(reclaim);
  return reclaimed;
}



/**
 * sn_ensure_space - synch metadata and check to see if there is room for another n tasks in the queue
 *
 * @param rb the ring buffer to check
 * @param n  number of free spaces needed
 */
void sn_ensure_space(sn_ring_t *rb, int n) {

  rb->nensure++;
  // could busy wait or try again instead of error if this doesn't give us enough
  TC_START_TSCTIMER(ensure);

  sn_reclaim_space(rb);

  if ((rb->max_size - sn_size(rb)) < n) {
    // Error: amount of reclaimable space is less than what we need.
    // Try increasing the size of the queue.
    gtc_lprintf(DBGSHRB, "sn_ensure_space: not enough space in the queue to push %d elements\n", n);
    sn_print(rb);
    assert(0);
  }
  TC_STOP_TSCTIMER(ensure);
}

/*==================== PUSH OPERATIONS ====================*/


/**
 * sn_push_n_head_impl - push N items onto queue
 *
 * @param rb   the ring buffer to push to
 * @param proc rank of the target task queue
 * @param e    buffer containing tasks to push
 * @param n    number of tasks
 * @param size size of each task to push
 */
static inline void sn_push_n_head_impl(sn_ring_t *rb, int proc, void *e, int n, int size) {
  int head, old_head;

  assert(size <= rb->elem_size);
  assert(size == rb->elem_size || n == 1);  // n > 1 ==> size == rb->elem_size
  assert(proc == rb->procid);
  TC_START_TSCTIMER(pushhead);

  // Make sure there is enough space for n elements
  sn_ensure_space(rb, n);

  // Proceed with the push
  old_head    = sn_head(rb);
  rb->nlocal += n;
  head        = sn_head(rb);

  if (head > old_head || old_head == rb->max_size - 1) {
    memcpy(sn_elem_addr(rb, (old_head+1)%rb->max_size), e, n*size);
  }

  // This push wraps around, break it into two parts
  else {
    int part_size = rb->max_size - 1 - old_head;

    memcpy(sn_elem_addr(rb, old_head+1), e, part_size*size);
    memcpy(sn_elem_addr(rb, 0), sn_buff_elem_addr(rb, e, part_size), (n - part_size)*size);
  }
  TC_STOP_TSCTIMER(pushhead);
}

void sn_push_head(sn_ring_t *rb, int proc, void *e, int size) {
  int old_head;
  static int cc = 0; // ensure is more expensive now

  TC_START_TSCTIMER(pushhead);
  assert(size <= rb->elem_size);
  assert(proc == rb->procid);

  // Make sure there is enough space for n elements
  if ((cc++ % rb->reclaimfreq) == 0)
    sn_ensure_space(rb, rb->reclaimfreq);

  // Proceed with the push
  old_head    = sn_head(rb);
  rb->nlocal += 1;

  memcpy(sn_elem_addr(rb, (old_head+1)%rb->max_size), e, size);
  TC_STOP_TSCTIMER(pushhead);
}



/**
 * sn_push_n_head - push N items to head of tasks queue
 *
 * @param rb   the ring buffer to push to
 * @param proc rank of the target task queue
 * @param e    buffer containing tasks to push
 * @param n    number of tasks
 * @param size size of each task to push
 */
void sn_push_n_head(void *b, int proc, void *e, int n) {
  sn_ring_t *rb = (sn_ring_t *)b;
  sn_push_n_head_impl(rb, proc, e, n, rb->elem_size);
}


/**
 * sn_alloc_head - allocate a task entry at the head of the task queue
 *
 * @param rb   the ring buffer
 * @return pointer to the new element
 */
void *sn_alloc_head(sn_ring_t *rb) {
  // Make sure there is enough space for 1 element
  sn_ensure_space(rb, 1);

  rb->nlocal += 1;

  return sn_elem_addr(rb, sn_head(rb));
}



/*==================== POP OPERATIONS ====================*/


/**
 * sn_pop_head - fetch the task at the head of the queue
 *
 * @param rb   the ring buffer
 * @param proc rank of task queue
 * @param buf  buffer to place the task into
 * @return 0 on failure (empty), 1 on success
 */
int sn_pop_head(void *b, int proc, void *buf) {
  sn_ring_t *rb = (sn_ring_t *)b;
  int   old_head;
  int   buf_valid = 0;

  assert(proc == rb->procid);

  // If we are out of local work, try to reacquire
  if (sn_local_isempty(rb)) {
    sn_reacquire(rb);
  }

  if (sn_local_size(rb) > 0) {
    old_head = sn_head(rb);

    memcpy(buf, sn_elem_addr(rb, old_head), rb->elem_size);

    rb->nlocal--;
    buf_valid = 1;
  }

  // Assertion: !buf_valid => sn_isempty(rb)
  assert(buf_valid || (!buf_valid && sn_isempty(rb)));

  return buf_valid;
}
