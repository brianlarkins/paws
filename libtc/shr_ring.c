/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <mutex.h>
#include <tc.h>
#include "shr_ring.h"

static void shrb_fetch_remote_trb(shrb_t *myrb, shrb_t *trb, int proc);
static void shrb_update_remote_trb(shrb_t *myrb, shrb_t *trb, int proc, int xtra_counts);
static void shrb_push_n_head_impl(shrb_t *myrb, int proc, void *e, int n, int elem_size);

/**
 * Ring Buffer head/tail semantics:
 *
 * Head - Always points to the element at the head of the buffer unless the buffer is empty.
 *      - The head moves to the RIGHT.
 * Tail - Always points to the element at the tail of the buffer unless the buffer is empty.
 *      - The tail moves to the LEFT.
 *
 * Invariant: empty(rb) => head = tail
 *
 * The ring buffer can in in several states:
 *
 * 1. Empty.  In this case we have head == tail and size == 0.
 *
 * 2. No Wrap-around:
 *  _______________________________________________________________
 * |           |/////////|                                         |
 * |___________|/////////|_________________________________________|
 *             ^         ^
 *           tail       head
 *
 * 2. Wrapped-around:
 *  _______________________________________________________________
 * |/////////|                                           |/////////|
 * |/////////|___________________________________________|/////////|
 *           ^                                           ^
 *         head                                        tail
 */


shrb_t *shrb_create(int elem_size, int max_size) {
  shrb_t  *rb;
  ptl_me_t me;
  ptl_md_t md;
  int      err;

  //gtc_lprintf(DBGSHRB, "  shrb_create()\n");

  // Allocate the struct and the buffer contiguously in shared space
  rb = malloc(sizeof(shrb_t) + elem_size*max_size);
  memset(rb, 0, sizeof(shrb_t) + elem_size*max_size);

  rb->procid  = _c->rank;
  rb->nproc   = _c->size;
  rb->elem_size = elem_size;
  rb->max_size  = max_size;
  shrb_reset(rb);

  /* setup Portals structures for queue management */

  err = PtlCTAlloc(_c->lni, &rb->rb_ct);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_create: CTAlloc error\n");
    exit(1);
  }

  err = PtlEQAlloc(_c->lni, 1024, &rb->rb_eq);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_create: EQAlloc error\n");
    exit(1);
  }

  md.start = NULL;
  md.length    = PTL_SIZE_MAX;
  md.options   = PTL_MD_EVENT_SUCCESS_DISABLE | PTL_MD_EVENT_CT_ACK | PTL_MD_EVENT_CT_REPLY | PTL_MD_EVENT_SEND_DISABLE;
  md.eq_handle = rb->rb_eq;
  md.ct_handle = rb->rb_ct;

  err = PtlMDBind(_c->lni, &md, &rb->rb_md);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_create: PtlMDBind error\n");
    exit(1);
  }

  err = PtlPTAlloc(_c->lni, PTL_PT_MATCH_UNORDERED, PTL_EQ_NONE, PTL_PT_ANY, &rb->ptindex);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_create: PTAlloc error\n");
    exit(1);
  }

  me.start      = rb;
  me.length     = sizeof(shrb_t) + (elem_size * max_size);
  me.ct_handle  = PTL_CT_NONE;
  me.uid        = PTL_UID_ANY;
  me.options    = PTL_ME_OP_PUT | PTL_ME_OP_GET | PTL_ME_EVENT_CT_COMM
    | PTL_ME_EVENT_LINK_DISABLE | PTL_ME_EVENT_COMM_DISABLE
    | PTL_ME_EVENT_UNLINK_DISABLE | PTL_ME_EVENT_SUCCESS_DISABLE;
  me.match_id.rank = PTL_RANK_ANY;
  me.match_bits = __GTC_SHRB_QUEUE_MATCH;
  me.ignore_bits = 0;
  err = PtlMEAppend(_c->lni, rb->ptindex, &me, PTL_PRIORITY_LIST, NULL, &rb->rb_me);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sdc_shrb_create: PtlMEAppend error (%s)\n", gtc_ptl_error(err));
    exit(1);
  }

  // Initialize the lock
  synch_mutex_init(&rb->lock);

  gtc_barrier();

  return rb;
}


void shrb_destroy(shrb_t *rb) {
  if (rb) {
    if (rb->rb_ct != PTL_INVALID_HANDLE) PtlCTFree(rb->rb_ct);
    if (rb->rb_eq != PTL_INVALID_HANDLE) PtlEQFree(rb->rb_eq);
    if (rb->rb_md != PTL_INVALID_HANDLE) PtlMDRelease(rb->rb_md);
    if (rb->rb_me != PTL_INVALID_HANDLE) PtlMEUnlink(rb->rb_me);
    PtlPTFree(_c->lni, rb->ptindex);

    free(rb->rbs);
  }
}


void shrb_print(shrb_t *rb) {
  printf("rb: %p {\n", rb);
  printf("   procid  = %d\n", rb->procid);
  printf("   nproc  = %d\n", rb->nproc);
  printf("   head      = %d\n", rb->head);
  printf("   tail      = %d\n", rb->tail);
  printf("   size      = %d\n", rb->size);
  printf("   max_size  = %d\n", rb->max_size);
  printf("   elem_size = %d\n", rb->elem_size);
  printf("   lock      = %ld\n", *((uint64_t *) (rb->lock.lockval)));
  printf("   rbs       = %p\n", rb->rbs);
  printf("   q         = %p\n", rb->q);
  printf("}\n");
}



void shrb_lock(shrb_t *rb, int proc) {
  synch_mutex_lock(&rb->lock, proc);
}


void shrb_unlock(shrb_t *rb, int proc) {
  synch_mutex_unlock(&rb->lock, proc);
}



int shrb_trylock(shrb_t *rb, int proc) {
  return synch_mutex_trylock(&rb->lock, proc);
}



static void shrb_fetch_remote_trb(shrb_t *myrb, shrb_t *trb, int proc) {
  ptl_process_t p;
  ptl_ct_event_t current, ctevent;
  ptl_event_t fault;
  int ret;

  p.rank = proc;

  PtlCTGet(myrb->rb_ct, &current);

  ret = PtlGet(myrb->rb_md, (ptl_size_t)trb, sizeof(shrb_t), p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, 0, NULL);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: trb PtlGet error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: trb PtlCTWait error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // check for error
  if (ctevent.failure > current.failure) {
    ret = PtlEQWait(myrb->rb_eq, &fault);
    if (ret == PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: trb found fail event: %s\n", gtc_event_to_string(fault.type));
      gtc_dump_event(&fault);
    } else {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: trb PtlEQWait error (%s)\n", gtc_ptl_error(ret));
    }
  }
}



static void shrb_update_remote_trb(shrb_t *myrb, shrb_t *trb, int proc, int xtra_counts) {
  ptl_process_t p;
  ptl_ct_event_t current, ctevent;
  ptl_event_t fault;
  int ret;

  p.rank = proc;

  if (xtra_counts == 0) {
    PtlCTGet(myrb->rb_ct, &current); // get current counter values
    xtra_counts = current.success + 1;
  } else {
    xtra_counts += 1;
  }

  ret = PtlPut(myrb->rb_md, (ptl_size_t)trb, 3*sizeof(int), PTL_CT_ACK_REQ, p, myrb->ptindex,
      __GTC_SHRB_QUEUE_MATCH, 0, NULL, 0);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // allow xtra_counts to account for other in-flight communications on our counter
  ret = PtlCTWait(myrb->rb_ct, xtra_counts, &ctevent);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // check for error
  if (ctevent.failure > current.failure) {
    ret = PtlEQWait(myrb->rb_eq, &fault);
    if (ret == PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
      gtc_dump_event(&fault);
    } else {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
    }
  }
}


static void shrb_push_n_head_impl(shrb_t *myrb, int proc, void *e, int n, int elem_size) {
  int nexthead, nextfree;
  shrb_t *trb = malloc(sizeof(shrb_t));  // Target RB
  ptl_process_t p;
  ptl_ct_event_t current, ctevent;
  ptl_event_t fault;
  int rem_addr;
  int ret;

  assert(trb != NULL);
  assert(proc < myrb->nproc);

  p.rank = proc;

  shrb_lock(myrb, proc);

  // copy the remote RB descriptor
  shrb_fetch_remote_trb(myrb, trb, proc);

  // Check for an overflow
  assert(trb->size+n <= trb->max_size);

  // Check if the rb is empty.  If it is, we don't advance head the first time
  if (trb->size == 0) {
    nexthead = (trb->head + n - 1) % trb->max_size;
    nextfree = trb->head;
  } else {
    nexthead = (trb->head + n) % trb->max_size;
    nextfree = (trb->head + 1) % trb->max_size;
  }

  PtlCTGet(myrb->rb_ct, &current); // get current counter values

  // If this push will cause us to wrap around, break it in two parts.
  if (nextfree >= trb->tail && trb->max_size - nextfree < n) {
    int part_size = trb->max_size - nextfree;

    rem_addr = offsetof(shrb_t, q) + (nextfree * trb->elem_size);
    ret = PtlPut(myrb->rb_md, (ptl_size_t)e, part_size * trb->elem_size, PTL_CT_ACK_REQ,
        p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL, 0);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: split 1 PtlPut error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    rem_addr = offsetof(shrb_t, q);
    ret = PtlPut(myrb->rb_md, (ptl_size_t)shrb_buff_elem_addr(trb, e, part_size), (n-part_size) * trb->elem_size,
        PTL_CT_ACK_REQ, p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL, 0);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: split 2 PtlPut error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    ret = PtlCTWait(myrb->rb_ct, current.success+2, &ctevent);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: split PtlCTWait error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    // check for error
    if (ctevent.failure > current.failure) {
      ret = PtlEQWait(myrb->rb_eq, &fault);
      if (ret == PTL_OK) {
        gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: split found fail event: %s\n", gtc_event_to_string(fault.type));
        gtc_dump_event(&fault);
      } else {
        gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: split PtlEQWait error (%s)\n", gtc_ptl_error(ret));
      }
    }

  } else {

    rem_addr = offsetof(shrb_t, q) + (nextfree * trb->elem_size);
    ret = PtlPut(myrb->rb_md, (ptl_size_t)e, n*trb->elem_size, PTL_CT_ACK_REQ, p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL, 0);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: PtlPut error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    // check for error
    if (ctevent.failure > current.failure) {
      ret = PtlEQWait(myrb->rb_eq, &fault);
      if (ret == PTL_OK) {
        gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
        gtc_dump_event(&fault);
      } else {
        gtc_lprintf(DBGSHRB,"shrb_push_n_head_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
      }
    }
  }

  trb->head  = nexthead;
  trb->size += n;

  shrb_update_remote_trb(myrb, trb, proc, 0);

  shrb_unlock(myrb, proc);

  shrb_free(trb);
}


void shrb_push_head(shrb_t *rb, int proc, void *e, int size) {
  shrb_push_n_head_impl(rb, proc, e, 1, size);
}


void shrb_push_n_head(shrb_t *myrb, int proc, void *e, int n) {
  shrb_push_n_head_impl(myrb, proc, e, n, myrb->elem_size);
}


void shrb_push_tail(shrb_t *rb, int proc, void *e, int size) {
  int nextfree;
  shrb_t *trb;
  ptl_process_t p;
  ptl_ct_event_t current;
  int rem_addr;
  int ret;

  assert(proc < rb->nproc);
  assert(size <= rb->elem_size);

  p.rank = proc;

  trb = malloc(sizeof(shrb_t));  // Target RB
  assert(trb != NULL);

  shrb_lock(rb, proc);

  // copy the remote RB descriptor
  shrb_fetch_remote_trb(rb, trb, proc);

  assert(!shrb_full(trb));

  // Check if the rb is empty.  If it is, we don't advance tail the first time
  if (shrb_empty(trb))
    nextfree = trb->tail;
  else
    nextfree = trb->tail - 1;

  if (nextfree < 0)
    nextfree = trb->max_size - 1;

  trb->tail = nextfree;
  trb->size++;

  PtlCTGet(rb->rb_ct, &current); // get current counter values
  rem_addr = offsetof(shrb_t, q) + (trb->tail * trb->elem_size);
  ret = PtlPut(rb->rb_md, (ptl_size_t)e, size, PTL_CT_ACK_REQ, p, rb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL, 0);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"shrb_push_tail: PtlGet error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // make update wait on CT for tail put, xtra_counts=1
  shrb_update_remote_trb(rb, trb, proc, current.success+1);

  shrb_unlock(rb, proc);

  shrb_free(trb);
}


int shrb_pop_head(shrb_t *rb, int proc, void *buf) {
  int   old_head;
  int   buf_valid = 0;
  shrb_t *trb; // Target RB
  ptl_process_t p;
  ptl_ct_event_t current;
  int rem_addr;
  int ret;

  assert(proc < rb->nproc);

  p.rank = proc;

  shrb_lock(rb, proc);

  if (proc != rb->procid) {
    // copy the remote RB descriptor
    trb = shrb_malloc(sizeof(shrb_t));
    shrb_fetch_remote_trb(rb, trb, proc);
  } else {
    trb = rb;
  }

  if (!shrb_empty(trb)) {
    old_head = trb->head;

    // Will this empty out the queue?
    if (trb->head != trb->tail)
      trb->head = trb->head - 1;

    if (trb->head < 0)
      trb->head = trb->max_size - 1;

    trb->size--;

    // copy out
    buf_valid = 1;
    if (proc != rb->procid) {

      PtlCTGet(rb->rb_ct, &current); // get current counter values

      rem_addr = offsetof(shrb_t, q) + (old_head * rb->elem_size);
      ret = PtlGet(rb->rb_md, (ptl_size_t)buf, trb->elem_size, p, rb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB,"shrb_pop_head: PtlGet error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }
      shrb_update_remote_trb(rb, trb, proc, current.success+1);
    } else {
      uint8_t *p = rb->q + (old_head*rb->elem_size);
      memcpy(buf, p, trb->elem_size);
    }
  }

  shrb_unlock(rb, proc);

  if (proc != rb->procid)
    shrb_free(trb);

  return buf_valid;
}


int shrb_pop_tail(shrb_t *rb, int proc, void *buf) {
  return shrb_pop_n_tail(rb, proc, 1, buf, STEAL_CHUNK);
}


/** Pop N things off the tail.  The result is returned in buf.
*/
int shrb_pop_n_tail(shrb_t *myrb, int proc, int n, void *e, int steal_vol) {
  int   nexttail;
  ptl_process_t p;
  ptl_ct_event_t current;
  int rem_addr, pending;
  shrb_t *trb = shrb_malloc(sizeof(shrb_t));  // Target RB
  int ret;

  assert(proc < myrb->nproc);

  p.rank = proc;

  shrb_lock(myrb, proc);

  // Copy the remote RB descriptor
  shrb_fetch_remote_trb(myrb, trb, proc);

  switch (steal_vol) {
    case STEAL_HALF:
      n = MIN((int) ceil(trb->size/2.0), n);
      break;
    case STEAL_ALL:
      n = MIN(trb->size, n);
      break;
    case STEAL_CHUNK:
      // Get as much as possible up to N.
      n = MIN(n, trb->size);
      break;
    default:
      printf("Error: Unknown steal volume heuristic.\n");
      assert(0);
  }

  if (n == 0) {
    shrb_unlock(myrb, proc);
    return n;
  }

  // Will this empty the RB?
  if (trb->size == n)
    nexttail = trb->head;
  else
    nexttail = (trb->tail+n) % trb->max_size;


  PtlCTGet(myrb->rb_ct, &current); // get current counter values

  //
  // If this pop wraps, break it in two parts.
  //
  if (trb->head < trb->tail && trb->max_size - trb->tail < n) {
    int part_size = trb->max_size - trb->tail;
    gtc_lprintf(DBGSHRB, " pop_n_tail popping in 2 parts.  nexttail=%d tail=%d head=%d part_size=%d\n",
        nexttail, trb->tail, trb->head, part_size);

    // shrb_elem_addr(rb,p,i) = rb->qhead + (i*elem_size)
    // shrb_buff(rb,e,idx)   =  e         + (i*elem_size)

    rem_addr = offsetof(shrb_t, q) + (trb->tail * myrb->elem_size);
    ret = PtlGet(myrb->rb_md, (ptl_size_t)shrb_buff_elem_addr(trb, e, 0), part_size * trb->elem_size,
        p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    rem_addr = offsetof(shrb_t, q);
    ret = PtlGet(myrb->rb_md, (ptl_size_t)shrb_buff_elem_addr(trb,e,part_size),
        (n-part_size) * trb->elem_size, p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }
    pending = 2;

    //
    // pop does not wrap around ring buffer
    //
  } else {
    gtc_lprintf(DBGSHRB, "%d: pop_n_tail popping in 1 part.  nexttail=%d tail=%d head=%d\n",
        nexttail, trb->tail, trb->head);

    rem_addr = offsetof(shrb_t, q) + (trb->tail * myrb->elem_size);
    ret = PtlGet(myrb->rb_md, (ptl_size_t)e, n * trb->elem_size, p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"shrb_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    pending = 1;
  }

  trb->tail  = nexttail;
  trb->size -= n;

  shrb_update_remote_trb(myrb, trb, proc, current.success+pending);

  shrb_unlock(myrb, proc);

  shrb_free(trb);
  return n;
}


int shrb_size(shrb_t *rb) {
  return rb->size;
}


int shrb_full(shrb_t *rb) {
  if (rb->size == rb->max_size)
    return 1;

  return 0;
}


int shrb_empty(shrb_t *rb) {
  if (rb->size == 0) {
    assert(rb->head == rb->tail);
    return 1;
  }
  return 0;
}


void shrb_reset(shrb_t *rb) {
  rb->nrelease   = 0;
  rb->nreacquire = 0;

  rb->head = 0;
  rb->tail = 0;
  rb->size = 0;
}
