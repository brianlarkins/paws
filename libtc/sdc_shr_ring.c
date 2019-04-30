/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <mutex.h>

#include "sdc_shr_ring.h"
#include "tc.h"


/**
 * Split Deferred-Copy Shared Ring Buffer Semantics:
 * ================================================
 *
 * This queue is split into three parts, as show in the diagram below: a part that is for
 * local access only (allowing for lock-free local interaction with the queue), a part for
 * shared access, and a reserved part that corresponds to remote copies that are in-progress
 * (ie deferred copies).
 *
 * Queue state:
 *
 * Head  - Always points to the element at the head of the local portion.
 *       - The head moves to the RIGHT.  Push and pop are both allowed wrt the head.
 * Tail  - Always points to the next available element at the tail of the shared portion.
 *       - The tail moves to the RIGHT.  Only pop is allowed from the tail.
 * Split - Always points to the element at the tail of the local portion of the queue.
 * Vtail - Virtual tail: points to the element at the tail of the reserved portion of the
 *         queue.  Everything between the head and the vtail is free space.
 * Itail - Tells us the collective progress of all transactions in the reserved portion.
 *         When Itail == tail we can reclaim the reserved space.
 *
 * The ring buffer can in in several states:
 *
 * 1. Empty.  In this case we have nlocal == 0, tail == split, and vtail == itail == tail
 *
 * 2. No Wrap-around:
 *  _______________________________________________________________
 * | |:::|::::::|$$$$$$|/////|                                     |
 * |_|:::|::::::|$$$$$$|/////|_____________________________________|
 *   ^   ^      ^      ^     ^
 * vtail itail tail  split  head
 *
 * : -- Reserved space in the queue (transaction in progress)
 * $ -- Available elements in the shared portion of the queue
 * / -- Reserved elements in the local portion of the queue
 *   -- Free space
 *
 * 2. Wrapped-around:
 *  _______________________________________________________________
 * |/////////|                      |:::::|::::::::::::::|$$$$|////|
 * |/////////|______________________|:::::|::::::::::::::|$$$$|////|
 *           ^                      ^     ^              ^    ^
 *         head                   vtail  itail         tail  split
 */


sdc_shrb_t *sdc_shrb_create(int elem_size, int max_size) {
  sdc_shrb_t  *rb;
  ptl_me_t me;
  ptl_md_t md;
  int err;

  gtc_lprintf(DBGSHRB,"  sdc_shrb_create()\n");

  // allocate memory for our ring buffer
  err = posix_memalign((void **)&rb, sizeof(int64_t), sizeof(sdc_shrb_t) + elem_size*max_size);
  if (err != 0) {
    gtc_lprintf(DBGSHRB, "sdc_shrb_create: unable to get aligned memory for atomic ops - %s\n", strerror(errno));
    exit(1);
  }
  memset(rb, 0, sizeof(sdc_shrb_t) + elem_size*max_size);

  rb->procid  = _c->rank;
  rb->nproc   = _c->size;
  rb->elem_size = elem_size;
  rb->max_size  = max_size;
  sdc_shrb_reset(rb);

  /* setup Portals structures for queue management */

  err = PtlCTAlloc(_c->lni, &rb->rb_ct);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sdc_shrb_create: CTAlloc error\n");
    exit(1);
  }

  err = PtlEQAlloc(_c->lni, 1024, &rb->rb_eq);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sdc_shrb_create: EQAlloc error\n");
    exit(1);
  }

  md.start     = NULL;
  md.length    = PTL_SIZE_MAX;
  md.options   = PTL_MD_EVENT_SUCCESS_DISABLE | PTL_MD_EVENT_CT_ACK | PTL_MD_EVENT_CT_REPLY | PTL_MD_EVENT_SEND_DISABLE;
  md.eq_handle = rb->rb_eq;
  md.ct_handle = rb->rb_ct;

  err = PtlMDBind(_c->lni, &md, &rb->rb_md);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sdc_shrb_create: PtlMDBind error\n");
    exit(1);
  }

  //err = PtlPTAlloc(_c->lni, PTL_PT_MATCH_UNORDERED, PTL_EQ_NONE, __GTC_SDC_SHRB_INDEX, &rb->ptindex);
  err = PtlPTAlloc(_c->lni, 0, PTL_EQ_NONE, PTL_PT_ANY, &rb->ptindex);
  if (err != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sdc_shrb_create: PTAlloc error: %s\n", gtc_ptl_error(err));
    exit(1);
  }

  me.start      = rb;
  me.length     = sizeof(sdc_shrb_t) + (elem_size * max_size);
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


void sdc_shrb_reset(sdc_shrb_t *rb) {
  // Reset state to empty
  rb->nlocal = 0;
  rb->tail   = 0;
  rb->itail  = 0;
  rb->vtail  = 0;
  rb->split  = 0;

  rb->waiting= 0;

  // Reset queue statistics
  rb->nrelease   = 0;
  rb->nreacquire = 0;
  rb->nwaited    = 0;
  rb->nreclaimed = 0;
}


void sdc_shrb_destroy(sdc_shrb_t *rb) {
  if (rb) {
    if (rb->rb_ct != PTL_INVALID_HANDLE) PtlCTFree(rb->rb_ct);
    if (rb->rb_eq != PTL_INVALID_HANDLE) PtlEQFree(rb->rb_eq);
    if (rb->rb_md != PTL_INVALID_HANDLE) PtlMDRelease(rb->rb_md);
    if (rb->rb_me != PTL_INVALID_HANDLE) PtlMEUnlink(rb->rb_me);
    PtlPTFree(_c->lni, rb->ptindex);

    free(rb);
  }
}


void sdc_shrb_print(sdc_shrb_t *rb) {
  printf("rb: %p {\n", rb);
  printf("   procid  = %d\n", rb->procid);
  printf("   nproc  = %d\n", rb->nproc);
  printf("   nlocal    = %"PRId64"\n", rb->nlocal);
  printf("   head      = %d\n", sdc_shrb_head(rb));
  printf("   split     = %"PRId64"\n", rb->split);
  printf("   tail      = %"PRId64"\n", rb->tail);
  printf("   itail     = %"PRId64"\n", rb->itail);
  printf("   vtail     = %"PRId64"\n", rb->vtail);
  printf("   max_size  = %d\n", rb->max_size);
  printf("   elem_size = %d\n", rb->elem_size);
  printf("   local_size = %d\n", sdc_shrb_local_size(rb));
  printf("   shared_size= %d\n", sdc_shrb_shared_size(rb));
  printf("   public_size= %d\n", sdc_shrb_public_size(rb));
  printf("   size       = %d\n", sdc_shrb_size(rb));
  printf("}\n");
}


/*==================== SYNCHRONIZATION ====================*/


void sdc_shrb_lock(sdc_shrb_t *rb, int proc) {
  synch_mutex_lock(&rb->lock, proc);
}


int sdc_shrb_trylock(sdc_shrb_t *rb, int proc) {
  return synch_mutex_trylock(&rb->lock, proc);
}


void sdc_shrb_unlock(sdc_shrb_t *rb, int proc) {
  synch_mutex_unlock(&rb->lock, proc);
}

/*==================== STATE QUERIES ====================*/


int sdc_shrb_head(sdc_shrb_t *rb) {
  return (rb->split + rb->nlocal - 1) % rb->max_size;
}


int sdc_shrb_local_isempty(sdc_shrb_t *rb) {
  return rb->nlocal == 0;
}


int sdc_shrb_shared_isempty(sdc_shrb_t *rb) {
  return rb->tail == rb->split;
}


int sdc_shrb_isempty(sdc_shrb_t *rb) {
  return sdc_shrb_local_isempty(rb) && sdc_shrb_shared_isempty(rb);
}


int sdc_shrb_local_size(sdc_shrb_t *rb) {
  return rb->nlocal;
}


int sdc_shrb_shared_size(sdc_shrb_t *rb) {
  if (sdc_shrb_shared_isempty(rb)) // Shared is empty
    return 0;
  else if (rb->tail < rb->split)   // No wrap-around
    return rb->split - rb->tail;
  else                             // Wrap-around
    return rb->split + rb->max_size - rb->tail;
}


int sdc_shrb_public_size(sdc_shrb_t *rb) {
  if (rb->vtail == rb->split) {    // Public is empty
    assert (rb->tail == rb->itail && rb->tail == rb->split);
    return 0;
  }
  else if (rb->vtail < rb->split)  // No wrap-around
    return rb->split - rb->vtail;
  else                             // Wrap-around
    return rb->split + rb->max_size - rb->vtail;
}


int sdc_shrb_size(void *b) {
  sdc_shrb_t *rb = (sdc_shrb_t *)b;
  return sdc_shrb_local_size(rb) + sdc_shrb_shared_size(rb);
}


/*==================== SPLIT MOVEMENT ====================*/


int sdc_shrb_reclaim_space(sdc_shrb_t *rb) {
  int reclaimed = 0;
  int vtail = rb->vtail;
  int itail = rb->itail; // Capture these values since we are doing this
  int tail  = rb->tail;  // without a lock
  TC_START_TSCTIMER(reclaim);
  rb->nreccalls++;

  if (vtail != tail && itail == tail) {
    rb->vtail = tail;
    if (tail > vtail)
      reclaimed = tail - vtail;
    else
      reclaimed = rb->max_size - vtail + tail;

    assert(reclaimed > 0);
    rb->nreclaimed++;
  }

  TC_STOP_TSCTIMER(reclaim);
  return reclaimed;
}


void sdc_shrb_ensure_space(sdc_shrb_t *rb, int n) {
  // Ensure that there is enough free space in the queue.  If there isn't
  // wait until others finish their deferred copies so we can reclaim space.
  TC_START_TSCTIMER(ensure);
  rb->nensure++;
  if (rb->max_size - (sdc_shrb_local_size(rb) + sdc_shrb_public_size(rb)) < n) {
    sdc_shrb_lock(rb, rb->procid);
    {
      if (rb->max_size - sdc_shrb_size(rb) < n) {
        // Error: amount of reclaimable space is less than what we need.
        // Try increasing the size of the queue.
        printf("SDC_SHRB: Error, not enough space in the queue to push %d elements\n", n);
        sdc_shrb_print(rb);
        assert(0);
      }
      rb->waiting = 1;
      while (sdc_shrb_reclaim_space(rb) == 0) /* Busy Wait */ ;
      rb->waiting = 0;
      rb->nwaited++;
    }
    sdc_shrb_unlock(rb, rb->procid);
  }
  TC_STOP_TSCTIMER(ensure);
}


void sdc_shrb_release(sdc_shrb_t *rb) {
  // Favor placing work in the shared portion -- if there is only one task
  // available this scheme will put it in the shared portion.
  TC_START_TSCTIMER(release);
  if (sdc_shrb_local_size(rb) > 0 && sdc_shrb_shared_size(rb) == 0) {
    int amount  = sdc_shrb_local_size(rb)/2 + sdc_shrb_local_size(rb) % 2;
    rb->nlocal -= amount;
    rb->split   = (rb->split + amount) % rb->max_size;
    rb->nrelease++;
  }
  TC_STOP_TSCTIMER(release);
}


void sdc_shrb_release_all(sdc_shrb_t *rb) {
  int amount  = sdc_shrb_local_size(rb);
  rb->nlocal -= amount;
  rb->split   = (rb->split + amount) % rb->max_size;
  rb->nrelease++;
}


int sdc_shrb_reacquire(sdc_shrb_t *rb) {
  int amount = 0;

  TC_START_TSCTIMER(reacquire);
  // Favor placing work in the local portion -- if there is only one task
  // available this scheme will put it in the local portion.
  sdc_shrb_lock(rb, rb->procid);
  {
    if (sdc_shrb_shared_size(rb) > sdc_shrb_local_size(rb)) {
      int diff    = sdc_shrb_shared_size(rb) - sdc_shrb_local_size(rb);
      amount      = diff/2 + diff % 2;
      rb->nlocal += amount;
      rb->split   = (rb->split - amount);
      if (rb->split < 0)
        rb->split += rb->max_size;
      rb->nreacquire++;
    }

    // Assertion: sdc_shrb_local_isempty(rb) => sdc_shrb_isempty(rb)
    assert(!sdc_shrb_local_isempty(rb) || (sdc_shrb_isempty(rb) && sdc_shrb_local_isempty(rb)));
  }
  sdc_shrb_unlock(rb, rb->procid);
  TC_STOP_TSCTIMER(reacquire);

  return amount;
}



void sdc_shrb_fetch_remote_trb(sdc_shrb_t *myrb, sdc_shrb_t *trb, int proc) {
  ptl_process_t p;
  ptl_ct_event_t current, ctevent;
  ptl_event_t fault;
  int ret;

  TC_START_TSCTIMER(getmeta);
  p.rank = proc;
  myrb->ngets++; // move this here, because it counts checks in get_buf()
  myrb->nmeta++; 

  // Copy the remote RB's metadata
  PtlCTGet(myrb->rb_ct, &current); // get current counter values

  myrb->nxfer += sizeof(sdc_shrb_t);
  ret = PtlGet(myrb->rb_md, (ptl_size_t)trb, sizeof(sdc_shrb_t), p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, 0, NULL);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sdc_shrb_fetch_remote_trb: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // check for error
  if (ctevent.failure > current.failure) {
    ret = PtlEQWait(myrb->rb_eq, &fault);
    if (ret == PTL_OK) {
      gtc_lprintf(DBGSHRB,"sdc_shrb_fetch_remote_trb: found fail event: %s\n", gtc_event_to_string(fault.type));
      gtc_dump_event(&fault);
    } else {
      gtc_lprintf(DBGSHRB,"sdc_shrb_fetch_remote_trb: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
    }
  }
  TC_STOP_TSCTIMER(getmeta);
}


/*==================== PUSH OPERATIONS ====================*/


static inline void sdc_shrb_push_n_head_impl(sdc_shrb_t *rb, int proc, void *e, int n, int size) {
  int head, old_head;

  assert(size <= rb->elem_size);
  assert(size == rb->elem_size || n == 1);  // n > 1 ==> size == rb->elem_size
  assert(proc == rb->procid);
  TC_START_TSCTIMER(pushhead);

  // Make sure there is enough space for n elements
  sdc_shrb_ensure_space(rb, n);

  // Proceed with the push
  old_head    = sdc_shrb_head(rb);
  rb->nlocal += n;
  head        = sdc_shrb_head(rb);

  if (head > old_head || old_head == rb->max_size - 1) {
    memcpy(sdc_shrb_elem_addr(rb, (old_head+1)%rb->max_size), e, n*size);
  }

  // This push wraps around, break it into two parts
  else {
    int part_size = rb->max_size - 1 - old_head;

    memcpy(sdc_shrb_elem_addr(rb, old_head+1), e, part_size*size);
    memcpy(sdc_shrb_elem_addr(rb, 0), sdc_shrb_buff_elem_addr(rb, e, part_size), (n - part_size)*size);
  }
  TC_STOP_TSCTIMER(pushhead);
}

void sdc_shrb_push_head(sdc_shrb_t *rb, int proc, void *e, int size) {
  // sdc_shrb_push_n_head_impl(rb, proc, e, 1, size);
  int old_head;
  TC_START_TSCTIMER(pushhead);

  assert(size <= rb->elem_size);
  assert(proc == rb->procid);

  // Make sure there is enough space for n elements
  sdc_shrb_ensure_space(rb, 1);

  // Proceed with the push
  old_head    = sdc_shrb_head(rb);
  rb->nlocal += 1;

  memcpy(sdc_shrb_elem_addr(rb, (old_head+1)%rb->max_size), e, size);
  TC_STOP_TSCTIMER(pushhead);
}

void sdc_shrb_push_n_head(void *b, int proc, void *e, int n) {
  sdc_shrb_t *rb = (sdc_shrb_t *)b;
  sdc_shrb_push_n_head_impl(rb, proc, e, n, rb->elem_size);
}


void *sdc_shrb_alloc_head(sdc_shrb_t *rb) {
  // Make sure there is enough space for 1 element
  sdc_shrb_ensure_space(rb, 1);

  rb->nlocal += 1;

  return sdc_shrb_elem_addr(rb, sdc_shrb_head(rb));
}



/*==================== POP OPERATIONS ====================*/


int sdc_shrb_pop_head(void *b, int proc, void *buf) {
  sdc_shrb_t *rb = (sdc_shrb_t *)b;
  int   old_head;
  int   buf_valid = 0;

  assert(proc == rb->procid);

  // If we are out of local work, try to reacquire
  if (sdc_shrb_local_isempty(rb)) {
    sdc_shrb_reacquire(rb);
  }

  if (sdc_shrb_local_size(rb) > 0) {
    old_head = sdc_shrb_head(rb);

    memcpy(buf, sdc_shrb_elem_addr(rb, old_head), rb->elem_size);

    rb->nlocal--;
    buf_valid = 1;
  }

  // Assertion: !buf_valid => sdc_shrb_isempty(rb)
  assert(buf_valid || (!buf_valid && sdc_shrb_isempty(rb)));

  return buf_valid;
}


int sdc_shrb_pop_tail(sdc_shrb_t *rb, int proc, void *buf) {
  return sdc_shrb_pop_n_tail(rb, proc, 1, buf, STEAL_CHUNK);
}


/** Pop up to N elements off the tail of the queue, putting the result into the user-
 *  supplied buffer.
 *
 *  @param myrb  Pointer to the RB
 *  @param proc  Process to perform the pop on
 *  @param n     Requested/Max. number of elements to pop.
 *  @param e     Buffer to store result in.  Should be rb->elem_size*n bytes big and
 *               should also be allocated with sdc_shrb_malloc().
 *  @param steal_vol Enumeration that selects between different schemes for determining
 *               the amount we steal.
 *  @param trylock Indicates whether to use trylock or lock.  Using trylock will result
 *               in a fail return value when trylock does not succeed.
 *
 *  @return      The number of tasks stolen or -1 on failure
 */
static inline int sdc_shrb_pop_n_tail_impl(sdc_shrb_t *myrb, int proc, int n, void *e, int steal_vol, int trylock) {
  TC_START_TSCTIMER(poptail);
  sdc_shrb_t *trb = malloc(sizeof(sdc_shrb_t));
  ptl_process_t p;
  ptl_ct_event_t current, ctevent;
  ptl_event_t fault;
  int ret;

  assert(trb != NULL);
  p.rank = proc;

  // Attempt to get the lock
  if (trylock) {
    if (!sdc_shrb_trylock(myrb, proc)) {
      free(trb);
      return -1;
    }
  } else {
     sdc_shrb_lock(myrb, proc);
  }

  sdc_shrb_fetch_remote_trb(myrb, trb, proc);

  switch (steal_vol) {
    case STEAL_HALF:
      n = MIN(sdc_shrb_shared_size(trb)/2 + sdc_shrb_shared_size(trb) % 2, n);
      break;
    case STEAL_ALL:
      n = MIN(sdc_shrb_shared_size(trb), n);
      break;
    case STEAL_CHUNK:
      // Get as much as possible up to N.
      n = MIN(n, sdc_shrb_shared_size(trb));
      break;
    default:
      printf("Error: Unknown steal volume heuristic.\n");
      assert(0);
  }

  // Reserve N elements by advancing the victim's tail
  if (n > 0) {
    int  new_tail;
    int *metadata;
    int  xfer_size;
    int *loc_addr, rem_addr;

    metadata = (int*) malloc(2*sizeof(int));
    assert(metadata != NULL);

    new_tail    = (trb->tail + n) % trb->max_size;
    metadata[0] = new_tail; // itail field
    metadata[1] = new_tail; // tail field in rb struct

    loc_addr    = &metadata[1];
    rem_addr    = offsetof(sdc_shrb_t, tail);
    xfer_size   = 1*sizeof(int);

    PtlCTGet(myrb->rb_ct, &current); // get current counter values

    myrb->nxfer += xfer_size;
    ret = PtlPut(myrb->rb_md, (ptl_size_t)loc_addr, xfer_size, PTL_CT_ACK_REQ, p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL, 0);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlPut error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    // check for error
    if (ctevent.failure > current.failure) {
      ret = PtlEQWait(myrb->rb_eq, &fault);
      if (ret == PTL_OK) {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
        gtc_dump_event(&fault);
      } else {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
      }
    }

    //gtc_dprintf("stealing %d [ %d..%d ] : new: %d itail: %d split: %d nlocal: %d ] from p%d\n",
    //    n, sdc_shrb_head(trb), trb->tail, new_tail, trb->itail,  trb->split, trb->nlocal, proc);

    sdc_shrb_unlock(myrb, proc); // Deferred copy unlocks early

    PtlCTGet(myrb->rb_ct, &current); // get current counter values

    myrb->nxfer += n*trb->elem_size;
    //
    // Transfer work into the local buffer -- contiguous case
    //
    if (trb->tail + (n-1) < trb->max_size) {

      rem_addr = offsetof(sdc_shrb_t, q) + (trb->tail * myrb->elem_size); // offset of tail element
      ret = PtlGet(myrb->rb_md, (ptl_size_t)e, n*trb->elem_size, p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }

      ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }

      // check for error
      if (ctevent.failure > current.failure) {
        ret = PtlEQWait(myrb->rb_eq, &fault);
        if (ret == PTL_OK) {
          gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
          gtc_dump_event(&fault);
        } else {
          gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
        }
      }

    //
    // Transfer work into the local buffer -- wraparound case
    //
    } else {
      int part_size  = trb->max_size - trb->tail;

      rem_addr = offsetof(sdc_shrb_t, q) + (trb->tail * myrb->elem_size); // offset of tail element
      ret = PtlGet(myrb->rb_md, (ptl_size_t)sdc_shrb_buff_elem_addr(trb, e, 0), part_size * trb->elem_size,
          p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }

      rem_addr = offsetof(sdc_shrb_t, q); // offset of beginning of queue
      ret = PtlGet(myrb->rb_md, (ptl_size_t)sdc_shrb_buff_elem_addr(trb, e, part_size), (n-part_size) * trb->elem_size,
          p, myrb->ptindex, __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }

      ret = PtlCTWait(myrb->rb_ct, current.success+2, &ctevent);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }

      // check for error
      if (ctevent.failure > current.failure) {
        ret = PtlEQWait(myrb->rb_eq, &fault);
        if (ret == PTL_OK) {
          gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
          gtc_dump_event(&fault);
        } else {
          gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
        }
      }
    }

    // Accumulate itail_inc onto the victim's intermediate tail
    {
      int64_t itail_inc, err;
      int count = sizeof(int64_t);

      PtlCTGet(myrb->rb_ct, &current); // get current counter values

      // How much should we add to the itail?  If we caused a wraparound, we need to also wrap itail.
      if (new_tail > trb->tail)
        itail_inc = n;
      else
        itail_inc = n - trb->max_size;
      //gtc_dprintf("itail_inc: %d : %d :: %d :: %d : %d\n", itail_inc, new_tail, trb->itail, n, trb->max_size);

      myrb->nxfer += sizeof(itail_inc);
      rem_addr = offsetof(sdc_shrb_t, itail);
      err = PtlAtomic(myrb->rb_md, (ptl_size_t)&itail_inc, count, PTL_CT_ACK_REQ, p, myrb->ptindex,
          __GTC_SHRB_QUEUE_MATCH, rem_addr, NULL, 0, PTL_SUM, PTL_INT64_T);

      // check for completion
      ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }

      // check for error
      if (ctevent.failure > current.failure) {
        ret = PtlEQWait(myrb->rb_eq, &fault);
        if (ret == PTL_OK) {
          gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
          gtc_dump_event(&fault);
        } else {
          gtc_lprintf(DBGSHRB,"sdc_shrb_pop_n_tail_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
        }
      }

      assert(err == 0);
    }

    free(metadata);
    myrb->nsteals++;

  } else /* (n <= 0) */ {
    sdc_shrb_unlock(myrb, proc);
  }

  free(trb);
  TC_STOP_TSCTIMER(poptail);

  return n;
}

int sdc_shrb_pop_n_tail(void *b, int proc, int n, void *e, int steal_vol) {
  sdc_shrb_t *myrb = (sdc_shrb_t *)b;
  return sdc_shrb_pop_n_tail_impl(myrb, proc, n, e, steal_vol, 0);
}

int sdc_shrb_try_pop_n_tail(void *b, int proc, int n, void *e, int steal_vol) {
  sdc_shrb_t *myrb = (sdc_shrb_t *)b;
  return sdc_shrb_pop_n_tail_impl(myrb, proc, n, e, steal_vol, 1);
}
