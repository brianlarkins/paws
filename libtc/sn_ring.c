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

static void sn_release_impl(sn_ring_t *rb, sn_releasetype_t rtype);
static inline int sn_pop_n_tail_impl(sn_ring_t *myrb, int proc, int n, void *e, int steal_vol);

/**
 * Portals Steal-N Queue Implementation
 * ================================================
 *
 * This queue is split into two parts, as shown in the diagram below: a part that is for
 * local access only (allowing for lock-free local interaction with the queue), a part for
 * shared access. The remaining portion of the queue is free.
 *
 * Queue state:
 *
 * Head  - Always points to the element at the head of the local portion.
 *       - The head moves to the RIGHT.  Push and pop are both allowed wrt the head.
 * Tail  - Always points to the next available element at the tail of the shared portion.
 *       - The tail moves to the RIGHT.  Only pop is allowed from the tail.
 * Split - Always points to the element at the tail of the local portion of the queue.
 *
 * A Portals ME is created for each group of N tasks, where N is defined at creation
 *
 * The ring buffer can in in several states:
 *
 * 1. Empty.  In this case we have nlocal == 0, tail == split, and vtail == itail == tail
 *
 * 2. No Wrap-around: (N = 3)
 *
 *              ME1 ME2
 *               v  v
 *  _______________________________________________________________
 * |            |$$$$$$|/////|                                     |
 * |____________|$$$$$$|/////|_____________________________________|
 *              ^      ^     ^
 *             tail  split  head
 *
 * $ -- Available elements in the shared portion of the queue
 * / -- Reserved elements in the local portion of the queue
 *   -- Free space
 *
 * 2. Wrapped-around: (N = 5)
 *
 *                                   ME1  ME2  ME3  ME4  ME5
 *                                   v    v    v    v    v
 *  _______________________________________________________________
 * |/////////|                      |$$$$$$$$$$$$$$$$$$$$$$$$$|////|
 * |/////////|______________________|$$$$$$$$$$$$$$$$$$$$$$$$$|////|
 *           ^                      ^                         ^
 *         head                    tail                     split
 */


/**
 * sn_release - release half of the local tasks to the shared portion
 *
 * @param rb the ring buffer to release
 */
void sn_release(sn_ring_t *rb) {
  sn_release_impl(rb, SNReleaseHalf);
}



/**
 * sn_release_all - release all of the local tasks to the shared portion
 *
 * @param rb the ring buffer to release
 */
void sn_release_all(sn_ring_t *rb) {
  sn_release_impl(rb, SNReleaseAll);
}



/**
 * sn_release_impl - release implementation function
 *
 * @param rb  the ring buffer to release
 * @param all 0 to release half, 1 to release all
 */
static void sn_release_impl(sn_ring_t *rb, sn_releasetype_t rtype) {
  ptl_me_t me;
  ptl_size_t len;
  int preamount, prelocal, preshared, presize;
  int amount, nchunks, splitpt, err, chunksize;
  sn_matchpair_t  mp, *mpp;

  // Favor placing work in the shared portion -- if there are only N tasks
  // available this scheme will put it in the shared portion.
  if ((sn_local_size(rb) > rb->chunk_size) && (sn_shared_size(rb) == 0)) {

    // release all? or just half?
    if (rtype == SNReleaseAll)  {
      amount  = sn_local_size(rb);
      nchunks = (rb->chunk_size + amount) / rb->chunk_size;
    } else {
      amount  = sn_local_size(rb)/2 + sn_local_size(rb) % 2;
      // only release tasks in blocks of N
      nchunks = amount / rb->chunk_size;
      amount = nchunks * rb->chunk_size;
    }

    preamount = amount;
    prelocal  = sn_local_size(rb);
    preshared = sn_shared_size(rb);
    presize   = sn_size(rb);

    //gtc_lprintf(DBGSHRB, "sn_release: moving %d tasks (%d chunks of %d tasks) of %d local to shared\n", amount, nchunks, rb->chunk_size, sn_local_size(rb));

    // update local queue state
    rb->nlocal -= amount;

    // track progress for matchpair list
    splitpt = rb->split;

    // setup static parts of MEs
    me.ct_handle  = rb->rb_shct;
    me.uid        = PTL_UID_ANY;
    //me.options    = PTL_ME_OP_PUT | PTL_ME_OP_GET | PTL_IOVEC | PTL_ME_USE_ONCE | PTL_ME_MANAGE_LOCAL
    me.options    = PTL_ME_OP_PUT | PTL_ME_OP_GET | PTL_IOVEC | PTL_ME_MANAGE_LOCAL
      | PTL_ME_EVENT_LINK_DISABLE | PTL_ME_EVENT_OVER_DISABLE | PTL_ME_EVENT_CT_COMM; // PTL_ME_EVENT_COMM_DISABLE | PTL_ME_EVENT_SUCCESS_DISABLE
    me.match_id.rank = PTL_RANK_ANY;
    me.match_bits = __GTC_SN_QUEUE_MATCH;
    me.ignore_bits = 0;
    me.min_free = 1; // only unlink ME once all bytes have been consumed


    // loop from split towards head, in chunks of chunk_size
    for (int i=0; i<nchunks; i++) {
      ptl_size_t wrapped = 0;

      // release-all may result in final ME being of smaller than rb->chunk_size
      chunksize = (amount >= rb->chunk_size) ? rb->chunk_size : amount;

      // setup ME for stealing-N tasks, working forwards from split
      if ((splitpt + chunksize) < rb->max_size) {
        // simple case: add next chunk to left: rb[(split_cursor - chunk_size)..split_cursor]
        mp.iov = calloc(1,sizeof(ptl_iovec_t));
        mp.iov->iov_base = sn_elem_addr(rb, splitpt);
        mp.iov->iov_len = chunksize * rb->elem_size;
        //printf("  split: %d :: iov_base %d : iov_len %d\n", splitpt, splitpt, mp.iov->iov_len);
        len = 1;

      } else {
        // wraparound case: use iovec to handle dis-contiguous regions
        wrapped = (splitpt + chunksize) % rb->max_size; // # of wrapped entries

        // allocate 2 entry iovec
        mp.iov = calloc(2,sizeof(ptl_iovec_t));

        // rb[splitpt..qmax]
        mp.iov[0].iov_base = sn_elem_addr(rb, splitpt);
        mp.iov[0].iov_len  = (chunksize - wrapped) * rb->elem_size;

        // rb[0..wrapped]
        mp.iov[1].iov_base = sn_elem_addr(rb, 0);
        mp.iov[1].iov_len  = wrapped * rb->elem_size;
        len = 2;
      }

      mp.index = splitpt;
      mp.tasks = chunksize;
      mp_push_tail(rb, &mp); // append to matchpair list tail
      mpp = mp_tail(rb);     // points to in-list entry, needed for user_ptr check

      me.start      = mp.iov;
      me.length     = len;

      // save pointer to matchpair list entry in user_ptr
      err = PtlMEAppend(_c->lni, rb->ptindex, &me, PTL_PRIORITY_LIST, mpp, &(mpp->me));
      if (err != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sn_release: PtlMEAppend error (%s)\n", gtc_ptl_error(err));
        exit(1);
      }

      // update state for next iteration of the loop
      if ((splitpt + chunksize) < rb->max_size)
        splitpt = splitpt + chunksize;
      else
        splitpt = wrapped;
      amount   -= chunksize;
    }

    //gtc_lprintf(DBGSHRB, "sn_release: moving %d tasks pre: (%d:%d/%d) post: (%d:%d/%d)\n",
    //        preamount, prelocal, preshared, presize,
    //        sn_local_size(rb), sn_shared_size(rb), sn_size(rb));
    UNUSED(preamount);
    UNUSED(prelocal);
    UNUSED(preshared);
    UNUSED(presize);

    rb->split = splitpt;
    assert(amount == 0);

    rb->nrelease++;
  }
}



/**
 * sn_reacquire - move work from the shared portion of the task queue to the local part
 *
 * @param rb the ring buffer to reacquire
 */
int sn_reacquire(sn_ring_t *rb) {
  int amount = 0, ret, diff, nchunks;
  sn_matchpair_t mp;

  // Favor placing work in the local portion -- in units chunk size
  if (sn_shared_size(rb) > sn_local_size(rb)) {

    // disable PTE to block remote steals temporarily
    ret = PtlPTDisable(_c->lni, rb->ptindex);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB, "sn_reacquire: PtlPTDisable error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    // determine amount of work to move to local section
    diff   = sn_shared_size(rb) - sn_local_size(rb);

    if (sn_shared_size(rb) <= rb->chunk_size) {
      // special case when shared size is <= chunk_size
      amount = sn_shared_size(rb);
      nchunks = 1;
    } else {
      // only acquire tasks in blocks of N
      amount = diff/2 + diff % 2;
      nchunks = amount / rb->chunk_size;
      amount = nchunks * rb->chunk_size;
    }

    //gtc_dprintf("reacquiring: local: %d shared: %d empty: %d acquiring %d chunks (%d tasks)\n",
    //    sn_local_size(rb), sn_shared_size(rb), sn_isempty(rb), nchunks, amount);
    //sn_print_matchpairs(rb, "before reacquire\n");

    // ensure matchpair list / MEs are in known state
    sn_reclaim_space(rb);

    // remove tasks from shared section
    for (int i=0; i < nchunks; i++) {
      assert(!mp_isempty(rb));
      mp = mp_pop_head(rb);

      // unlink ME
      ret = PtlMEUnlink(mp.me);
      if (ret != PTL_OK) {
        gtc_lprintf(DBGSHRB, "sn_reacquire: PtlMEUnlink error (%s)\n", gtc_ptl_error(ret));
        exit(1);
      }

      if (mp.iov) free(mp.iov);
    }

    // adjust split
    rb->nlocal += amount;
    rb->split   = (rb->split - amount);
    if (rb->split < 0)
      rb->split += rb->max_size;

    assert(!sn_local_isempty(rb) || (sn_isempty(rb) && sn_local_isempty(rb)));

    // enable PTE
    ret = PtlPTEnable(_c->lni, rb->ptindex);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB, "sn_reacquire: PtlPTEnable error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    rb->nreacquire++; // bookkeep
    //sn_print_matchpairs(rb, "after reacquire\n");
  }

  return amount;
}



/**
 * sn_pop_tail - fetch the task at the tail of the queue
 *
 * @param rb   the ring buffer
 * @param proc rank of task queue
 * @param buf  buffer to place the task into
 * @return -1 on failure (empty), 1 on success
 */
int sn_pop_tail(sn_ring_t *rb, int proc, void *buf) {
  // this will always blow an assertion unless chunk size == 1
  return sn_pop_n_tail(rb, proc, 1, buf, STEAL_CHUNK);
}


/** Pop up to N elements off the tail of the queue, putting the result into the user-
 *  supplied buffer.
 *
 *  @param myrb  Pointer to the RB
 *  @param proc  Process to perform the pop on
 *  @param n     Requested/Max. number of elements to pop.
 *  @param e     Buffer to store result in.  Should be rb->elem_size*n bytes big and
 *               should also be allocated with sn_malloc().
 *  @param steal_vol Enumeration that selects between different schemes for determining
 *               the amount we steal.
 *  @return      The number of tasks stolen or -1 on failure
 */
static inline int sn_pop_n_tail_impl(sn_ring_t *myrb, int proc, int n, void *e, int steal_vol) {
  ptl_process_t p;
  ptl_event_t ev;
  int notfound = 1, ret, nstolen = 0;

  p.rank = proc;
  TC_START_TSCTIMER(poptail);

  switch (steal_vol) {
    case STEAL_HALF:
      n = MIN(myrb->max_steal, n);
      break;
    case STEAL_ALL:
      n = MIN(myrb->max_size, n);
      break;
    case STEAL_CHUNK:
      n = MIN(myrb->chunk_size, n);
      break;
  }

  //gtc_lprintf(DBGSHRB, "pop_n: requesting %d (%d bytes) tasks from %d\n", n, n*myrb->elem_size, proc);

  // fetch N tasks from victim
  ret = PtlGet(myrb->rb_md, (ptl_size_t)e, n*myrb->elem_size, p, myrb->ptindex, __GTC_SN_QUEUE_MATCH, 0, NULL);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sn_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }
  myrb->ngets++;

  while (notfound) {
    ret = PtlEQWait(myrb->rb_eq, &ev);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sn_pop_n_tail_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
    } else if (ev.type != PTL_EVENT_REPLY) {
      gtc_lprintf(DBGSHRB,"sn_pop_n_tail_impl: found unexpected event\n");
      gtc_dump_event(&ev);
    } else {
      if (ev.ni_fail_type == PTL_NI_OK) {
        // PTL_NI_OK for success

        // check we matched EOQ marker
        if ((ev.mlength == sizeof(myrb->eoq_sentinel)) && ((*(uint64_t *)e) == 0xdeadb33f)) {
          // found end-of-queue
          myrb->neoq++;
          nstolen = 0;
        } else {
          nstolen += (ev.mlength / myrb->elem_size);
          assert((ev.mlength % myrb->elem_size) == 0); // no partial tasks
          myrb->nsteals++;
        }
        break;

      } else if (ev.ni_fail_type == PTL_NI_DROPPED) {
        // nothing to steal
        notfound = 0;
        myrb->ndropped++;

      } else if (ev.ni_fail_type == PTL_NI_PT_DISABLED) {
        // victim is modifying its matchlist
        nstolen = -1;
        notfound = 0;
        myrb->ndisabled++;

      } else {
        gtc_lprintf(DBGSHRB,"sn_pop_n_tail_impl: found unexpected reply event\n");
        gtc_dump_event(&ev);
      }
    }
  }
  //if (nstolen > 0)
    //gtc_lprintf(DBGSHRB, "sn_pop_n_tail_impl: stole %d tasks\n", nstolen);

  TC_STOP_TSCTIMER(poptail);
  // return number of tasks stolen
  return nstolen;
}



/**
 * sn_pop_n_tail - fetch N tasks from tail of a remote task queue
 * @param b         the ring buffer
 * @param proc      rank of victim task queue
 * @param n         number of tasks to pop
 * @param e         buffer to place the task into
 * @return -1 on failure (empty), 1 on success
 */
int sn_pop_n_tail(void *b, int proc, int n, void *e, int steal_vol) {
  sn_ring_t *myrb = (sn_ring_t *)b;
  return sn_pop_n_tail_impl(myrb, proc, n, e, steal_vol);
}


/**
 * sn_try_pop_n_tail - same as sn_pop_n_tail for this implementation
 * @param b         the ring buffer
 * @param proc      rank of victim task queue
 * @param n         number of tasks to pop
 * @param e         buffer to place the task into
 * @param steal_vol must be STEAL_CHUNK
 * @return -1 on failure (empty), 1 on success
 */
int sn_try_pop_n_tail(void *b, int proc, int n, void *e, int steal_vol) {
  sn_ring_t *myrb = (sn_ring_t *)b;
  return sn_pop_n_tail_impl(myrb, proc, n, e, steal_vol);
}
