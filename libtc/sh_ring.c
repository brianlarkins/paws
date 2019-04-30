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

#define SH_MAX_BUSY 10

static void sh_clean_matchlist(sn_ring_t *rb);
static void sh_release_impl(sn_ring_t *rb, sn_releasetype_t rtype, int csz);

/**
 * Portals Steal-Half Queue Implementation
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
 * 2. No Wrap-around:
 *
 *              ME1  ME2
 *               v   v
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
 * 2. Wrapped-around:
 *
 *                                   ME1       ME2     ME3  ME4
 *                                   v         v       v    v
 *  _______________________________________________________________
 * |/////////|                      |$$$$$$$$$$$$$$$$$$$$$$$$$|////|
 * |/////////|______________________|$$$$$$$$$$$$$$$$$$$$$$$$$|////|
 *           ^                      ^                         ^
 *         head                    tail                     split
 */


/*==================== SPLIT MOVEMENT ====================*/

#if 0
/**
 * sh_clean_matchlist - helper function to remove all task MEs
 *
 * @param rb the ring buffer to release
 */
static void sh_clean_matchlist(sn_ring_t *rb) {
  int ret, unlinked, busy = 0;
  sn_matchpair_t mp, *mpp = NULL;
  ptl_event_t ev;

  // clean task MEs from list
  for (unlinked = 0, mp_start_tail(rb); mp_has_prev(rb); unlinked++) {

    // try to unlink first
    mp = mp_next(rb);
    ret = PtlMEUnlink(mp.me);
    if (ret == PTL_IN_USE) {
      // ME in use, remember it and assume rest are stolen/unlinked
      gtc_lprintf(DBGSHRB, "sh_clean_matchlist: found in-use ME: (%p)\n", &rb->mpcursor->mp);
      mpp = mp_cur_evptr(rb);
      busy = 1;
      break;

    } else if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB, "sh_clean_matchlist: PtlMEUnlink error (%s) :: %d\n", gtc_ptl_error(ret), __gtc_marker[4]);
    }
  }

  // remove MPs for each ME that was unlinked
  for (; unlinked > 0; unlinked--)
    mp_pop_tail(rb);

  // common case is !busy and EQ empty
  while (1) {

    if (!busy) {
      ret = PtlEQGet(rb->rb_sheq, &ev);  // don't block if all MEs unlinked
      if (ret == PTL_EQ_EMPTY) break;    // all done

    } else {
      ret = PtlEQWait(rb->rb_sheq, &ev); // block if found a busy ME
    }

    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sn_clean_matchlist: PtlEQGet/PtlEQWait error: %d (%s)\n", busy, gtc_ptl_error(ret));
      break;
    }

    // process pending steal and unlink events for all auto-unlinked MEs
    if (mp_head(rb))
      sn_process_event(rb, &ev);

    if (busy && (ev.user_ptr == mpp) && (ev.type == PTL_EVENT_AUTO_UNLINK))
      break; // exit when we've unlinked the busy ME/MP
  }
}
#endif

//#define SCIOTO_CLEANUP_DISABLE
/**
 * sh_clean_matchlist - helper function to remove all task MEs
 *
 * @param rb the ring buffer to release
 */
static void sh_clean_matchlist(sn_ring_t *rb) {
  int busy = 0, busyevs = 0, ret;
  sn_matchpair_t mp, *busymp[SH_MAX_BUSY]; // handle up to three busy MEs
  ptl_event_t ev;

  //mp_print_matchpairs(rb, "start cleaning");

#ifdef SCIOTO_CLEANUP_DISABLE
  // disable PTE to block remote steals temporarily
  ret = PtlPTDisable(_c->lni, rb->ptindex);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB, "sn_reacquire: PtlPTDisable error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }
#endif // SCIOTO_CLEANUP_DISABLE

  mp_start_tail(rb);
  while (mp_has_prev(rb)) {

    // try to unlink all MEs
    mp = mp_next(rb);
    ret = PtlMEUnlink(mp.me);
    if (ret == PTL_IN_USE) {
      // ME in use, remember it
      //gtc_lprintf(DBGSHRB, "sh_clean_matchlist: found in-use ME: (%p)\n", &rb->mpcursor->mp);
      busymp[busy] = mp_cur_evptr(rb); // event user_ptr of each busy ME
      busyevs += 2;                     // we _must_ see an unlink and get event for each busy ME
      busy++;                           // count of busy MEs
      assert(busy < SH_MAX_BUSY);       // need to make array bigger

    } else if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB, "sh_clean_matchlist: PtlMEUnlink error (%s) :: %d\n", gtc_ptl_error(ret), __gtc_marker[4]);
    }
  }

  // common case is !busy and EQ empty
  while (1) {

    if (!busyevs) {
      ret = PtlEQGet(rb->rb_sheq, &ev);  // don't block if all MEs unlinked
      if (ret == PTL_EQ_EMPTY) break;    // all done

    } else {
      ret = PtlEQWait(rb->rb_sheq, &ev); // block if found a busy ME
    }

    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sn_clean_matchlist: PtlEQGet/PtlEQWait error: %d (%s)\n", busy, gtc_ptl_error(ret));
      break;
    }

    // check to see if we should switch from blocking to non-blocking
    if (busyevs) {
      for (int i=0; i < busy; i++) {
        if (ev.user_ptr == busymp[i])
          busyevs--;  //
      }
    }

    sn_process_event(rb, &ev);
  }

  //mp_print_matchpairs(rb, "after events");

  // any remaining MP entries should just be reclaimed
  while (!mp_isempty(rb))
    mp_pop_tail(rb);

#ifdef SCIOTO_CLEANUP_DISABLE
  // enable PTE
  ret = PtlPTEnable(_c->lni, rb->ptindex);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB, "sn_reacquire: PtlPTEnable error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }
#endif // SCIOTO_CLEANUP_DISABLE

  //mp_print_matchpairs(rb, "after mp cleanup");
}



/**
 * sh_release - release half of the local tasks to the shared portion
 *
 * @param rb the ring buffer to release
 */
void sh_release(sn_ring_t *rb) {
  sh_release_impl(rb, SNReleaseHalf, 0);
}



/**
 * sh_release_all - release all of the local tasks to the shared portion
 *
 * @param rb the ring buffer to release
 */
void sh_release_all(sn_ring_t *rb) {
  sh_release_impl(rb, SNReleaseAll, 0);
}



/**
 * sh_release_all - release all of the local tasks to the shared portion, but in chunks for timing
 *
 * @param rb the ring buffer to release
 */
void sh_release_all_chunks(sn_ring_t *rb, int chunksize) {
  sh_release_impl(rb, SNReleaseAllChunk, chunksize);
}



extern int maxchunk;
//#define SCIOTWO_USE_PTL_IOVEC
#ifndef SCIOTWO_USE_PTL_IOVEC
/**
 * sh_release_impl - release implementation function
 *
 * @param rb  the ring buffer to release
 * @param all 0 to release half, 1 to release all
 */
static void sh_release_impl(sn_ring_t *rb, sn_releasetype_t rtype, int csz) {
  ptl_me_t me;
  int amount = 0 , diff, splitpt = rb->split, err, chunksize;
  sn_matchpair_t  mp, *mpp;
  int preamount = -1, prelocal = -1, preshared = -1, presize = -1;
  char *rty, *astr = "all", *hstr = "half", *acstr = "reacq";
  task_t *task;
  rty = astr;

  if (rtype != SNReacquire)
    TC_START_TSCTIMER(release);

  // Favor placing work in the shared portion -- if there are only N tasks
  // available this scheme will put it in the shared portion.
  if ((rtype == SNReacquire) || ((sn_local_size(rb) > 0) && (sn_shared_size(rb) == 0))) {
    prelocal  = sn_local_size(rb);
    preshared = sn_shared_size(rb);
    presize   = sn_size(rb);

    // release all? or just half?
    switch (rtype) {
      case SNReleaseAll:
      case SNReleaseAllChunk:
        amount  = sn_local_size(rb);
        rb->nlocal -= amount; // update local queue state
        splitpt = rb->split; // track progress for matchpair list
        rty = astr;
        rb->nrelease++;
        break;

      case SNReleaseHalf:
        amount  = sn_local_size(rb)/2 + sn_local_size(rb) % 2;
        rb->nlocal -= amount; // update local queue state
        splitpt = rb->split; // track progress for matchpair list
        rty = hstr;
        rb->nrelease++;
        break;

      case SNReacquire:
        // reacquire -- re-split queue favoring work on the local side
        diff   = sn_shared_size(rb) - sn_local_size(rb);
        amount = sn_size(rb) - (diff/2 + diff%2); // more work stays local
        rb->nlocal += (diff/2 + diff%2);
        splitpt = rb->tail; // acquire needs to work from tail to new split
        rty = acstr;
        break;
    }

    preamount = amount;

    // setup static parts of MEs
    me.ct_handle  = rb->rb_shct;
    me.uid        = PTL_UID_ANY;
    me.options    = PTL_ME_OP_GET | PTL_ME_USE_ONCE | PTL_ME_EVENT_LINK_DISABLE
                  | PTL_ME_EVENT_OVER_DISABLE | PTL_ME_EVENT_CT_COMM; //  | PTL_ME_EVENT_SUCCESS_DISABLE;
    me.match_id.rank = PTL_RANK_ANY;
    me.match_bits = __GTC_SN_QUEUE_MATCH;
    me.ignore_bits = 0;
    me.min_free = 1; // only unlink ME once all bytes have been consumed

    // loop from split towards head, halving the chunksize every step
    while (amount > 0) {
      ptl_size_t wrapped = 0;

      // release may result in final ME being of smaller than rb->chunk_size
      if (rtype != SNReleaseAllChunk)
        chunksize = (amount > rb->chunk_size) ? (amount / 2) : amount;
      else
        chunksize = csz; // only need for get timing (need steal-N behavior)

      // setup ME for stealing half of remaining tasks, working backwards from split
      if ((splitpt + chunksize) < rb->max_size) {
        // simple case: add next chunk to left: rb[(split_cursor - chunk_size)..split_cursor]
        me.start  = sn_elem_addr(rb, splitpt);
        me.length = chunksize * rb->elem_size;

        task = (task_t *)sn_elem_addr(rb, splitpt);
        task->count = chunksize; // tell the thief how many tasks they stole

        mp.index = splitpt;
        mp.tasks = chunksize;
        mp.unlinked = 0;
        mp.iov = NULL;          // not using iovecs in steal-half
        mp_push_tail(rb, &mp);  // append to matchpair list tail
        mpp = mp_tail(rb);      // points to in-list entry, needed for user_ptr check

        // save pointer to matchpair list entry in user_ptr
        err = PtlMEAppend(_c->lni, rb->ptindex, &me, PTL_PRIORITY_LIST, mpp, &(mpp->me));
        if (err != PTL_OK) {
          gtc_lprintf(DBGSHRB,"sh_release: PtlMEAppend error (%s)\n", gtc_ptl_error(err));
          exit(1);
        }

        splitpt = splitpt + chunksize;

      } else {
        // wraparound case: add two MEs to handle dis-contiguous regions
        wrapped = (splitpt + chunksize) % rb->max_size; // # of wrapped entries

        // first ME to take tasks from split to end
        me.start  = sn_elem_addr(rb, splitpt);
        me.length = (chunksize - wrapped) * rb->elem_size;

        task = (task_t *)sn_elem_addr(rb, splitpt);
        task->count = (chunksize - wrapped); // tell the thief how many tasks they stole

        mp.index = splitpt;
        mp.tasks = (chunksize - wrapped);
        mp.unlinked = 0;
        mp.iov = NULL;          // not using iovecs in steal-half
        mp_push_tail(rb, &mp);  // append to matchpair list tail
        mpp = mp_tail(rb);      // points to in-list entry, needed for user_ptr check

        err = PtlMEAppend(_c->lni, rb->ptindex, &me, PTL_PRIORITY_LIST, mpp, &(mpp->me));
        if (err != PTL_OK) {
          gtc_lprintf(DBGSHRB,"sh_release: PtlMEAppend error (%s)\n", gtc_ptl_error(err));
          exit(1);
        }

        if (wrapped > 0) {
          // second ME covers tasks from front of queue
          me.start  = sn_elem_addr(rb, 0);
          me.length = wrapped * rb->elem_size;

          task = (task_t *)sn_elem_addr(rb, 0);
          task->count = wrapped; // tell the thief how many tasks they stole

          mp.index = 0;           // start of queue
          mp.tasks = wrapped;     // # of wrapped tasks
          mp.unlinked = 0;
          mp.iov = NULL;          // not using iovecs in steal-half
          mp_push_tail(rb, &mp);  // append to matchpair list tail
          mpp = mp_tail(rb);      // points to in-list entry, needed for user_ptr check

          err = PtlMEAppend(_c->lni, rb->ptindex, &me, PTL_PRIORITY_LIST, mpp, &(mpp->me));
          if (err != PTL_OK) {
            gtc_lprintf(DBGSHRB,"sh_release: PtlMEAppend error (%s)\n", gtc_ptl_error(err));
            exit(1);
          }
        }

        splitpt = wrapped;
      }

      //if (chunksize > maxchunk) maxchunk = chunksize; //XXX add actual counters for chunk size and max queue length?

      amount   -= chunksize;
      //maxchunk++;
    }

    rb->split = splitpt;
    assert(amount == 0);

    gtc_lprintf(DBGSHRB, "sh_release: %s moving %d tasks pre: (%d:%d/%d) post: (%d:%d/%d)\n",
            rty, preamount, prelocal, preshared, presize,
            sn_local_size(rb), sn_shared_size(rb), sn_size(rb));
    //sn_print(rb);
    UNUSED(rty);
  }
  if (rtype != SNReacquire)
    TC_STOP_TSCTIMER(release);
}
#else
/**
 * sh_release_impl - release implementation function
 *
 * @param rb  the ring buffer to release
 * @param all 0 to release half, 1 to release all
 */
static void sh_release_impl(sn_ring_t *rb, sn_releasetype_t rtype) {
  ptl_me_t me;
  ptl_size_t len;
  int amount = 0 , diff, splitpt = rb->split, err, chunksize;
  sn_matchpair_t  mp, *mpp;
  //int preamount = -1, prelocal = -1, preshared = -1, presize = -1;
  char *rty, *astr = "all", *hstr = "half", *acstr = "reacq";
  rty = astr;

  if (rtype != SNReacquire)
    TC_START_TSCTIMER(release);

  // Favor placing work in the shared portion -- if there are only N tasks
  // available this scheme will put it in the shared portion.
  if ((rtype == SNReacquire) || ((sn_local_size(rb) > 0) && (sn_shared_size(rb) == 0))) {
    //prelocal  = sn_local_size(rb);
    //preshared = sn_shared_size(rb);
    //presize   = sn_size(rb);

    // release all? or just half?
    switch (rtype) {
      case SNReleaseAll:
        amount  = sn_local_size(rb);
        rb->nlocal -= amount; // update local queue state
        splitpt = rb->split; // track progress for matchpair list
        rty = astr;
        rb->nrelease++;
        break;

      case SNReleaseHalf:
        amount  = sn_local_size(rb)/2 + sn_local_size(rb) % 2;
        rb->nlocal -= amount; // update local queue state
        splitpt = rb->split; // track progress for matchpair list
        rty = hstr;
        rb->nrelease++;
        break;

      case SNReacquire:
        // reacquire -- re-split queue favoring work on the local side
        diff   = sn_shared_size(rb) - sn_local_size(rb);
        amount = sn_size(rb) - (diff/2 + diff%2); // more work stays local
        rb->nlocal += (diff/2 + diff%2);
        splitpt = rb->tail; // acquire needs to work from tail to new split
        rty = acstr;
        break;
    }

    //preamount = amount;

    // setup static parts of MEs
    me.ct_handle  = rb->rb_shct;
    me.uid        = PTL_UID_ANY;
    me.options    = PTL_ME_OP_PUT | PTL_ME_OP_GET | PTL_IOVEC | PTL_ME_USE_ONCE // | PTL_ME_MANAGE_LOCAL
      | PTL_ME_EVENT_LINK_DISABLE | PTL_ME_EVENT_OVER_DISABLE; //  PTL_ME_EVENT_CT_COMM | PTL_ME_COMM_DISABLE | PTL_ME_EVENT_SUCCESS_DISABLE
    me.match_id.rank = PTL_RANK_ANY;
    me.match_bits = __GTC_SN_QUEUE_MATCH;
    me.ignore_bits = 0;
    me.min_free = 1; // only unlink ME once all bytes have been consumed

    // loop from split towards head, halving the chunksize every step
    while (amount > 0) {
      ptl_size_t wrapped = 0;

      // release-all may result in final ME being of smaller than rb->chunk_size
      chunksize = (amount > rb->chunk_size) ? (amount / 2) : amount;

      // setup ME for stealing-N tasks, working backwards from split
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

      //if (chunksize > maxchunk) maxchunk = chunksize;

      // save pointer to matchpair list entry in user_ptr
      err = PtlMEAppend(_c->lni, rb->ptindex, &me, PTL_PRIORITY_LIST, mpp, &(mpp->me));
      if (err != PTL_OK) {
        gtc_lprintf(DBGSHRB,"sh_release: PtlMEAppend error (%s)\n", gtc_ptl_error(err));
        exit(1);
      }

      // update state for next iteration of the loop
      if ((splitpt + chunksize) < rb->max_size)
        splitpt = splitpt + chunksize;
      else
        splitpt = wrapped;
      amount   -= chunksize;
      //maxchunk++;
    }

    rb->split = splitpt;
    assert(amount == 0);

    //gtc_lprintf(DBGSHRB, "sh_release: %s moving %d tasks pre: (%d:%d/%d) post: (%d:%d/%d)\n",
    //        rty, preamount, prelocal, preshared, presize,
    //        sn_local_size(rb), sn_shared_size(rb), sn_size(rb));
    UNUSED(rty);
  }
  if (rtype != SNReacquire)
    TC_STOP_TSCTIMER(release);
}
#endif // SCIOTWO_PTL_USE_IOVEC



/**
 * sn_reacquire - move work from the shared portion of the task queue to the local part
 *
 * @param rb the ring buffer to reacquire
 */
int sh_reacquire(sn_ring_t *rb) {
  int amount = 0;

  TC_START_TSCTIMER(reacquire);
  // we could remove entries from the tail of the matchpairs list,
  // but the remaining MEs wouldn't reflect the steal-half structure

  if (sn_shared_size(rb) > (sn_local_size(rb))) {
    // ensure matchpair list / MEs are in known state
    //TC_START_ATIMER(rb->acqtime);
    sn_reclaim_space(rb);
    sh_clean_matchlist(rb);              // clean existing MEs from priority list
    sh_release_impl(rb, SNReacquire, 0); // call release to re-split the queue
    //TC_STOP_ATIMER(rb->acqtime);
    rb->nreacquire++;
  }

  assert(!sn_local_isempty(rb) || (sn_isempty(rb) && sn_local_isempty(rb)));
  TC_STOP_TSCTIMER(reacquire);

  return amount;
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
int sh_pop_head(void *b, int proc, void *buf) {
  sn_ring_t *rb = (sn_ring_t *)b;
  int   old_head;
  int   buf_valid = 0;

  assert(proc == rb->procid);

  // If we are out of local work, try to reacquire
  if (sn_local_isempty(rb)) {
    sh_reacquire(rb);
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



/**
 * sh_pop_tail - fetch the task at the tail of the queue
 *
 * @param rb   the ring buffer
 * @param proc rank of task queue
 * @param buf  buffer to place the task into
 * @return -1 on failure (empty), 1 on success
 */
int sh_pop_tail(sn_ring_t *rb, int proc, void *buf) {
  return sh_pop_n_tail(rb, proc, 1, buf, STEAL_CHUNK);
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
static inline int sh_pop_n_tail_impl(sn_ring_t *myrb, int proc, int n, void *e, int steal_vol) {
  ptl_process_t p;
  ptl_event_t fault;
  ptl_ct_event_t current, ctevent;
  int ret, nstolen = 0;
  task_t *task;
#ifdef SCIOTO_CLEANUP_DISABLE
  ptl_ct_event_t orig, reset;
  int busy = 0, again = 0;
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_sec = _c->rank * 100; // all ranks have different hold timers
#endif

  TC_START_TSCTIMER(poptail);

  p.rank = proc;
  myrb->ngets++;
  __gtc_marker[2] = proc;

  n = MIN(myrb->max_steal, n);

  //gtc_lprintf(DBGSHRB, "pop_n: requesting %d (%d bytes) tasks from %d\n", n, n*myrb->elem_size, proc);

#ifndef SCIOTO_CLEANUP_DISABLE

  PtlCTGet(myrb->rb_ct, &current);

  // fetch N tasks from victim
  ret = PtlGet(myrb->rb_md, (ptl_size_t)e, n*myrb->elem_size, p, myrb->ptindex, __GTC_SN_QUEUE_MATCH, 0, NULL);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // check for error
  if (ctevent.failure > current.failure) {
    ret = PtlEQWait(myrb->rb_eq, &fault);
    if (ret == PTL_OK) {
      gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
      gtc_dump_event(&fault);
    } else {
      gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
    }
    goto error;
  }

  if ((*(uint64_t *)e) == 0xdeadb33f) {
    // found end-of-queue
    myrb->neoq++;
  } else {
    // stole some tasks
    task = (task_t *)e;    // need to recover how many tasks in fetch
    nstolen = task->count; // renaming this means fixing all queue tests...
    myrb->nsteals++;
  }

#else // SCIOTO_CLEANUP_DISABLE

  // we need to account for possibility of flow-control events

  PtlCTGet(myrb->rb_ct, &orig); // save initial counter values, for flow control progress
  current = orig;

  // may re-attempt steal if we run into flow control situation
  do {
    busy = 0;

    // fetch N tasks from victim
    ret = PtlGet(myrb->rb_md, (ptl_size_t)e, n*myrb->elem_size, p, myrb->ptindex, __GTC_SN_QUEUE_MATCH, 0, NULL);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: PtlGet error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    ret = PtlCTWait(myrb->rb_ct, current.success+1, &ctevent);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: PtlCTWait error (%s)\n", gtc_ptl_error(ret));
      exit(1);
    }

    // check for error
    if (ctevent.failure > current.failure) {
      ret = PtlEQWait(myrb->rb_eq, &fault);
      if (ret == PTL_OK) {
        if (fault.ni_fail_type == PTL_NI_PT_DISABLED) {
          if (!again)
            gtc_dprintf("found flow control on %d\n", proc);

          nanosleep(&ts, NULL);
          reset.success = 0;
          reset.failure = -1;
          PtlCTInc(myrb->rb_ct, reset);

          PtlCTGet(myrb->rb_ct, &current);
          again = 1;
          busy = 1;
          myrb->ndisabled++;

        } else {
          gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: found fail event: %s\n", gtc_event_to_string(fault.type));
          gtc_dump_event(&fault);
          goto error;
        }
      } else {
        gtc_lprintf(DBGSHRB,"sh_pop_n_tail_impl: PtlEQWait error (%s)\n", gtc_ptl_error(ret));
        goto error;
      }
    } else  if (again && (ctevent.success == orig.success)) {
      PtlCTGet(myrb->rb_ct, &current);
      nanosleep(&ts, NULL);
      busy = 1;
      
    } else if ((*(uint64_t *)e) == 0xdeadb33f) {
      // found end-of-queue
      myrb->neoq++;

    } else {
      // stole some tasks
      task = (task_t *)e;    // need to recover how many tasks in fetch
      nstolen = task->count; // renaming this means fixing all queue tests...
      myrb->nsteals++;
    }
  } while (busy);
#endif

  if (nstolen > 0)
    gtc_lprintf(DBGSHRB, "sh_pop_n_tail_impl: stole %d tasks from creator %d owner: %d\n", nstolen, task->created_by, proc);

error:
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
int sh_pop_n_tail(void *b, int proc, int n, void *e, int steal_vol) {
  sn_ring_t *myrb = (sn_ring_t *)b;
  return sh_pop_n_tail_impl(myrb, proc, n, e, steal_vol);
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
int sh_try_pop_n_tail(void *b, int proc, int n, void *e, int steal_vol) {
  sn_ring_t *myrb = (sn_ring_t *)b;
  return sh_pop_n_tail_impl(myrb, proc, n, e, steal_vol);
}
