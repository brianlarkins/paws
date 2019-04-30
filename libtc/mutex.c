/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>

#include "tc.h"
#include "mutex.h"

#define SYNCH_MUTEX_LOCKED   1
#define SYNCH_MUTEX_UNLOCKED 0
#define SYNCH_MUTEX_INVALID -1

#define LINEAR_BACKOFF

// Stats to track performance of locking
double        synch_mutex_lock_nattempts_squares = 0; // Use to calculate variance as the of the squares
                                                      // minus the square of the mean
unsigned long synch_mutex_lock_nattempts_last    = 0;
unsigned long synch_mutex_lock_nattempts_sum     = 0;
unsigned long synch_mutex_lock_nattempts_max     = 0;
unsigned long synch_mutex_lock_nattempts_min     = 0;
unsigned long synch_mutex_lock_ncalls_contention = 0;
unsigned long synch_mutex_lock_ncalls            = 0;

ptl_handle_ni_t synch_mutex_ni    = PTL_INVALID_HANDLE;
ptl_handle_eq_t synch_mutex_eq    = PTL_INVALID_HANDLE;
uint64_t        synch_mutex_mbits = 0x00000042;

/** Initialize structures for mutex usage
 */
void synch_init(ptl_handle_ni_t ni) {
  int ret;
  ptl_pt_index_t requested, assigned;

  synch_mutex_ni = ni;
  requested = __GTC_SYNCH_INDEX;

  if ((ret = PtlEQAlloc(ni, 100, &synch_mutex_eq)) != PTL_OK) {
    gtc_lprintf(DBGSYNCH,"synch_init: EQAlloc error\n");
  }

  // allocate a newv PT entry for atomic ops
  //if ((ret = PtlPTAlloc(ni, PTL_PT_MATCH_UNORDERED, synch_mutex_eq, requested, &assigned)) != PTL_OK) {
  if ((ret = PtlPTAlloc(ni, 0, synch_mutex_eq, requested, &assigned)) != PTL_OK) {
    gtc_lprintf(DBGSYNCH,"synch_init: PTAlloc error\n");
  }

  if (requested != assigned) {
    gtc_lprintf(DBGSYNCH, "synch_init: PTAlloc failed to allocate requested PT index\n");
  }
}


/** Cleanup Portals lock structures
 */
void synch_fini(void) {
  if (synch_mutex_ni != PTL_INVALID_HANDLE) {
    PtlPTFree(synch_mutex_ni, __GTC_SYNCH_INDEX);
    PtlEQFree(synch_mutex_eq);
  }
}



/** Initialize a local mutex.
 *
 *  @param[out] m        Pointer to the mutex in local memory.
 */
void synch_mutex_init(synch_mutex_t *m) {
  ptl_md_t md;
  ptl_me_t me;
  int ret;

  memset(m, 0, sizeof(synch_mutex_t));
  m->mbits   = synch_mutex_mbits++;
  m->ptindex = __GTC_SYNCH_INDEX;
  m->opcount = 0;

  // create CT for waiting on lock/unlock completion
  ret = PtlCTAlloc(synch_mutex_ni, &m->ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_init: CTAlloc error\n");
  }

  // allocate aligned memory for remote access
  ret = posix_memalign((void **)&m->lockval, sizeof(int64_t), sizeof(int64_t));
  if (ret != 0) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_init: global alignment error\n");
  }
  *(int64_t *)m->lockval = SYNCH_MUTEX_UNLOCKED;

  // append ME to accept remote lock/unlock requests
  me.start      = m->lockval;
  me.length     = sizeof(int64_t);
  me.ct_handle  = PTL_CT_NONE;
  me.uid        = PTL_UID_ANY;
  me.options    = PTL_ME_OP_PUT   | PTL_ME_OP_GET | PTL_ME_EVENT_CT_COMM
    | PTL_ME_EVENT_LINK_DISABLE   | PTL_ME_EVENT_COMM_DISABLE
    | PTL_ME_EVENT_UNLINK_DISABLE | PTL_ME_EVENT_SUCCESS_DISABLE;
  me.match_id.rank = PTL_RANK_ANY;
  me.match_bits    = m->mbits;
  me.ignore_bits    = 0;

  ret = PtlMEAppend(synch_mutex_ni, m->ptindex, &me, PTL_PRIORITY_LIST, NULL, &m->me);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_init: PtlMEAppend error\n");
  }

  //gtc_lprintf(DBGSYNCH, "synch_mutex_init: added match entry %12"PRIx64" to PTE: %d\n", m->mbits, m->ptindex);

  // allocate aligned memory for local values to be swapped with lock/unlock op
  ret = posix_memalign((void **)&m->scratch, sizeof(int64_t), sizeof(synch_scratch_t));
  if (ret != 0) {
    gtc_lprintf(DBGSYNCH,"synch_mutex_init: local alignment error\n");
  }
  memset(m->scratch, 0, sizeof(synch_scratch_t));

  // create MD for putting local values for atomic ops
  md.start     = m->scratch;
  md.length    = sizeof(synch_scratch_t);
  md.options   = PTL_MD_EVENT_CT_REPLY | PTL_MD_EVENT_CT_ACK;
  md.eq_handle = PTL_EQ_NONE;
  md.ct_handle = m->ct;

  ret = PtlMDBind(synch_mutex_ni, &md, &m->md);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_init: PtlMDBind error\n");
  }
}



/** Lock the given mutex on the given processor.  Blocks until mutex is acquired.
 *
 *  @param[in] m    Address of the mutex.  If using ARMCI mutexes, this is a
 *                  pointer to an integer that contains the mutex number.
 *  @param[in] proc Processor id where this mutex is located.
 */
#ifdef LINEAR_BACKOFF
#define SPINCOUNT 1000
#define MAXSPIN   100000
double synch_mutex_dummy_work = 0.0;
#endif /* LINEAR_BACKOFF */

void synch_mutex_lock(synch_mutex_t *m, int proc) {
  int i, nattempts = 0, ret;
  synch_scratch_t *as;
  ptl_ct_event_t ctevent;
  ptl_process_t rank;
  ptl_size_t oldvoff, newvoff;

  gtc_lprintf(DBGSYNCH, "synch_mutex_lock  (%p, %d)\n", m, proc);


  as = (synch_scratch_t *)m->scratch;
  as->oldv = SYNCH_MUTEX_INVALID; // will contain oldv value on remote side
  as->newv = SYNCH_MUTEX_LOCKED;  // contains value we want to swap
  oldvoff = offsetof(synch_scratch_t, oldv);
  newvoff = offsetof(synch_scratch_t, newv);
  rank.rank = proc;


  do {
    as->newv = SYNCH_MUTEX_LOCKED;

    //PtlCTGet(m->ct, &ctsuccess);

    // perform atomic swap
    ret = PtlSwap(m->md, oldvoff, m->md, newvoff, sizeof(int64_t),
        rank, m->ptindex, m->mbits, 0, NULL, 0, NULL, PTL_SWAP, PTL_INT64_T);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlSwap failure (%s)\n", gtc_ptl_error(ret));
    }

    // wait for completion
    ret = PtlCTWait(m->ct, m->opcount + 1, &ctevent);
    if (ret != PTL_OK) {
      gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlCTWait failure\n");
    }

    if (ctevent.failure > 0) {
      ptl_event_t ev;
      gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlSwap failure event received\n");
      //if ((ret = PtlEQWait(synch_mutex_eq, &ev)) != PTL_OK)
      if ((ret = PtlEQGet(synch_mutex_eq, &ev)) != PTL_OK) {
        gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlEQGet failure %s\n", gtc_ptl_error(ret));
        ptl_ct_event_t ct2;
        ct2.success = 0;
        ct2.failure = -1;
        PtlCTInc(m->ct, ct2);
      } else {
        gtc_dump_event(&ev);
      }
    }

    m->opcount++;

    // please recall that this is in the inner loop of a spin lock before uncommenting.
    //gtc_lprintf(DBGSYNCH, "synch_mutex_lock - attempt: %d oldv: %d newv: %d\n", nattempts, as->oldv, as->newv);


#ifdef LINEAR_BACKOFF
    // Linear backoff to avoid flooding the network and bogging down the
    // remote data server.
    // TODO: Add a different backoff strategy for the local proces?
    uint64_t backoff = MIN(SPINCOUNT*nattempts, MAXSPIN);
    for (i = 0; i < backoff; i++) synch_mutex_dummy_work += 1.0;
#endif /* LINEAR_BACKOFF */
    //gtc_lprintf(DBGSYNCH, "synch_mutex_lock - linear backoff complete\n");
    nattempts++;

  } while (as->oldv != SYNCH_MUTEX_UNLOCKED);

  gtc_lprintf(DBGSYNCH, "synch_mutex_lock  acquired lock : (%p, %d)\n", m, proc);

  synch_mutex_lock_nattempts_sum     += nattempts;
  synch_mutex_lock_nattempts_squares += nattempts*nattempts;
  if (synch_mutex_lock_nattempts_min == 0)
    synch_mutex_lock_nattempts_min    = nattempts;
  else
    synch_mutex_lock_nattempts_min    = MIN(synch_mutex_lock_nattempts_min, nattempts);
  synch_mutex_lock_nattempts_max      = MAX(synch_mutex_lock_nattempts_max, nattempts);
  synch_mutex_lock_nattempts_last     = nattempts;

  if (nattempts > 1)
    synch_mutex_lock_ncalls_contention++;

  assert(as->oldv == SYNCH_MUTEX_UNLOCKED);

  synch_mutex_lock_ncalls++;
}



/** Attempt to lock the given mutex on the given processor.
 *
 *  @param[in] m    Address of the mutex.  If using ARMCI mutexes, this is a
 *                  pointer to an integer that contains the mutex number.
 *  @param[in] proc Processor id where this mutex is located.
 *  @return         0 on failure, 1 on success
 */
int synch_mutex_trylock(synch_mutex_t *m, int proc) {
  synch_scratch_t *as;
  ptl_ct_event_t ctevent;
  ptl_process_t rank;
  ptl_size_t oldvoff, newvoff;
  int ret;

  as = (synch_scratch_t *)m->scratch;
  as->oldv = SYNCH_MUTEX_INVALID; // will contain oldv value on remote side
  as->newv = SYNCH_MUTEX_LOCKED;  // contains value we want to swap
  oldvoff = offsetof(synch_scratch_t, oldv);
  newvoff = offsetof(synch_scratch_t, newv);
  rank.rank = proc;

  //PtlCTGet(m->ct, &ctevent);

  as->newv = SYNCH_MUTEX_LOCKED;

  // perform atomic swap
  ret = PtlSwap(m->md, oldvoff, m->md, newvoff, sizeof(int64_t),
      rank, m->ptindex, m->mbits, 0, NULL, 0, NULL, PTL_SWAP, PTL_INT64_T);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlSwap failure\n");
  }

  // wait for completion
  ret = PtlCTWait(m->ct, m->opcount+1, &ctevent);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlCTWait failure\n");
  }
  m->opcount++;

  return (as->oldv == SYNCH_MUTEX_UNLOCKED);
}


/** Unlock the given mutex on the given processor.
 *
 * @param[in] m    Address of the mutex.  If using ARMCI mutexes, this is a
 *                 pointer to an integer that contains the mutex number.
 * @param[in] proc Processor id where this mutex is located.
 */
void synch_mutex_unlock(synch_mutex_t *m, int proc) {
  synch_scratch_t *as;
  ptl_ct_event_t ctevent, ctsuccess;
  ptl_process_t rank;
  ptl_size_t oldvoff, newvoff;
  int ret;


  as = (synch_scratch_t *)m->scratch;
  as->oldv = SYNCH_MUTEX_INVALID; // will contain oldv value on remote side
  as->newv = SYNCH_MUTEX_UNLOCKED;  // contains value we want to swap
  oldvoff = offsetof(synch_scratch_t, oldv);
  newvoff = offsetof(synch_scratch_t, newv);
  rank.rank = proc;

  gtc_lprintf(DBGSYNCH, "synch_mutex_unlock(%p, %d) oldv: %"PRId64" newv: %"PRId64"\n", m, proc, as->oldv, as->newv);

  //PtlCTGet(m->ct, &ctsuccess);

  // perform atomic swap
  ret = PtlSwap(m->md, oldvoff, m->md, newvoff, sizeof(int64_t),
      rank, m->ptindex, m->mbits, 0, NULL, 0, NULL, PTL_SWAP, PTL_INT64_T);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_unlock - PtlSwap failure\n");
  }

  // wait for completion
  ret = PtlCTWait(m->ct, m->opcount+1, &ctevent);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGSYNCH, "synch_mutex_unlock - PtlCTWait failure\n");
  }

  if (ctevent.failure > 0) {
    ptl_event_t ev;
    gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlSwap failure event received\n");
    //if ((ret = PtlEQWait(synch_mutex_eq, &ev)) != PTL_OK)
    if ((ret = PtlEQGet(synch_mutex_eq, &ev)) != PTL_OK) {
      gtc_lprintf(DBGSYNCH, "synch_mutex_lock - PtlEQGet failure %s\n", gtc_ptl_error(ret));
      ptl_ct_event_t ct2;
      ct2.success = 0;
      ct2.failure = -1;
      PtlCTInc(m->ct, ct2);
      PtlCTGet(m->ct, &ctsuccess);
      gtc_lprintf(DBGSYNCH, "resetting: s: %d f: %d\n", ctsuccess.success, ctsuccess.failure);
    } else {
      gtc_dump_event(&ev);
    }
  }

  m->opcount++;

  gtc_lprintf(DBGSYNCH, "synch_mutex_unlock(%p, %d) oldv: %"PRId64" newv: %"PRId64"\n", m, proc, as->oldv, as->newv);

  assert(as->oldv == SYNCH_MUTEX_LOCKED);
}
