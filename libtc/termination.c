/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>

#include "tc.h"

#ifdef TD_TRIGGERED
static void token_reset(td_token_t *t);
static int  vote_received(td_t *td);
static void set_triggers(td_t *td, int init);
static void start_reduction(td_t *td);
static void cast_vote(td_t *td, ptl_process_t rank);


/*
 * private helper functions
 */

/*
 * token_reset - helper to zero out a token
 * @param t the token to reset
 */
static void token_reset(td_token_t *t) {
  t->state     = ACTIVE;
  t->spawned   = 0;
  t->completed = 0;
}



/*
 * vote_received - check to see if we have a valid termination result vote
 */
static int vote_received(td_t *td) {
  ptl_ct_event_t now_uscount, now_ufcount, now_dtcount;
  int nspawn, nfinal, ndown;
  int ret = 0;

  // if we are a leaf and we have never attempted a vote yet,
  // start a reduction to get things started
  if ((td->nchildren == 0) && (td->have_voted == -1)) {
    td->have_voted = 0;
    return 1;
  }

  PtlCTGet(td->uspawn_ct, &now_uscount);
  PtlCTGet(td->ufinal_ct, &now_ufcount);
  PtlCTGet(td->down_ct, &now_dtcount);
  nspawn = now_uscount.success;
  nfinal = now_ufcount.success;
  ndown  = now_dtcount.success;

  // if we're the root, then check to see if we have results from children
  if (_c->rank == 0) {
    if ((nspawn >= td->uslastset)  && (nfinal >= td->uflastset))
      ret = 1;

    return ret;
  }

  //  gtc_lprintf(DBGTD, "td: vote_received: spawn: n: %d trig: %d :: completed: n: %d trig: %d\n", nspawn, td->uslastset, nfinal, td->uflastset);
  // otherwise, interior or leaf nodes need to check from parent
  //gtc_lprintf(DBGTD, "td: vote_received: down: now: %d trig: %d\n", ndown, td->vrecvd);

  if (ndown > td->vrecvd) {
    ret = 1;
    gtc_lprintf(DBGTD, "RECEIVED BROADCAST FROM PARENT. (down: %d vrecvd: %d) uspawn: %d ufinal: %d : recvd s: %d c: %d t: %d\n",
        ndown, td->vrecvd, nspawn, nfinal,
        td->down_token->spawned, td->down_token->completed, td->down_token->state);
    td->have_voted = 0;
  }
  td->vrecvd += ret;

  return ret;
}



/**
 * start_reduction() - called by leaves to initiate another termination vote
 * @param td termination detection state
 */
static void start_reduction(td_t *td) {
  ptl_ct_event_t now_dtcount;
  PtlCTGet(td->down_ct, &now_dtcount);
  gtc_lprintf(DBGTD, "td: starting reduction: down: %d last: %d\n", now_dtcount.success, td->vrecvd);

  // update our state
  td->have_voted = 1;

  cast_vote(td, td->p);
}



/**
 * start_broadcast() - called by root to initiate triggered broadcast
 * @param td termination detection state
 */
static void start_broadcast(td_t *td) {
  int spawned   = td->up_token->spawned   + td->token.spawned;
  int completed = td->up_token->completed + td->token.completed;

  if (((spawned == td->last_spawned) && (completed == td->last_completed)) &&
      (spawned == completed))
    td->token.state = TERMINATED;

  // reduction is complete, reset the root token values for the next upward pass
  token_reset(td->up_token);
  token_reset(td->down_token);

  td->down_token->state     = td->token.state; // our state reflects all processes
  td->down_token->spawned   = spawned;
  td->down_token->completed = completed;

  // set target counter values (no triggered ops on root) to detect when reduction is complete
  td->uslastset += td->nchildren;
  td->uflastset += td->nchildren;

  gtc_lprintf(DBGTD, "td: starting broadcast: down_token: [ %s %d %d ] now: s: %d c: %d last: s: %d c: %d\n",
      td->down_token->state == ACTIVE ? "a" : "t", td->down_token->spawned, td->down_token->completed,
      spawned, completed, td->last_spawned, td->last_completed);
  td->last_spawned   = spawned;
  td->last_completed = completed;


  if (td->nchildren > 0) {
    PtlPut(td->down_md, 0, sizeof(td_token_t), PTL_NO_ACK_REQ,
        td->l, td->ptindex, __GTC_TERMINATION_DOWN_MATCH, 0, NULL, 0);

    if (td->nchildren == 2) {
      PtlPut(td->down_md, 0, sizeof(td_token_t), PTL_NO_ACK_REQ,
          td->r, td->ptindex, __GTC_TERMINATION_DOWN_MATCH, 0, NULL, 0);
    }
  }

  // update counts from previous attempt to terminate
  td->num_cycles++;
  td->have_voted      = 0;
}



/**
 * cast_vote - atomically updates the spawn/completed counters
 * @param td   termindation detection state
 * @param rank rank of the process to vote on
 */
static void cast_vote(td_t *td, ptl_process_t rank) {
  unsigned loffset = 0;
  int ret;

  set_triggers(td, 0); // set/reset triggers if needed

  gtc_lprintf(DBGTD, "CASTING VOTE TO %d\n", rank.rank);
  loffset = offsetof(td_token_t, spawned);
  ret = PtlAtomic(td->token_md, loffset, sizeof(int64_t),
      PTL_NO_ACK_REQ, rank, td->ptindex, __GTC_TERMINATION_USPAWN_MATCH,
      0, NULL, 0, PTL_SUM, PTL_INT64_T);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "cast_vote: leaf PtlAtomic failed (spawn)\n");
  }

  loffset = offsetof(td_token_t, completed);
  ret = PtlAtomic(td->token_md, loffset, sizeof(int64_t),
      PTL_NO_ACK_REQ, rank, td->ptindex, __GTC_TERMINATION_UFINAL_MATCH,
      0, NULL, 0, PTL_SUM, PTL_INT64_T);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "cast_vote: leaf PtlAtomic failed (completed)\n");
  }
}



/*
 * set_triggers - replenish triggered atomic operations, only if necessary
 */
static void set_triggers(td_t *td, int init) {
  unsigned loffset = 0;
  ptl_ct_event_t now_uscountev, now_ufcountev, now_dtcountev;
  ptl_process_t self = { .rank = _c->rank };
  static int suppress[3] = { 0, 0, 0 };
  int now_uscount, now_ufcount, now_dtcount;
  int ret;

  // if interior node - set up atomic triggers and down triggered puts to children
  // if leaf node - no triggers, always send to parent and check counters for broadcast
  // if root node - no triggers, always send to children

  // find out the current situation
  PtlCTGet(td->uspawn_ct, &now_uscountev);
  PtlCTGet(td->ufinal_ct, &now_ufcountev);
  PtlCTGet(td->down_ct, &now_dtcountev);

#ifndef NO_SEATBELTS
  if (!suppress[0] && (now_uscountev.failure > 0)) {
    gtc_lprintf(DBGTD, "td: set_triggers: trigger failed with upward task spawn trigger\n");
    suppress[0]++;
  }
  if (!suppress[1] && (now_ufcountev.failure > 0)) {
    gtc_lprintf(DBGTD, "td: set_triggers: trigger failed with upward task completion trigger\n");
    suppress[1]++;
  }
  if (!suppress[2] && (now_dtcountev.failure > 0)) {
    gtc_lprintf(DBGTD, "td: set_triggers: trigger failed with downward triggered put\n");
    suppress[2]++;
  }
#endif

  now_uscount = now_uscountev.success;
  now_ufcount = now_ufcountev.success;
  now_dtcount = now_dtcountev.success;

  if ((_c->rank != 0) && (td->nchildren > 0)) {

    // interior node only
    // gtc_lprintf(DBGTD, "td: set_triggers: now: us: %d uf: %d dt: %d > last: %d\n", now_uscount, now_ufcount, now_dtcount, td->dlastset);

    // check and setup upward pass triggers after previous reduction
    if ((init) || (now_uscount > td->uslastset)) {
      // trigger to send spawned count

      if (!init)
        td->uslastset += td->upcycle_thresh; // subsequent triggers should account for zero atomic events too
      else
        td->uslastset += 1 + td->nchildren; // initial threshold at atomic for self + each child

      gtc_lprintf(DBGTD, "td: set_triggers: setting uspawn @ %d triggering @ %d  init:%d\n", now_uscount, td->uslastset,init);

      loffset = offsetof(td_token_t, spawned);
      ret = PtlTriggeredAtomic(td->up_md, loffset, sizeof(int64_t),
          PTL_NO_ACK_REQ, // okay?
          td->p,          // parent
          td->ptindex, __GTC_TERMINATION_USPAWN_MATCH, 0,
          NULL, 0, PTL_SUM, PTL_INT64_T, td->uspawn_ct, td->uslastset);
      if (ret != PTL_OK)
        gtc_lprintf(DBGTD, "td: set_triggers: uspawn trigger reset failed\n");
    }

    if ((init) || (now_ufcount > td->uflastset)) {
      // trigger to send completed counter (tasks spawned)

      if (!init)
        td->uflastset += td->upcycle_thresh; // subsequent triggers should account for zero atomic events too
      else
        td->uflastset += 1 + td->nchildren; // initial threshold at atomic for self + each child

      gtc_lprintf(DBGTD, "td: set_triggers: setting ufinal @ %d triggering @ %d\n", now_ufcount, td->uflastset);

      loffset = offsetof(td_token_t, completed);
      ret = PtlTriggeredAtomic(td->up_md, loffset, sizeof(int64_t),
          PTL_NO_ACK_REQ, // okay?
          td->p,          // parent
          td->ptindex, __GTC_TERMINATION_UFINAL_MATCH, 0,
          NULL, 0, PTL_SUM, PTL_INT64_T, td->ufinal_ct, td->uflastset);
      if (ret != PTL_OK)
        gtc_lprintf(DBGTD, "td: set_triggers: ufinal trigger reset failed\n");
    }


    // check and setup downward pass and token reset triggers
    if ((init) || (now_dtcount >= td->dlastset)) {
      // set triggers to activate on the downward broadcast cycle
      // these put 0 values into the local counters we use for atomic accumulations

      td->dlastset += 1; // we only get one downward message from parent

      gtc_lprintf(DBGTD, "td: set_triggers: setting atomic zero triggers @ %d triggering @ %d init:%d\n", now_dtcount, td->dlastset, init);

      ret = PtlTriggeredPut(td->zero_md, 0, sizeof(int64_t), PTL_NO_ACK_REQ,
          self, td->ptindex, __GTC_TERMINATION_USPAWN_MATCH, 0, NULL, 0, td->down_ct, td->dlastset);
      if (ret != PTL_OK)
        gtc_lprintf(DBGTD, "td: set_triggers: uspawn zero trigger failed\n");

      ret = PtlTriggeredPut(td->zero_md, 0, sizeof(int64_t), PTL_NO_ACK_REQ,
          self, td->ptindex, __GTC_TERMINATION_UFINAL_MATCH, 0, NULL, 0, td->down_ct, td->dlastset);
      if (ret != PTL_OK)
        gtc_lprintf(DBGTD, "td: set_triggers: ufinal zero trigger failed\n");
    }


    if ((init) || (now_ufcount >= td->zlastset)) {
      // setup triggers to send termination token broadcast,
      //   these fire only when reset triggers above have fired (after reduction)

      td->zlastset += td->upcycle_thresh; // only zero after a full up cycle has completed
      gtc_lprintf(DBGTD, "td: set_triggers: setting broadcast triggers @ %d\n", td->zlastset);

      ret = PtlTriggeredPut(td->down_md, 0, sizeof(td_token_t), PTL_NO_ACK_REQ,
          td->l, td->ptindex, __GTC_TERMINATION_DOWN_MATCH, 0,
          NULL, 0, td->ufinal_ct, td->zlastset);
      if (ret != PTL_OK)
        gtc_lprintf(DBGTD, "td: set_triggers: left broadcast trigger failed\n");

      if (td->nchildren == 2) {
        ret = PtlTriggeredPut(td->down_md, 0, sizeof(td_token_t), PTL_NO_ACK_REQ,
            td->r, td->ptindex, __GTC_TERMINATION_DOWN_MATCH, 0,
            NULL, 0, td->ufinal_ct, td->zlastset);
        if (ret != PTL_OK)
          gtc_lprintf(DBGTD, "td: set_triggers: right broadcast trigger failed\n");
      }
    }
  }
}

/*
 * PUBLIC FUNCTIONS
 */


/** Update the values of the two counters <tasks created, tasks executed>.
 *
 * @param[in] td     Termination detection context.
 * @param[in] count1 New value for counter 1.
 * @param[in] count2 New value for counter 2.
 */
void td_set_counters(td_t *td, int count1, int count2) {
    td->token.spawned = count1;
    td->token.completed = count2;
}


/** Create a termination detection context.
  *
  * @return         Termination detection context.
  */
td_t *td_create(void) {
  ptl_md_t md;
  ptl_me_t me;
  td_t *td = calloc(1,sizeof(td_t));
  int ret;

  assert(td);

  // about us
  td->p.rank = ((_c->rank + 1) >> 1) - 1;
  td->l.rank = ((_c->rank + 1) << 1) - 1;
  td->r.rank = td->l.rank + 1;

  td->nchildren = 0;
  if (td->l.rank < _c->size)
    td->nchildren++;
  if (td->r.rank < _c->size)
    td->nchildren++;

  /*
   * setup info for sending TD votes to others
   */

  // allocate an MD for outgoing messages
  md.start   = &td->token;
  md.length  = sizeof(td_token_t);
  md.options = PTL_MD_EVENT_SEND_DISABLE | PTL_MD_EVENT_SUCCESS_DISABLE;
  md.eq_handle = PTL_EQ_NONE;
  md.ct_handle = PTL_CT_NONE;

  ret = PtlMDBind(_c->lni, &md, &td->token_md);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: MDBind error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // allocate an MD for triggered counter reset
  td->zero = 0;
  md.start   = &td->zero;
  md.length = sizeof(int64_t);
  ret = PtlMDBind(_c->lni, &md, &td->zero_md);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: MDBind error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  /*
   * setup info for receiving TD votes from others
   */

  // allocate a new PTE for termination detection
  //ret = PtlPTAlloc(_c->lni, PTL_PT_MATCH_UNORDERED, PTL_EQ_NONE, __GTC_TERMINATION_INDEX, &td->ptindex);
  ret = PtlPTAlloc(_c->lni, PTL_PT_MATCH_UNORDERED, PTL_EQ_NONE, PTL_PT_ANY, &td->ptindex);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: PTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // allocate counters for upward and downward pass token landing zones
  ret = PtlCTAlloc(_c->lni, &td->uspawn_ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: CTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  ret = PtlCTAlloc(_c->lni, &td->ufinal_ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: CTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  ret = PtlCTAlloc(_c->lni, &td->down_ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: CTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }
  //gtc_lprintf(DBGTD, "td_create: CTAlloc - uspawn: %p\n", &td->uspawn_ct);
  //gtc_lprintf(DBGTD, "td_create: CTAlloc - ufinal: %p\n", &td->ufinal_ct);
  //gtc_lprintf(DBGTD, "td_create: CTAlloc - down: %p\n", &td->down_ct);

  // allocate aligned memory for upward pass atomic ops

  // upward token
  ret = posix_memalign((void **)&td->up_token, sizeof(int64_t), sizeof(td_token_t));
  if (ret != 0) {
    gtc_lprintf(DBGTD, "td_create: unable to get aligned memory for atomic ops (up) -%s\n", strerror(errno));
    exit(1);
  }
  // allocate an MD for upward triggered sends
  md.start   = td->up_token;
  md.length  = sizeof(td_token_t);
  ret = PtlMDBind(_c->lni, &md, &td->up_md);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: MDBind error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // downward token (alignment not strictly necessary, for PtlPut)
  ret = posix_memalign((void **)&td->down_token, sizeof(int64_t), sizeof(td_token_t));
  if (ret != 0) {
    gtc_lprintf(DBGTD, "td_create: unable to get aligned memory for down token -%s\n", strerror(errno));
    exit(1);
  }
  // allocate an MD for downward triggered sends
  md.start   = td->down_token;
  md.length  = sizeof(td_token_t);
  ret = PtlMDBind(_c->lni, &md, &td->down_md);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: MDBind error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // append matchlist entries for each field in the upward/downward pass tokens
  // up pass # tasks spawned
  me.start      = &td->up_token->spawned;
  me.length     = sizeof(td->up_token->spawned);
  me.ct_handle  = td->uspawn_ct;
  me.uid        = PTL_UID_ANY;
  me.options    = PTL_ME_OP_PUT | PTL_ME_OP_GET  | PTL_ME_EVENT_CT_COMM
    | PTL_ME_EVENT_LINK_DISABLE | PTL_ME_EVENT_COMM_DISABLE
    | PTL_ME_EVENT_UNLINK_DISABLE | PTL_ME_EVENT_SUCCESS_DISABLE;
  me.match_id.rank = PTL_RANK_ANY;
  me.match_bits    = __GTC_TERMINATION_USPAWN_MATCH;
  me.ignore_bits   = 0;
  ret = PtlMEAppend(_c->lni, td->ptindex, &me, PTL_PRIORITY_LIST, NULL, &td->uspawn_me);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: PtlMEAppend error - spawn (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // up pass # tasks completed
  me.start      = &td->up_token->completed;
  me.length     = sizeof(td->up_token->completed);
  me.ct_handle  = td->ufinal_ct;
  me.match_bits    = __GTC_TERMINATION_UFINAL_MATCH;
  ret = PtlMEAppend(_c->lni, td->ptindex, &me, PTL_PRIORITY_LIST, NULL, &td->ufinal_me);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: PtlMEAppend error - completed (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // down pass token ME
  me.start      = td->down_token;
  me.length     = sizeof(td_token_t);
  me.ct_handle  = td->down_ct;
  me.match_bits    = __GTC_TERMINATION_DOWN_MATCH;
  ret = PtlMEAppend(_c->lni, td->ptindex, &me, PTL_PRIORITY_LIST, NULL, &td->down_me);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: PtlMEAppend error - down (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  td_reset(td);

  gtc_lprintf(DBGTD, "TD created (%d of %d): parent=%d, left_child=%d, right_child=%d\n",
      _c->rank, _c->size,
      (_c->rank != 0)         ? td->p.rank : -1,
      (td->l.rank < _c->size) ? td->l.rank : -1,
      (td->r.rank < _c->size) ? td->r.rank : -1);
  return td;
}


/** Reset a termination detection context so that it can be re-used.
  *
  * @param[in] td Termination detection context.
  */
void  td_reset(td_t *td) {
  ptl_ct_event_t zero = { 0, 0 };
  gtc_barrier();

  // clear all stale triggers
  if (td->uspawn_ct != PTL_INVALID_HANDLE)  PtlCTCancelTriggered(td->uspawn_ct);
  if (td->ufinal_ct != PTL_INVALID_HANDLE)  PtlCTCancelTriggered(td->ufinal_ct);
  if (td->down_ct   != PTL_INVALID_HANDLE)  PtlCTCancelTriggered(td->down_ct);

  // set initial counter values
  if (_c->rank == 0)  {
    td->uslastset = td->uflastset = td->nchildren; // initially root is waiting for reduction to arrive from children
    td->zlastset  = td->dlastset = 0;
  } else {
    td->uslastset = td->uflastset = td->zlastset = td->dlastset = 0;
  }
  td->upcycle_thresh = 2 + td->nchildren;  // self+children reduction + atomic for zeroing - counter after one full up cycle

  td->vrecvd = 0; // haven't heard anything from our parent

  PtlCTSet(td->uspawn_ct, zero);
  PtlCTSet(td->ufinal_ct, zero);
  PtlCTSet(td->down_ct, zero);

  token_reset(&td->token);
  token_reset(td->up_token);
  token_reset(td->down_token);

  td->num_cycles = 0;
  if (td->nchildren == 0)
    td->have_voted = -1;
  else
    td->have_voted = 0;

  set_triggers(td, 1);

  td->last_spawned   = 0;
  td->last_completed = 0;

  gtc_barrier(); // ensure that triggers are all set before we go
}



/** Free the termination detection context.
 *
 * @param[in] td Termination detection context.
 */
void td_destroy(td_t *td) {
  gtc_lprintf(DBGTD, "destroying TD (%d of %d)\n", _c->rank, _c->size);

  // release portals resources
  if (td->uspawn_ct != PTL_INVALID_HANDLE)  PtlCTCancelTriggered(td->uspawn_ct);
  if (td->ufinal_ct != PTL_INVALID_HANDLE)  PtlCTCancelTriggered(td->ufinal_ct);
  if (td->down_ct   != PTL_INVALID_HANDLE)  PtlCTCancelTriggered(td->down_ct);

  if (td->uspawn_ct != PTL_INVALID_HANDLE)  PtlCTFree(td->uspawn_ct);
  if (td->ufinal_ct != PTL_INVALID_HANDLE)  PtlCTFree(td->ufinal_ct);
  if (td->down_ct   != PTL_INVALID_HANDLE)  PtlCTFree(td->down_ct);

  if (td->token_md  != PTL_INVALID_HANDLE)  PtlMDRelease(td->token_md);
  if (td->up_md     != PTL_INVALID_HANDLE)  PtlMDRelease(td->up_md);
  if (td->down_md   != PTL_INVALID_HANDLE)  PtlMDRelease(td->down_md);

  if (td->uspawn_me != PTL_INVALID_HANDLE)  PtlMEUnlink(td->uspawn_me);
  if (td->ufinal_me != PTL_INVALID_HANDLE)  PtlMEUnlink(td->ufinal_me);
  if (td->down_me   != PTL_INVALID_HANDLE)  PtlMEUnlink(td->down_me);

  PtlPTFree(_c->lni, td->ptindex);
  free(td->up_token);
  free(td->down_token);

  free(td);
}



/** Attempt to detect termination detection.
  *
  * @param[in] td Termination detection context.
  * @return       Non-zero upon termination, zero othersize.
  */
int td_attempt_vote(td_t *td) {
  int have_vote = 0;

  //gtc_dprintf("td_attempt_vote\n");

  // special case: single thread
  if (_c->size == 1) {
    if (   (td->token.spawned == td->last_spawned)
        && (td->token.completed == td->last_completed)
        && (td->token.spawned == td->token.completed)) {

      td->token.state = TERMINATED;
      gtc_lprintf(DBGTD, "td_attempt_vote: thread detected termination\n");
    }

    td->last_spawned   = td->token.spawned;
    td->last_completed = td->token.completed;
    return td->token.state == TERMINATED ? 1 : 0;
  }

  // need to determine if messages have arrived prior to resetting
  // triggers, because resetting updates the current counter values
  have_vote = vote_received(td);

  if (have_vote) {
    // downward pass
    if (td->down_token->state == TERMINATED) {
      // we're all done!
      td->token.state = TERMINATED;
    } else if (_c->rank == 0) {
      // root node has recieved global counts of spawned/completed tasks
      start_broadcast(td);
    } else if ((td->nchildren == 0) && (td->have_voted != 1)) {
      // if we're a leaf and not terminated, then start vote again
      start_reduction(td);
    } else {
      td->have_voted = 0;
    }

  } else {
    // upward pass
    // interior node -> atomic update on ourself
    if ((td->nchildren > 0)  && (_c->rank != 0) && (td->have_voted != 1)) {
      gtc_lprintf(DBGTD, "adding vote to ourself\n");
      ptl_process_t self = { .rank = _c->rank };
      cast_vote(td, self);
      td->have_voted = 1;
    }
  }


  if (td->token.state == TERMINATED) {
    gtc_lprintf(DBGTD, "td_attempt_vote: thread detected termination\n");
  } else {
    set_triggers(td, 0); // set/reset triggers if needed
  }

  return td->token.state == TERMINATED ? 1 : 0;
}
#endif // TD_TRIGGERED
