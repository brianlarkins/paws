/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>

#include "tc.h"

#ifndef TD_TRIGGERED

static void token_reset(td_token_t *t);

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


/**
 * pass_token_down - broadcast global termination status
 * @param td termination detection state
 */
static void pass_token_down(td_t *td) {
  ptl_ct_event_t current, ctevent;

  gtc_lprintf(DBGTD, "td: passing token down: send_token: [ %s %d %d ] last: s: %d c: %d nkids: %d l: %d r: %d\n",
      td->send_token.state == ACTIVE ? "a" : "t", td->send_token.spawned, td->send_token.completed,
      td->last_spawned, td->last_completed, td->nchildren, td->l.rank, td->r.rank);

  if (td->nchildren > 0) {
    PtlCTGet(td->send_ct, &current);

    PtlPut(td->token_md, 0, sizeof(td_token_t), PTL_CT_ACK_REQ,
        td->l, td->ptindex, __GTC_TERMINATION_DOWN_MATCH, 0, NULL, 0);

    if (td->nchildren == 2) {
      PtlPut(td->token_md, 0, sizeof(td_token_t), PTL_CT_ACK_REQ,
          td->r, td->ptindex, __GTC_TERMINATION_DOWN_MATCH, 0, NULL, 0);
    }
    PtlCTWait(td->send_ct, current.success+td->nchildren, &ctevent);
    if (ctevent.failure > current.failure) {
      gtc_dprintf("pass_token_down: counter failure\n");
    }
  }

  // update counts from previous attempt to terminate
  td->num_cycles++;
}



/**
 * pass_token_up - reduce global termination status
 * @param td termination detection state
 */
static void pass_token_up(td_t *td) {
  ptl_ct_event_t current, ctevent;
  uint64_t mbits;

  gtc_lprintf(DBGTD, "td: passing token up: send_token: [ %s %d %d ] last: s: %d c: %d\n",
      td->send_token.state == ACTIVE ? "a" : "t", td->send_token.spawned, td->send_token.completed,
      td->last_spawned, td->last_completed);

  PtlCTGet(td->send_ct, &current);

  if ((_c->rank % 2) == 1)
    mbits = __GTC_TERMINATION_USPAWN_MATCH; // odd uses USPAWN landing pad
  else
    mbits = __GTC_TERMINATION_UFINAL_MATCH; // even uses UFINAL landing pad

  PtlPut(td->token_md, 0, sizeof(td_token_t), PTL_CT_ACK_REQ,
        td->p, td->ptindex, mbits, 0, NULL, 0);

  PtlCTWait(td->send_ct, current.success+1, &ctevent);
  if (ctevent.failure > current.failure) {
    gtc_dprintf("pass_token_up: counter failure\n");
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
  // allocate counters for upward and downward pass token landing zones
  ret = PtlCTAlloc(_c->lni, &td->send_ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: CTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // allocate an MD for outgoing messages
  md.start   = &td->send_token;
  md.length  = sizeof(td_token_t);
  md.options = PTL_MD_EVENT_SEND_DISABLE | PTL_MD_EVENT_SUCCESS_DISABLE
    | PTL_MD_EVENT_CT_REPLY | PTL_MD_EVENT_CT_ACK;
  md.eq_handle = PTL_EQ_NONE;
  md.ct_handle = td->send_ct;
  // allocate an MD for all sends
  ret = PtlMDBind(_c->lni, &md, &td->token_md);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: MDBind error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  /*
   * setup info for receiving TD votes from others
   */

  // allocate a new PTE for termination detection
  ret = PtlPTAlloc(_c->lni, PTL_PT_MATCH_UNORDERED, PTL_EQ_NONE, PTL_PT_ANY, &td->ptindex);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: PTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // allocate counters for upward and downward pass token landing zones
  ret = PtlCTAlloc(_c->lni, &td->left_ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: CTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  ret = PtlCTAlloc(_c->lni, &td->right_ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: CTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  ret = PtlCTAlloc(_c->lni, &td->down_ct);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: CTAlloc error (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // allocate aligned memory for upward pass atomic ops

  // upward left token
  ret = posix_memalign((void **)&td->upleft_token, sizeof(int64_t), sizeof(td_token_t));
  if (ret != 0) {
    gtc_lprintf(DBGTD, "td_create: unable to get aligned memory (up) left -%s\n", strerror(errno));
    exit(1);
  }

  ret = posix_memalign((void **)&td->upright_token, sizeof(int64_t), sizeof(td_token_t));
  if (ret != 0) {
    gtc_lprintf(DBGTD, "td_create: unable to get aligned memory (up) left -%s\n", strerror(errno));
    exit(1);
  }
 
  // downward token (alignment not strictly necessary, for PtlPut)
  ret = posix_memalign((void **)&td->down_token, sizeof(int64_t), sizeof(td_token_t));
  if (ret != 0) {
    gtc_lprintf(DBGTD, "td_create: unable to get aligned memory for down token -%s\n", strerror(errno));
    exit(1);
  }


  // append matchlist entries for each field in the upward/downward pass tokens
  // up pass left
  me.start      = td->upleft_token;
  me.length     = sizeof(td_token_t);
  me.ct_handle  = td->left_ct;
  me.uid        = PTL_UID_ANY;
  me.options    = PTL_ME_OP_PUT | PTL_ME_OP_GET  | PTL_ME_EVENT_CT_COMM
    | PTL_ME_EVENT_LINK_DISABLE | PTL_ME_EVENT_COMM_DISABLE
    | PTL_ME_EVENT_UNLINK_DISABLE | PTL_ME_EVENT_SUCCESS_DISABLE;
  me.match_id.rank = PTL_RANK_ANY;
  me.match_bits    = __GTC_TERMINATION_USPAWN_MATCH;
  me.ignore_bits   = 0;
  ret = PtlMEAppend(_c->lni, td->ptindex, &me, PTL_PRIORITY_LIST, NULL, &td->uleft_me);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: PtlMEAppend error - left (%s)\n", gtc_ptl_error(ret));
    exit(1);
  }

  // up pass # tasks completed
  me.start      = td->upright_token;
  me.length     = sizeof(td_token_t);
  me.ct_handle  = td->right_ct;
  me.match_bits    = __GTC_TERMINATION_UFINAL_MATCH;
  ret = PtlMEAppend(_c->lni, td->ptindex, &me, PTL_PRIORITY_LIST, NULL, &td->uright_me);
  if (ret != PTL_OK) {
    gtc_lprintf(DBGTD, "td_create: PtlMEAppend error - right (%s)\n", gtc_ptl_error(ret));
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

  PtlCTSet(td->left_ct, zero);
  PtlCTSet(td->right_ct, zero);
  PtlCTSet(td->down_ct, zero);

  token_reset(&td->send_token);
  token_reset(&td->token);
  token_reset(td->upleft_token);
  token_reset(td->upright_token);
  token_reset(td->down_token);

  td->ullastset = 0;
  td->urlastset = 0;
  td->dlastset  = 0;

  td->num_cycles = 0;
  td->have_voted = 0;

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
  if (td->left_ct != PTL_INVALID_HANDLE)  PtlCTFree(td->left_ct);
  if (td->right_ct != PTL_INVALID_HANDLE)  PtlCTFree(td->right_ct);
  if (td->down_ct   != PTL_INVALID_HANDLE)  PtlCTFree(td->down_ct);

  if (td->token_md  != PTL_INVALID_HANDLE)  PtlMDRelease(td->token_md);

  if (td->uleft_me != PTL_INVALID_HANDLE)   PtlMEUnlink(td->uleft_me);
  if (td->uright_me != PTL_INVALID_HANDLE)  PtlMEUnlink(td->uright_me);
  if (td->down_me   != PTL_INVALID_HANDLE)  PtlMEUnlink(td->down_me);

  PtlPTFree(_c->lni, td->ptindex);
  free(td->upleft_token);
  free(td->upright_token);
  free(td->down_token);

  free(td);
}



/** Attempt to detect termination detection.
  *
  * @param[in] td Termination detection context.
  * @return       Non-zero upon termination, zero othersize.
  */
int td_attempt_vote(td_t *td) {
  ptl_ct_event_t now_ulcount, now_urcount, now_dtcount;
  int nleft, nright, ndown;
  int have_votes;

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

  PtlCTGet(td->left_ct, &now_ulcount);
  PtlCTGet(td->right_ct, &now_urcount);
  PtlCTGet(td->down_ct, &now_dtcount);
  nleft  = now_ulcount.success;
  nright = now_urcount.success;
  ndown  = now_dtcount.success;

  //gtc_lprintf(DBGTD, "td_attempt_vote: counters last: l: %d r: %d d: %d current:  l: %d r: %d d: %d\n",
  //    td->ullastset, td->urlastset, td->dlastset, nleft, nright, ndown);

  //if (_c->rank == 1) { gtc_dprintf("dir: %d ndown: %d :: lastset: %d\n", td->token_direction, ndown, td->dlastset); } 
  if (td->token_direction == DOWN) {
    // have we received a vote from the parent?
    if ((_c->rank == 0) || (ndown > td->dlastset)) {
      if (td->nchildren == 0) {
        //     leaf:
        if (td->down_token->state == TERMINATED) {
          td->token.state = TERMINATED;
        } else {
          // restart reduction
          gtc_lprintf(DBGTD, "td_attempt_vote: restarting vote\n");
          td->send_token = td->token;
          pass_token_up(td);
          td->have_voted = 1;
        }
      } else {
        //     interior node
        gtc_lprintf(DBGTD, "td_attempt_vote: casting downward votes\n");
        if (td->down_token->state == TERMINATED)
          td->token.state = TERMINATED;
        td->send_token = *(td->down_token);
        pass_token_down(td);
        td->have_voted = 0;
        td->token_direction = UP;

        if (td->down_token->state != TERMINATED) {
          // expect another vote from parent
          td->dlastset = ndown;
        }
      }
    }
  } else {
    // if we've received votes from left and right:
    have_votes = 0;
    switch (td->nchildren) {
      case 0:
        have_votes = 1;
        break;
      case 1:
        if (nleft > td->ullastset)
          have_votes = 1;
        break;
      case 2:
        if ((nleft > td->ullastset) && (nright > td->urlastset))
          have_votes = 1;
        break;
    }

    if (have_votes) {

      //    if root:
      if (_c->rank == 0) {
        int spawned   = td->token.spawned   + td->upleft_token->spawned   + td->upright_token->spawned;
        int completed = td->token.completed + td->upleft_token->completed + td->upright_token->completed;

        if (((spawned == td->last_spawned) && (completed == td->last_completed)) &&
            (spawned == completed))
          td->token.state = TERMINATED;
        
        td->last_spawned   = spawned;
        td->last_completed = completed;

        gtc_lprintf(DBGTD, "td_attempt_vote: broadcasting termination state\n");
        td->send_token.state     = td->token.state;
        td->send_token.spawned   = spawned;
        td->send_token.completed = completed;
        pass_token_down(td);
        td->token_direction = UP;
        td->have_voted = 0;

      } else {
        // else if interior node
        gtc_lprintf(DBGTD, "td_attempt_vote: reducing termination state\n");
        td->send_token.state     = td->token.state;
        td->send_token.spawned   = td->token.spawned   + td->upleft_token->spawned   + td->upright_token->spawned;
        td->send_token.completed = td->token.completed + td->upleft_token->completed + td->upright_token->completed;
        pass_token_up(td);
        td->token_direction = DOWN;
        td->have_voted = 1;
      }

      if (td->token.state != TERMINATED) {
        td->ullastset = nleft;
        td->urlastset = nright;
      }
    }
  }

  if (td->token.state == TERMINATED) {
    gtc_lprintf(DBGTD, "td_attempt_vote: thread detected termination\n");
  } 

  return td->token.state == TERMINATED ? 1 : 0;
}
#endif // TD_TRIGGERED
