/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include "sn_ring.h"
#include "tc.h"

#define MP_DEFAULT_PREALLOC 100000
#define MP_FORWARD  0
#define MP_BACKWARD 1

static void mp_allocfree(sn_ring_t *rb, int count);
static void mp_remove(sn_ring_t *rb, sn_mplist_t *el);
static void mp_fixcursor(sn_ring_t *rb);


/**
 * mp_init - initialize and pre-allocate matchpair list
 *
 * @param rb Portals ring buffer
 */
void mp_init(sn_ring_t *rb) {
  rb->matchpairs = rb->matchtail = rb->mpcursor = NULL;
  rb->mpiterdir = MP_FORWARD;
  mp_allocfree(rb, MP_DEFAULT_PREALLOC);
}



/**
 * sn_print_matchpairs - print the contents of the matchpairs list
 *
 * @param rb the task queue to print
 */
void mp_print_matchpairs(sn_ring_t *rb, char *msg) {
  sn_mplist_t *cur;

  if (msg)
    gtc_dprintf("%s\n", msg);
  if (mp_isempty(rb))
    return;

  printf("%4d: ", _c->rank);
  for (cur = rb->matchpairs; cur; cur = cur->next) {
    printf(" %"PRId64":%d:%c (%p)", cur->mp.index, cur->mp.tasks, cur->mp.unlinked ? 'u' : 'l', &cur->mp);
    //printf(" %"PRId64":%d:%c ", cur->mp.index, cur->mp.tasks, cur->mp.unlinked ? 'u' : 'l');
  }
  printf("\n");
}



/**
 * mp_isempty - predicate on list contents
 *
 * @param rb Portals ring buffer
 * @returns 1 if MP list is empty, 0 otherwise
 */
int mp_isempty(sn_ring_t *rb) {
  return (rb->matchpairs == NULL);
}



/**
 * mp_head - accessor for first MP entry
 *
 * @param rb Portals ring buffer
 * @returns MP entry at head of queue
 */
sn_matchpair_t *mp_head(sn_ring_t *rb) {
  gtc_lprintf(DBGMP, "mp_head: %p\n", rb->matchpairs);
  return rb->matchpairs ? &(rb->matchpairs->mp) : NULL;
}



/**
 * mp_tail - accessor for last MP entry
 *
 * @param rb Portals ring buffer
 * @returns MP entry at tail of queue
 */
sn_matchpair_t *mp_tail(sn_ring_t *rb) {
  gtc_lprintf(DBGMP, "mp_tail: %p\n", rb->matchtail);
  return rb->matchtail ? &(rb->matchtail->mp) : NULL;
}



/**
 * mp_push_head - append MP entry to head of queue
 *
 * @param rb Portals ring buffer
 * @param n new entry
 */
void mp_push_head(sn_ring_t *rb, sn_matchpair_t *n) {
  sn_mplist_t *nmp;

  // get free mplist struct
  assert(rb->freepairs); // ran out of entries on free list
  nmp = rb->freepairs;
  rb->freepairs = rb->freepairs->next;
  nmp->mp = *n; // copy matchpair into list node

  gtc_lprintf(DBGMP, "mp_push_head: pushing: %p\n", nmp);

  // add to mp list
  if (mp_isempty(rb)) {
    nmp->prev = nmp->next = NULL;
    rb->matchtail = nmp;

  } else {
    nmp->next = rb->matchpairs;
    nmp->prev = NULL;
    rb->matchpairs->prev = nmp;
  }
  rb->matchpairs = nmp;
}



/**
 * mp_update_tasks - modify task count in specified MP entry
 *
 * @param rb  Portals ring buffer
 * @param mpp MP entry specified by event user pointer
 * @param inc amount to increment tasks
 * @returns 1 if removed entry, 0 otherwise
 */
int mp_update_tasks(sn_ring_t *rb, sn_matchpair_t *mpp, int dec) {
  sn_mplist_t *cur;
  int ret = 0;

  assert(!mp_isempty(rb));

  if (mpp) {
    rb->nmpseek++;       // track number of updates
    // traverse mp list, looking for PTL_GET event entry
    cur = rb->matchpairs;
    while (cur) {
      rb->nmptrav++;     // track number of list entries checked
      if (mpp == &(cur->mp))
        break;
      cur = cur->next;
    }
  } else {
    // we were iterating and need to unlink the current entry
    cur = rb->mpcursor;
  }

  if (cur->mp.unlinked) {
    if (!mpp)
      mp_fixcursor(rb); // only patch up iterator cursor if are using it
    mp_remove(rb, cur);
    ret = 1;
  } else {
    assert((cur->mp.tasks - dec) >= 0);    // can't steal more than are there
    cur->mp.tasks -= dec;
  }
  return ret;
}



/**
 * mp_unlink - mark ME as unlinked and remove MP if all tasks stolen
 *
 * @param rb   Portals ring buffer
 * @param mpp  MP entry specified by event user pointer
 *             use iterator cursor if NULL
 * @returns 1 if removed entry, 0 otherwise
 */
int mp_unlink(sn_ring_t *rb, sn_matchpair_t *mpp) {
  sn_mplist_t *cur;
  int ret = 0;

  assert(!mp_isempty(rb));

  if (mpp) {
    rb->nmpseek++;       // track number of updates
    // we need to search the list for the matching MP entry
    cur = rb->matchpairs;
    while (cur) {
      rb->nmptrav++;     // track number of list entries checked
      if (mpp == &(cur->mp))
        break;
      cur = cur->next;
    }

  } else {
    // we were iterating and need to unlink the current entry
    cur = rb->mpcursor;
  }

  // remove MP entry if all tasks stolen and ME unlinked
  if (cur->mp.tasks <= 0) {
    if (!mpp)
      mp_fixcursor(rb); // only patch up iterator cursor if are using it
    mp_remove(rb, cur);
    ret = 1;
  } else {
    cur->mp.unlinked = 1;
  }
  return ret;
}



/**
 * mp_remove_entry - remove MP entry connected to an unlink event
 *
 * @param rb   Portals ring buffer
 * @param mpp  MP entry specified by event user pointer
 */
sn_matchpair_t mp_remove_entry(sn_ring_t *rb, sn_matchpair_t *mpp) {
  sn_mplist_t *cur, *next, *prev;
  sn_matchpair_t mp;

  assert(!mp_isempty(rb));

  // bookkeep for search depth, we always look at the first element
  rb->nmptrav++;
  rb->nmpseek++;

  // most likely: event MP is at list head
  if (mpp == mp_head(rb))
    return mp_pop_head(rb);

  // traverse mp list, looking for PTL_AUTO_UNLINK event entry
  prev = rb->matchpairs;       // head
  cur  = rb->matchpairs->next; // second element
  while (cur) {
    rb->nmptrav++;

    // found it, remove it and return
    if (mpp == &(cur->mp)) {

      mp = cur->mp;

      if (cur->next) {
        // entries follow cursor
        next = cur->next;
        prev->next = next;
        next->prev = prev;

      } else {
        // cursor is at tail
        prev->next = NULL;
        rb->matchtail = prev;
      }

      // put removed list entry onto freelist
      cur->next = rb->freepairs;
      cur->prev = NULL;
      if (rb->freepairs)
        rb->freepairs->prev = cur;
      rb->freepairs = cur;
      break;
    }
    cur = cur->next;
  }

  return mp;
}



/**
 * mp_pop_head - remove MP entry at head of queue
 *
 * @param rb Portals ring buffer
 * @returns  MP entry at head of queue
 */
sn_matchpair_t mp_pop_head(sn_ring_t *rb) {
  sn_matchpair_t mp;
  sn_mplist_t *fmp;

  assert(!mp_isempty(rb));

  gtc_lprintf(DBGMP, "mp_pop_head: popping: %p (next: %p)\n", rb->matchpairs, rb->matchpairs ? rb->matchpairs->next : NULL);

  mp = rb->matchpairs->mp;  // matchpair to return
  fmp = rb->matchpairs;     // mplist entry to free list

  if (rb->matchpairs == rb->matchtail) {
    gtc_lprintf(DBGMP, "mp_pop_head: singleton\n");
    rb->matchpairs = rb->matchtail = NULL;
  } else {
    gtc_lprintf(DBGMP, "mp_pop_head: not singleton\n");
    rb->matchpairs = rb->matchpairs->next;
    rb->matchpairs->prev = NULL;
  }

  // add popped entry to head of free list
  fmp->next = rb->freepairs;
  fmp->prev = NULL;
  if (rb->freepairs)
    rb->freepairs->prev = fmp;
  rb->freepairs = fmp;

  return mp;
}



/**
 * mp_push_tail - appends MP entry to end of queue
 *
 * @param rb Portals ring buffer
 * @param n  new MP entry
 */
void mp_push_tail(sn_ring_t *rb, sn_matchpair_t *n) {
  sn_mplist_t *nmp;

  // get free mplist struct
  assert(rb->freepairs); // ran out of entries on free list
  nmp = rb->freepairs;
  rb->freepairs = rb->freepairs->next;
  nmp->mp = *n; // copy matchpair into list node

  gtc_lprintf(DBGMP, "mp_push_tail: pushing: %p\n", nmp);

  // add to mp list
  if (mp_isempty(rb)) {
    nmp->prev = nmp->next = NULL;
    rb->matchpairs = nmp;

  } else {
    nmp->prev = rb->matchtail;
    nmp->next = NULL;
    rb->matchtail->next = nmp;
  }
  rb->matchtail = nmp;
}



/**
 * mp_pop_tail - removes MP entry at end of queue
 *
 * @param   rb Portals ring buffer
 * @returns MP entry at end of queue
 */
sn_matchpair_t mp_pop_tail(sn_ring_t *rb) {
  sn_matchpair_t mp;
  sn_mplist_t *fmp;

  assert(!mp_isempty(rb));

  gtc_lprintf(DBGMP, "mp_pop_tail: popping: %p\n", rb->matchpairs);

  mp = rb->matchtail->mp;  // matchpair to return
  fmp = rb->matchtail;     // mplist entry to free list

  if (rb->matchpairs == rb->matchtail) {
    rb->matchpairs      = rb->matchtail = NULL;
  } else {
    rb->matchtail       = rb->matchtail->prev;
    rb->matchtail->next = NULL;
  }

  // add popped entry to head of free list
  fmp->next = rb->freepairs;
  fmp->prev = NULL;
  if (rb->freepairs)
    rb->freepairs->prev = fmp;
  rb->freepairs = fmp;

  return mp;
}



/**
 * mp_start_head - initializes iterator at head
 *
 * @param rb Portals ring buffer
 */
void mp_start_head(sn_ring_t *rb) {
  rb->mpcursor   = NULL;
  rb->mpiterdir  = MP_FORWARD;
}



/**
 * mp_start_tail - initializes iterator at tail
 *
 * @param rb Portals ring buffer
 */
void mp_start_tail(sn_ring_t *rb) {
  rb->mpcursor   = NULL;
  rb->mpiterdir  = MP_BACKWARD;
}



/**
 * mp_has_next - predictate for forward iteration
 *
 * @param    rb Portals ring buffer
 * @returns  1 if next MP exists, 0 otherwise
 */
int mp_has_next(sn_ring_t *rb) {
  if (mp_isempty(rb))
    return 0;
  if (rb->mpcursor)
    return rb->mpcursor->next ? 1 : 0;
  else
    return rb->matchpairs ? 1 : 0;
}



/**
 * mp_has_prev - predictate for reverse iteration
 *
 * @param    rb Portals ring buffer
 * @returns  1 if prev MP exists, 0 otherwise
 */
int mp_has_prev(sn_ring_t *rb) {
  if (mp_isempty(rb))
    return 0;
  if (rb->mpcursor)
    return rb->mpcursor->prev ? 1 : 0;
  else
    return rb->matchtail ? 1 : 0;
}



/**
 * mp_next - fetches next MP in iteration
 *
 * @param   rb Portals ring buffer
 * @returns next MP entry (depends on direction)
 */
sn_matchpair_t mp_next(sn_ring_t *rb) {
  assert(!mp_isempty(rb));

  if (rb->mpiterdir == MP_FORWARD) {
    if (rb->mpcursor)
      rb->mpcursor = rb->mpcursor->next;
    else
      rb->mpcursor = rb->matchpairs;
  } else {
    if (rb->mpcursor)
      rb->mpcursor = rb->mpcursor->prev;
    else
      rb->mpcursor = rb->matchtail;
  }
  assert(rb->mpcursor);
  return rb->mpcursor->mp;
}



/**
 * mp_cur_evptr - returns direct access to matchpair pointer
 *   - used only for debugging with user pointer value in ME/event
 *
 * @param   rb Portals ring buffer
 * @returns pointer to MP entry
 */
sn_matchpair_t *mp_cur_evptr(sn_ring_t *rb) {
  assert(rb->mpcursor);
  return &(rb->mpcursor->mp);
}



/**
 * mp_remove - remove MP entry from the matchpair list (internal)
 *
 * @param rb   Portals ring buffer
 * @param mpl  MP list pointer
 */
void mp_remove(sn_ring_t *rb, sn_mplist_t *el) {
  sn_mplist_t *prev, *next;

  prev = el->prev;
  next = el->next;

  if (!next)
    rb->matchtail = prev; // at tail
  else
    next->prev    = prev; // not at tail

  if (!prev)
    rb->matchpairs = next; // at head
  else
    prev->next     = next; // not at head


  if (el->mp.iov) free(el->mp.iov);

  // put removed list entry onto freelist
  el->next = rb->freepairs;
  el->prev = NULL;
  if (rb->freepairs)
    rb->freepairs->prev = el;
  rb->freepairs = el;
}



/**
 * mp_fixcursor - puts iterator in a good state after removing the cursor element
 *
 * @param   rb Portals ring buffer
 * @returns next MP entry (depends on direction)
 */
void mp_fixcursor(sn_ring_t *rb) {
  assert(!mp_isempty(rb));
  assert(rb->mpcursor);

  // this performs an "undo" of the advancement of the cursor.
  // if we delete the cursor entry, move the cursor back to
  // the previous good entry

  if (rb->mpiterdir == MP_FORWARD)
    rb->mpcursor = rb->mpcursor->prev;
  else
    rb->mpcursor = rb->mpcursor->next;
}



/**
 * mp_allocfree - preallocate all MP entries for run
 *
 * @param rb    Portals ring buffer
 * @param size  size of free MP pool
 */
static void mp_allocfree(sn_ring_t *rb, int count) {
  sn_mplist_t *newfree;

  newfree = calloc(count, sizeof(sn_mplist_t));
  if (!newfree) {
    gtc_dprintf("mp_allocfree: calloc error\n");
    return;
  }

  rb->freepairs = newfree;
  rb->freepairs[0].prev = NULL;
  rb->freepairs[0].next = &(rb->freepairs[1]);
  rb->freepairs[count-1].next = NULL;
  rb->freepairs[count-1].prev = &(rb->freepairs[count-2]);
  for (int i=1; i < count-1; i++) {
    rb->freepairs[i].prev = &(rb->freepairs[i-1]);
    rb->freepairs[i].next = &(rb->freepairs[i+1]);
  }
}
