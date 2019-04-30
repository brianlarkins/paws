/********************************************************/
/*                                                      */
/*  commsynch.c - sciotwo communication completion ops  */
/*                                                      */
/*  author: d. brian larkins                            */
/*  created: 3/20/16                                    */
/*                                                      */
/********************************************************/

#include <tc.h>

/**
 * @file
 *
 * portals distributed hash table synchronization ops
 */

static int barrier_landing = 0;

static void reduce_zip(void *dest, void *src, gtc_reduceop_t op, gtc_datatype_t ty, int s);
static void reduce_sum(void *dest, void *src, gtc_datatype_t ty, int s);
static void reduce_min(void *dest, void *src, gtc_datatype_t ty, int s);
static void reduce_max(void *dest, void *src, gtc_datatype_t ty, int s);
static int gtc_collective_size(gtc_datatype_t type);

/**
 * gtc_collective_init() - initializes the collective universe
 */
void gtc_collective_init(gtc_context_t *c) {
  ptl_md_t md;
  ptl_pt_index_t index;
  ptl_me_t me;
  int ret;

  // initialize our collective count to one (for ourself)
  c->barrier_count    = 0;
  c->bcast_count      = 0;
  c->reduce_count     = 0;

  // allocate some scratch place for reductions (assumes longs/doubles are 64-bits each)
  c->collective_lscratch = calloc(3*GTC_MAX_REDUCE_ELEMS, sizeof(uint64_t));
  // set start to middle of scratch space (danger, pointer math)
  c->collective_rscratch = ((char *)(c->collective_lscratch)) + (GTC_MAX_REDUCE_ELEMS * sizeof(uint64_t));
  c->collective_scratch = ((char *)(c->collective_rscratch)) + (GTC_MAX_REDUCE_ELEMS * sizeof(uint64_t));

  // create/bind send side MD
  md.start     = NULL;
  md.length    = PTL_SIZE_MAX;
  md.options   = PTL_MD_UNORDERED;
  md.eq_handle = PTL_EQ_NONE;
  md.ct_handle = PTL_CT_NONE;

  ret = PtlMDBind(c->lni, &md, &c->collective_md);
  if (ret != PTL_OK) {
    gtc_dprintf("collective_init: MD Bind failed: %s\n", gtc_ptl_error(ret));
    exit(1);
  }
  // create portals table entry
  ret = PtlPTAlloc(c->lni, 0, PTL_EQ_NONE, __GTC_COLLECTIVE_INDEX, &index);
  if ((ret != PTL_OK) || (index != __GTC_COLLECTIVE_INDEX)) {
    gtc_dprintf("collective_init: PTAlloc failed\n");
    exit(1);
  }

  // create ME with counter for collective message send/receive
  c->reduce_ct     = PTL_INVALID_HANDLE;
  c->bcast_ct      = PTL_INVALID_HANDLE;
  c->barrier_ct    = PTL_INVALID_HANDLE;

  // get counter for broadcasts
  ret = PtlCTAlloc(c->lni, &c->bcast_ct);
  if (ret != PTL_OK)  {
    gtc_dprintf("collective_init: CTAlloc failed\n");
    exit(1);
  }

  // get counter for reductions
  ret = PtlCTAlloc(c->lni, &c->reduce_ct);
  if (ret != PTL_OK)  {
    gtc_dprintf("collective_init: CTAlloc failed\n");
    exit(1);
  }

  // get counter for barriers
  ret = PtlCTAlloc(c->lni, &c->barrier_ct);
  if (ret != PTL_OK)  {
    gtc_dprintf("collective_init: CTAlloc failed\n");
    exit(1);
  }

  // allocate matchlist entry for barriers
  me.start      = &barrier_landing;
  me.length     = sizeof(barrier_landing);
  me.ct_handle  = c->barrier_ct;
  me.uid        = PTL_UID_ANY;
  me.options    = PTL_ME_OP_PUT | PTL_ME_ACK_DISABLE | PTL_ME_EVENT_CT_COMM | PTL_ME_EVENT_LINK_DISABLE;
  me.match_id.rank = PTL_RANK_ANY;
  me.match_bits  = __GTC_BARRIER_MATCH;
  me.ignore_bits = 0;
  ret = PtlMEAppend(c->lni, __GTC_COLLECTIVE_INDEX, &me, PTL_PRIORITY_LIST, NULL, &c->barrier_me);
  if (ret != PTL_OK)  {
    gtc_dprintf("collective_init: PtlMEAppend failed\n");
    exit(1);
  }

  // allocate matchlist entry for broadcasts
  me.start      = c->collective_lscratch; // re-use common scratch space
  me.length     = sizeof(uint64_t)*GTC_MAX_REDUCE_ELEMS;
  me.ct_handle  = c->bcast_ct;
  me.match_bits  = __GTC_BCAST_MATCH;
  ret = PtlMEAppend(c->lni, __GTC_COLLECTIVE_INDEX, &me, PTL_PRIORITY_LIST, NULL, &c->bcast_me);
  if (ret != PTL_OK)  {
    gtc_dprintf("collective_init: PtlMEAppend failed\n");
    exit(1);
  }

  // allocate left matchlist entry for reductions
  me.start      = c->collective_lscratch;
  me.length     = sizeof(uint64_t)*GTC_MAX_REDUCE_ELEMS;
  me.ct_handle  = c->reduce_ct;
  me.match_bits  = __GTC_REDUCE_LMATCH;
  ret = PtlMEAppend(c->lni, __GTC_COLLECTIVE_INDEX, &me, PTL_PRIORITY_LIST, NULL, &c->reduce_lme);
  if (ret != PTL_OK)  {
    gtc_dprintf("collective_init: PtlMEAppend failed\n");
    exit(1);
  }

  // allocate right matchlist entry for reductions
  me.start      = c->collective_rscratch;
  me.match_bits  = __GTC_REDUCE_RMATCH;
  ret = PtlMEAppend(c->lni, __GTC_COLLECTIVE_INDEX, &me, PTL_PRIORITY_LIST, NULL, &c->reduce_rme);
  if (ret != PTL_OK)  {
    gtc_dprintf("collective_init: PtlMEAppend failed\n");
    exit(1);
  }
}



/**
 * gtc_collective_fini - cleans up collective op infrastructure
 */
void gtc_collective_fini(void) {
  if (_c->collective_lscratch)
    free(_c->collective_lscratch);

  PtlMEUnlink(_c->barrier_me);
  PtlMEUnlink(_c->reduce_lme);
  PtlMEUnlink(_c->reduce_rme);
  PtlCTFree(_c->bcast_ct);
  PtlCTFree(_c->reduce_ct);
  PtlCTFree(_c->barrier_ct);
  PtlPTFree(_c->lni, __GTC_COLLECTIVE_INDEX);
  PtlMDRelease(_c->collective_md);
}



/**
 * gtc_barrier - blocks process until all processes reach the barrier
 */
void gtc_barrier(void) {
  ptl_process_t  p, l, r;
  int            nchildren = 0;
  int            nparent   = (_c->rank != 0) ? 1 : 0; // root has no parent
  ptl_size_t     count_base;
  ptl_ct_event_t cval;
  int ret;
  int x = 1;

  // use binary tree of processes
  p.rank = ((_c->rank + 1) >> 1) - 1;
  l.rank = ((_c->rank + 1) << 1) - 1;
  r.rank = l.rank + 1;

  //ptl_ct_event_t cval2;
  //PtlCTGet(_c->barrier_ct, &cval2); // for debug output only
  //gtc_dprintf("**** p: %lu l: %lu r: %lu barrier_count: %d counter: %lu\n", (_c->rank != 0) ? p.rank : 0, l.rank, r.rank, _c->barrier_count, cval2.success);

  if (l.rank < _c->size)
    nchildren++;
  if (r.rank < _c->size)
    nchildren++;

  // barrier_count tracks barrier events, the number of received messages is related to
  // our tree connectivity
  count_base = _c->barrier_count * (nparent + nchildren);


  // if we have children, wait for them to send us a barrier entry message
  if (nchildren > 0) {
    //gtc_dprintf("waiting for %d messages from l: %lu r: %lu \n", nchildren, l.rank, r.rank);
    ret = PtlCTWait(_c->barrier_ct, count_base + nchildren, &cval);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_barrier: CTWait failed (children)\n");
      exit(1);
    }

    if (cval.failure > 0) {
      gtc_dprintf("gtc_barrier: found failure event waiting on child messages\n");
      exit(1);
    }
  }


  // if we are a child, send a message to our parent (our subtree has all arrived at barrier)
  if (_c->rank > 0)  {

    //gtc_dprintf("notifying %lu : ct: %d count: %d\n", p.rank, cval2.success, c->ptl.barrier_count);
    ret = PtlPut(_c->collective_md, (ptl_size_t)&x, sizeof(x), PTL_NO_ACK_REQ, p, __GTC_COLLECTIVE_INDEX,
        __GTC_BARRIER_MATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("barrier: Put failed (put parent): %s\n", gtc_ptl_error(ret));
      exit(1);
    }

    // wait for downward broadcast from parent
    ret = PtlCTWait(_c->barrier_ct, count_base + (nchildren + nparent), &cval);
    if (ret != PTL_OK) {
      gtc_dprintf("barrier: CTWait failed (parent)\n");
      exit(1);
    }
    if (cval.failure > 0) {
      gtc_dprintf("gtc_barrier: found failure event waiting on child messages\n");
      exit(1);
    }
  }

  // wake up waiting children
  if (nchildren > 0) {
    //gtc_dprintf("waking left: %lu\n", l.rank);
    ret = PtlPut(_c->collective_md, (ptl_size_t)&x, sizeof(x), PTL_NO_ACK_REQ, l, __GTC_COLLECTIVE_INDEX,
        __GTC_BARRIER_MATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("barrier: Put failed (wake left): %s\n", gtc_ptl_error(ret));
      exit(1);
    }
  }

  if (nchildren > 1) {
    //gtc_dprintf("waking right: %lu\n", l.rank);
    ret = PtlPut(_c->collective_md, (ptl_size_t)&x, sizeof(x), PTL_NO_ACK_REQ, r, __GTC_COLLECTIVE_INDEX,
        __GTC_BARRIER_MATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("barrier: Put failed (wake right): %s\n", gtc_ptl_error(ret));
      exit(1);
    }
  }

  // keep track of how many barriers have occurred
  _c->barrier_count++;
}



/**
 * gtc_reduce - blocks process until all processes reach the barrier
 * @param in buffer containing this processes source data
 * @param out buffer containing the final reduction value (only valid on root process)
 * @param op operation to perform
 * @param type type data type of elements
 * @param elems number of elements
 * @returns status of operation
 */
gtc_status_t gtc_reduce(void *in, void *out, gtc_reduceop_t op, gtc_datatype_t type, int elems) {
  ptl_process_t  p, l, r;
  int            nchildren = 0;
  int            nparent   = (_c->rank != 0) ? 1 : 0; // root has no parent
  ptl_size_t     count_base;
  ptl_ct_event_t cval;
  ptl_match_bits_t destme;
  int ret = 0,  tysize = 0, x = 0;

  switch (op) {
  case GtcReduceOpSum:
  case GtcReduceOpMin:
  case GtcReduceOpMax:
    break;
  default:
    gtc_dprintf("gtc_reduce: unsupported reduction operation\n");
    exit(1);
  }

  tysize = gtc_collective_size(type);

  // figure out if we are the left or right child of our parent
  destme = ((_c->rank % 2) != 0) ? __GTC_REDUCE_LMATCH : __GTC_REDUCE_RMATCH;

  // use binary tree of processes
  p.rank = ((_c->rank + 1) >> 1) - 1;
  l.rank = ((_c->rank + 1) << 1) - 1;
  r.rank = l.rank + 1;

  if (l.rank < _c->size)
    nchildren++;
  if (r.rank < _c->size)
    nchildren++;

  // reduce_count tracks reduce events, the number of received messages is related to
  // our tree connectivity
  count_base = _c->reduce_count * (nparent + nchildren);

  // if we have children, wait for them to send us reduce message
  if (nchildren > 0) {
    //gtc_dprintf("gtc_reduce: waiting for %d messages from l: %lu r: %lu \n", nchildren, l.rank, r.rank);
    ret = PtlCTWait(_c->reduce_ct, count_base + nchildren, &cval);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_reduce: CTWait failed (children)\n");
      exit(1);
    } else if (cval.failure > 0) {
      gtc_dprintf("gtc_reduce: found failure event waiting on child messages\n");
      exit(1);
    }
  }

  // move data from our input buffer to scratch space
  memcpy(_c->collective_scratch, in, (tysize*elems));

  // reduce child buffers into our own
  if (nchildren > 0) {
    reduce_zip(_c->collective_scratch, _c->collective_lscratch, op, type, elems);
    if (nchildren == 2)
      reduce_zip(_c->collective_scratch, _c->collective_rscratch, op, type, elems);
  }

  // send our reduced buffer to our parent
  if (_c->rank > 0)  {
    //gtc_dprintf("notifying %lu : ct: %d count: %d\n", p.rank, cval2.success, c->ptl.barrier_count);
    ret = PtlPut(_c->collective_md, (ptl_size_t)_c->collective_scratch, tysize*elems, PTL_NO_ACK_REQ, p, __GTC_COLLECTIVE_INDEX,
        destme, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_reduce: Put failed (put parent): %s\n", gtc_ptl_error(ret));
      exit(1);
    }

    // wait for downward broadcast from parent
    ret = PtlCTWait(_c->reduce_ct, count_base + (nchildren + nparent), &cval);
    if (ret != PTL_OK) {
      gtc_dprintf("barrier: CTWait failed (parent)\n");
      exit(1);
    }
    if (cval.failure > 0) {
      gtc_dprintf("gtc_barrier: found failure event waiting on child messages\n");
      exit(1);
    }
  } else {
    // we're the root our local data has been zipped with each l/r subtree
    memcpy(out, _c->collective_scratch, tysize*elems);
  }

  // wake up waiting children
  if (nchildren > 0) {
    //gtc_dprintf("waking left: %lu\n", l.rank);
    ret = PtlPut(_c->collective_md, (ptl_size_t)&x, sizeof(x), PTL_NO_ACK_REQ, l, __GTC_COLLECTIVE_INDEX,
        __GTC_REDUCE_LMATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("barrier: Put failed (wake left): %s\n", gtc_ptl_error(ret));
      exit(1);
    }
  }

  if (nchildren > 1) {
    //gtc_dprintf("waking right: %lu\n", l.rank);
    ret = PtlPut(_c->collective_md, (ptl_size_t)&x, sizeof(x), PTL_NO_ACK_REQ, r, __GTC_COLLECTIVE_INDEX,
        __GTC_REDUCE_LMATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("barrier: Put failed (wake right): %s\n", gtc_ptl_error(ret));
      exit(1);
    }
  }

  // keep track of how many reductions have occurred
  _c->reduce_count++;

  return GtcStatusOK;
}



/**
 * gtc_broadcast - broadcasts data from rank zero to all other processes
 * @param buf buffer to brodast from (rank 0)/to (other ranks)
 * @param type type of data to broadcast
 * @param elems number of data elements to broadcast
 * @returns status of operation
 */
gtc_status_t gtc_broadcast(void *buf, gtc_datatype_t type, int elems) {
  ptl_process_t  p, l, r;
  ptl_size_t     bcast_base;
  ptl_ct_event_t cval;
  int nparent   = (_c->rank != 0) ? 1 : 0; // root has no parent
  int tysize = 0, nchildren = 0, ret, x;

  // use binary tree of processes
  p.rank = ((_c->rank + 1) >> 1) -1;
  l.rank = ((_c->rank + 1) << 1) - 1;
  r.rank = l.rank + 1;

  tysize = gtc_collective_size(type);

  if (l.rank < _c->size)
    nchildren++;
  if (r.rank < _c->size)
    nchildren++;

  // bcast_count tracks broadcast events, compute the right number of
  //    expected messages for our counter threshold
  bcast_base = _c->bcast_count * (nparent + nchildren);

  // wait for parent to send message
  if (_c->rank > 0) {
    ret = PtlCTWait(_c->bcast_ct, bcast_base + nparent, &cval);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_broadcast: CTWait failed (parent)\n");
      exit(1);
    }
    // copy recieved data to our buffer
    memcpy(buf, _c->collective_lscratch, tysize*elems);
  }

  // parents send the message to children (if any)
  if (nchildren > 0) {
    ret = PtlPut(_c->collective_md, (ptl_size_t)buf, tysize*elems, PTL_NO_ACK_REQ, l,
                 __GTC_COLLECTIVE_INDEX, __GTC_BCAST_MATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_broadcast: Put failed (send left)\n");
      exit(1);
    }
  }

  if (nchildren > 1) {
    ret = PtlPut(_c->collective_md, (ptl_size_t)buf, tysize*elems, PTL_NO_ACK_REQ, r,
                 __GTC_COLLECTIVE_INDEX, __GTC_BCAST_MATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_broadcast: Put failed (send right)\n");
      exit(1);
    }
  }

  // now we need to wait for confirmation from our children
  if (nchildren != 0)  {
    ret = PtlCTWait(_c->bcast_ct, bcast_base + (nparent + nchildren), &cval);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_broadcast: CTWait failed (parent)\n");
      exit(1);
    }
  }

  // notify our parent that we've received the broadcast
  if (_c->rank > 0) {
    x = 0;
    ret = PtlPut(_c->collective_md, (ptl_size_t)&x, sizeof(x), PTL_NO_ACK_REQ, p, __GTC_COLLECTIVE_INDEX,
        __GTC_BCAST_MATCH, 0, NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_broadcast: Put failed (parent confirmation)\n");
      exit(1);
    }
  }

  _c->bcast_count++;
  return GtcStatusOK;
}



/**
 * gtc_allreduce - performs a reduce followed by a broadcast operation
 * @param in buffer containing this processes source data
 * @param out buffer containing the final reduction value (only valid on root process)
 * @param op operation to perform
 * @param type type data type of elements
 * @param elems number of elements
 * @returns status of operation
 */
gtc_status_t gtc_allreduce(void *in, void *out, gtc_reduceop_t op, gtc_datatype_t type, int elems) {
   gtc_status_t ret;

   // this is a lazy implementation. i get it.

   ret = gtc_reduce(in, out, op, type, elems);

   if (ret != GtcStatusOK)
      return ret;
   gtc_barrier();
   ret = gtc_broadcast(out, type, elems);
   return ret;
}



/**
 * gtc_gather - gathers disparate data onto the root
 * @param in buffer containing this processes source data
 * @param out buffer containing the gather values
 * @param type type data type of elements
 * @param elems number of elements
 * @param root  process holding the gathered data
 * @returns status of operation
 */
gtc_status_t gtc_gather(void *in, void *out, gtc_datatype_t type, int elems, int root) {
  int ret;
  ptl_me_t m;
  ptl_handle_ct_t ct;
  ptl_handle_me_t me;
  ptl_ct_event_t  ctevent;
  ptl_process_t r = { .rank = root };
  uint8_t *p;
  int tysize = gtc_collective_size(type);

  if (_c->rank == root) {
    // setup CT to watch completion on local ME
    ret = PtlCTAlloc(_c->lni, &ct);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_gather: PtlCTAlloc failed\n");
      exit(1);
    }

    // setup ME for receiving contributions
    m.start         = out;
    m.length        = _c->size*(tysize*elems);
    m.ct_handle     = ct;
    m.uid           = PTL_UID_ANY;
    m.options       =   PTL_ME_OP_PUT | PTL_ME_EVENT_CT_COMM | PTL_ME_EVENT_LINK_DISABLE
      | PTL_ME_EVENT_UNLINK_DISABLE | PTL_ME_EVENT_COMM_DISABLE | PTL_ME_EVENT_SUCCESS_DISABLE;
    m.match_id.rank = PTL_RANK_ANY;
    m.match_bits    = __GTC_GATHER_MATCH;
    m.ignore_bits   = 0;
    m.min_free      = 0;

    ret = PtlMEAppend(_c->lni, __GTC_COLLECTIVE_INDEX, &m, PTL_PRIORITY_LIST, NULL, &me);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_gather: PtlMEAppend failed\n");
      exit(1);
    }

    gtc_barrier();

    // copy over our local contribution
    p = (uint8_t *)out + (_c->rank*tysize*elems);
    memcpy(p, in, tysize*elems);

    // wait for everone else's puts to complete
    ret = PtlCTWait(ct, _c->size-1, &ctevent);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_gather: PtlCTWait failed\n");
      exit(1);
    } else if (ctevent.failure > 0) {
      gtc_dprintf("gtc_gather: PtlCTWait returned a failure event\n");
      exit(1);
    }

    // clean up after we're done
    ret = PtlMEUnlink(me);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_gather: PtlMEUnlink failed\n");
      exit(1);
    }

  } else {

    gtc_barrier();

    ret = PtlPut(_c->collective_md, (ptl_size_t)in, tysize*elems, PTL_NO_ACK_REQ, r,
        __GTC_COLLECTIVE_INDEX, __GTC_GATHER_MATCH, _c->rank*(tysize*elems), NULL, 0);
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_gather: PtlPut failed\n");
      exit(1);
    }
    // no need to wait for completion
  }
  return GtcStatusOK;
}

/**
 * reduce_zip - helper function to accumulate (sum-only) reduced data
 * @param dest  accumulation buffer
 * @param src source data
 * @param ty data type
 * @param s number of elements
 */
static void reduce_zip(void *dest, void *src, gtc_reduceop_t op, gtc_datatype_t ty, int s) {

  switch(op) {
  case GtcReduceOpSum:
    reduce_sum(dest, src, ty, s);
    break;
  case GtcReduceOpMin:
    reduce_min(dest, src, ty, s);
    break;
  case GtcReduceOpMax:
    reduce_max(dest, src, ty, s);
    break;
  }
}



/**
 * reduce_sum - helper function to accumulate (sum-only) reduced data
 * @param dest  accumulation buffer
 * @param src source data
 * @param ty data type
 * @param s number of elements
 */
static void reduce_sum(void *dest, void *src, gtc_datatype_t ty, int s) {
  int    *is, *id;
  long   *ls, *ld;
  double *ds, *dd;
  char   *cs, *cd;
  unsigned long *uls, *uld;

  switch(ty) {
    case IntType:
      is = src;
      id = dest;
      for (int i=0;i<s;i++) id[i] += is[i];
      break;
    case LongType:
      ls = src;
      ld = dest;
      for (int i=0;i<s;i++) ld[i] += ls[i];
      break;
    case UnsignedLongType:
      uls = src;
      uld = dest;
      //for (int i=0;i<s;i++) { gtc_dprintf("%lu + %lu\n", uld[i], uls[i]); uld[i] += uls[i]; }
      for (int i=0;i<s;i++) uld[i] += uls[i];
      break;
    case DoubleType:
      ds = src;
      dd = dest;
      for (int i=0;i<s;i++) dd[i] += ds[i];
      break;
    case CharType:
    case BoolType:
      cs = src;
      cd = dest;
      for (int i=0;i<s;i++) cd[i] += cs[i];
      break;
    default:
      gtc_dprintf("gtc_reduce: unsupported reduction datatype\n");
      exit(1);
  }
}



/**
 * reduce_min - helper function to accumulate reduced data
 * @param dest  accumulation buffer
 * @param src source data
 * @param ty data type
 * @param s number of elements
 */
static void reduce_min(void *dest, void *src, gtc_datatype_t ty, int s) {
  int    *is, *id;
  long   *ls, *ld;
  double *ds, *dd;
  char   *cs, *cd;
  unsigned long *uls, *uld;

  switch(ty) {
    case IntType:
      is = src;
      id = dest;
      for (int i=0;i<s;i++) id[i] = id[i] < is[i] ? id[i] : is[i];
      break;
    case LongType:
      ls = src;
      ld = dest;
      for (int i=0;i<s;i++) ld[i] = ld[i] < ls[i] ? ld[i] : ls[i];
      break;
    case UnsignedLongType:
      uls = src;
      uld = dest;
      for (int i=0;i<s;i++) uld[i] = uld[i] < uls[i] ? uld[i] : uls[i];
      break;
    case DoubleType:
      ds = src;
      dd = dest;
      for (int i=0;i<s;i++) dd[i] = dd[i] < ds[i] ? dd[i] : ds[i];
      break;
    case CharType:
    case BoolType:
      cs = src;
      cd = dest;
      for (int i=0;i<s;i++) cd[i] = cd[i] < cs[i] ? cd[i] : cs[i];
      break;
    default:
      gtc_dprintf("gtc_reduce: unsupported reduction datatype\n");
      exit(1);
  }
}



/**
 * reduce_max - helper function to accumulate reduced data
 * @param dest  accumulation buffer
 * @param src source data
 * @param ty data type
 * @param s number of elements
 */
static void reduce_max(void *dest, void *src, gtc_datatype_t ty, int s) {
  int    *is, *id;
  long   *ls, *ld;
  double *ds, *dd;
  char   *cs, *cd;
  unsigned long *uls, *uld;

  switch(ty) {
    case IntType:
      is = src;
      id = dest;
      for (int i=0;i<s;i++) id[i] = id[i] > is[i] ? id[i] : is[i];
      break;
    case LongType:
      ls = src;
      ld = dest;
      for (int i=0;i<s;i++) ld[i] = ld[i] > ls[i] ? ld[i] : ls[i];
      break;
    case UnsignedLongType:
      uls = src;
      uld = dest;
      for (int i=0;i<s;i++) uld[i] = uld[i] > uls[i] ? uld[i] : uls[i];
      break;
    case DoubleType:
      ds = src;
      dd = dest;
      for (int i=0;i<s;i++) dd[i] = dd[i] > ds[i] ? dd[i] : ds[i];
      break;
    case CharType:
    case BoolType:
      cs = src;
      cd = dest;
      for (int i=0;i<s;i++) cd[i] = cd[i] > cs[i] ? cd[i] : cs[i];
      break;
    default:
      gtc_dprintf("gtc_reduce: unsupported reduction datatype\n");
      exit(1);
  }
}


/**
 * gtc_collective_size - helper function to return data type size for collective ops
 * @param type PDHT datatype
 * @returns size of datatype
 */
static int gtc_collective_size(gtc_datatype_t type) {
  int tysize;
  switch(type) {
    case IntType:
      tysize = sizeof(int);
      break;
    case LongType:
    case UnsignedLongType:
      tysize = sizeof(long);
      break;
    case DoubleType:
      tysize = sizeof(double);
      break;
    case CharType:
    case BoolType:
      tysize = sizeof(char);
      break;
    default:
      gtc_dprintf("gtc_reduce: unsupported reduction datatype\n");
      exit(1);
  }
  return tysize;
}

