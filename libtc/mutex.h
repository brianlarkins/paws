/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#pragma once
#include <portals4.h>

struct synch_scratch_s {
  int64_t oldv;
  int64_t newv;
};
typedef struct synch_scratch_s synch_scratch_t;

#define SYNCH_RMW_OP PTL_CSWAP
struct synch_mutex_s {
  void           *lockval;     //!< globally accessible lock value
  void           *scratch;     //!< landing zone for swap ops
  uint64_t        mbits;       //!< task collection id for atomic ops
  uint64_t        opcount;     //!< value for counter waits
  ptl_pt_index_t  ptindex;     //!< PT index for remote lock
  ptl_handle_me_t me;          //!< handle to lock ME
  ptl_handle_md_t md;          //!< handle to lock MD
  ptl_handle_ct_t ct;          //!< handle to lock CT
};
typedef struct synch_mutex_s synch_mutex_t;

// Statistics measuring lock contention
extern double        synch_mutex_lock_nattempts_squares;
extern unsigned long synch_mutex_lock_nattempts_last;
extern unsigned long synch_mutex_lock_nattempts_sum;
extern unsigned long synch_mutex_lock_nattempts_max;
extern unsigned long synch_mutex_lock_nattempts_min;
extern unsigned long synch_mutex_lock_ncalls_contention;
extern unsigned long synch_mutex_lock_ncalls;

void synch_init(ptl_handle_ni_t ni);
void synch_mutex_init(synch_mutex_t *m);
void synch_mutex_lock(synch_mutex_t *m, int proc);
int  synch_mutex_trylock(synch_mutex_t *m, int proc);
void synch_mutex_unlock(synch_mutex_t *m, int proc);
