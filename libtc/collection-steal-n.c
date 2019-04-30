/*
 * Copyright (C) 2018. See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <alloca.h>
#include <math.h>

#include "tc.h"

#include "sdc_shr_ring.h"
#include "shr_ring.h"
#include "sn_ring.h"
int maxchunk = 0;

/**
 * Create a new task collection.  Collective call.
 *
 * @param[in] max_body_size Max size of a task descriptor's body in bytes for this tc.
 *                         Any task that is added must be smaller or equal to this size.
 * @param[in] shrb_size    Size of the local task queue (in tasks).
 * @param[in] cfg          Load balancer configuation.  NULL for default configuration.
 *
 * @return                 Portable task collection handle.
 */
gtc_t gtc_create_sn(gtc_t gtc, int max_body_size, int shrb_size, gtc_ldbal_cfg_t *cfg) {
  tc_t  *tc;
  int chunk_size;

  tc  = gtc_lookup(gtc);

  if (cfg)
    chunk_size = cfg->chunk_size;
  else
    chunk_size = 3;

  // Allocate the shared ring buffer.  Total task size is the size
  // of the header + max_body size. - shared ring buffer is cast, but callbacks know what type to expect
  tc->shared_rb = (sdc_shrb_t *)sn_create(tc->max_body_size + sizeof(task_t), shrb_size, chunk_size);
  tc->inbox     = shrb_create(tc->max_body_size + sizeof(task_t), shrb_size);

  tc->cb.destroy                = gtc_destroy_sn;
  tc->cb.reset                  = gtc_reset_sn;
  tc->cb.get_buf                = gtc_get_buf_sn;
  tc->cb.add                    = gtc_add_sn;
  tc->cb.inplace_create_and_add = gtc_task_inplace_create_and_add_sn;
  tc->cb.inplace_ca_finish      = gtc_task_inplace_create_and_add_finish_sn;
  tc->cb.progress               = gtc_progress_sn;
  tc->cb.tasks_avail            = gtc_tasks_avail_sn;
  tc->cb.queue_name             = gtc_queue_name_sn;
  tc->cb.print_stats            = gtc_print_stats_sn;
  tc->cb.print_gstats           = gtc_print_gstats_sn;

  tc->cb.pop_head               = sn_pop_head;
  tc->cb.pop_n_tail             = sn_pop_n_tail;
  tc->cb.try_pop_n_tail         = sn_pop_n_tail;
  tc->cb.push_n_head            = sn_push_n_head;
  tc->cb.work_avail             = sn_size;

  tc->qsize = sizeof(sn_ring_t);

  gtc_barrier();

  return gtc;
}



/**
 * Create a new task collection.  Collective call.
 *
 * @param[in] max_body_size Max size of a task descriptor's body in bytes for this tc.
 *                         Any task that is added must be smaller or equal to this size.
 * @param[in] shrb_size    Size of the local task queue (in tasks).
 * @param[in] cfg          Load balancer configuation.  NULL for default configuration.
 *
 * @return                 Portable task collection handle.
 */
gtc_t gtc_create_sh(gtc_t gtc, int max_body_size, int shrb_size, gtc_ldbal_cfg_t *cfg) {
  tc_t  *tc;
  int chunk_size;

  tc  = gtc_lookup(gtc);

  if (cfg)
    chunk_size = cfg->chunk_size;
  else
    chunk_size = 3;

  // Allocate the shared ring buffer.  Total task size is the size
  // of the header + max_body size. - shared ring buffer is cast, but callbacks know what type to expect
  tc->shared_rb = (sdc_shrb_t *)sn_create(tc->max_body_size + sizeof(task_t), shrb_size, chunk_size);
  tc->inbox     = shrb_create(tc->max_body_size + sizeof(task_t), shrb_size);

  tc->cb.destroy                = gtc_destroy_sn;
  tc->cb.reset                  = gtc_reset_sn;
  tc->cb.get_buf                = gtc_get_buf_sn;
  tc->cb.add                    = gtc_add_sn;//
  tc->cb.inplace_create_and_add = gtc_task_inplace_create_and_add_sn;
  tc->cb.inplace_ca_finish      = gtc_task_inplace_create_and_add_finish_sn;
  tc->cb.progress               = gtc_progress_sh;
  tc->cb.tasks_avail            = gtc_tasks_avail_sn;
  tc->cb.queue_name             = gtc_queue_name_sh;
  tc->cb.print_stats            = gtc_print_stats_sn;
  tc->cb.print_gstats            = gtc_print_gstats_sn;

  tc->cb.pop_head               = sh_pop_head;
  tc->cb.pop_n_tail             = sh_pop_n_tail;
  tc->cb.try_pop_n_tail         = sh_pop_n_tail;
  tc->cb.push_n_head            = sn_push_n_head;
  tc->cb.work_avail             = sn_size;

  tc->qsize = sizeof(sn_ring_t);

  gtc_barrier();

  return gtc;
}



/**
 * Destroy task collection.  Collective call.
 */
void gtc_destroy_sn(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);

  sn_destroy((sn_ring_t *)tc->shared_rb);
  shrb_destroy(tc->inbox);
}



/**
 * Reset a task collection so it can be reused and removes any remaining tasks.
 * Collective call.
 */
void gtc_reset_sn(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);

  sn_reset((sn_ring_t *)tc->shared_rb);
  shrb_reset(tc->inbox);
}



/**
 * String that gives the name of this queue
 */
char *gtc_queue_name_sn() {
  return "Portals Steal-N";
}



/**
 * String that gives the name of this queue
 */
char *gtc_queue_name_sh() {
  return "Portals Steal-Half";
}



/** Invoke the progress engine.  Update work queues, balance the schedule,
 *  make progress on communication.
 */
void gtc_progress_sn(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);


  // Check the inbox for new work
  if (shrb_size(tc->inbox) > 0) {
    int   ntasks, npopped;
    void *work;

    ntasks = 100;
    work   = malloc((tc->max_body_size+sizeof(task_t))*ntasks);
    npopped= shrb_pop_n_tail(tc->inbox, _c->rank, ntasks, work, STEAL_CHUNK);

    sn_push_n_head(tc->shared_rb, _c->rank, work, npopped);

    shrb_free(work);

    gtc_lprintf(DBGINBOX, "gtc_progress: Moved %d tasks from inbox to my queue\n", npopped);
  }

  // Update the split
  sn_release((sn_ring_t *)tc->shared_rb);

  // Attempt to reclaim space
  sn_reclaim_space((sn_ring_t *)tc->shared_rb);

  ((sn_ring_t *)tc->shared_rb)->nprogress++;
}



/** Invoke the progress engine.  Update work queues, balance the schedule,
 *  make progress on communication.
 */
void gtc_progress_sh(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);
  static int cc =0;
  TC_START_TSCTIMER(progress);

  //gtc_dprintf("progressing\n");

  // Check the inbox for new work
  if (shrb_size(tc->inbox) > 0) {
    int   ntasks, npopped;
    void *work;

    ntasks = 100;
    work   = malloc((tc->max_body_size+sizeof(task_t))*ntasks);
    npopped= shrb_pop_n_tail(tc->inbox, _c->rank, ntasks, work, STEAL_CHUNK);

    sn_push_n_head(tc->shared_rb, _c->rank, work, npopped);

    shrb_free(work);

    gtc_lprintf(DBGINBOX, "gtc_progress: Moved %d tasks from inbox to my queue\n", npopped);
  }

  //gtc_dprintf("releasing\n");

  // Update the split
  sh_release((sn_ring_t *)tc->shared_rb);
  //if (maxchunk < sn_size((sn_ring_t *)tc->shared_rb)) maxchunk = sn_size((sn_ring_t *)tc->shared_rb);

  // Attempt to reclaim space
  if ((cc++ % ((sn_ring_t *)tc->shared_rb)->reclaimfreq) == 0)
    sn_reclaim_space((sn_ring_t *)tc->shared_rb);

  ((sn_ring_t *)tc->shared_rb)->nprogress++;
  TC_STOP_TSCTIMER(progress);
}



/**
 * Number of tasks available in local task collection.  Note, this is an
 * approximate number since we're not locking the data structures.
 */
int gtc_tasks_avail_sn(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);

  return sn_size((sn_ring_t *)tc->shared_rb) + shrb_size(tc->inbox);
}


/**
 * Find work to do, search everywhere. Use when you write your own
 * gtc_process() implementation.  Do NOT use together with
 * gtc_process().  This function invokes load balancing to attempt to locate
 * work when none is available locally.  It returns NULL only when global
 * termination has been detected.
 *
 * @param tc       IN Ptr to task collection
 * @return         Ptr to task (from local queue or stolen). NULL if none
 *                 found.  A NULL result here means that global termination
 *                 has occurred.  Returned buffer should be deleted by the user.
 */
extern double gtc_get_dummy_work;
int gtc_get_buf_sn(gtc_t gtc, int priority, task_t *buf) {
  tc_t   *tc = gtc_lookup(gtc);
  int     got_task = 0;
  int     v, steal_size;
  int     passive = 0;
  int     searching = 0;
  gtc_vs_state_t vs_state = {0};

  tc->getcalls++;
  TC_START_TSCTIMER(getbuf);

  // Invoke the progress engine
  gtc_progress(gtc);

  // Try to take my own work first.  We take from the head of our own queue.
  // When we steal, we take work off of the tail of the victim's queue.
  got_task = gtc_get_local_buf(gtc, priority, buf);

  // Time dispersion.  If I had work to start this should be ~0.
  if (!tc->dispersed) TC_START_TIMER(tc, dispersion);

  // No more local work, try to steal some
  if (!got_task && tc->ldbal_cfg.stealing_enabled) {
    //gtc_lprintf(DBGGET, " Thread %d: gtc_get() searching for work\n", _c->rank);

#ifndef NO_SEATBELTS
    TC_START_TIMER(tc, passive);
    TC_INIT_TIMER(tc, imbalance);
    TC_START_TIMER(tc, imbalance);
    passive = 1;
    tc->passive_count++;
#endif

    // Keep searching until we find work or detect termination
    while (!got_task && !tc->terminated) {

      tc->state = STATE_SEARCHING;

      if (!searching) {
        TC_START_TIMER(tc, search);
        searching = 1;
      }

      // Select the next victim
      v = gtc_select_victim(gtc, &vs_state);

      tc->state = STATE_STEALING;

      // attempt remote steal
      steal_size = gtc_steal_tail(gtc, v);

      // Steal succeeded: Got some work from remote node
      if (steal_size > 0) {
        tc->tasks_stolen += steal_size;
        tc->num_steals += 1;
        tc->last_victim = v;

        // Steal failed: no longer any work on remote node
      } else { //  (steal_size == 0) {
        tc->failed_steals_unlocked++;
      }

      // Invoke the progress engine
      gtc_progress(gtc);

      // Still no work? Lock to be sure and check for termination.
      // Locking is only needed here if we allow pushing.
      if (gtc_tasks_avail(gtc) == 0 && !tc->external_work_avail) {
        td_set_counters(tc->td, tc->tasks_spawned, tc->tasks_completed);
        tc->terminated = td_attempt_vote(tc->td);
      } else if (gtc_tasks_avail(gtc)) {
        got_task = gtc_get_local_buf(gtc, priority, buf);
      }
    }
  } else {
    tc->getlocal++;
  }

#ifndef NO_SEATBELTS
  if (passive) TC_STOP_TIMER(tc, passive);
  if (passive) TC_STOP_TIMER(tc, imbalance);
  if (searching) TC_STOP_TIMER(tc, search);
#endif

  // Record how many attempts it took for our first get, this is the number of
  // attempts during the work dispersion phase.
  if (!tc->dispersed) {
    if (passive) TC_STOP_TIMER(tc, dispersion);
    tc->dispersed = 1;
    tc->dispersion_attempts_unlocked = tc->failed_steals_unlocked;
    tc->dispersion_attempts_locked   = tc->failed_steals_locked;
  }

  TC_STOP_TSCTIMER(getbuf);
  gtc_lprintf(DBGGET, " Thread %d: gtc_get() %s\n", _c->rank, got_task? "got work":"no work");
  if (got_task) tc->state = STATE_WORKING;
  return got_task;
}



/**
 * Add task to the task collection.  Task is copied in and task buffer is available
 * to the user when call returns.  Non-collective call.
 *
 * @param tc       IN Ptr to task collection
 * @param proc     IN Process # whose task collection this task is to be added
 *                    to. Common case is tc->procid
 * @param task  INOUT Task to be added. user manages buffer when call
 *                    returns. Preferably allocated in ARMCI local allocated memory when
 *                    proc != tc->procid for improved RDMA performance.  This call fills
 *                    in task field and the contents of task will match what is in the queue
 *                    when the call returns.
 *
 * @return 0 on success.
 */
int gtc_add_sn(gtc_t gtc, task_t *task, int proc) {
  tc_t *tc = gtc_lookup(gtc);

  assert(gtc_task_body_size(task) <= tc->max_body_size);
  assert(tc->state != STATE_TERMINATED);

  TC_START_TSCTIMER(add);
  task->created_by = _c->rank;

  if (proc == _c->rank) {
    // Local add: put it straight onto the local work list
    sn_push_head((sn_ring_t *)tc->shared_rb, _c->rank, task, sizeof(task_t) + gtc_task_body_size(task));
  } else {
    // Remote adds: put this in the remote node's inbox
    if (task->affinity == 0)
      shrb_push_head(tc->inbox, proc, task, sizeof(task_t) + gtc_task_body_size(task));
    else
      shrb_push_tail(tc->inbox, proc, task, sizeof(task_t) + gtc_task_body_size(task));
  }
  //sh_release((sn_ring_t *)tc->shared_rb); // breaks under steal-N

  ++tc->tasks_spawned;
  TC_STOP_TSCTIMER(add);

  return 0;
}


/**
 * Create-and-add a task in-place on the head of the queue.  Note, you should
 * not do *ANY* other queue operations until all outstanding in-place creations
 * have finished.  The pointer returned points directly to an element in the
 * queue.  Do not add it, do not free it, discard the pointer when you are finished
 * assigning the task body.
 *
 * @param gtc    Portable reference to the task collection
 * @param tclass Desired task class
 */
task_t *gtc_task_inplace_create_and_add_sn(gtc_t gtc, task_class_t tclass) {
  tc_t   *tc = gtc_lookup(gtc);
  task_t *t;

  TC_START_TSCTIMER(addinplace);
  //assert(gtc_group_steal_ismember(gtc)); // Only masters can do this

  t = (task_t*) sn_alloc_head((sn_ring_t *)tc->shared_rb);
  gtc_task_set_class(t, tclass);

  t->created_by = _c->rank;
  t->affinity   = 0;
  t->priority   = 0;

  ++tc->tasks_spawned;
  TC_STOP_TSCTIMER(addinplace);

  return t;
}


/**
 * Complete an in-place task creation.  Note, you should not do *ANY* other
 * queue operations until all outstanding in-place creations have finished.
 *
 * @param gtc    Portable reference to the task collection
 * @param task   The pointer that was returned by inplace_create_and_add()
 */
void gtc_task_inplace_create_and_add_finish_sn(gtc_t gtc, task_t *t) {
  tc_t   *tc = gtc_lookup(gtc);
  // TODO: Maintain a counter of how many are outstanding to avoid corruption at the
  // head of the queue
  TC_START_TSCTIMER(addfinish);

  // Can't release until the inplace op completes
  tc->cb.progress(gtc);
  TC_STOP_TSCTIMER(addfinish);
}



/**
 * Print stats for this task collection.
 * @param tc       IN Ptr to task collection
 */
void gtc_print_stats_sn(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);
  sn_ring_t *rb = (sn_ring_t *)tc->shared_rb;
  double total = TC_READ_TIMER_MSEC(tc,t[0]) + TC_READ_TIMER_MSEC(tc,t[1]) +
    TC_READ_ATIMER_MSEC(rb->reltime) + TC_READ_ATIMER_MSEC(rb->acqtime);

  double perget, peradd, perinplace, perfinish, perprogress, perreclaim, perensure, perrelease, perreacquire, perpoptail;

  if (!getenv("SCIOTO_DISABLE_STATS") && !getenv("SCIOTO_DISABLE_PERNODE_STATS")) {
    // avoid floating point exceptions...
    perget       = tc->getcalls      != 0 ? TC_READ_TSCTIMER_USEC(getbuf)    / tc->getcalls      : 0;
    peradd       = tc->tasks_spawned != 0 ? TC_READ_TSCTIMER_USEC(add)       / tc->tasks_spawned : 0;
    perinplace   = tc->tasks_spawned != 0 ? TC_READ_TSCTIMER_USEC(addinplace)/ tc->tasks_spawned : 0; // borrowed
    perfinish    = rb->nprogress     != 0 ? TC_READ_TSCTIMER_USEC(addfinish) / rb->nprogress     : 0; // borrowed, but why?
    perprogress  = rb->nprogress     != 0 ? TC_READ_TSCTIMER_USEC(progress)  / rb->nprogress     : 0;
    perreclaim   = rb->nreccalls     != 0 ? TC_READ_TSCTIMER_USEC(reclaim)   / rb->nreccalls     : 0;
    perensure    = rb->nensure       != 0 ? TC_READ_TSCTIMER_USEC(ensure)    / rb->nensure       : 0;
    perrelease   = rb->nrelease      != 0 ? TC_READ_TSCTIMER_USEC(release)   / rb->nrelease      : 0;
    perreacquire = rb->nreacquire    != 0 ? TC_READ_TSCTIMER_USEC(reacquire) / rb->nreacquire    : 0;
    perpoptail   = rb->ngets         != 0 ? TC_READ_TSCTIMER_USEC(poptail)   / rb->ngets         : 0;

    printf(" %4d -     Steal-N: nrelease %6lu, nreacquire %6lu, nreclaimed %6lu (%6lu att), nprogress %6lu nensure %6lu\n"
           " %4d -            : failed steals: %6lu, disabled steals: %6lu maxqsize: %d max_steal: %d\n"
           " %4d -            : gets: %6lu (%5.2fus/get), steals: %6lu, neoq: %6lu disabled: %6lu dropped: %6lu mpseek: %7.5f\n"
           " %4d -            : total: %5.2f ms steals: %5.2f ms fails: %5.2f release: %5.2f ms acquire: %5.2f\n",
        _c->rank,
          rb->nrelease, rb->nreacquire, rb->nreclaimed, rb->nreccalls, rb->nprogress, rb->nensure,
        _c->rank,
          tc->failed_steals_unlocked, tc->aborted_steals, maxchunk, rb->max_steal,
        _c->rank,
          rb->ngets, TC_READ_TIMER_USEC(tc, t[0])/(double)rb->ngets, rb->nsteals, rb->neoq, rb->ndisabled,
          rb->ndropped, ((double)rb->nmptrav)/(rb->nmpseek ? (double)rb->nmpseek : 1.0),
        _c->rank,
          total, TC_READ_TIMER_MSEC(tc, t[0]), TC_READ_TIMER_MSEC(tc, t[1]), TC_READ_ATIMER_MSEC(rb->reltime), TC_READ_ATIMER_MSEC(rb->acqtime));
    printf(" %4d - TSC: get: %.2f ms (%.2f us x %"PRIu64")  add: %.2f ms (%.2f us x %"PRIu64") inplace: %.2f ms (%.2f us)\n",
        _c->rank,
        TC_READ_TSCTIMER_MSEC(getbuf), perget, tc->getcalls,
        TC_READ_TSCTIMER_MSEC(add), peradd, tc->tasks_spawned,
        TC_READ_TSCTIMER_MSEC(addinplace), perinplace);
    printf(" %4d - TSC: addfinish: %.2f ms (%.2f us) progress: %.2f us (%.2f us x %"PRIu64") reclaim: %.2f us (%.2f us x %"PRIu64")\n",
        _c->rank,
        TC_READ_TSCTIMER_MSEC(addfinish), perfinish,
        TC_READ_TSCTIMER_USEC(progress), perprogress, rb->nprogress,
        TC_READ_TSCTIMER_USEC(reclaim), perreclaim, rb->nreccalls);
    printf(" %4d - TSC: ensure: %.2f us (%.2f us x %"PRIu64") release: %.2f us (%.2f us x %"PRIu64") "
           "reacquire: %.2f us (%.2f us x %"PRIu64")\n",
        _c->rank,
        TC_READ_TSCTIMER_USEC(ensure), perensure, rb->nensure,
        TC_READ_TSCTIMER_USEC(release), perrelease, rb->nrelease,
        TC_READ_TSCTIMER_USEC(reacquire), perreacquire, rb->nreacquire);
    printf(" %4d - TSC: pushhead: %.2f us (%"PRIu64") poptail: %.2f us (%.2f us x %"PRIu64")\n",
        _c->rank,
        TC_READ_TSCTIMER_USEC(pushhead), (uint64_t)0,
        TC_READ_TSCTIMER_USEC(poptail), perpoptail, rb->ngets);
  }
}



/**
 * Print global stats for this task collection.
 * @param tc       IN Ptr to task collection
 */
void gtc_print_gstats_sn(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);
  sn_ring_t *rb = (sn_ring_t *)tc->shared_rb;
  char buf1[200], buf2[200], buf3[200];
  double perprogress, perreclaim, perensure, perrelease, perreacquire, perpoptail;
  // avoid floating point exceptions...
  perpoptail   = rb->ngets         != 0 ? TC_READ_TSCTIMER_MSEC(poptail)   / rb->ngets         : 0.0;
  perprogress  = rb->nprogress     != 0 ? TC_READ_TSCTIMER_USEC(progress)  / rb->nprogress     : 0.0;
  perreclaim   = rb->nreccalls     != 0 ? TC_READ_TSCTIMER_USEC(reclaim)   / rb->nreccalls     : 0.0;
  perensure    = rb->nensure       != 0 ? TC_READ_TSCTIMER_USEC(ensure)    / rb->nensure       : 0.0;
  perreacquire = rb->nreacquire    != 0 ? TC_READ_TSCTIMER_USEC(reacquire) / rb->nreacquire    : 0.0;
  perrelease   = rb->nrelease      != 0 ? TC_READ_TSCTIMER_USEC(release)   / rb->nrelease      : 0.0;

  eprintf("        : gets         %-32stime %35s per %s\n",
      gtc_print_mmau(buf1, "", rb->ngets, 1),
      gtc_print_mmad(buf2, "ms", TC_READ_TSCTIMER_MSEC(poptail), 0),
      gtc_print_mmad(buf3, "us", perpoptail, 0));
  eprintf("        :   localget   %-32s\n", gtc_print_mmau(buf1, "", tc->getlocal, 1));
  eprintf("        :   steals     %-32s time %35s\n",
      gtc_print_mmau(buf1, "", rb->nsteals, 1),
      gtc_print_mmad(buf2, "ms", TC_READ_TSCTIMER_MSEC(getsteal), 0));
  eprintf("        :   fails      %-32s time %35s\n",
      gtc_print_mmau(buf1, "", rb->neoq, 1),
      gtc_print_mmad(buf2, "ms", TC_READ_TSCTIMER_MSEC(getfail), 0));
  eprintf("        : progress   %32s  time %35s    per %s\n",
      gtc_print_mmau(buf1, "", rb->nprogress, 0),
      gtc_print_mmad(buf2, "us", TC_READ_TSCTIMER_USEC(progress), 0),
      gtc_print_mmad(buf3, "us", perprogress, 0));
  eprintf("        : reclaim    %32s  time %35s    per %s\n",
      gtc_print_mmau(buf1, "", rb->nreccalls, 0),
      gtc_print_mmad(buf2, "us", TC_READ_TSCTIMER_USEC(reclaim), 0),
      gtc_print_mmad(buf3, "us", perreclaim, 0));
  eprintf("        : ensure     %32s  time %35s    per %s\n",
      gtc_print_mmau(buf1, "", rb->nensure, 0),
      gtc_print_mmad(buf2, "us", TC_READ_TSCTIMER_USEC(ensure), 0),
      gtc_print_mmad(buf3, "us", perensure, 0));
  eprintf("        : reacquire  %32s  time %35s    per %s\n",
      gtc_print_mmau(buf1, "", rb->nreacquire, 0),
      gtc_print_mmad(buf2, "us", TC_READ_TSCTIMER_USEC(reacquire), 0),
      gtc_print_mmad(buf3, "us", perreacquire, 0));
  eprintf("        : release    %32s  time %35s    per %s\n",
      gtc_print_mmau(buf1, "", rb->nrelease, 0),
      gtc_print_mmad(buf2, "us", TC_READ_TSCTIMER_USEC(release), 0),
      gtc_print_mmad(buf3, "us", perrelease, 0));

#if 0
  unsigned long tRelease, tReacquire, tProgress, tgets;
  double tgettime, mingettime, maxgettime, gettime, perget;
  gtc_reduce(&rb->nrelease,   &tRelease,    GtcReduceOpSum, LongType, 1);
  gtc_reduce(&rb->nreacquire, &tReacquire,  GtcReduceOpSum, LongType, 1);
  gtc_reduce(&rb->nprogress,  &tProgress,   GtcReduceOpSum, LongType, 1);

  gettime = TC_READ_TIMER_USEC(tc, t[0]);
  perget   = gettime / (double)rb->ngets;
  gtc_reduce(&rb->ngets,      &tgets,       GtcReduceOpSum, LongType, 1);
  gtc_reduce(&gettime,        &tgettime,    GtcReduceOpSum, DoubleType, 1);
  gtc_reduce(&perget,         &mingettime,  GtcReduceOpMin, DoubleType, 1);
  gtc_reduce(&perget,         &maxgettime,  GtcReduceOpMax, DoubleType, 1);
  unsigned long tmaxchunk;
  gtc_reduce(&maxchunk,       &tmaxchunk,   GtcReduceOpMax, LongType, 1);

  eprintf("   SNH:  nrelease: %6lu nreacq: %6lu nprogress: %6lu\n", tRelease, tReacquire, tProgress);
  eprintf("   SNH:  avg release: %6lu avg reacq: %6lu avg progress: %6lu\n", tRelease/_c->size, tReacquire/_c->size, tProgress/_c->size);
  eprintf("   SNH:  gettime: avg: %5.2f us max: %5.2f us min: %5.2f us maxchunk: %d\n", tgettime/(double)tgets, maxgettime, mingettime, tmaxchunk);
#endif
}



/**
 * Delete all tasks in my patch of the task collection.  Useful when
 * simulating failure.
 */
void gtc_queue_reset_sn(gtc_t gtc) {
  tc_t *tc = gtc_lookup(gtc);

  // Clear out the ring buffer
  sn_reset((sn_ring_t *)tc->shared_rb);

  // Clear out the inbox
  shrb_lock(tc->inbox, _c->rank);
  shrb_reset(tc->inbox);
  shrb_unlock(tc->inbox, _c->rank);
}
