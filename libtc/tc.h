/********************************************************/
/*                                                      */
/*  tc.h - portals task collections                     */
/*                                                      */
/*  author: d. brian larkins                            */
/*  created: 3/20/16                                    */
/*                                                      */
/* ******************************************************/

/**
 * @file
 *
 * portals tasks collection specification
 */

#pragma once
#define _XOPEN_SOURCE 700

#ifdef __cplusplus
extern "C" {
#endif

//#define WITH_MPI
#ifdef WITH_MPI
  #include <mpi.h>
#endif

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>

#include <pthread.h>

#ifndef __cplusplus
#ifdef __APPLE__
#include <portals4/pmi.h>
#else
#include <slurm/pmi.h>
#endif // pmi.h
#endif // c++

// compiler intrinsics for TSC timing, should be okay on clang, gcc, and icc
#include <x86intrin.h>

#include <portals4.h>

#include "clod.h"
#include "termination.h"
#include "mutex.h"


// queue implementation choice
#define SDC_QUEUE

#define GTC_MAX_TC              10
#define GTC_MAX_TASK_CLASSES    10
#define GTC_MAX_COUNTERS        10
#define GTC_MAX_COLLECTIONS      2
#define GTC_MAX_CHUNKS       10000
#define GTC_MAX_REDUCE_ELEMS   128
#define GTC_MAX_CLOD_CLOS      100

#define GTC_COLLECTIVE_PTES      3 // barrier, mutex, termination
#define GTC_COLLECTIVE_CTS       2
#define GTC_COLLECTIVE_INDEX     0
#define GTC_ATOMIC_CTS           2

#define GTC_TERMINATION_PTES  1
#define GTC_TERMINATION_CTS   2

#define GTC_DEFAULT_RANK_HINT -1 // use PMI-defined rank
#define GTC_BIND_TO_CORE       1
#define GTC_BIND_TO_SOCKET     1


#ifdef __INTEL_COMPILER
#pragma warning (disable:869)           /* parameter "param" was never referenced */
#pragma warning (disable:981)           /* operands are evaluated in unspecified order */
#pragma warning (disable:1418)          /* external function definition with no prior declaration */
#endif

#define UNUSED(expr) do { (void)(expr); } while (0)

#ifndef NO_SEATBELTS
#include <assert.h>
#else
#warning "NO_SEATBELTS Defined -- Building without safety check and performance analysis"
#define assert(X)
#endif

    // Enable/Disable different classes of debugging statements by OR-ing these flags
// together to form DEBUGLEVEL
#define DBGINIT    1
#define DBGPROCESS 2
#define DBGGET     4
#define DBGTD      8
#define DBGTASK    16
#define DBGSHRB    32
#define DBGINBOX   64
#define DBGGROUP   128
#define DBGSYNCH   256
#define DBGERR     512
#define DBGWARN   1024
#define DBGMP     2048

#define QUEUE_POP_HEAD    sdc_shrb_pop_head
#define QUEUE_LOCK(Q,P)   //sdc_shrb_lock
#define QUEUE_UNLOCK(Q,P) //sdc_shrb_unlock
#define QUEUE_PUSH_N_HEAD sdc_shrb_push_n_head
#define QUEUE_POP_N_TAIL  sdc_shrb_pop_n_tail
#define QUEUE_TRY_POP_N_TAIL sdc_shrb_try_pop_n_tail
#define QUEUE_WORK_AVAIL(RB) sdc_shrb_size(RB)
#define QUEUE_T           sdc_shrb_t
#define QUEUE_SIZE        sizeof(sdc_shrb_t)
#define QUEUE_ALLOC       shrb_malloc
#define QUEUE_FREE        shrb_free
#define INBOX_LOCK        shrb_lock
#define INBOX_UNLOCK      shrb_unlock

//#define GTC_DEFAULT_DEBUGLEVEL DBGERR|DBGWARN|DBGTD|DBGINIT
//#define GTC_DEFAULT_DEBUGLEVEL DBGERR|DBGINIT|DBGPROCESS|DBGGET|DBGGROUP // |DBGSHRB
//#define GTC_DEFAULT_DEBUGLEVEL DBGERR|DBGWARN|DBGINIT|DBGGET|DBGSHRB|DBGMP// |DBGTD
//#define GTC_DEFAULT_DEBUGLEVEL DBGERR|DBGWARN|DBGINIT|DBGGET|DBGSHRB// |DBGTD
#define GTC_DEFAULT_DEBUGLEVEL DBGERR|DBGWARN|DBGINIT // |DBGSHRB//|DBGTD
#ifndef GTC_DEFAULT_DEBUGLEVEL
 #define GTC_DEFAULT_DEBUGLEVEL  0
#endif

// // Execute CMD if the given debug FLAG is set
#define DEBUG(FLAG, CMD) if((FLAG) & (DEBUGLEVEL)) { CMD; }

#define gtc_lookup(_GTCLKUP) _c->tcs[_GTCLKUP]

#define SCIOTO_DEBUG
#ifdef SCIOTO_DEBUG
  #define gtc_dprintf(...) gtc_dbg_printf(__VA_ARGS__)
  #define gtc_lprintf(lvl, ...) gtc_lvl_dbg_printf(lvl, __VA_ARGS__)
  #define gtc_eprintf(lvl, ...) gtc_lvl_dbg_eprintf(lvl, __VA_ARGS__)
#else
  #define gtc_dprintf(...) while (0) {};
  #define gtc_lprintf(...) while (0) {};
  #define gtc_eprintf(...) while (0) {};
#endif

#ifndef offsetof
#define offsetof(type, member)  __builtin_offsetof (type, member)
#endif // offsetof

// internal use only
#define __GTC_COLLECTIVE_INDEX                    0
#define __GTC_SYNCH_INDEX                         1
#define __GTC_BARRIER_MATCH              0xdeadbeef
#define __GTC_REDUCE_LMATCH              0xfa570911
#define __GTC_REDUCE_RMATCH              0xfa570991
#define __GTC_BCAST_MATCH                0xfffffffe
#define __GTC_COUNTER_INDEX                       2
#define __GTC_COUNTER_MATCH              0xfeedbeef
#define __GTC_GATHER_MATCH               0x0c0113c7

#define __GTC_TERMINATION_INDEX                   3
#define __GTC_TERMINATION_USPAWN_MATCH   0xdadadada
#define __GTC_TERMINATION_UFINAL_MATCH   0xbabababa
#define __GTC_TERMINATION_DOWN_MATCH     0x1ef7f007

#define __GTC_SHRB_INDEX                          4
#define __GTC_SDC_SHRB_INDEX                      5
#define __GTC_SHRB_QUEUE_MATCH           0x005c1070
#define __GTC_SDC_SHRB_LOCK_MATCH        0x00000042
#define __GTC_SHRB_LOCK_MATCH            0x00000043

#define __GTC_SN_QUEUE_MATCH             0x05c10702

#define __GTC_MAX_STEAL_SIZE                 500000  // max # of tasks to steal when doing work splitting

//#define __GTC_RECLAIM_POLLFREQ                   2 // how often do we call ensure()/reclaim()?
#define __GTC_RECLAIM_POLLFREQ                   1 // how often do we call ensure()/reclaim()?
//#define __GTC_RECLAIM_POLLFREQ                  20 // use for UTS

// forward refs
struct task_s;
struct sdc_shrb_s;
struct shrb_s;

// basic unit typedefs
typedef int gtc_t;
typedef int task_class_t;
typedef unsigned long tc_counter_t;

// Automatically determine the body size when creating a task pool by scanning
// the list of registered tasks
#define AUTO_BODY_SIZE -1

enum victim_select_e { VICTIM_RANDOM, VICTIM_ROUND_ROBIN };
enum steal_method_e  { STEAL_HALF, STEAL_ALL, STEAL_CHUNK };
enum tc_states { STATE_WORKING = 0, STATE_SEARCHING, STATE_STEALING, STATE_INACTIVE, STATE_TERMINATED };

typedef struct {
  int stealing_enabled;          /* Is stealing enabled?  If not, the load balance is static with pushing */
  int victim_selection;          /* One of victim_select_e */
  int steal_method;              /* One of steal_method_e  */
  int steals_can_abort;          /* Allow aborting steals  */
  int max_steal_retries;         /* Max number of retries before we abort.  Set low to limit contention. -1 means infinity */
  int max_steal_attempts_local;  /* Max number of lock attempts before we "retry" a local victim. */
  int max_steal_attempts_remote; /* Max number of lock attempts before we "retry" a remote victim. */
  int chunk_size;                /* Size of a steal when using STEAL_CHUNK */
  int local_search_factor;       /* Percent of steal attempts (0-100) that should target intra-node victims */
} gtc_ldbal_cfg_t;


/* Contains information about task descriptors
 *
*/
struct task_class_desc_s {
  int body_size;
  void (*cb_execute)(gtc_t gtc, struct task_s *descriptor);
  void *pool; // Allocation pool to avoid alloc cost XXX may want to make this a linked list or do slab allocations
};
typedef struct task_class_desc_s task_class_desc_t;

/* Contains all information about the task
 *
*/
struct task_s{
  uint32_t      count;      // used by steal-half to determine chunk size
  task_class_t  task_class;
  int           created_by;
  int           affinity;
  int           priority;
  char          body[0];
};
typedef struct task_s task_t;

/** Victim selector state.  Initialize to 0.  */
typedef struct {
  int victim_retry;
  int num_retries;
  int last_victim;
} gtc_vs_state_t;

/** queue implementation type */
enum gtc_qtype_e {
  GtcQueueSDC,
  GtcQueuePortalsN,
  GtcQueuePortalsHalf
};
typedef enum gtc_qtype_e gtc_qtype_t;

/**
 *  timer counter/accumulator (we can use either clock_gettime() or rtsdc() )
 */
struct tc_tsctimer_s {
  uint64_t total;
  uint64_t last;
  uint64_t temp;
};
typedef struct tc_tsctimer_s tc_tsctimer_t;

struct tc_timer_s {
  struct timespec total;  //!< total time accumulated
  struct timespec last;   //!< keeps last start time
  struct timespec temp;   //!< for accumulation
};
typedef struct tc_timer_s tc_timer_t;

struct tc_timers_s {
  tc_timer_t process;
  tc_timer_t passive;
  tc_timer_t search;
  tc_timer_t active;
  tc_timer_t steal;
  tc_timer_t put;
  tc_timer_t get;
  tc_timer_t dispersion;
  tc_timer_t imbalance;
  tc_timer_t t[5]; // general purpose
};
typedef struct tc_timers_s tc_timers_t;

// reclaim, ensure, release, reacquire
struct tc_tsctimers_s {
  tc_tsctimer_t getbuf;
  tc_tsctimer_t add;
  tc_tsctimer_t addinplace;
  tc_tsctimer_t addfinish;
  tc_tsctimer_t progress;
  tc_tsctimer_t reclaim;
  tc_tsctimer_t ensure;
  tc_tsctimer_t release;
  tc_tsctimer_t reacquire;
  tc_tsctimer_t pushhead;
  tc_tsctimer_t poptail;
  tc_tsctimer_t getsteal;
  tc_tsctimer_t getfail;
  tc_tsctimer_t getmeta;
  tc_tsctimer_t sanity;
};
typedef struct tc_tsctimers_s tc_tsctimers_t;

/*
 * Sciotwo task queue implementation callbacks
 */
struct tqi_s {
  void     (*destroy)(gtc_t gtc);
  void     (*reset)(gtc_t gtc);
  int      (*get_buf)(gtc_t gtc, int priority, task_t *buf);
  int      (*add)(gtc_t gtc, task_t *task, int proc);
  task_t * (*inplace_create_and_add)(gtc_t gtc, task_class_t tclass);
  void     (*inplace_ca_finish)(gtc_t gtc, task_t *t);
  void     (*progress)(gtc_t gtc);
  int      (*tasks_avail)(gtc_t gtc);
  char*    (*queue_name)(void);
  int      (*pop_head)(void *b, int proc, void *buf);
  int      (*pop_n_tail)(void *b, int proc, int n, void *e, int steal_vol);
  int      (*try_pop_n_tail)(void *b, int proc, int n, void *buf, int steal_vol);
  void     (*push_n_head)(void *b, int proc, void *e, int size);
  int      (*work_avail)(void *b);
  void     (*print_stats)(gtc_t gtc);
  void     (*print_gstats)(gtc_t gtc);
};
typedef struct tqi_s tqi_t;



/*
 * Sciotwo Task Collection
 */
struct tc_s {
  tqi_t               cb;                         // implementation callbacks
  gtc_qtype_t         qtype;                      // type discriminator for queue implementation
  size_t              qsize;                      // used for common allocations, clears
  int                 valid;                      // in use flag
  void               *steal_buf;                  // buffer for performing steals (allocation not on crit path)
  int                 chunk_size;                 //  number of tasks we can steal at a time
  int                 max_body_size;
  int                 last_victim;                // Global round robin -- remember our last victim

  gtc_ldbal_cfg_t     ldbal_cfg;                  // load balancer configuration

  td_t               *td;                         // termination detection data

  struct sdc_shrb_s  *shared_rb;                  // split, deferred copy task queue
  struct shrb_s      *inbox;                      // task inbox

  // STATISTICS:
  tc_timers_t         *timers;                    // timers used for internal performance monitoring
  tc_counter_t         passive_count;             // Number of transitions to passive state
  tc_counter_t         tasks_spawned;             // Number of tasks spawned by this thread
  tc_counter_t         tasks_completed;           // Number of tasks processed by this thread
  tc_counter_t         tasks_stolen;              // Number of tasks stolen by this thread
  tc_counter_t         num_steals;                // Number of successful steals
  tc_counter_t         failed_steals_locked;      // # steal attempts that failed after locking
  tc_counter_t         failed_steals_unlocked;    // # steal attempts that failed before locking
  tc_counter_t         aborted_steals;            // # steal attempts that were aborted due to contention
  tc_counter_t         aborted_victims;           // # victims that were aborted due to contention
  tc_counter_t         dispersion_attempts_locked;   // failed_steals_locked during dispersion
  tc_counter_t         dispersion_attempts_unlocked; // failed_steals_unlocked during dispersion
  tc_counter_t         getcalls;                  // # of calls to get_buf
  tc_counter_t         getlocal;                  // # of calls resulting in local work found

  // STATE FLAGS:
  int                 state;                       // task collection state
  int                 dispersed;                   // flag: have i recieved my first task yet?
  int                 terminated;                  // flag: has the collection terminated?
  int                 external_work_avail;         // flag: used in termination detection

  clod_t              clod;                        // common local object database
};
typedef struct tc_s tc_t;


/*
 * Sciotwo global context
 */
struct gtc_context_s {
  tc_t               *tcs[GTC_MAX_TC];
  int                 open[GTC_MAX_TC];
  int                 total_tcs;
  task_class_desc_t   task_class_req[GTC_MAX_TASK_CLASSES];
  int                 task_class_count;
  int                 auto_teardown;
  ptl_handle_ni_t     lni;                 //!< logical NI
  ptl_process_t      *mapping;             //!< physical/logical NI mapping
  ptl_ni_limits_t     ni_limits;           //!< logical NI limits
  int                 dbglvl;
  int                 quiet;
  int                 size;
  int                 rank;
  int                 binding;             //!< are we binding to cores/socket?
  ptl_handle_md_t     collective_md;       //!< collective MD handle
  ptl_handle_le_t     barrier_me;          //!< barrier ME handle
  ptl_handle_ct_t     barrier_ct;          //!< barrier CT handle
  ptl_size_t          barrier_count;       //!< barrier  count
  ptl_handle_me_t     reduce_lme;          //!< reduce left ME handle
  ptl_handle_me_t     reduce_rme;          //!< reduce right ME handle
  ptl_handle_ct_t     reduce_ct;           //!< reduce CT handle
  ptl_size_t          reduce_count;        //!< broadcast count
  ptl_handle_ct_t     bcast_ct;            //!< collective CT handle
  ptl_handle_me_t     bcast_me;            //!< broadcast ME handle
  ptl_size_t          bcast_count;         //!< broadcast count
  void               *collective_lscratch; //!< scratch space for collective ops
  void               *collective_rscratch; //!< scratch space for collective ops
  void               *collective_scratch;  //!< scratch space for collective ops
};
typedef struct gtc_context_s gtc_context_t;


/* TC operatation status */
enum gtc_status_e {
  GtcStatusOK,
  GtcStatusError
};
typedef enum gtc_status_e gtc_status_t;

enum gtc_datatype_e {
  IntType,
  LongType,
  UnsignedLongType,
  DoubleType,
  CharType,
  BoolType
};
typedef enum gtc_datatype_e gtc_datatype_t;

enum gtc_reduceop_e {
  GtcReduceOpSum,
  GtcReduceOpMin,
  GtcReduceOpMax
};
typedef enum gtc_reduceop_e gtc_reduceop_t;

// Global variables
extern gtc_context_t *_c;
extern int gtc_is_initialized;
extern char *victim_methods[2];
extern char *steal_methods[3];
extern int __gtc_marker[5];
extern tc_tsctimers_t *tsctimers;


/****  Assorted utility functions  ****/

#define MAX(X,Y) ((X) > (Y)) ? X : Y
#define MIN(X,Y) ((X) < (Y)) ? X : Y


// initialization and task collection creation -- init.c
void    gtc_ldbal_cfg_init(gtc_ldbal_cfg_t *cfg);
void    gtc_ldbal_cfg_set(gtc_t gtc, gtc_ldbal_cfg_t *cfg);
void    gtc_ldbal_cfg_get(gtc_t gtc, gtc_ldbal_cfg_t *cfg);

gtc_context_t *gtc_init(void);
void           gtc_fini(void);
void           gtc_bthandler(int sig, siginfo_t *, void *);

// common queue operations -- common.c
gtc_t   gtc_create(int max_body_size, int chunk_size, int shrb_size, gtc_ldbal_cfg_t *ldbal_cfg, gtc_qtype_t qtype);
void    gtc_destroy(gtc_t gtc);
void    gtc_reset(gtc_t gtc);
void    gtc_process(gtc_t gtc);
void    gtc_print_config(gtc_t gtc);
void    gtc_print_stats(gtc_t gtc);
char   *gtc_queue_name(gtc_t gtc);

void    gtc_progress(gtc_t gtc);
int     gtc_add(gtc_t gtc, task_t *task, int proc);
int     gtc_tasks_avail(gtc_t gtc);
void    gtc_enable_stealing(gtc_t gtc);
void    gtc_disable_stealing(gtc_t gtc);
int     gtc_get_local_buf(gtc_t gtc, int priority, task_t *buf);
int     gtc_steal_tail(gtc_t gtc, int victim);
int     gtc_try_steal_tail(gtc_t gtc, int victim);
int     gtc_select_victim(gtc_t gtc, gtc_vs_state_t *state);
task_t *gtc_get(gtc_t gtc, int priority);
void    gtc_set_external_work_avail(gtc_t gtc, int flag);
task_t *gtc_task_inplace_create_and_add(gtc_t gtc, task_class_t tclass);
void    gtc_task_inplace_create_and_add_finish(gtc_t gtc, task_t *t);

unsigned long gtc_stats_tasks_completed(gtc_t gtc);
unsigned long gtc_stats_tasks_spawned(gtc_t gtc);


// Communication Completion Operations -- commsynch.c
void               gtc_collective_init(gtc_context_t *c);
void               gtc_collective_fini(void);
void               gtc_barrier(void);
gtc_status_t       gtc_reduce(void *in, void *out, gtc_reduceop_t op, gtc_datatype_t type, int elems);
gtc_status_t       gtc_allreduce(void *in, void *out, gtc_reduceop_t op, gtc_datatype_t type, int elems);
gtc_status_t       gtc_broadcast(void *buf, gtc_datatype_t type, int elems);
gtc_status_t       gtc_gather(void *in, void *out, gtc_datatype_t type, int elems, int root);

// atomics / counter support atomics.c
int                gtc_atomic_init(tc_t *ht);
void               gtc_atomic_free(tc_t *ht);
//gtc_status_t         gtc_atomic_cswap(tc_t *ht, void *key, size_t offset, int64_t *old, int64_t new);

// handle.c
gtc_t              gtc_handle_register(tc_t *tc);
tc_t              *gtc_handle_release(gtc_t gtc);

// pmi.c
void init_pmi_rank_size(int rank_hint);
void init_pmi();

// task.c
task_class_t       gtc_task_class_register(int body_size, void (*cb_execute)(gtc_t gtc, task_t *descriptor));
task_t            *gtc_task_alloc(int body_size);
task_t            *gtc_task_create(task_class_t tclass);
void               gtc_task_destroy(task_t *task);
void               gtc_task_reuse(task_t *task);
#define            gtc_task_set_priority(_TASK, _PRIORITY) (_TASK)->priority = (_PRIORITY)
#define            gtc_task_set_affinity(_TASK, _AFFINITY) (_TASK)->affinity = (_AFFINITY)
void               gtc_task_set_class(task_t *task, task_class_t tclass);
task_class_t       gtc_task_get_class(task_t *task);
int                gtc_task_class_largest_body_size(void);
task_class_desc_t *gtc_task_class_lookup(task_class_t tclass);
#define            gtc_task_body_size(TSK) gtc_task_class_lookup((TSK)->task_class)->body_size
void               gtc_task_execute(gtc_t gtc, task_t *task);
#define            gtc_task_body(TSK) (&((TSK)->body))

//util.c
static struct timespec gtc_get_wtime(void);
int                eprintf(const char *format, ...);
int                gtc_dbg_printf(const char *format, ...);
int                gtc_lvl_dbg_printf(int lvl, const char *format, ...);
int                gtc_lvl_dbg_eprintf(int lvl, const char *format, ...);
char              *gtc_ptl_error(int error_code);
char              *gtc_event_to_string(ptl_event_kind_t evtype);
void               gtc_dump_event(ptl_event_t *ev);
void               gtc_dump_event_queue(ptl_handle_eq_t eq);
void               gtc_get_mmad(double *counter, double *tot, double *min, double *max, double *avg);
void               gtc_get_mmau(tc_counter_t *counter, tc_counter_t *tot, tc_counter_t *min, tc_counter_t *max, double *avg);
void               gtc_get_mmal(long *counter, long *tot, long *min, long *max, double *avg);
char              *gtc_print_mmad(char *buf, char *unit, double stat, int total);
char              *gtc_print_mmau(char *buf, char *unit, tc_counter_t stat, int total);
char              *gtc_print_mmal(char *buf, char *unit, long stat, int total);

// collection-sdc.c
gtc_t   gtc_create_sdc(gtc_t gtc, int max_body_size, int shrb_size, gtc_ldbal_cfg_t *ldbal_cfg);
void    gtc_destroy_sdc(gtc_t gtc);
void    gtc_reset_sdc(gtc_t gtc);
char   *gtc_queue_name_sdc();
void    gtc_progress_sdc();
int     gtc_tasks_avail_sdc(gtc_t gtc);
int     gtc_get_buf_sdc(gtc_t, int priority, task_t *buf);
int     gtc_add_sdc(gtc_t gtc, task_t *task, int proc);
task_t *gtc_task_inplace_create_and_add_sdc(gtc_t gtc, task_class_t tclass);
void    gtc_task_inplace_create_and_add_finish_sdc(gtc_t gtc, task_t *t);
void    gtc_print_stats_sdc(gtc_t gtc);
void    gtc_print_gstats_sdc(gtc_t gtc);
void    gtc_queue_reset_sdc(gtc_t gtc);

// collection-sn.c
gtc_t   gtc_create_sn(gtc_t gtc, int max_body_size, int shrb_size, gtc_ldbal_cfg_t *cfg);
gtc_t   gtc_create_sh(gtc_t gtc, int max_body_size, int shrb_size, gtc_ldbal_cfg_t *cfg);
void    gtc_destroy_sn(gtc_t gtc);
void    gtc_reset_sn(gtc_t gtc);
char   *gtc_queue_name_sn(void);
char   *gtc_queue_name_sh(void);
void    gtc_progress_sn(gtc_t gtc);
void    gtc_progress_sh(gtc_t gtc);
int     gtc_tasks_avail_sn(gtc_t gtc);
int     gtc_get_buf_sn(gtc_t gtc, int priority, task_t *buf);
int     gtc_add_sn(gtc_t gtc, task_t *task, int proc);
task_t *gtc_task_inplace_create_and_add_sn(gtc_t gtc, task_class_t tclass);
void    gtc_task_inplace_create_and_add_finish_sn(gtc_t gtc, task_t *);
void    gtc_print_stats_sn(gtc_t gtc);
void    gtc_print_gstats_sn(gtc_t gtc);
void    gtc_queue_reset_sn(gtc_t gtc);

// common local object routines
clod_key_t gtc_clo_associate(gtc_t gtc, void *ptr);
void      *gtc_clo_lookup(gtc_t gtc, clod_key_t id);
void       gtc_clo_assign(gtc_t gtc, clod_key_t id, void *ptr);
void       gtc_clo_reset(gtc_t gtc);

/**
 *  gtc_mythread - gets rank of calling process
 *  @returns rank of calling process
 */
static inline long gtc_mythread(void) {
    return _c->rank;
}

/**
 *  gtc_nthreads - gets count of processors in computation
 *  @returns size of calling process
 */
static inline long gtc_nthreads(void) {
    return _c->size;
}


/* timing routines */

/**
 *    pdht_get_wtime - get a wall clock time for performance analysis
 */
static inline struct timespec gtc_get_wtime() {
    struct timespec ts;
#if __APPLE__
      clock_gettime(_CLOCK_MONOTONIC, &ts);
#else
        clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
#endif // clock_gettime
          return ts;
}

static inline uint64_t gtc_get_tsctime() {
  return __rdtsc();
}

#define TC_INIT_ATIMER(TMR) do { TMR.total.tv_sec = 0; TMR.total.tv_nsec = 0; } while (0)
#define TC_START_ATIMER(TMR) TMR.last   = gtc_get_wtime();
#define TC_STOP_ATIMER(TMR) do {\
                                  TMR.temp = gtc_get_wtime();\
                                  TMR.total.tv_sec += (TMR.temp.tv_sec - TMR.last.tv_sec);\
                                  TMR.total.tv_nsec += (TMR.temp.tv_nsec - TMR.last.tv_nsec);\
                                } while (0)
// TC_READ_TIMER returns elapsed time in nanoseconds
#define TC_READ_ATIMER(TMR)  (double)((1000000000L * (TMR.total.tv_sec)) + TMR.total.tv_nsec)
#define TC_READ_ATIMER_USEC(TMR)  TC_READ_ATIMER(TMR)/1000.0
#define TC_READ_ATIMER_MSEC(TMR)  (double)(TC_READ_ATIMER(TMR)/(double)1e6)
#define TC_READ_ATIMER_SEC(TMR)   (double)(TC_READ_ATIMER(TMR)/(double)1e9)

// TC_READ_TIMER returns elapsed time in nanoseconds
#define TC_USE_INTERNAL_TIMERS
#ifdef TC_USE_INTERNAL_TIMERS
  #define TC_INIT_TIMER(TC,TMR)  TC_INIT_ATIMER(TC->timers->TMR)
  #define TC_START_TIMER(TC,TMR) TC_START_ATIMER(TC->timers->TMR)
  #define TC_STOP_TIMER(TC,TMR)  TC_STOP_ATIMER(TC->timers->TMR)
  #define TC_READ_TIMER(TC,TMR)  TC_READ_ATIMER(TC->timers->TMR)

  #define TC_READ_TIMER_USEC(TC,TMR)  TC_READ_TIMER(TC,TMR)/1000.0
  #define TC_READ_TIMER_MSEC(TC,TMR)  TC_READ_TIMER(TC,TMR)/(double)1e6
  #define TC_READ_TIMER_SEC(TC,TMR)   TC_READ_TIMER(TC,TMR)/(double)1e9
#else
  #define TC_START_TIMER(TC,TMR)
  #define TC_STOP_TIMER(TC,TMR)
  #define TC_READ_TIMER(TC,TMR)      0

  #define TC_READ_TIMER_USEC(TC,TMR) 0
  #define TC_READ_TIMER_MSEC(TC,TMR) 0
  #define TC_READ_TIMER_SEC(TC,TMR)  0
#endif // TC_USE_INTERNAL_TIMERS

// TSC hardware counter support

#define TC_CPU_MHZ_COMET 2500.000
#define TC_CPU_MHZ_SENNA 2300.000
#define TC_CPU_HZ               TC_CPU_MHZ_COMET*(double)1e6
//#define TC_CPU_HZ               TC_CPU_MHZ_SENNA*(double)1e6
#define TC_INIT_ATSCTIMER(TMR)   do { TMR.total = 0; } while (0)
#define TC_START_ATSCTIMER(TMR)  TMR.last = gtc_get_tsctime();
#define TC_STOP_ATSCTIMER(TMR)   do {\
                                      TMR.temp = gtc_get_tsctime();\
                                      TMR.total += TMR.temp - TMR.last;\
                                } while (0)
#define TC_INIT_TSCTIMER(TMR)   TC_INIT_ATSCTIMER(tsctimers->TMR)
#define TC_START_TSCTIMER(TMR)  TC_START_ATSCTIMER(tsctimers->TMR)
#define TC_STOP_TSCTIMER(TMR)   TC_STOP_ATSCTIMER(tsctimers->TMR)

#define TC_READ_ATSCTIMER(TMR)        TMR.total
#define TC_READ_ATSCTIMER_M(TMR)     (TMR.total/1000000)
#define TC_READ_ATSCTIMER_NSEC(TMR)  (double)(TC_READ_ATSCTIMER(TMR)/(TC_CPU_HZ)) * (double)1e9
#define TC_READ_ATSCTIMER_USEC(TMR)  (double)(TC_READ_ATSCTIMER(TMR)/(TC_CPU_HZ)) * (double)1e6
#define TC_READ_ATSCTIMER_MSEC(TMR)  (double)(TC_READ_ATSCTIMER(TMR)/(TC_CPU_HZ)) * (double)1000.0
#define TC_READ_ATSCTIMER_SEC(TMR)   (double)TC_READ_ATSCTIMER(TMR)/(TC_CPU_HZ)

#define TC_READ_TSCTIMER(TMR)        TC_READ_ATSCTIMER(tsctimers->TMR)
#define TC_READ_TSCTIMER_M(TMR)      TC_READ_ATSCTIMER_M(tsctimers->TMR)
#define TC_READ_TSCTIMER_NSEC(TMR)   TC_READ_ATSCTIMER_NSEC(tsctimers->TMR)
#define TC_READ_TSCTIMER_USEC(TMR)   TC_READ_ATSCTIMER_USEC(tsctimers->TMR)
#define TC_READ_TSCTIMER_MSEC(TMR)   TC_READ_ATSCTIMER_MSEC(tsctimers->TMR)
#define TC_READ_TSCTIMER_SEC(TMR)    TC_READ_ATSCTIMER_SEC(tsctimers->TMR)

#ifdef __cplusplus
}
#endif
