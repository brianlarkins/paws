/********************************************************/
/*                                                      */
/*  init.c - scioto portals initialization              */
/*                                                      */
/*  author: d. brian larkins                            */
/*  created: 4/25/18                                    */
/*                                                      */
/********************************************************/

#define _GNU_SOURCE 1
#define _XOPEN_SOURCE 700
#include <alloca.h>
#include <assert.h>
#include <execinfo.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <time.h>
#include <tc.h>
#include <sys/stat.h>
#include <ucontext.h>

#include "shr_ring.h"
#include "sdc_shr_ring.h"

int rankno = -1;
int __gtc_marker[5] = { 0, 0, 0, 0, 0};

gtc_context_t *gtc_init(void);
void gtc_fini(void);
static void gtc_exit_handler(void);

// defined in pmi.c
void init_only_barrier(void);

gtc_context_t *_c;
int gtc_is_initialized = 0;
tc_tsctimers_t *tsctimers = NULL;

/**
 * @file
 *
 * portals distributed load balancer startup/shutdown
 */

/**
 * usage:
 * gtc_task_class_register()
 * gtc_create()
 *   - gtc_task_create()  // new task
 *   - gtc_task_body()    // set task cb params
 *   - gtc_add()          // add to active pool
 *   - gtc_task_destroy() // free setup task
 * gtc_destroy(gtc);
 */


/** Set the behavior of the load balancer.
  */
void gtc_ldbal_cfg_set(gtc_t gtc, gtc_ldbal_cfg_t *cfg) {
  tc_t *tc = gtc_lookup(gtc);

  assert(cfg->victim_selection == VICTIM_RANDOM || cfg->victim_selection == VICTIM_ROUND_ROBIN);
  assert(cfg->steal_method == STEAL_HALF || cfg->steal_method == STEAL_ALL || cfg->steal_method == STEAL_CHUNK);
  assert(cfg->max_steal_retries >= 0);
  assert(cfg->max_steal_attempts_local >= 0);
  assert(cfg->max_steal_attempts_remote >= 0);
  assert(cfg->chunk_size >= 1);
  assert(cfg->local_search_factor >= 0 && cfg->local_search_factor <= 100);

  tc->ldbal_cfg = *cfg;
}


/** Get the behavior of the load balancer.
  */
void gtc_ldbal_cfg_get(gtc_t gtc, gtc_ldbal_cfg_t *cfg) {
  tc_t *tc = gtc_lookup(gtc);

  *cfg = tc->ldbal_cfg;
}


/** Set up a ldbal_cfg struct with the default values.
  */
void gtc_ldbal_cfg_init(gtc_ldbal_cfg_t *cfg) {
  cfg->stealing_enabled    = 1;
  cfg->victim_selection    = VICTIM_RANDOM;
  cfg->steal_method        = STEAL_HALF;
  cfg->steals_can_abort    = 1;
  cfg->max_steal_retries   = 5;
  cfg->max_steal_attempts_local = 1000;
  cfg->max_steal_attempts_remote= 10;
  cfg->chunk_size          = 1;
  cfg->local_search_factor = 75;
}


/**
 * gtc_init - initializes sciotwo system
 */
gtc_context_t *gtc_init(void) {
  ptl_ni_limits_t ni_req_limits;
  int stderrfd = -1;
  int ret;
  int max_ptes, max_mds;
  char *envp = NULL;
  int nproc = sysconf(_SC_NPROCESSORS_ONLN);
  int cores_per_socket;
  cpu_set_t mask;
  int mycore;
  char progcore[10];

  stderrfd = dup2(STDERR_FILENO,stderrfd);
  // turn off output buffering for everyone's sanity
  setbuf(stdout, NULL);

  //setenv("PTL_LOG_LEVEL","1",1);  // set c->verbosity, in gtc_init
  //setenv("PTL_DEBUG","3",1);      // don't do this
  setenv("PTL_IGNORE_UMMUNOTIFY", "1", 1); // needed on non-ummunotify systems
  setenv("PTL_PROGRESS_NOSLEEP","1",1);
  setenv("PTL_NUM_SBUF","2000",1);

  // check for explicit core binding schema
  char *corebinding = getenv("SCIOTO_CORE_BINDING");


  _c = (gtc_context_t *)malloc(sizeof(gtc_context_t));
  memset(_c,0,sizeof(gtc_context_t));

  // need _c->rank and _c->size to handle core-binding
  init_pmi_rank_size(GTC_DEFAULT_RANK_HINT);

  _c->binding = 0;

  if (corebinding) {
    if (!strncmp(corebinding, "core",strlen("core")))
      _c->binding = 1;
    else if (!strncmp(corebinding, "socket", strlen("socket")))
      _c->binding = 2;
    else if (!strncmp(corebinding, "none", strlen("none")))
      _c->binding = 0;
    else if (!strncmp(corebinding, "equal", strlen("equal")))
      _c->binding = nproc/2;
    else
      _c->binding = 0;

    switch (_c->binding) {
      case 1:
        // assume that slurm pins to cores 0..ncores-2
        //mycore = _c->rank % (nproc-1);
        snprintf(progcore, sizeof(progcore), "%d", nproc-1);
        setenv("PTL_BIND_PROGRESS", progcore, 1);
        break;

      case 2:
        cores_per_socket = nproc/2; // assumes 2 sockets per node
        mycore = _c->rank % (nproc - 2);

        if (mycore < (cores_per_socket-1))
          snprintf(progcore, sizeof(progcore), "%d", cores_per_socket-1);
        else {
          snprintf(progcore, sizeof(progcore), "%d", nproc-1);
          mycore++;
        }

        CPU_ZERO(&mask);
        CPU_SET(mycore, &mask);
        if (sched_setaffinity(0, sizeof(cpu_set_t), &mask) == -1) {
          perror("sched_setaffinity");
          goto error;
        }
        setenv("PTL_BIND_PROGRESS", progcore, 1);
        break;

      default:
        mycore = 2 * (_c->rank % (nproc/2));
        snprintf(progcore, sizeof(progcore), "%d", mycore+1);
        break;
    }


    char hoststr[100];
    gethostname(hoststr, 100);
    //gtc_dprintf("binding rank: %d to core %d and progress: %s on host %s\n", _c->rank, mycore, progcore, hoststr);
  }


  _c->total_tcs = -1;
  for (int i=0; i< GTC_MAX_TC; i++) {
    _c->tcs[i] = NULL;
  }
  _c->dbglvl = GTC_DEFAULT_DEBUGLEVEL;
  _c->quiet  = 1;

  atexit(gtc_exit_handler);

  // register backtrace handler
  struct sigaction sa;
  sa.sa_sigaction = (void *)gtc_bthandler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESTART | SA_SIGINFO;

  sigaction(SIGSEGV, &sa, NULL);
  sigaction(SIGINT, &sa, NULL);

  ret = PtlInit();
  if (ret != PTL_OK) {
    gtc_dprintf("portals initialization error\n");
    exit(-1);
  }

  // deal with special case Portals limits
  envp = getenv("GTC_MAX_PTES");
  if (envp) {
    max_ptes = (2*GTC_MAX_COLLECTIONS) + GTC_COLLECTIVE_PTES + 1;
    max_ptes += atoi(envp);
    max_mds  = 1024 + (4*max_ptes); // termination detection uses 4 MDs per PTE
  } else {
    max_ptes = (2*GTC_MAX_COLLECTIONS) + GTC_COLLECTIVE_PTES + 1;
    max_mds  = 1024;
  }

  // request portals NI limits
  ni_req_limits.max_entries = GTC_MAX_CHUNKS + GTC_COLLECTIVE_CTS + GTC_ATOMIC_CTS + 1;
  ni_req_limits.max_unexpected_headers = 1024;
  ni_req_limits.max_mds = max_mds;
  ni_req_limits.max_eqs = GTC_MAX_COLLECTIONS + 3;
  ni_req_limits.max_cts = (GTC_MAX_COLLECTIONS * GTC_MAX_CHUNKS) + GTC_COLLECTIVE_CTS + GTC_ATOMIC_CTS + 1;
  ni_req_limits.max_pt_index = max_ptes;
  ni_req_limits.max_iovecs = 1024;
  ni_req_limits.max_list_size = (GTC_MAX_COLLECTIONS * GTC_MAX_CHUNKS);
  ni_req_limits.max_triggered_ops = 1000;
  ni_req_limits.max_msg_size = LONG_MAX;
  ni_req_limits.max_atomic_size = 512;
  ni_req_limits.max_fetch_atomic_size = 512;
  ni_req_limits.max_waw_ordered_size = 512;
  ni_req_limits.max_war_ordered_size = 512;
  ni_req_limits.max_volatile_size = 512;
  ni_req_limits.features = 0;


  // create matching logical NI
  ret = PtlNIInit(PTL_IFACE_DEFAULT,
      PTL_NI_MATCHING | PTL_NI_LOGICAL,
      PTL_PID_ANY,
      &ni_req_limits,
      &(_c->ni_limits),
      &(_c->lni));

  if (ret != PTL_OK) {
    gtc_eprintf(DBGERR, "Portals logical NI initialization error: (%s)\n", gtc_ptl_error(ret));
    goto error;
  }

  init_pmi();

  if (!_c->quiet) {
    gtc_eprintf(DBGINIT, "Initializing Portals 4 Network Interface\n");
    gtc_eprintf(DBGINIT, "\tmax_entries: %d\n", _c->ni_limits.max_entries);
    gtc_eprintf(DBGINIT, "\tmax_unexpected_headers: %d\n", _c->ni_limits.max_unexpected_headers);
    gtc_eprintf(DBGINIT, "\tmax_mds: %d\n", _c->ni_limits.max_mds);
    gtc_eprintf(DBGINIT, "\tmax_eqs: %d\n", _c->ni_limits.max_eqs);
    gtc_eprintf(DBGINIT, "\tmax_cts: %d\n", _c->ni_limits.max_cts);
    gtc_eprintf(DBGINIT, "\tmax_pt_index: %d\n", _c->ni_limits.max_pt_index);
    gtc_eprintf(DBGINIT, "\tmax_iovecs: %d\n", _c->ni_limits.max_iovecs);
    gtc_eprintf(DBGINIT, "\tmax_list_size: %d\n", _c->ni_limits.max_list_size);
    gtc_eprintf(DBGINIT, "\tmax_triggered_ops: %d\n", _c->ni_limits.max_triggered_ops);
    gtc_eprintf(DBGINIT, "\tmax_msg_size: %d\n", _c->ni_limits.max_msg_size);
    gtc_eprintf(DBGINIT, "\tmax_atomic_size: %d\n", _c->ni_limits.max_atomic_size);
    gtc_eprintf(DBGINIT, "\tmax_fetch_atomic_size: %d\n", _c->ni_limits.max_fetch_atomic_size);
    gtc_eprintf(DBGINIT, "\tmax_waw_ordered_size: %d\n", _c->ni_limits.max_waw_ordered_size);
    gtc_eprintf(DBGINIT, "\tmax_war_ordered_size: %d\n", _c->ni_limits.max_war_ordered_size);
    gtc_eprintf(DBGINIT, "\tmax_volatile_size: %d\n", _c->ni_limits.max_volatile_size);
  }

  ret = PtlSetMap(_c->lni, _c->size, _c->mapping);
  if (ret != PTL_OK) {
    gtc_eprintf(DBGERR, "Portals physical/logical mapping failed : %s.\n", gtc_ptl_error(ret));
    goto error;
  }
  rankno = _c->rank;

  /*
   * have to call one more PMI/MPI barrier to guarantee we have our own counters
   * ready for our internal collective operations...
   */
  gtc_collective_init(_c);
  init_only_barrier(); // safe to use gtc_barrier() after this

  // need to initialize the synch mutex system, so there's a PTE to use, etc.
  synch_init(_c->lni);

  // remember how global initialization is done to decide auto teardown vs. explicit cleanup
  _c->auto_teardown = (gtc_is_initialized == -1) ? 1 : 0;
  gtc_is_initialized = 1; // mark ourselves as initialized in either case

  // initialize high-speed timers
  if (!tsctimers) {
    tsctimers = calloc(1,sizeof(tc_tsctimers_t));
    TC_INIT_TSCTIMER(getbuf);
    TC_INIT_TSCTIMER(add);
    TC_INIT_TSCTIMER(addinplace);
    TC_INIT_TSCTIMER(addfinish);
    TC_INIT_TSCTIMER(progress);
    TC_INIT_TSCTIMER(reclaim);
    TC_INIT_TSCTIMER(ensure);
    TC_INIT_TSCTIMER(release);
    TC_INIT_TSCTIMER(reacquire);
    TC_INIT_TSCTIMER(pushhead);
    TC_INIT_TSCTIMER(poptail);
    TC_INIT_TSCTIMER(getsteal);
    TC_INIT_TSCTIMER(getfail);
    TC_INIT_TSCTIMER(getmeta);
    TC_INIT_TSCTIMER(sanity);
  }

  return _c;

error:
  if (_c->mapping)
    free(_c->mapping);
  exit(-1);
}



/**
 * gtc_fini - initializes sciotwo system
 */
void gtc_fini(void) {
  // free up collective initialization stuff (PT Entry, MD)
  gtc_collective_fini();

  PtlNIFini(_c->lni);
  if (_c->mapping)
    free(_c->mapping);
  PtlFini();

  free(_c);
  _c = NULL;
}



static void gtc_exit_handler(void) {
   ;
}



/*
 * backtrace handler
 */
void gtc_bthandler(int sig, siginfo_t *si, void *vctx) {
  void *a[100];
  char sprog[256], str[256];
  size_t size;
  ucontext_t *ctx = (ucontext_t *)vctx;
  char **msgs = NULL, *pstr = NULL;
  FILE *fp = NULL;

  size = backtrace(a, 100);
  printf("rank: %d pid : %d signal: %d marker: %d %d %d %d %d\n", _c->rank, getpid(), sig,
      __gtc_marker[0], __gtc_marker[1], __gtc_marker[2], __gtc_marker[3], __gtc_marker[4]);
  fflush(stdout);

  //backtrace_symbols_fd(a,size, STDERR_FILENO);
  msgs = backtrace_symbols(a, size);
  a[1] = (void *)ctx->uc_mcontext.gregs[REG_RIP];
  for (int i=1; i<size; i++) {
    size_t p = 0;
    while (msgs[i][p] != '(' && msgs[i][p] != ' ' && msgs[i][p] != 0)
      p++;
    sprintf(sprog, "/usr/bin/addr2line %p -e %.*s 2>&1", a[i], (int)p, msgs[i]);
    //fp = popen(sprog, "r");
    if (fp) {
      printf("%d: %d %d %p %p\n", _c->rank, (int)p, (int)size, str, fp); fflush(stdout);
      pstr = fgets(str, 256, fp);
      printf("%d: %d %d%5s\n", _c->rank, (int)p, (int)size, str); fflush(stdout);
      if (pstr && (str[0] != '?'))
        gtc_dprintf(" (backtrace) #%d %s\n\t%s", i, msgs[i], str);
      pclose(fp);
    } else {
        gtc_dprintf(" (backtrace) #%d %s\n", i, msgs[i]);
    }
  }
  exit(1);
}
