/*
 * Copyright (C) 2010. See COPYRIGHT in top-level directory.
 */

#pragma once

enum token_states { ACTIVE, TERMINATED };

enum token_directions { UP, DOWN };

typedef struct {
  int64_t  state;                              // active or terminated
  int64_t  spawned;                            // tasks spawned
  int64_t  completed;                          // tasks completed
} td_token_t;

#define TD_TRIGGERED
struct td_s {
  int                   num_cycles;
  int                   have_voted;

  ptl_process_t         p;                     // parent rank
  ptl_process_t         l;                     // left child rank
  ptl_process_t         r;                     // right child rank
  int                   nchildren;             // number of children
  ptl_pt_index_t        ptindex;               // portal table index

#ifdef TD_TRIGGERED
  ptl_handle_ct_t       uspawn_ct;             // handle to up pass counter
  ptl_handle_ct_t       ufinal_ct;             // handle to up pass counter
#else 
  ptl_handle_ct_t       left_ct;               // handle to up pass counter
  ptl_handle_ct_t       right_ct;              // handle to up pass counter
  ptl_handle_ct_t       send_ct;               // track completion for puts
#endif
  ptl_handle_ct_t       down_ct;               // handle to down pass counter

  ptl_handle_md_t       token_md;              // handle to MD for local sends
  ptl_handle_md_t       down_md;               // handle to MD for down triggered sends
#ifdef TD_TRIGGERED
  ptl_handle_md_t       up_md;                 // handle to MD for up triggered sends
  ptl_handle_md_t       zero_md;               // handle to MD for zero-ing out counters
  int64_t               zero;                  // zero value to use with triggered counter resets
#endif

#ifdef TD_TRIGGERED
  ptl_handle_me_t       uspawn_me;             // handle to up pass #spawned ME
  ptl_handle_me_t       ufinal_me;             // handle to up pass #completed ME
#else
  ptl_handle_me_t       uleft_me;              // handle to up pass left child ME
  ptl_handle_me_t       uright_me;             // handle to up pass left child ME
#endif
  ptl_handle_me_t       down_me;               // handle to down pass token ME

  int                   upcycle_thresh;        // number of counting events for a full upward cycle
  int                   uslastset;             // trigger threshold for last spawn atomic trigger
  int                   uflastset;             // trigger threshold for last final atomic trigger
  int                   zlastset;              // trigger threshold for last zero atomic triggers
  int                   dlastset;              // trigger threshold for last broadcast trigger
  int                   vrecvd;                // votes received
  int                   ullastset; 
  int                   urlastset;
  enum token_directions token_direction;       // blocking only

  td_token_t            token;                 // local token counter contributions
#ifdef TD_TRIGGERED
  td_token_t           *up_token;              // token used for upward reduction
  td_token_t           *down_token;            // token used for downward broadcast
#else 
  td_token_t           *upleft_token;          // token used for upward reduction
  td_token_t           *upright_token;         // token used for upward reduction
  td_token_t           *down_token;            // token used for downward broadcast
  td_token_t            send_token;
#endif

  int                   last_spawned;         // tasks spawned last attempt
  int                   last_completed;       // tasks completed last attempt
};
typedef struct td_s td_t;


td_t *td_create(void);
void  td_destroy(td_t *td);
void  td_reset(td_t *td);

int   td_attempt_vote(td_t *td);
void  td_set_counters(td_t *td, int count1, int count2);
int   td_get_counter1(td_t *td);
int   td_get_counter2(td_t *td);
