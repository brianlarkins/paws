#include <iostream>
using namespace std;

#include "MatMulIterator.h"

#include "mpi.h"

extern "C" {
#include <stdio.h>
#include <stdlib.h>

//#include <macdecls.h>
#include <ga.h>
#include <math.h>
#include <tc.h>
#include <tc-internals.h>
}

#define NUM_TASKS 1000

static int me, nproc;

void create_task(gtc_t gtc, task_class_t tclass, MatMulTask mmt);
void task_fcn(gtc_t gtc, task_t *task);


/**
 * MMM Task wrapper
**/
void task_fcn(gtc_t gtc, task_t *task) {
  MatMulTask *t = (MatMulTask*) gtc_task_body(task);

  t->process();
}


/**
 * Put an instance of task_fcn() into the task collection.
 *
 * @param tc        Where to enqueue the task.
 * @param tclass    Task's class
**/
void create_task(gtc_t gtc, task_class_t tclass, MatMulTask mmt) {
  task_t     *task = gtc_task_create(tclass);
  MatMulTask *t    = (MatMulTask*) gtc_task_body(task);

  *t = mmt;

  gtc_add(gtc, task, me);
  gtc_task_destroy(task);
}


void ldbal_scioto(MatMulIterator mmiter) {
  // Initialize the Task Collection
  gtc_t         gtc        = gtc_create(sizeof(MatMulTask), NUM_TASKS, NULL, MPI_COMM_WORLD);
  task_class_t  task_class = gtc_task_class_register(sizeof(MatMulTask), task_fcn);
  task_t       *mmtask     = gtc_task_create(task_class);

  me    = GA_Nodeid();
  nproc = GA_Nnodes();

  // Populate the Task Collection
  MatMulIterator my_mmiter = mmiter.slice(me);

  while(my_mmiter.hasNext()) {
    gtc_task_reuse(mmtask);  // Reset and reuse local task buffer
    MatMulTask *t = (MatMulTask*) gtc_task_body(mmtask);
    *t = my_mmiter.next();
    gtc_add(gtc, mmtask, me);
  }

  // Process the Task Collection
  gtc_process(gtc);
  gtc_print_stats(gtc);

  gtc_task_destroy(mmtask);
  gtc_destroy(gtc);
}


#define WORK_THRESH 10
#define SPAWN_COUNT 10
void ldbal_scioto_progressive(MatMulIterator mmiter) {
  // Initialize the Task Collection
  gtc_t         gtc        = gtc_create(sizeof(MatMulTask), NUM_TASKS, NULL, MPI_COMM_WORLD);
  task_class_t  task_class = gtc_task_class_register(sizeof(MatMulTask), task_fcn);
  task_t       *mmtask     = gtc_task_create(task_class);
  tc_t         *tc         = gtc_lookup(gtc);

  me    = GA_Nodeid();
  nproc = GA_Nnodes();

  // Slice the iterator and set my external work flag for termination detection
  MatMulIterator my_mmiter = mmiter.slice(me);
  gtc_set_external_work_avail(gtc, 1);

  TC_START_TIMER(tc, process);
  
  // Process the Task Collection
  for (bool done=false; (!done); ) {

    // If we're running low on work, attempt to spawn SPAWN_COUNT tasks from the iterator
    if (gtc_tasks_avail(gtc) < WORK_THRESH) {
      for (int i = 0; i < SPAWN_COUNT && my_mmiter.hasNext(); i++) {
        gtc_task_reuse(mmtask);  // Reset and reuse local task buffer
        MatMulTask *t = (MatMulTask*) gtc_task_body(mmtask);
        *t = my_mmiter.next();
        gtc_add(gtc, mmtask, me);
      }
    }

    // If the iterator is empty, we no longer have external work
    if (!my_mmiter.hasNext()) {
      gtc_set_external_work_avail(gtc, 0);
    }

    // Fetch and execute the next task, a failed get indicates global termination
    if (gtc_get_buf(gtc, 0, mmtask)) {
      gtc_task_execute(gtc, mmtask);

    // Termination was detected  
    } else {
      done = true;
    }

  } /* while (!done) */
  
  TC_STOP_TIMER(tc, process);

  gtc_print_stats(gtc);

  gtc_task_destroy(mmtask);
  gtc_destroy(gtc);
}
