#include <iostream>
using namespace std;

#include "MatMulIterator.h"

#include <mpi.h>

extern "C" {
#include <macdecls.h>
#include <ga.h>
}


void print_stats(int nprocessed, int nsteals) {
  int me    = GA_Nodeid();
  int nproc = GA_Nnodes();
  int tprocessed, tsteals;

  printf("%2d: processed %3d tasks, %3d steals\n", me, nprocessed, nsteals);

  MPI_Reduce(&nprocessed, &tprocessed, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&nsteals, &tsteals, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  if (me == 0)
    printf("Total tasks processed: %d Total steals: %d\n", tprocessed, tsteals);
}


// Static partitioning scheme
void ldbal_partitioned(MatMulIterator mitr) {
  int me    = GA_Nodeid();
  int nproc = GA_Nnodes();
  int nprocessed = 0, nsteals = 0;

  MatMulIterator myitr = mitr.slice(me);
  while(myitr.hasNext()) {
    MatMulTask t = myitr.next();
    t.process();
    ++nprocessed;
  }

  print_stats(nprocessed, nsteals);
}


// Global shared counter scheme
void ldbal_glob_getnext(MatMulIterator mitr) {
  int nprocessed = 0, nsteals = 0;
  int idx = 1, chunk = -1;
  int g_ctr = NGA_Create(MT_C_INT, 1, &idx, "ctr", &chunk);
  GA_Zero(g_ctr);

  idx = 0;
  int id = NGA_Read_inc(g_ctr, &idx, 1);

  while(mitr.validAbsoluteIndex(id)) {
    mitr.createTaskAbsolute(id).process();
    ++nprocessed;
    id = NGA_Read_inc(g_ctr, &idx, 1);
  }

  print_stats(nprocessed, nsteals);

  GA_Sync();
  GA_Destroy(g_ctr);
}


// Fragmented global counters scheme
void ldbal_frag_getnext(MatMulIterator mitr) {
  int me    = GA_Nodeid();
  int nproc = GA_Nnodes();
  int nprocessed = 0, nsteals = 0;
  int idx   = nproc;
  int chunk = -1;
  int g_ctr = NGA_Create(MT_C_INT, 1, &idx, "counters", &chunk);

  GA_Zero(g_ctr);

  int proc, id;

  for (int i=0; i < nproc; i++) {
    proc = (me + i) % nproc;
    MatMulIterator itr = mitr.slice(proc);
#ifdef RELATIVE
    for (;;) {
      id = NGA_Read_inc(g_ctr, &proc, 1);
      if (itr.validRelativeIndex(id)) {
        itr.createTaskRelative(id).process();
        ++nprocessed;
        if (proc != me) ++nsteals;
      } else {
        break;
      }
    }
  }
#else
    do {
      id = NGA_Read_inc(g_ctr, &proc, 1);
      if (itr.validAbsoluteIndex(id)) {
        itr.createTaskAbsolute(id).process();
        ++nprocessed;
        if (proc != me) ++nsteals;
      }
    } while (mitr.validAbsoluteIndex(id));
  }
#endif

  print_stats(nprocessed, nsteals);

  GA_Sync();
  GA_Destroy(g_ctr);
}
