#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>

#include <mpi.h>
#include <armci.h>

#define NITER 10000

int mythread, nthreads;

int main(int argc, char **argv) {
  int    i, j;
  double timer;
  int  **counters;
  
  MPI_Init(&argc, &argv);
  ARMCI_Init();

  MPI_Comm_rank(MPI_COMM_WORLD, &mythread);
  MPI_Comm_size(MPI_COMM_WORLD, &nthreads);

  counters = malloc(nthreads*sizeof(int*));
  ARMCI_Malloc((void**)counters, sizeof(int));

  *counters[mythread] = 0;

  if (mythread == 0)
    printf("Counter test starting on %d processes\n", nthreads);

  timer = MPI_Wtime();
  ARMCI_Barrier();
  
  for (i = 0; i < NITER; i++) {
    for (j = 0; j < nthreads; j++) {
      int incr_val;
      ARMCI_Rmw(ARMCI_FETCH_AND_ADD, &incr_val, counters[j],  ((mythread % 2) ? 1 : -1), 0);
      //usleep(1 + (rand()%10));
      //ARMCI_Rmw(ARMCI_FETCH_AND_ADD, &incr_val, counters[j], -((mythread % 2) ? 1 : -1), 0);
    }
  }

  ARMCI_Barrier();
  timer = MPI_Wtime() - timer;

  if (*counters[mythread] != 0)
    printf(" -- Thread %d error: my counter was %d, not 0\n", mythread, *counters[mythread]);

  if (mythread == 0)
    printf("Counter test completed %d rmw ops in %f sec\n", 2*NITER*nthreads, timer);

  ARMCI_Finalize();
  MPI_Finalize();

  return 0;
}
