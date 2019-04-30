#include <iostream>
using namespace std;

#include "MatMulIterator.h"
#include "LoadBalancers.h"

#include "mpi.h"

extern "C" {
#include "macdecls.h"
#include "ga.h"
#include <math.h>
#include <libgen.h>
}

#define VERBOSE   0


/**
 * Concisely print an n*n Global Array
 */
void printMatrix(int n, int ga) {
  int i, j;
  double val;
  int idx1[2], idx2[2];
  int ld = 1;

  for (i = 0; i < n; i++)
    for (j = 0; j < n; j++) {
      idx1[0] = i; idx1[1] = j; idx2[0]=i; idx2[1]=j;
      NGA_Get(ga, idx1, idx2, &val, &ld);
      cout << val;
      (j == n-1) ? cout << endl : cout << " ";
    }
}


int main(int argc, char *argv[]) {
  int N;
  int i, j, ret;
  int ga, gb, gc;
  int me, nproc;
  double val;
  int ld=1;
  int idx1[2], idx2[2];
  int dims[2], chunk[2]={-1,-1};

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);

  GA_Initialize();

  // Read in N from the args
  if (argc < 2) {
    if (me == 0) 
      printf("Usage: %s N\n", basename(argv[0]));
    GA_Terminate();
    MPI_Finalize();
    return 1;
  } else {
    N = atoi(argv[1]);
    dims[0] = dims[1] = N;
  }
  
  ga = NGA_Create(MT_C_DBL, 2, dims, "ga", chunk);
  gb = NGA_Create(MT_C_DBL, 2, dims, "gb", chunk);
  gc = NGA_Create(MT_C_DBL, 2, dims, "gc", chunk);

  GA_Zero(gc);
  GA_Zero(ga);

  if (me == 0) {
    printf("Performing matrix-matrix multiply test on %dx%d matrix with %d processes\n", N, N, nproc);
    
    // Set A to the identity matrix
    val = 1.0;
    for (i = 0; i < N; i++) {
      idx1[0] = idx1[1] = i; idx2[0] = idx2[1] = i; 
      NGA_Put(ga, idx1, idx1, &val, &ld);
    }

    // Set B to { 0 1 2 ... , 0 1 2 ... , ... }
    for (i = 0; i < N; i++) {
      for (j = 0; j < N; j++) {
	idx1[0] = i; idx1[1] = j; idx2[0]=i; idx2[1]=j;
	val = j;
	NGA_Put(gb, idx1, idx2, &val, &ld);
      }
    }
  }

  GA_Sync();

  MatMulIterator mmiter(N, ga, gb, gc);

#if defined(LDBAL_PARTITIONED)
  ldbal_partitioned(mmiter);
#elif defined(LDBAL_GLOB_GETNEXT)
  ldbal_glob_getnext(mmiter);
#elif defined(LDBAL_FRAG_GETNEXT)
  ldbal_frag_getnext(mmiter);
#elif defined(LDBAL_SCIOTO)
  ldbal_scioto(mmiter);
#elif defined(LDBAL_SCIOTO_PROGRESSIVE)
  ldbal_scioto_progressive(mmiter);
#else
#error Please select a load balancer
#endif
  
  GA_Sync();

  ret = 0;
  if (me == 0) {
    // Verify C
    for (i = 0; i < N; i++) {
      for (j = 0; j < N; j++) {
	idx1[0] = i; idx1[1] = j; idx2[0]=i; idx2[1]=j;
	NGA_Get(gc, idx1, idx2, &val, &ld);
	if (fabs(val-j) > 1e-6)  {
          ret = 1;
	  break;
	}
      }
    }

    if (VERBOSE) {
      cout << "===== A Matrix =====" << endl;
      printMatrix(N, ga);
      cout << "===== B Matrix =====" << endl;
      printMatrix(N, gb);
      cout << "===== C Matrix =====" << endl;
      printMatrix(N, gc);
    }

    if (ret == 0) {
      cout << "Test: PASS" << endl;
    }  else {
      cout << "Test: FAIL" << endl;
    }
  }

  GA_Terminate();
  MPI_Finalize();

  return ret;
}
