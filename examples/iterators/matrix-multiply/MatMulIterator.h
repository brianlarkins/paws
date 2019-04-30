#ifndef __MatMulIterator_h__
#define __MatMulIterator_h__

#include <iostream>
#include <cassert>
#include <cstdlib>

extern "C" {
#include "ga.h"
#include "alloca.h"
#include "unistd.h"
#include "stdlib.h"
}


class MatMulTask {
 private:
  int i, j, n;
  int ga, gb, gc;

 public:
  MatMulTask(int _i, int _j, int _n, int _ga, int _gb, int _gc) {
    i = _i;
    j = _j;
    n = _n;
    ga = _ga;
    gb = _gb;
    gc = _gc;
  }
 
  void process() {
    double C;
    double *A = (double *)alloca(sizeof(double)*n);
    double *B = (double *)alloca(sizeof(double)*n);
    int idx1[2];
    int idx2[2];
    int ld=1;

    //std::cout<<"Processing task. i="<<i<<" j="<<j<<endl;

    idx1[0] = i; idx1[1] = 0; idx2[0] = i; idx2[1] = n-1;
    NGA_Get(ga, idx1, idx2, A, &ld);
    idx1[0] = 0; idx1[1] = j; idx2[0] = n-1; idx2[1] = j;
    NGA_Get(gb, idx1, idx2, B, &ld);

    C = 0;
    for (int k = 0; k < n; k++)
      C += A[k] * B[k];
    
    idx1[0] = i; idx1[1] = j; idx2[0] = i; idx2[1] = j;
    NGA_Put(gc, idx1, idx2, &C, &ld);
  }
}; /* class MatMulTask */


class MatMulIterator {
 private:
  int n;
  int i, j;
  int ga, gb, gc;
  int proc; /* The processor to which this iterator is sliced (-1 => not sliced) */
  int clo[2], chi[2];

 public:
  MatMulIterator(int _n, int _ga, int _gb, int _gc, int _proc=-1) {
    n = _n;
    ga = _ga;
    gb = _gb;
    gc = _gc;
    proc = _proc;
    if(proc>=0) {
      NGA_Distribution(gc, proc, clo, chi);      
      i = clo[0];
      j = clo[1];
    }
    else {
      clo[0] = clo[1] = 0;
      chi[0] = chi[1] = n-1;
      i = j = 0;
    }
  }

  bool hasNext() { 
    return (i<=chi[0]) && (j<=chi[1]);
  }

  
  MatMulTask next() {
    int ti=i, tj=j;
    
    if (++j > chi[1]) {
      j = clo[1];
      ++i;
    }
    return MatMulTask(ti, tj, n, ga, gb, gc);
  }

  
  void reset() {
    i = clo[0];
    j = clo[1];    
  }

  
  bool validAbsoluteIndex(int id) {
    int ti, tj, k;
    
    ti = id/n;
    tj = id%n;

    if (ti >= clo[0] && ti <= chi[0] &&
        tj >= clo[1] && tj <= chi[1])
      return true;

    return false;
  }
 
  
  bool validRelativeIndex(int id) {
    int rows = chi[0] - clo[0] + 1;
    int cols = chi[1] - clo[1] + 1;
    
    if (id >= 0 && id < rows*cols)
      return true;

    return false;
  }
  
  
  MatMulTask createTaskAbsolute(int id) {
    assert(validAbsoluteIndex(id));
    return MatMulTask(id/n, id%n, n, ga, gb, gc);
  }


  MatMulTask createTaskRelative(int id) {
    int rows = chi[0] - clo[0] + 1;
    int cols = chi[1] - clo[1] + 1;
   
    int ti = id/cols + clo[0];
    int tj = id%cols + clo[1];
    
    assert(validRelativeIndex(id));
    return MatMulTask(ti, tj, n, ga, gb, gc);
  }

  
  MatMulIterator slice(int _proc) {
    return MatMulIterator(n, ga, gb, gc, _proc);
  }
}; /* class MatMulIterator */

#endif /*__MatMulIterator_h__*/

