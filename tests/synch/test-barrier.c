#define _BSD_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>

#include <tc.h>

#define NITER 10

int mythread, nthreads, rankno;

int main(int argc, char **argv) {
  gtc_t gtc;
  
  gtc = gtc_create(100, 3, 50, NULL, GtcQueueSDC);

  for (int i=0; i<NITER; i++) {
    for (int r=0; r<_c->size; r++) {
      if (r == _c->rank) {
        printf("hello from %d\n", _c->rank); fflush(stdout);
      }
      gtc_barrier();
    }
    gtc_barrier();
    eprintf("***************\n");
  }

  gtc_destroy(gtc);
  return 0;
}
