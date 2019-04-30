#include <stdio.h>
#include <assert.h>
#include <tc.h>

#define MAX_SIZE 100

int main(int argc, char **argv) {
  int64_t i;
  gtc_t gtc;
  int   ids[MAX_SIZE];

  printf("GTC CLOD Test: Starting\n");

  gtc = gtc_create(sizeof(void*), 10, 1000, NULL, GtcQueueSDC);

  printf(" + Performing assignment\n");

  for (i = 0; i < MAX_SIZE; i++)
    ids[i] = gtc_clo_associate(gtc, (void*)i);

  printf(" + Performing lookups\n");

  for (i = 0; i < MAX_SIZE; i++) {
    void *p = gtc_clo_lookup(gtc, ids[i]);
    assert((int64_t)p == i);
  }

  printf("GTC CLOD Test: Passed.\n");

  gtc_destroy(gtc);

  return 0;
}
