#include <stdio.h>
#include <assert.h>
#include <tc.h>

#define MAX_SIZE 1024

int main(int argc, char **argv) {
  clod_key_t i;
  clod_t     clod;

  printf("CLOD Test: Starting\n");

  clod = clod_create(MAX_SIZE);

  printf("CLOD Test: Performing assignment\n");

  for (i = 0; i < MAX_SIZE; i++) {
    int id = clod_nextfree(clod);
    clod_assign(clod, id, (void*) i);
  }

  assert(i == MAX_SIZE);

  printf("CLOD Test: Performing lookups\n");

  for (i = 0; i < MAX_SIZE; i++) {
    void *p = clod_lookup(clod, i);
    assert((int64_t)p == i);
  }

  assert(i == MAX_SIZE);

  printf("CLOD Test: Passed.\n");

  return 0;
}
