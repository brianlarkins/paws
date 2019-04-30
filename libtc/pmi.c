#include <tc.h>


// this file needs comments

static int encode(const void *inval, int invallen, char *outval, int outvallen);
static int decode(const char *inval, void *outval, int outvallen);

void init_pmi_rank_size(int rank_hint) {
#ifdef __APPLE__
  int initialized;
#else
  PMI_BOOL initialized;
#endif

  PMI_Init(&initialized);

  if (rank_hint == GTC_DEFAULT_RANK_HINT)
    PMI_Get_rank(&(_c->rank));
  else
    _c->rank = rank_hint; // assigning our rank from hint (used when running with UPC or MPI)
  PMI_Get_size(&(_c->size));
}

void init_pmi() {
  ptl_process_t me;
  int name_max, key_max, val_max;
  char *name, *key, *val;

  PMI_KVS_Get_name_length_max(&name_max);
  PMI_KVS_Get_key_length_max(&key_max);
  PMI_KVS_Get_value_length_max(&val_max);

  name = (char *)calloc(1,name_max);
  key  = (char *)calloc(1,key_max);
  val  = (char *)calloc(1,val_max);

  PMI_KVS_Get_my_name(name, name_max);

  PtlGetPhysId(_c->lni, &me);
  //printf("PMI rank: %lu Portals rank: %ld\n", c->rank, me.rank);

  snprintf(key, key_max, "pdht-%ld-nid", (long unsigned) _c->rank);
  //printf("%d: nid: %x\n", _c->rank, me.phys.nid);
  encode(&me.phys.nid, sizeof(me.phys.nid), val, val_max);
  PMI_KVS_Put(name, key, val);

  snprintf(key, key_max, "pdht-%ld-pid", (long unsigned) _c->rank);
  //printf("%d: pid: %x\n", _c->rank, me.phys.pid);
  encode(&me.phys.pid, sizeof(me.phys.pid), val, val_max);
  PMI_KVS_Put(name, key, val);

  PMI_KVS_Commit(name);
  PMI_Barrier();

  _c->mapping = (ptl_process_t *)calloc(_c->size, sizeof(ptl_process_t));

  for (int i=0; i< _c->size; i++) {
    snprintf(key, key_max, "pdht-%lu-nid", (long unsigned) i);
    PMI_KVS_Get(name, key, val, val_max);
    decode(val, &(_c->mapping[i]).phys.nid, sizeof(_c->mapping[i].phys.nid));

    snprintf(key, key_max, "pdht-%lu-pid", (long unsigned) i);
    PMI_KVS_Get(name, key, val, val_max);
    decode(val, &(_c->mapping[i].phys.pid), sizeof(_c->mapping[i].phys.pid));
  }
  free(name);
  free(key);
  free(val);
}


void init_only_barrier(void) { PMI_Barrier(); }


  static int
encode(const void *inval, int invallen, char *outval, int outvallen)
{
  static unsigned char encodings[] = {
    '0','1','2','3','4','5','6','7', \
      '8','9','a','b','c','d','e','f' };
  int i;

  if (invallen * 2 + 1 > outvallen) {
    return 1;
  }

  for (i = 0; i < invallen; i++) {
    outval[2 * i] = encodings[((unsigned char *)inval)[i] & 0xf];
    outval[2 * i + 1] = encodings[((unsigned char *)inval)[i] >> 4];
  }

  outval[invallen * 2] = '\0';

  return 0;
}


  static int
decode(const char *inval, void *outval, int outvallen)
{
  int i;
  char *ret = (char*) outval;

  if (outvallen != strlen(inval) / 2) {
    return 1;
  }

  for (i = 0 ; i < outvallen ; ++i) {
    if (*inval >= '0' && *inval <= '9') {
      ret[i] = *inval - '0';
    } else {
      ret[i] = *inval - 'a' + 10;
    }
    inval++;
    if (*inval >= '0' && *inval <= '9') {
      ret[i] |= ((*inval - '0') << 4);
    } else {
      ret[i] |= ((*inval - 'a' + 10) << 4);
    }
    inval++;
  }

  return 0;
}
