#include <tc.h>

/**
 *  eprintf - error printing wrapper
 *    @return number of bytes written to stderr
 */
int eprintf(const char *format, ...) {
  va_list ap;
  int ret;

  if (_c->rank == 0) {
    va_start(ap, format);
    ret = vfprintf(stdout, format, ap);
    va_end(ap);
    fflush(stdout);
    return ret;
  }
  else
    return 0;
}



#ifdef DEPRECATED
/**
 *  gtc_get_wtime - get a wall clock time for performance analysis
 */
double gtc_get_wtime() {
  double t;
  struct timeval tv;

  gettimeofday(&tv, NULL);

  t = (tv.tv_sec*1000000LL + tv.tv_usec)/1000000.0;

  return t;
}
#endif // moved to inline code, uses clock_gettime()


/**
 *  gtc_dbg_printf - optionally compiled debug printer
 *    @return number of bytes written to stderr
 */
int gtc_dbg_printf(const char *format, ...) {
  va_list ap;
  int len, ret;
  char buf[1024], obuf[1024];

  va_start(ap, format);
  ret = vsprintf(buf, format, ap);
  va_end(ap);
  len = sprintf(obuf, "%4d: %s", _c->rank, buf);
  len = write(STDOUT_FILENO, obuf, len);
  /*
  fprintf(stdout, "%d: %s", _c->rank, buf);
  fflush(stdout);
  */
  return ret;
}



/**
 *  gtc_lvl_dbg_printf - optionally compiled debug printer with priority level
 *    @param lvl priority level
 *    @param format format string
 *    @return number of bytes written to stderr
 */
int gtc_lvl_dbg_printf(int lvl, const char *format, ...) {
  va_list ap;
  int len, ret = 0;
  char buf[1024], obuf[1024];

  if (lvl &= _c->dbglvl) {
    va_start(ap, format);
    ret = vsprintf(buf, format, ap);
    va_end(ap);
    len = sprintf(obuf, "%4d: %s", _c->rank, buf);
    len = write(STDOUT_FILENO, obuf, len);
    /*
    fprintf(stdout, "%d: %s", _c->rank, buf);
    fflush(stdout);
    */
  }
  return ret;
}



/**
 *  gtc_lvl_dbg_printf - optionally compiled debug printer with priority level
 *    @param lvl priority level
 *    @param format format string
 *    @return number of bytes written to stderr
 */
int gtc_lvl_dbg_eprintf(int lvl, const char *format, ...) {
  va_list ap;
  int ret = 0;

  if ((_c->rank == 0) && (lvl &= _c->dbglvl)) {
    va_start(ap, format);
    ret = vfprintf(stdout, format, ap);
    va_end(ap);
    fflush(stdout);
  }
  return ret;
}


/**
 * gtc_ptl_error() - returns string for Portals error codes
 * @param error_code number
 * @returns string for error code
 */
char *gtc_ptl_error(int error_code)
{
  switch (error_code) {
    case PTL_OK:
      return "PTL_OK";

    case PTL_ARG_INVALID:
      return "PTL_ARG_INVALID";

    case PTL_CT_NONE_REACHED:
      return "PTL_CT_NONE_REACHED";

    case PTL_EQ_DROPPED:
      return "PTL_EQ_DROPPED";

    case PTL_EQ_EMPTY:
      return "PTL_EQ_EMPTY";

    case PTL_FAIL:
      return "PTL_FAIL";

    case PTL_IN_USE:
      return "PTL_IN_USE";

    case PTL_IGNORED:
      return "PTL_IGNORED";

    case PTL_INTERRUPTED:
      return "PTL_INTERRUPTED";

    case PTL_LIST_TOO_LONG:
      return "PTL_LIST_TOO_LONG";

    case PTL_NO_INIT:
      return "PTL_NO_INIT";

    case PTL_NO_SPACE:
      return "PTL_NO_SPACE";

    case PTL_PID_IN_USE:
      return "PTL_PID_IN_USE";

    case PTL_PT_FULL:
      return "PTL_PT_FULL";

    case PTL_PT_EQ_NEEDED:
      return "PTL_PT_EQ_NEEDED";

    case PTL_PT_IN_USE:
      return "PTL_PT_IN_USE";

    default:
      return "Unknown Portals return code";
  }
}



/**
 * gtc_event_to_string - returns string for a Portals event type
 * @param evtype Portals4 event type
 * @returns character string for event
 */
char *gtc_event_to_string(ptl_event_kind_t evtype) {
  char *ret = "(unmatched)";
  switch (evtype) {
    case PTL_EVENT_GET:
      ret = "PTL_EVENT_GET";
      break;
    case PTL_EVENT_GET_OVERFLOW:
      ret = "PTL_EVENT_GET_OVERFLOW";
      break;
    case PTL_EVENT_PUT:
      ret = "PTL_EVENT_PUT";
      break;
    case PTL_EVENT_PUT_OVERFLOW:
      ret = "PTL_EVENT_PUT_OVERFLOW";
      break;
    case PTL_EVENT_ATOMIC:
      ret = "PTL_EVENT_ATOMIC";
      break;
    case PTL_EVENT_ATOMIC_OVERFLOW:
      ret = "PTL_EVENT_ATOMIC_OVERFLOW";
      break;
    case PTL_EVENT_FETCH_ATOMIC:
      ret = "PTL_EVENT_FETCH_ATOMIC";
      break;
    case PTL_EVENT_FETCH_ATOMIC_OVERFLOW:
      ret = "PTL_EVENT_FETCH_ATOMIC_OVERFLOW";
      break;
    case PTL_EVENT_REPLY:
      ret = "PTL_EVENT_REPLY";
      break;
    case PTL_EVENT_SEND:
      ret = "PTL_EVENT_SEND";
      break;
    case PTL_EVENT_ACK:
      ret = "PTL_EVENT_ACK";
      break;
    case PTL_EVENT_PT_DISABLED:
      ret = "PTL_EVENT_PT_DISABLED";
      break;
    case PTL_EVENT_LINK:
      ret = "PTL_EVENT_LINK";
      break;
    case PTL_EVENT_AUTO_UNLINK:
      ret = "PTL_EVENT_AUTO_UNLINK";
      break;
    case PTL_EVENT_AUTO_FREE:
      ret = "PTL_EVENT_AUTO_FREE";
      break;
    case PTL_EVENT_SEARCH:
      ret = "PTL_EVENT_SEARCH";
      break;
  }
  return ret;
}


/**
 * gtc_dump_event - prints out entire event
 *  @param ev Portals event
 */
void gtc_dump_event(ptl_event_t *ev) {
  char *fail = "(unset)";
  gtc_dprintf("event type: %s\n", gtc_event_to_string(ev->type));
  gtc_dprintf("\tstart: %p\n", ev->start);
  gtc_dprintf("\tuser_ptr: %p\n", ev->user_ptr);
  gtc_dprintf("\tmatch: %lu\n", ev->match_bits);
  gtc_dprintf("\trlength: %lu\n", ev->rlength);
  gtc_dprintf("\tmlength: %lu\n", ev->mlength);
  gtc_dprintf("\tremote: %lu\n", ev->remote_offset);
  gtc_dprintf("\tuid: %lu\n", ev->uid);
  gtc_dprintf("\tinitator: %lu\n", ev->initiator.rank);
  gtc_dprintf("\tpt_index: %lu\n", ev->pt_index);
  switch (ev->ni_fail_type) {
    case PTL_NI_OK:
      fail = "PTL_NI_OK";
      break;
    case PTL_NI_UNDELIVERABLE:
      fail = "PTL_NI_UNDELIVERABLE";
      break;
    case PTL_NI_PT_DISABLED:
      fail = "PTL_NI_PT_DISABLED";
      break;
    case PTL_NI_DROPPED:
      fail = "PTL_NI_DROPPED";
      break;
    case PTL_NI_PERM_VIOLATION:
      fail = "PTL_NI_PERM_VIOLATION";
      break;
    case PTL_NI_OP_VIOLATION:
      fail = "PTL_NI_OP_VIOLATION";
      break;
    case PTL_NI_SEGV:
      fail = "PTL_NI_SEGV";
      break;
    case PTL_NI_NO_MATCH:
      fail = "PTL_NI_NO_MATCH";
      break;
  }
  gtc_dprintf("\tni_fail: %s\n", fail);
  UNUSED(fail);
}



/**
 * gtc_dump_event_queue - prints contents of an event queue
 *
 *    @param eq the event queue to print
 */
void gtc_dump_event_queue(ptl_handle_eq_t eq) {
  int ret, i = 0;
  ptl_event_t ev;

  gtc_dprintf("event queue contents:\n");

  // walk through the event queue
  while ((ret = PtlEQGet(eq, &ev)) != PTL_EQ_EMPTY) {
    if (ret != PTL_OK) {
      gtc_dprintf("gtc_dump_event_queue: PtlEQGet error (%s)\n", gtc_ptl_error(ret));
      break;
    } else {
      gtc_dprintf("event %d:\n", i);
      gtc_dump_event(&ev);
    }
    i++;
  }
  gtc_dprintf("\n");
}


/**
 * gtc_get_mmad() - get min/max/avg for doubles
 *
 * @param counter counter to reduce
 * @param min     reduced min value
 * @param max     reduced max value
 * @param avg     reduced average value
 */
void gtc_get_mmad(double *counter, double *tot, double *min, double *max, double *avg) {
  gtc_reduce(counter, max, GtcReduceOpMax, DoubleType, 1);
  gtc_reduce(counter, min, GtcReduceOpMin, DoubleType, 1);
  gtc_reduce(counter, tot, GtcReduceOpSum, DoubleType, 1);
  *avg = *tot/((double)_c->size);
}



/**
 * gtc_get_mmau() - get min/max/avg for unsigned longs
 *
 * @param counter counter to reduce
 * @param min     reduced min value
 * @param max     reduced max value
 * @param avg     reduced average value
 */
void gtc_get_mmau(tc_counter_t *counter, tc_counter_t *tot, tc_counter_t *min, tc_counter_t *max, double *avg) {
  gtc_reduce(counter, max, GtcReduceOpMax, UnsignedLongType, 1);
  gtc_reduce(counter, min, GtcReduceOpMin, UnsignedLongType, 1);
  gtc_reduce(counter, tot, GtcReduceOpSum, UnsignedLongType, 1);
  *avg = *tot/((double)_c->size);
}



/**
 * gtc_get_mmal() - get min/max/avg for signed longs
 *
 * @param counter counter to reduce
 * @param min     reduced min value
 * @param max     reduced max value
 * @param avg     reduced average value
 */
void gtc_get_mmal(long *counter, long *tot, long *min, long *max, double *avg) {
  gtc_reduce(counter, max, GtcReduceOpMax, LongType, 1);
  gtc_reduce(counter, min, GtcReduceOpMin, LongType, 1);
  gtc_reduce(counter, tot, GtcReduceOpSum, LongType, 1);
  *avg = *tot/((double)_c->size);
}



/**
 * gtc_print_mmad() - print min/max/avg for doubles
 *
 * @param buf     buffer to place output string
 * @param unit    string for printing units
 * @param stat    our local value of the stat
 * @param total   print total count or not?
 */
char *gtc_print_mmad(char *buf, char *unit, double stat, int total) {
  double tot, min, max, avg;
  gtc_get_mmad(&stat, &tot, &min, &max, &avg);
  if (total)
    sprintf(buf, "%6.2f%s (%6.2f%s/%6.2f%s/%6.2f%s)", tot, unit, avg, unit, min, unit, max, unit);
  else
    sprintf(buf, "%6.2f%s/%6.2f%s/%6.2f%s", avg, unit, min, unit, max, unit);
  return buf;
}



/**
 * gtc_print_mmau() - print min/max/avg for unsigned longs
 *
 * @param buf     buffer to place output string
 * @param unit    string for printing units
 * @param stat    our local value of the stat
 * @param total   print total count or not?
 */
char *gtc_print_mmau(char *buf, char *unit, tc_counter_t stat, int total) {
  tc_counter_t tot, min, max;
  double  avg;
  gtc_get_mmau(&stat, &tot, &min, &max, &avg);
  if (total)
    sprintf(buf, "%6lu%s (%6.2f%s/%3lu%s/%3lu%s)", tot, unit, avg, unit, min, unit, max, unit);
  else
    sprintf(buf, "%6.2f%s/%3lu%s/%3lu%s", avg, unit, min, unit, max, unit);
  return buf;
}



/**
 * gtc_print_mmal() - print min/max/avg for signed longs
 *
 * @param buf     buffer to place output string
 * @param unit    string for printing units
 * @param stat    our local value of the stat
 * @param total   print total count or not?
 */
char *gtc_print_mmal(char *buf, char *unit, long stat, int total) {
  long tot, min, max;
  double  avg;
  gtc_get_mmal(&stat, &tot, &min, &max, &avg);
  if (total)
    sprintf(buf, "%3lu%s (%6.2f%s/%3lu%s/%3lu%s)", tot, unit, avg, unit, min, unit, max, unit);
  else
    sprintf(buf, "%6.2f%s/%3lu%s/%3lu%s", avg, unit, min, unit, max, unit);
  return buf;
}
