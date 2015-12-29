/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
#define HOSTSTATSLINUX_C 1
/*========================================================================
 *
 * Description:  Linux Implementation of code to fetch operating system stats.
 *
 * 
 Notes about /proc/<pid> on Linux

 See  gemfire/src/com/gemstone/gemfire/internal/linuxref for the baseline
 linux sources upon which the fscanf logic below is dependent.

 The /proc/<pid>/xxx files are text files on Linux  .
   on Solaris they are binary files , with system include files
   defining structs that define their contents .

  See /usr/src/linux-2.4.7-10/fs/proc/array.c
  for the code that generates the contents of the virtual text files

    /proc/<pid>/status - labeled status info 
       from proc_pid_status() in array.c, also see task_mem()
       (may be expensive to compute, iterates over all vm page maps for the
        process ??)

    /proc/<pid>/stat  - unlabled information, 
      including time usage not available in the labled form
      from proc_pid_stat() in array.c

 *
 *
 *========================================================================
 */
#include "flag.ht"
#define index work_around_gcc_bug
#include "jni.h"
#undef index


/* this file is an empty compilation on non-Linux platforms */
#if defined(FLG_LINUX_UNIX)
/*******  GemStone constants/types defined in this and other modules ********/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/param.h>
#include <sys/sysinfo.h>
#include <sched.h>
#include <limits.h> // LLONG_MAX
#include <inttypes.h>

#include "global.ht"
#include "host.hf"
#include "utl.hf"
#define index work_around_gcc_bug
#include "com_gemstone_gemfire_internal_HostStatHelper.h"
#include "com_gemstone_gemfire_internal_LinuxProcessStats.h"
#include "com_gemstone_gemfire_internal_LinuxSystemStats.h"
#undef index
/* no more includes. KEEP THIS LINE */

typedef struct {
  int64 activeTime;    /* user time + system time */  /* linux */
  int32 userTime;       /* user time used */  /* linux */
  int32 sysTime;        /* system time used */  /* linux */
  int32 imageSize;      /* size of process image in megabytes */ /* linux vsize*/
  int32 rssSize;        /* resident set size in megabytes */  /* linux rss */
} HostRUsageSType;

static int32 linuxPageSize = 0;

static uint32 memSizeToMB(uint32 aSize, int32 page_size) {
  int64 result = aSize;
  result = (result * page_size ) / (1024 * 1024) ;
  return (uint32)result; 
}

/* get the system statistics for the specified process. ( cpu time, page
   faults etc. Look at the description of HostRUsageSType for a detailed
   explanation ). the members of the struct may be zero on platforms that
   do not provide information about specific fields .
*/
static BoolType getHrUsage(PidType thePid, HostRUsageSType *hrUsage)
{
  char procFileName[64];
  FILE* fPtr;

  int pid = 0;
  char commandLine[100]; 
  commandLine[0] = '\0';
  commandLine[sizeof(commandLine) - 1] = '\0';
  char state = 0;
  uint64 userTime = 0;
  uint64 sysTime = 0;
  uint64 vsize = 0;
  uint64 rss = 0;

  memset(hrUsage, 0, sizeof(HostRUsageSType)); /* fix for bug 28668 */

#if 1
  sprintf(procFileName, "/proc/%u/stat", (uint32)thePid);
  fPtr = fopen(procFileName, "r" ); /* read only */ 
  if (fPtr != NULL) {
    /* Technically rss should be long double, but due to our ILLEGAL_USE_OF_long
     * I have choosen to use uint64 instead and hope that we never have a 
     * process that has that many pages swapped. This code will soon be replaced
     * by a Java based implementation.
     */
    int status = fscanf(fPtr, 
"%d %100s %c %*d %*d %*d %*d %*d %*u %*u \
%*u %*u %*u %"PRIu64" %"PRIu64" %*d %*d %*d %*d %*d %*d %*u %"PRIu64" %"PRIu64" ",
                &pid,
                &commandLine[0],
                &state,
                // ppid,
                // pgrp,
                // session,
                // tty_nr, 
                // tty_pgrp, 
                // flags,
                // min_flt,   // end first line of format string

                // cmin_flt,
                // maj_flt,
                // cmaj_flt,
                &userTime,  // task->times.tms_utime,
                &sysTime, // task->times.tms_stime,
                // deadChildUtime, // task->times.tms_cutime,
                // deadChildSysTime, // task->times.tms_cstime, 
                // priority,
                // nice,
                // unused1, // 0UL /* removed */,
                // it_real_value,
                // start_time, 
                &vsize,
                &rss  //  mm ? mm->rss : 0, /* you might want to shift this left 3 */
                );
    // expect 7 conversions  or possibly EOF if the process no longer exists
    if (status != 7 && status != EOF ) {  
      int errNum = errno; // for debugging
      errNum = errNum; // lint
      UTL_ASSERT(status == 7);
    }
    status = fclose(fPtr);
    if (status) {
      int errNum = errno; // for debugging
      errNum = errNum; // lint
      UTL_ASSERT(status == 0);
    }
  }

#endif
#if 0
  int32 linuxTimeToMs = 1000 / HZ ;
  UTL_ASSERT( linuxTimeToMs == 10 );

  hrUsage->userTime = userTime * linuxTimeToMs ;
  hrUsage->sysTime  = sysTime * linuxTimeToMs ;
  hrUsage->activeTime = hrUsage->sysTime;
  hrUsage->activeTime += hrUsage->userTime;
#endif
  hrUsage->imageSize = vsize / (1024*1024);  // assume linux units = Kbytes
  if (linuxPageSize == 0) {
    struct sysinfo info;
    int status = sysinfo(&info);
    if (status == 0) {
      linuxPageSize = info.mem_unit;
    }
  }
  hrUsage->rssSize = memSizeToMB((uint32)rss-3, linuxPageSize); // assume linux units = Kbytes
      
  return TRUE;
}


static void refreshProcess(jint pidArg, int32* intStorage, int64* longStorage, double* doubleStorage)
{
#define INT_FIELD_STORE(field, value) \
    (intStorage[com_gemstone_gemfire_internal_LinuxProcessStats_##field##INT] = value)
#define LONG_FIELD_STORE(field, value) \
    (longStorage[com_gemstone_gemfire_internal_LinuxProcessStats_##field##LONG] = value)
#define DOUBLE_FIELD_STORE(field, value) \
    (doubleStorage[com_gemstone_gemfire_internal_LinuxProcessStats_##field##DOUBLE] = value)
#define FLOAT_FIELD_STORE(field, value) \
    DOUBLE_FIELD_STORE(field, (double)(value))

  HostRUsageSType hrUsage;
  if (getHrUsage(pidArg, &hrUsage)) {
#if 0
    printf("DEBUG: pid=%d activeTime="FMT_I64"\n", pid, hrUsage.activeTime); fflush(stdout);
#endif
#if 0
    LONG_FIELD_STORE(activeTime, hrUsage.activeTime);
    LONG_FIELD_STORE(systemTime, hrUsage.sysTime);
    LONG_FIELD_STORE(userTime, hrUsage.userTime);
#endif
    INT_FIELD_STORE(imageSize, hrUsage.imageSize);
    INT_FIELD_STORE(rssSize, hrUsage.rssSize);
  } else {
    // It might just be a case of the process going away while we
    // where trying to get its stats. So a simple exception throw
    // here would be the wrong thing.
    // So for now lets just ignore the failure and leave the stats
    // as they are.
  }
#undef INT_FIELD_STORE
#undef LONG_FIELD_STORE
#undef FLOAT_FIELD_STORE
#undef DOUBLE_FIELD_STORE
}

enum {
  // 64-bit operands

  SYSTEM_NET_LOOPBACK_PACKETS,
  SYSTEM_NET_LOOPBACK_BYTES,
  SYSTEM_NET_RECV_PACKETS,
  SYSTEM_NET_RECV_BYTES,
  SYSTEM_NET_RECV_ERRS,
  SYSTEM_NET_RECV_DROP,
  SYSTEM_NET_XMIT_PACKETS,
  SYSTEM_NET_XMIT_BYTES,
  SYSTEM_NET_XMIT_ERRS,
  SYSTEM_NET_XMIT_DROP,
  SYSTEM_NET_XMIT_COLL,
  SYSTEM_CTXT,
  SYSTEM_PROCESSCREATES,
  SYSTEM_PAGEINS,
  SYSTEM_PAGEOUTS,
  SYSTEM_SWAPINS,
  SYSTEM_SWAPOUTS,

  SYSTEM_LONGDATA_SIZE // must be last
  };

enum {
  // Now for the 32-bit operands
  SYSTEM_CPUIDLE,
  SYSTEM_CPUNICE,
  SYSTEM_CPUSYSTEM,
  SYSTEM_CPUUSER,
  SYSTEM_IOWAIT,
  SYSTEM_IRQ,
  SYSTEM_SOFTIRQ,
  SYSTEM_CPUCOUNT,
  SYSTEM_PROCCOUNT,
  SYSTEM_LOADAV1,
  SYSTEM_LOADAV5,
  SYSTEM_LOADAV15,
  SYSTEM_PHYSMEM,
  SYSTEM_FREEMEM,
  SYSTEM_BUFFERMEM,
  SYSTEM_SHAREDMEM,
  SYSTEM_ALLOCSWAP,
  SYSTEM_UNALLOCSWAP,

  SYSTEM_DATA_SIZE /* must be last */
};



static uint32 floatBits(float fArg)
{
  typedef union {
    float f;
    uint32 l;
  } fb_u;
  fb_u fb;
  /* convert to float so we have 32 bits */
  /* we can't just cast to (uint32) because a float to int conversion
   * is done by the cast.
   */
  fb.f = fArg;
  return fb.l;
}

// SI_LOAD_SHIFT comes from kernel.h
#define FSCALE  (1 << SI_LOAD_SHIFT)

static uint32 loadAvgFilter(uint32 v) {
  return floatBits((double)v / FSCALE);
}

static float cvtFloatBits(uint32 bits) {
  union {
    uint32 i;
    float  f;
  } u;
  u.i = bits;
  return u.f;
}

enum {
  CPU_STAT_IDLE,
  CPU_STAT_USER,
  CPU_STAT_SYSTEM,
  CPU_STAT_NICE,
  CPU_STAT_IOWAIT,
  CPU_STAT_IRQ,
  CPU_STAT_SOFTIRQ,
  MAX_CPU_STATS
};
static int lastCpuStatsInvalid = 1;
static uint64 lastCpuStats[MAX_CPU_STATS];

/*
 * Fill in statistics in case of error
 * @see getNetStats
 */
static void zeroNetStats(uint64 lsd[]) {
  memset( (void *) lsd, 0, sizeof(uint64) * SYSTEM_LONGDATA_SIZE );
}

/*
 * Locate end of a numeral, starting at the given position in
 * the input string. The first non-numeric character is returned.
 *
 * @see getNumeral
 */
static const char *advanceNumeral(const char *start) {
  const char *result=start;
  /* skip leading white space */
  while (isspace(*result))
    result ++;
  while (isdigit(*result))
    result ++;
  return result;
  }

/*
 * Flippin' sscanf is not powerful enough.  I've been
 * writing Java too long...
 *
 * *end is initially the position to start parsing the numeral.
 * **end is returned, indicating the next place to start parsing.
 *
 * @see getNetStats
 */
static uint64 getNumeral(const char **end) {
  uint64 result;
  const char *start = *end;
  const char *p;

  while (isspace(*start) && *start != '\0')
    start ++;
  if (*start == '\0') {
#ifdef FLG_DEBUG
    fprintf(stderr, "getNumeral called at end of line\n");
#endif
    return 0; /* error */
    }

  *end = advanceNumeral(start);
  if (*end == start) {
#ifdef FLG_DEBUG
    fprintf(stderr, "getNumeral called when not at a numeral\n");
#endif
    return 0; /* error */
    }

  result = 0;
  for (p = start; p < *end;  p ++) {
    result *= (uint64)10;
    result += (uint64)(*p - '0');
    }
  return result;
  }

/*
 * Garner statistics from /proc/net/dev
 */
static void getNetStats(uint64 lsd[]) {
  uint64 total_recv_packets = 0;
  uint64 total_recv_bytes = 0;
  uint64 total_recv_errs = 0;
  uint64 total_recv_drop = 0;
  uint64 total_xmit_packets = 0;
  uint64 total_xmit_bytes = 0;
  uint64 total_xmit_errs = 0;
  uint64 total_xmit_drop = 0;
  uint64 total_xmit_colls = 0;

  /* recv and xmit loopback packets are equal, so we only collect one. */
  uint64 loopback_packets = 0;
  uint64 loopback_bytes = 0;

  int is_loopback;
  char ifname[6];
  uint64 recv_packets;
  uint64 recv_bytes;
  uint64 recv_errs;
  uint64 recv_drop;
  uint64 xmit_packets;
  uint64 xmit_bytes;
  uint64 xmit_errs;
  uint64 xmit_drop;
  uint64 xmit_colls;

  char line[512];
  const char *ptr;
  const char *junk;

  FILE *f = fopen("/proc/net/dev", "r");
  if (f == NULL) {
#ifdef FLG_DEBUG
    fprintf(stderr, "Could not open /proc/net/dev: %s\n", strerror(errno));
#endif
    zeroNetStats(lsd);
    return;
    }

  if (fgets(line, sizeof line, f) == NULL) {
#ifdef FLG_DEBUG
    fprintf(stderr, "/proc/net/dev: First line could not be read\n");
#endif
    fclose(f);
    zeroNetStats(lsd);
    return;
    }
  if (fgets(line, sizeof line, f) == NULL) {
#ifdef FLG_DEBUG
    fprintf(stderr, "/proc/net/dev: Second line could not be read\n");
#endif
    fclose(f);
    zeroNetStats(lsd);
    return;
    }

  for (;;) {
    if (fgets(line, sizeof line, f) == NULL) {
      /* no more devices */
      break;
      }

/*
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:1908275823 326949246    0    0    0     0          0         0 1908275823 326949246    0    0    0     0       0          0
*/

    /* ifname */
    ptr = &line[0];
    while (isspace(*ptr))
      ptr ++;
    if (*ptr == '\0') {
#ifdef FLG_DEBUG
      fprintf(stderr, "No interface name in /proc/net/dev\n");
#endif
      fclose(f);
      zeroNetStats(lsd); // don't trust anything
      return;
      }
    junk = strchr(ptr, ':');
    if (junk == NULL) {
#ifdef FLG_DEBUG
      fprintf(stderr, "No interface name colon in /proc/net/dev\n");
#endif
      fclose(f);
      zeroNetStats(lsd); // don't trust anything
      return;
      }
    memset(ifname, 0, sizeof ifname);
    memcpy(ifname, ptr, junk - ptr);

    is_loopback = strcmp(ifname, "lo") == 0;

    ptr = junk + 1; /* skip the colon */

    recv_bytes = getNumeral(&ptr);
    recv_packets = getNumeral(&ptr);
    recv_errs = getNumeral(&ptr);
    recv_drop = getNumeral(&ptr);
    getNumeral(&ptr); /* fifo */
    getNumeral(&ptr); /* frame */
    getNumeral(&ptr); /* compressed */
    getNumeral(&ptr); /* multicast */

    xmit_bytes = getNumeral(&ptr); /* bytes */
    xmit_packets = getNumeral(&ptr);
    xmit_errs = getNumeral(&ptr);
    xmit_drop = getNumeral(&ptr);
    getNumeral(&ptr); /* fifo */
    xmit_colls = getNumeral(&ptr);
    /* discard remainder of line */

#if 0
    printf("%s: recv_packets = %llu, recv_errs = %llu, recv_drop = %llu, xmit_packets = %llu, xmit_errs = %llu, xmit_drop = %llu, xmit_colls = %llu\n",
        ifname, recv_packets, recv_errs, recv_drop,
        xmit_packets, xmit_errs, xmit_drop, xmit_colls);
#endif

    if (is_loopback) {
      loopback_packets = recv_packets;
      loopback_bytes = recv_bytes;
      }
    else {
      total_recv_packets += recv_packets;
      total_recv_bytes += recv_bytes;
      }
    total_recv_errs += recv_errs;
    total_recv_drop += recv_drop;

    if (is_loopback) {
      /* loopback_xmit_packets = xmit_packets; */
      }
    else {
      total_xmit_packets += xmit_packets;
      total_xmit_bytes += xmit_bytes;
      }
    total_xmit_errs += xmit_errs;
    total_xmit_drop += xmit_drop;
    total_xmit_colls += xmit_colls;
    }

    fclose(f);
#if 0
  printf("\nloopback_packets = %llu, recv_packets = %llu, recv_errs = %llu, recv_drop = %llu, xmit_packets = %llu, xmit_errs = %llu, xmit_drop = %llu, xmit_colls = %llu\n",
      loopback_packets, 
      total_recv_packets, total_recv_errs, total_recv_drop,
      total_xmit_packets, total_xmit_errs, total_xmit_drop, total_xmit_colls);
#endif

  lsd[SYSTEM_NET_LOOPBACK_PACKETS] = loopback_packets;
  lsd[SYSTEM_NET_LOOPBACK_BYTES] = loopback_bytes;
  lsd[SYSTEM_NET_RECV_PACKETS] = total_recv_packets;
  lsd[SYSTEM_NET_RECV_BYTES] = total_recv_bytes;
  lsd[SYSTEM_NET_RECV_ERRS] = total_recv_errs;
  lsd[SYSTEM_NET_RECV_DROP] = total_recv_drop;
  lsd[SYSTEM_NET_XMIT_PACKETS] =  total_xmit_packets;
  lsd[SYSTEM_NET_XMIT_BYTES] =  total_xmit_bytes;
  lsd[SYSTEM_NET_XMIT_ERRS] = total_xmit_errs;
  lsd[SYSTEM_NET_XMIT_DROP] = total_xmit_drop;
  lsd[SYSTEM_NET_XMIT_COLL] = total_xmit_colls;
  }

static void refreshSystem(int32* intStorage, int64* longStorage, double* doubleStorage)
{
#define INT_FIELD_STORE(field, value) \
    (intStorage[com_gemstone_gemfire_internal_LinuxSystemStats_##field##INT] = value)
#define LONG_FIELD_STORE(field, value) \
    (longStorage[com_gemstone_gemfire_internal_LinuxSystemStats_##field##LONG] = value)
#define DOUBLE_FIELD_STORE(field, value) \
    (doubleStorage[com_gemstone_gemfire_internal_LinuxSystemStats_##field##DOUBLE] = value)
#define FLOAT_FIELD_STORE(field, value) \
    DOUBLE_FIELD_STORE(field, (double)(value))


    uint32 sd[SYSTEM_DATA_SIZE] = { 0 };
    uint64 lsd[SYSTEM_LONGDATA_SIZE] = { 0 };
    //Initialize stats to all 0
    memset( (void *) sd, 0, sizeof(uint32) * SYSTEM_DATA_SIZE);
    zeroNetStats( lsd );
    sd[SYSTEM_CPUCOUNT] = sysconf(_SC_NPROCESSORS_ONLN);

    // following from sysinfo
    { struct sysinfo info;
      int status = sysinfo(&info);
      if (status == 0) {
        linuxPageSize = info.mem_unit;
        sd[SYSTEM_LOADAV1] =  loadAvgFilter(info.loads[0]);
        sd[SYSTEM_LOADAV5] =  loadAvgFilter(info.loads[1]);
        sd[SYSTEM_LOADAV15] = loadAvgFilter(info.loads[2]);
        sd[SYSTEM_PHYSMEM] = memSizeToMB(info.totalram, info.mem_unit);
        sd[SYSTEM_FREEMEM] = memSizeToMB(info.freeram, info.mem_unit); 
        sd[SYSTEM_SHAREDMEM] = memSizeToMB(info.sharedram, info.mem_unit);
        sd[SYSTEM_BUFFERMEM] = memSizeToMB(info.bufferram, info.mem_unit);
        sd[SYSTEM_UNALLOCSWAP] = memSizeToMB(info.freeswap, info.mem_unit);
        sd[SYSTEM_ALLOCSWAP] = memSizeToMB(info.totalswap - info.freeswap, info.mem_unit);
        sd[SYSTEM_PROCCOUNT] = info.procs ;
      } else {
        UTL_ASSERT(0);
        sd[SYSTEM_LOADAV1] = 0;
        sd[SYSTEM_LOADAV5] = 0;
        sd[SYSTEM_LOADAV15] = 0;
        sd[SYSTEM_PHYSMEM] = 0;
        sd[SYSTEM_FREEMEM] = 0;
        sd[SYSTEM_SHAREDMEM] = 0;
        sd[SYSTEM_BUFFERMEM] = 0;
        sd[SYSTEM_UNALLOCSWAP] = 0;
        sd[SYSTEM_ALLOCSWAP] = 0;
      }
    }
    {
      int status;
      FILE* statF;
      char statBuff[4096];
      statF = fopen("/proc/stat", "r" ); /* read only */ 
      if (statF != NULL) {
        status = fread(statBuff, 1, sizeof(statBuff), statF);
        fclose(statF);
        if (status > 0) {
          uint64 userJiffies;
          uint64 niceJiffies;
          uint64 sysJiffies;
          uint64 idleJiffies;
          uint64 ioJiffies;
          uint64 irqJiffies;
          uint64 softirqJiffies;

          status = sscanf(statBuff, "cpu  %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64"",
                          &userJiffies,
                          &niceJiffies,
                          &sysJiffies,
                          &idleJiffies,
                          &ioJiffies,
                          &irqJiffies,
                          &softirqJiffies);
          // expect 7 or 4 conversions, 2.6 kernels have more values
          if (status != 7 && status != 4) {  
            int errNum = errno; // for debugging
            errNum = errNum; // lint
            UTL_ASSERT(status == 7);
          }
          // Calculate CPU usage
          if (lastCpuStatsInvalid) {
            lastCpuStatsInvalid = 0;
            lastCpuStats[CPU_STAT_IDLE] = idleJiffies;
            lastCpuStats[CPU_STAT_USER] = userJiffies;
            lastCpuStats[CPU_STAT_SYSTEM] = sysJiffies;
            lastCpuStats[CPU_STAT_NICE] = niceJiffies;
            lastCpuStats[CPU_STAT_IOWAIT] = ioJiffies;
            lastCpuStats[CPU_STAT_IRQ] = irqJiffies;
            lastCpuStats[CPU_STAT_SOFTIRQ] = softirqJiffies;
            sd[SYSTEM_CPUUSER] = 0;
            sd[SYSTEM_CPUSYSTEM] = 0;
            sd[SYSTEM_CPUNICE] = 0;
            sd[SYSTEM_CPUIDLE] = 100;
            sd[SYSTEM_IOWAIT] = 0;
            sd[SYSTEM_IRQ] = 0;
            sd[SYSTEM_SOFTIRQ] = 0;
          } else { /* compute cpu time percentages */
            int i;
            uint64 total_change = 0;
            uint64 half_total;
            uint32 diffs[MAX_CPU_STATS];
            uint64 newStats[MAX_CPU_STATS];

            newStats[CPU_STAT_IDLE] = idleJiffies;
            newStats[CPU_STAT_USER] = userJiffies;
            newStats[CPU_STAT_SYSTEM] = sysJiffies;
            newStats[CPU_STAT_NICE] = niceJiffies;
            newStats[CPU_STAT_IOWAIT] = ioJiffies;
            newStats[CPU_STAT_IRQ] = irqJiffies;
            newStats[CPU_STAT_SOFTIRQ] = softirqJiffies;
    
            /* calculate changes for each state and the overall change */
            for (i = 0; i < MAX_CPU_STATS; i++) {
              diffs[i] = (uint32)(newStats[i] - lastCpuStats[i]);
              total_change += diffs[i];
              lastCpuStats[i] = newStats[i];
            }

            /* avoid divide by zero potential */
            if (total_change == 0) {
              total_change = 1;
            }

            /* calculate percentages based on overall change, rounding up */
            half_total = total_change / 2;
            for (i = 0; i < MAX_CPU_STATS; i++) {
              diffs[i] = ((diffs[i] * 100) + half_total) / total_change;
            }
            sd[SYSTEM_CPUUSER] = diffs[CPU_STAT_USER];
            sd[SYSTEM_CPUSYSTEM] = diffs[CPU_STAT_SYSTEM];
            sd[SYSTEM_CPUNICE] = diffs[CPU_STAT_NICE];
            sd[SYSTEM_CPUIDLE] = diffs[CPU_STAT_IDLE];
            sd[SYSTEM_IOWAIT] = diffs[CPU_STAT_IOWAIT];
            sd[SYSTEM_IRQ] = diffs[CPU_STAT_IRQ];
            sd[SYSTEM_SOFTIRQ] = diffs[CPU_STAT_SOFTIRQ];
          }

          { // do page stats
            uint64 pageIns = 0;
            uint64 pageOuts = 0;
            uint64 swapIns = 0;
            uint64 swapOuts = 0;
            char* srcPtr = strstr(statBuff, "page ");
            if (srcPtr != NULL) {
              status = sscanf(srcPtr, "page %"PRIu64" %"PRIu64"\nswap %"PRIu64" %"PRIu64"",
                              &pageIns, &pageOuts, &swapIns, &swapOuts);
              if (status != 4) {  
                int errNum = errno; // for debugging
                errNum = errNum; // lint
                UTL_ASSERT(status == 4);
              }
            } else {
              char vmstatBuff[4096];
              statF = fopen("/proc/vmstat", "r" ); /* read only */ 
              if (statF != NULL) {
                status = fread(vmstatBuff, 1, sizeof(vmstatBuff), statF);
                fclose(statF);
                if (status > 0) {
                  srcPtr = strstr(vmstatBuff, "pgpgin ");
                  if(srcPtr != NULL) {
                    status = sscanf(srcPtr, 
                         "pgpgin %"PRIu64"\npgpgout %"PRIu64"\npswpin %"PRIu64"\npswpout %"PRIu64"",
                         &pageIns, &pageOuts, &swapIns, &swapOuts);
                    if (status != 4) {  
                      int errNum = errno; // for debugging
                      errNum = errNum; // lint
                      UTL_ASSERT(status == 4);
                    }
                  }
                }
              }
            }
            lsd[SYSTEM_PAGEINS] = pageIns;
            lsd[SYSTEM_PAGEOUTS] = pageOuts;
            lsd[SYSTEM_SWAPINS] = swapIns;
            lsd[SYSTEM_SWAPOUTS] = swapOuts;
          }
          
          { // do contextSwitches and processCreates
            uint64 contextSwitches = 0;
            uint64 processCreates = 0;
            char* srcPtr = strstr(statBuff, "ctxt ");
            if (srcPtr != NULL) {
              status = sscanf(srcPtr, "ctxt %"PRIu64"\nbtime %*u\nprocesses %"PRIu64"",
                              &contextSwitches, &processCreates);
              if (status != 2) {  
                int errNum = errno; // for debugging
                errNum = errNum; // lint
                UTL_ASSERT(status == 2);
              }
            }
            lsd[SYSTEM_CTXT] = contextSwitches;
            lsd[SYSTEM_PROCESSCREATES] = processCreates;
          }
        } else {
          sd[SYSTEM_CPUUSER] = 0;
          sd[SYSTEM_CPUSYSTEM] = 0;
          sd[SYSTEM_CPUNICE] = 0;
          sd[SYSTEM_CPUIDLE] = 100;
          sd[SYSTEM_IOWAIT] = 0;
          sd[SYSTEM_IRQ] = 0;
          sd[SYSTEM_SOFTIRQ] = 0;
          lsd[SYSTEM_PAGEINS] = 0;

          lsd[SYSTEM_PAGEOUTS] = 0;
          lsd[SYSTEM_SWAPINS] = 0;
          lsd[SYSTEM_SWAPOUTS] = 0;
          lsd[SYSTEM_CTXT] = 0;
          lsd[SYSTEM_PROCESSCREATES] = 0;
        }
      }
    }

    getNetStats(lsd);

    INT_FIELD_STORE(allocatedSwap, sd[SYSTEM_ALLOCSWAP]);
    INT_FIELD_STORE(bufferMemory, sd[SYSTEM_BUFFERMEM]);
    LONG_FIELD_STORE(contextSwitches, lsd[SYSTEM_CTXT]);
    INT_FIELD_STORE(cpuActive, sd[SYSTEM_CPUNICE]+sd[SYSTEM_CPUSYSTEM]+sd[SYSTEM_CPUUSER]);
    INT_FIELD_STORE(cpuIdle, sd[SYSTEM_CPUIDLE]);
    INT_FIELD_STORE(cpuNice, sd[SYSTEM_CPUNICE]);
    INT_FIELD_STORE(cpuSystem, sd[SYSTEM_CPUSYSTEM]);
    INT_FIELD_STORE(cpuUser, sd[SYSTEM_CPUUSER]);
    INT_FIELD_STORE(cpus, sd[SYSTEM_CPUCOUNT]);
    INT_FIELD_STORE(iowait, sd[SYSTEM_IOWAIT]);
    INT_FIELD_STORE(irq, sd[SYSTEM_IRQ]);
    INT_FIELD_STORE(softirq, sd[SYSTEM_SOFTIRQ]);
    INT_FIELD_STORE(freeMemory, sd[SYSTEM_FREEMEM]);
    FLOAT_FIELD_STORE(loadAverage1, cvtFloatBits(sd[SYSTEM_LOADAV1]));
    FLOAT_FIELD_STORE(loadAverage5, cvtFloatBits(sd[SYSTEM_LOADAV5]));
    FLOAT_FIELD_STORE(loadAverage15, cvtFloatBits(sd[SYSTEM_LOADAV15]));
    LONG_FIELD_STORE(pagesPagedIn, lsd[SYSTEM_PAGEINS]);
    LONG_FIELD_STORE(pagesPagedOut, lsd[SYSTEM_PAGEOUTS]);
    LONG_FIELD_STORE(pagesSwappedIn, lsd[SYSTEM_SWAPINS]);
    LONG_FIELD_STORE(pagesSwappedOut, lsd[SYSTEM_SWAPOUTS]);
    INT_FIELD_STORE(physicalMemory, sd[SYSTEM_PHYSMEM]);
    LONG_FIELD_STORE(processCreates, lsd[SYSTEM_PROCESSCREATES]);
    INT_FIELD_STORE(processes, sd[SYSTEM_PROCCOUNT]);
    INT_FIELD_STORE(sharedMemory, sd[SYSTEM_SHAREDMEM]);
    INT_FIELD_STORE(unallocatedSwap, sd[SYSTEM_UNALLOCSWAP]);

    LONG_FIELD_STORE(loopbackPackets, lsd[SYSTEM_NET_LOOPBACK_PACKETS]);
    LONG_FIELD_STORE(loopbackBytes, lsd[SYSTEM_NET_LOOPBACK_BYTES]);
    LONG_FIELD_STORE(recvPackets, lsd[SYSTEM_NET_RECV_PACKETS]);
    LONG_FIELD_STORE(recvBytes, lsd[SYSTEM_NET_RECV_BYTES]);
    LONG_FIELD_STORE(recvErrors, lsd[SYSTEM_NET_RECV_ERRS]);
    LONG_FIELD_STORE(recvDrops, lsd[SYSTEM_NET_RECV_DROP]);
    LONG_FIELD_STORE(xmitPackets, lsd[SYSTEM_NET_XMIT_PACKETS]);
    LONG_FIELD_STORE(xmitBytes, lsd[SYSTEM_NET_XMIT_BYTES]);
    LONG_FIELD_STORE(xmitErrors, lsd[SYSTEM_NET_XMIT_ERRS]);
    LONG_FIELD_STORE(xmitDrops, lsd[SYSTEM_NET_XMIT_DROP]);
    LONG_FIELD_STORE(xmitCollisions, lsd[SYSTEM_NET_XMIT_COLL]);
#undef INT_FIELD_STORE
#undef LONG_FIELD_STORE
#undef FLOAT_FIELD_STORE
#undef DOUBLE_FIELD_STORE
}

/*
 * JOM args
 * Class:     com_gemstone_gemfire_internal_HostStatHelper
 * Method:    refreshProcess
 * Signature: (I[I[J[D)V
 */
JNIEXPORT void JNICALL Java_com_gemstone_gemfire_internal_HostStatHelper_refreshProcess
(JNIEnv *env, jclass unused, jint pid, jintArray intStorage, jlongArray longStorage, jdoubleArray doubleStorage)
{
  jboolean isCopy;
  int32* intPtr = NULL;
  int64* longPtr = NULL;
  double* doublePtr = NULL;

  if (intStorage != NULL) {
    intPtr = (int32*)(*env)->GetPrimitiveArrayCritical(env, intStorage, &isCopy);
  }
  if (longStorage != NULL) {
    longPtr = (int64*)(*env)->GetPrimitiveArrayCritical(env, longStorage, &isCopy);
  }
  if (doubleStorage != NULL) {
    doublePtr = (double*)(*env)->GetPrimitiveArrayCritical(env, doubleStorage, &isCopy);
  }

  refreshProcess(pid, intPtr, longPtr, doublePtr);

  if (doubleStorage != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, doubleStorage, doublePtr, 0);
  }
  if (longStorage != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, longStorage, longPtr, 0);
  }
  if (intStorage != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, intStorage, intPtr, 0);
  }
}

/*
 * JOM args
 * Class:     com_gemstone_gemfire_internal_HostStatHelper
 * Method:    refreshSystem
 * Signature: ([I[J[D)V
 */
JNIEXPORT void JNICALL Java_com_gemstone_gemfire_internal_HostStatHelper_refreshSystem
(JNIEnv *env, jclass unused, jintArray intStorage, jlongArray longStorage, jdoubleArray doubleStorage)
{
  jboolean isCopy;
  int32* intPtr = NULL;
  int64* longPtr = NULL;
  double* doublePtr = NULL;

  if (intStorage != NULL) {
    intPtr = (int32*)(*env)->GetPrimitiveArrayCritical(env, intStorage, &isCopy);
  }
  if (longStorage != NULL) {
    longPtr = (int64*)(*env)->GetPrimitiveArrayCritical(env, longStorage, &isCopy);
  }
  if (doubleStorage != NULL) {
    doublePtr = (double*)(*env)->GetPrimitiveArrayCritical(env, doubleStorage, &isCopy);
  }

  refreshSystem(intPtr, longPtr, doublePtr);

  if (doublePtr != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, doubleStorage, doublePtr, 0);
  }
  if (longPtr != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, longStorage, longPtr, 0);
  }
  if (intPtr != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, intStorage, intPtr, 0);
  }
}

/*
 * Class:     com_gemstone_gemfire_internal_HostStatHelper
 * Method:    init
 * Signature: ()V
 */
JNIEXPORT jint JNICALL Java_com_gemstone_gemfire_internal_HostStatHelper_init
(JNIEnv *env, jclass unused)
{
  return 0;
}

/*
 * Class:     com_gemstone_gemfire_internal_HostStatHelper
 * Method:    readyRefresh
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_gemstone_gemfire_internal_HostStatHelper_readyRefresh
(JNIEnv *env, jclass unused)
{
}

/*
 * Class:     com_gemstone_gemfire_internal_HostStatHelper
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_gemstone_gemfire_internal_HostStatHelper_close
(JNIEnv *env, jclass unused)
{
  /* do nothing */
}

#endif /* FLG_LINUX_UNIX */
