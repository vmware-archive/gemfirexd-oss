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
#define HOSTSTATSSOLARIS_C TRUE
/*========================================================================
 *
 * Description:  Solaris Implementation of code to fetch operating system stats.
 *
 *========================================================================
 */
#include "flag.ht"
#define index work_around_gcc_bug
#include "jni.h"
#undef index


/* this file is an empty compilation on non-Solaris platforms */
#if defined(FLG_SOLARIS_UNIX)
#include <procfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/systeminfo.h>

#include <sys/proc.h>
#ifndef SOLARIS_VERSION
/* go with 2.8 as the default */
#define SOLARIS_VERSION 280
#endif

#include <kstat.h>
#include <sys/sysinfo.h>
#include <sys/param.h>

// #define HAS_MIB /* MIB code has not yet been ports from Facets */
#ifdef HAS_MIB
#include <stropts.h>
#include <sys/stream.h>
#include <sys/socket.h>
#include <sys/tihdr.h>
#include <tiuser.h>
#include <inet/led.h>
#include <inet/mib2.h>
#include <netinet/igmp_var.h>
#define DEV_TCP "/dev/tcp"
#define TCP_MODULE "tcp"
#endif

/*******  GemStone constants/types defined in this and other modules ********/

#include "global.ht"
#include "host.hf"
#include "utl.hf"
#include "com_gemstone_gemfire_internal_SolarisProcessStats.h"
#include "com_gemstone_gemfire_internal_SolarisSystemStats.h"
#include "com_gemstone_gemfire_internal_HostStatHelper.h"

/* no more includes. KEEP THIS LINE */


typedef struct {
  int64 activeTime;    /* user time + system time */
  int32 userTime;       /* user time used */
  int32 sysTime;        /* system time used */
  int32 majFlt;         /* page faults requiring physical I/O  */
  int32 numSwaps;       /* swaps */
  int32 msgSent;        /* messages sent */
  int32 msgRecv;        /* messages received */
  int32 signalsRecv;    /* number of signals received */
  int32 minorFaults;    /* minor page faults */
  int32 volCSW;         /* voluntary context switches */
  int32 ivolCSW;        /* involuntary context switches */
  int32 inblk;          /* input blocks */
  int32 oublk;          /* output blocks */
  int32 ioch;           /* chars read and written */
  int32 totalLwpCount;  /* number of contributing light weight processes */
  int32 curLwpCount;    /* number of lwps the process has right now */
  int32 heapSize;       /* size of heap in megabytes */
  int32 stackSize;      /* size of stack in megabytes */
  int32 trapTime;       /* other system trap CPU time */
  int32 tftime;         /* text page fault sleep time */
  int32 dftime;         /* data page fault sleep time */
  int32 kftime;         /* kernel page fault sleep time */
  int32 ltime;          /* user lock wait time */
  int32 slptime;        /* all other sleep time */
  int32 wtime;          /* wait-cpu (latency) time */
  int32 stoptime;       /* stopped time */
  int32 syscalls;       /* system calls */
  int32 imageSize;      /* size of process image in megabytes */
  int32 rssSize;        /* resident set size in megabytes */
  float pctCpu;        /* % of recent cpu time */
  float pctMem;        /* % of system memory used by the process */
} HostRUsageSType;

static float getPct(unsigned short binaryFraction)
{
  if (binaryFraction == 0) {
    return 0;
  } else {
    return (float)((binaryFraction / 32768.0) * 100);
  }
}

/* get the system statistics for the specified process. ( cpu time, page
   faults etc. Look at the description of HostRUsageSType for a detailed
   explanation ). the members of the struct may be zero on platforms that
   do not provide information about specific fields .
*/
static BoolType getHrUsage(PidType thePid, HostRUsageSType *hrUsage)
{
  char procFileName[64];
  int fd;
  prusage_t currentUsage;
  pstatus_t currentStatus;
  psinfo_t currentInfo;
  int32 usrTimeSecs, usrTimeNanos, sysTimeSecs, sysTimeNanos;
  int32 tmpTimeSecs, tmpTimeNanos;

  /* status info is optional and will not be available if we are not
   * root or the process does not belong to us.
   */
  sprintf(procFileName, "/proc/%u/status", (uint32)thePid);
  fd = open(procFileName, O_RDONLY, 0);
  if (fd != -1) {
    read(fd, &currentStatus, sizeof(currentStatus));
    close(fd);
#if 1
    if ((currentStatus.pr_flags & PR_MSACCT) == 0) {
#if 0
      fprintf(stderr, "pid=%d does not have microstate accounting.\n", thePid);
      fflush(stderr);
#endif
      sprintf(procFileName, "/proc/%u/ctl", (uint32)thePid);
      fd = open(procFileName, O_WRONLY, 0);
      if (fd != -1) {
        int32 ctlbuff[2];
        ctlbuff[0] = PCSET;
        ctlbuff[1] = PR_MSACCT;
        write(fd, ctlbuff, sizeof(ctlbuff));
        close(fd);
      }
    }
#endif
  } else {
    MEMSET(&currentStatus, 0, sizeof(currentStatus));
  }

#if 0
#define HOST_STAT_DEBUG true
#endif

  /* usage is world readable so only reason this should fail is if
   * process has gone away.
   */
  sprintf(procFileName, "/proc/%u/usage", (uint32)thePid);
  fd = open(procFileName, O_RDONLY, 0);
  if (fd == -1)
    {
#ifdef HOST_STAT_DEBUG
      fprintf(stderr, "could not open file %s. errno=%d.\n",
              procFileName, errno);
#endif
      return FALSE;
    }
  if (read(fd, &currentUsage, sizeof(currentUsage)) != sizeof(currentUsage)) {
#ifdef HOST_STAT_DEBUG
    fprintf(stderr, "read on %s. errno=%d.\n", procFileName, errno);
#endif
    close(fd);
    return FALSE;
  }
  close(fd);

  /* psinfo is world readable so only reason this should fail is if
   * process has gone away.
   */
  sprintf(procFileName, "/proc/%u/psinfo", (uint32)thePid);
  fd = open(procFileName, O_RDONLY, 0);
  if (fd == -1)
    {
#ifdef HOST_STAT_DEBUG
      fprintf(stderr, "could not open file %s. errno=%d.\n",
              procFileName, errno);
#endif
      return FALSE;
    }
  if (read(fd, &currentInfo, sizeof(currentInfo)) != sizeof(currentInfo)) {
#ifdef HOST_STAT_DEBUG
    fprintf(stderr, "read on %s. errno=%d.\n", procFileName, errno);
#endif
    close(fd);
    return FALSE;
  }
  close(fd);
#if 0
  fprintf(stderr, "pr_fname=%s\n", currentInfo.pr_fname);
  fprintf(stderr, "pr_psargs=%s\n", currentInfo.pr_psargs);
  fprintf(stderr, "pr_argc=%d\n", currentInfo.pr_argc);
  fprintf(stderr, "pr_argv=%p\n", currentInfo.pr_argv);
  {
    PidType processId;
    HostGetProcessId(&processId);
    if (thePid == processId) {
      char **argv = (char**)currentInfo.pr_argv;
      int i = 0;
      while (argv[i] != NULL) {
        fprintf(stderr, "pr_argv[%d]=%s\n", i, argv[i]);
        i++;
      }
    }
  }
#endif

  usrTimeSecs = currentUsage.pr_utime.tv_sec;
  usrTimeNanos = currentUsage.pr_utime.tv_nsec;
  hrUsage->userTime = (usrTimeSecs*1000L)+(usrTimeNanos/(1000*1000));
  sysTimeSecs = currentUsage.pr_stime.tv_sec;
  sysTimeNanos = currentUsage.pr_stime.tv_nsec;
  hrUsage->sysTime  = (sysTimeSecs*1000L)+(sysTimeNanos/(1000*1000));
  hrUsage->activeTime = hrUsage->sysTime;
  hrUsage->activeTime += hrUsage->userTime;
  hrUsage->majFlt = currentUsage.pr_majf;
  hrUsage->numSwaps = currentUsage.pr_nswap;
  hrUsage->msgSent = currentUsage.pr_msnd;
  hrUsage->msgRecv = currentUsage.pr_mrcv;
  hrUsage->volCSW = currentUsage.pr_vctx;
  hrUsage->ivolCSW = currentUsage.pr_ictx;
  hrUsage->signalsRecv = currentUsage.pr_sigs;
  hrUsage->minorFaults = currentUsage.pr_minf;
  hrUsage->totalLwpCount = currentUsage.pr_count;
  hrUsage->curLwpCount = currentInfo.pr_nlwp;
  hrUsage->heapSize = (currentStatus.pr_brksize / (1024*1024));
  hrUsage->stackSize = (currentStatus.pr_stksize / (1024*1024));
  tmpTimeSecs = currentUsage.pr_ttime.tv_sec;
  tmpTimeNanos = currentUsage.pr_ttime.tv_nsec;
  hrUsage->trapTime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  tmpTimeSecs = currentUsage.pr_tftime.tv_sec;
  tmpTimeNanos = currentUsage.pr_tftime.tv_nsec;
  hrUsage->tftime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  tmpTimeSecs = currentUsage.pr_dftime.tv_sec;
  tmpTimeNanos = currentUsage.pr_dftime.tv_nsec;
  hrUsage->dftime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  tmpTimeSecs = currentUsage.pr_kftime.tv_sec;
  tmpTimeNanos = currentUsage.pr_kftime.tv_nsec;
  hrUsage->kftime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  tmpTimeSecs = currentUsage.pr_ltime.tv_sec;
  tmpTimeNanos = currentUsage.pr_ltime.tv_nsec;
  hrUsage->ltime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  tmpTimeSecs = currentUsage.pr_slptime.tv_sec;
  tmpTimeNanos = currentUsage.pr_slptime.tv_nsec;
  hrUsage->slptime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  tmpTimeSecs = currentUsage.pr_wtime.tv_sec;
  tmpTimeNanos = currentUsage.pr_wtime.tv_nsec;
  hrUsage->wtime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  tmpTimeSecs = currentUsage.pr_stoptime.tv_sec;
  tmpTimeNanos = currentUsage.pr_stoptime.tv_nsec;
  hrUsage->stoptime = (tmpTimeSecs*1000L)+(tmpTimeNanos/(1000*1000));
  hrUsage->inblk = currentUsage.pr_inblk;
  hrUsage->oublk = currentUsage.pr_oublk;
  hrUsage->syscalls = currentUsage.pr_sysc;
  hrUsage->ioch = currentUsage.pr_ioch;

  hrUsage->imageSize = currentInfo.pr_size / 1024UL;
  hrUsage->rssSize = currentInfo.pr_rssize / 1024UL;
  hrUsage->pctCpu = getPct(currentInfo.pr_pctcpu);
  hrUsage->pctMem = getPct(currentInfo.pr_pctmem);
      
  return TRUE;
}

#ifdef HAS_MIB
static int TcpTypeCount = -1;
/* #define TRACE_MIB */

static BoolType getTcpStats(mib2_tcp_t* tcpPtr) {
    char buf[BUFSIZ];
    struct strbuf control;
    struct strbuf data;
    struct T_optmgmt_req *req_opt = (struct T_optmgmt_req *)buf;
    struct T_optmgmt_ack *ack_opt = (struct T_optmgmt_ack *)buf;
    struct T_error_ack *err_opt = (struct T_error_ack *)buf;
    struct opthdr *req_hdr;
    int n;
    int mib_sd = open(DEV_TCP, O_RDWR);
    if (mib_sd == -1) {
#ifdef TRACE_MIB
	fprintf(stderr, "open of %s failed. errno=%d\n", DEV_TCP, errno);
#endif
	return FALSE;
    }
    while (ioctl(mib_sd, I_POP, &n) != -1) {
	/* eat anything before we start pushing */
#ifdef TRACE_MIB
	fprintf(stderr, "popping\n"); 
#endif
    }
    if (ioctl(mib_sd, I_PUSH, TCP_MODULE) == -1) {
#ifdef TRACE_MIB
	fprintf(stderr, "push of module %s failed. errno=%d\n",
		TCP_MODULE, errno);
#endif
	close(mib_sd);
	return FALSE;
    }
    /* set up the request options */
    req_opt->PRIM_type = T_OPTMGMT_REQ;
    req_opt->OPT_offset = sizeof(struct T_optmgmt_req);
    req_opt->OPT_length = sizeof(struct opthdr);
    req_opt->MGMT_flags = T_CURRENT;

	/* set up the request header */
    req_hdr = (struct opthdr*)&req_opt[1];
    req_hdr->level = MIB2_IP;
    req_hdr->name = 0;
    req_hdr->len = 0;

	/* set up the control message */
    control.buf = buf;
    control.len = req_opt->OPT_length + req_opt->OPT_offset;

	/* send the message downstream */
    if (putmsg(mib_sd, &control, 0, 0) == -1) {
#ifdef TRACE_MIB
	fprintf(stderr, "cannot send control message. errno=%d", errno);
#endif
	close(mib_sd);
	return FALSE;
    }

    /* set up for the getmsg */
    req_hdr = (struct opthdr *) &ack_opt[1];
    control.maxlen = sizeof buf;

    for (;;) {
	/* start reading the response */
	int flags = 0;
	n = getmsg(mib_sd, &control, 0, &flags);
	if (n == -1) {
#ifdef TRACE_MIB
	    fprintf(stderr, "cannot read control message. errno=%d", errno);
#endif
	    close(mib_sd);
	    return FALSE;
	}

	/* end of data? */
	if ((n == 0) &&
	    (control.len >= SIZEOF(struct T_optmgmt_ack)) &&
	    (ack_opt->PRIM_type == T_OPTMGMT_ACK) &&
	    (ack_opt->MGMT_flags == T_SUCCESS) &&
	    (req_hdr->len == 0)) {
	    break;
	}

	/* check for error message sent back */
	if ((control.len >= SIZEOF(struct T_error_ack)) &&
	    (err_opt->PRIM_type == T_ERROR_ACK)) {
#ifdef TRACE_MIB
	    fprintf(stderr, "error reading control message. prim=%d TLIcode=%d unixCode=%d\n",
		    err_opt->ERROR_prim, err_opt->TLI_error, err_opt->UNIX_error);
#endif
	    close(mib_sd);
	    return FALSE;
	}

	/* check for valid response */
	if ((n != MOREDATA) ||
	    (control.len < SIZEOF(struct T_optmgmt_ack)) ||
	    (ack_opt->PRIM_type != T_OPTMGMT_ACK) ||
	    (ack_opt->MGMT_flags != T_SUCCESS)) {
#ifdef TRACE_MIB
	    fprintf(stderr, "invalid control message received\n");
#endif
	    close(mib_sd);
	    return FALSE;
	}

	/* cause the default case to happen */
	if (req_hdr->name != 0) {
	    req_hdr->level = -1;
	}
	if (req_hdr->level == MIB2_TCP) {
#ifdef TRACE_MIB
	    fprintf(stderr, "got a MIB2_TCP of length %d (is it %d?)\n", req_hdr->len,
		    sizeof(mib2_tcp_t));
#endif
	    if (req_hdr->len > sizeof(*tcpPtr)) {
		/* fix for bug 23020 */
		data.maxlen = sizeof(*tcpPtr);
	    } else {
		data.maxlen = req_hdr->len;
	    }
	    data.buf = (char *)tcpPtr;
	    data.len = 0;
	    flags = 0;
	    n = getmsg(mib_sd, 0, &data, &flags);
	    if (n != 0) {
#ifdef TRACE_MIB
		fprintf(stderr, "cannot read mib2_tcp data. errno=%d", errno);
#endif
		close(mib_sd);
		return FALSE;
	    }
	    close(mib_sd);
	    return TRUE;
	} else {
	    char *trash = new char[req_hdr->len];
#ifdef TRACE_MIB
	    fprintf(stderr, "got a %d; reading %d bytes of trash.\n",
		    req_hdr->level, req_hdr->len);
#endif
	    data.maxlen = req_hdr->len;
	    data.buf = trash;
	    data.len = 0;
	    flags = 0;
	    n = getmsg(mib_sd, 0, &data, &flags);
	    if (n != 0) {
#ifdef TRACE_MIB
		fprintf(stderr, "cannot read trash data. errno=%d", errno);
#endif
		close(mib_sd);
		delete trash;
		return FALSE;
	    }
	    delete trash;
	}
    }
    close(mib_sd);
    return FALSE;
}
static mib2_tcp_t tcpStats;
static BoolType tcpStatsAvailable(void) {
    static BoolType initialized = FALSE;
    static BoolType result = FALSE;
    if (!initialized) {
	initialized = TRUE;
	result = getTcpStats(&tcpStats);
    }
    return result;
}
static void fetchTcpStats(void) {
    getTcpStats(&tcpStats);
}
#endif /* HAS_MIB */

static void refreshProcess(jint pidArg, int32* intStorage, int64* longStorage, double* doubleStorage)
{
#define INT_FIELD_STORE(field, value) \
    (intStorage[com_gemstone_gemfire_internal_SolarisProcessStats_##field##INT] = value)
#define LONG_FIELD_STORE(field, value) \
    (longStorage[com_gemstone_gemfire_internal_SolarisProcessStats_##field##LONG] = value)
#define DOUBLE_FIELD_STORE(field, value) \
    (doubleStorage[com_gemstone_gemfire_internal_SolarisProcessStats_##field##DOUBLE] = value)
#define FLOAT_FIELD_STORE(field, value) \
    DOUBLE_FIELD_STORE(field, (double)(value))

  HostRUsageSType hrUsage;
  if (getHrUsage(pidArg, &hrUsage)) {
#if 0
    printf("DEBUG: pid=%d activeTime="FMT_I64"\n", pid, hrUsage.activeTime); fflush(stdout);
#endif
    LONG_FIELD_STORE(activeTime, hrUsage.activeTime);
    INT_FIELD_STORE(allOtherSleepTime, hrUsage.slptime);
    INT_FIELD_STORE(characterIo, hrUsage.ioch);
    FLOAT_FIELD_STORE(cpuUsed, hrUsage.pctCpu);
    INT_FIELD_STORE(dataFaultSleepTime, hrUsage.dftime);
    INT_FIELD_STORE(heapSize, hrUsage.heapSize);
    INT_FIELD_STORE(imageSize, hrUsage.imageSize);
    INT_FIELD_STORE(involContextSwitches, hrUsage.ivolCSW);
    INT_FIELD_STORE(kernelFaultSleepTime, hrUsage.kftime);
    INT_FIELD_STORE(lockWaitSleepTime, hrUsage.ltime);
    INT_FIELD_STORE(lwpCurCount, hrUsage.curLwpCount);
    INT_FIELD_STORE(lwpTotalCount, hrUsage.totalLwpCount);
    INT_FIELD_STORE(majorFaults, hrUsage.majFlt);
    FLOAT_FIELD_STORE(memoryUsed, hrUsage.pctMem);
    INT_FIELD_STORE(messagesRecv, hrUsage.msgRecv);
    INT_FIELD_STORE(messagesSent, hrUsage.msgSent);
    INT_FIELD_STORE(minorFaults, hrUsage.minorFaults);
    INT_FIELD_STORE(rssSize, hrUsage.rssSize);
    INT_FIELD_STORE(signalsReceived, hrUsage.signalsRecv);
    INT_FIELD_STORE(systemCalls, hrUsage.syscalls);
    INT_FIELD_STORE(stackSize, hrUsage.stackSize);
    INT_FIELD_STORE(stoppedTime, hrUsage.stoptime);
    INT_FIELD_STORE(systemTime, hrUsage.sysTime);
    INT_FIELD_STORE(textFaultSleepTime, hrUsage.tftime);
    INT_FIELD_STORE(trapTime, hrUsage.trapTime);
    INT_FIELD_STORE(userTime, hrUsage.userTime);
    INT_FIELD_STORE(volContextSwitches, hrUsage.volCSW);
    INT_FIELD_STORE(waitCpuTime, hrUsage.wtime);
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

static vminfo_t lastvminfo;
static sysinfo_t lastsysinfo;
/* CPU_STATS are idle, user, system, ioWait, swapWait */
enum {
  CPU_STAT_IDLE,
  CPU_STAT_USER,
  CPU_STAT_SYSTEM,
  CPU_STAT_IOWAIT,
  CPU_STAT_SWAPWAIT,
  MAX_CPU_STATS
};
enum {
  CPUSTAT_IDLE,
  CPUSTAT_USER,
  CPUSTAT_KERNEL,
  CPUSTAT_WIO,
  CPUSTAT_WSWAP,
  CPUSTAT_WPIO,
  CPUSTAT_BREAD,
  FIRST_CPUSTAT_TOWRITE = CPUSTAT_BREAD,
  CPUSTAT_BWRITE,
  CPUSTAT_LREAD,
  CPUSTAT_LWRITE,
  CPUSTAT_PHREAD,
  CPUSTAT_PHWRITE,
  CPUSTAT_PSWITCH,
  CPUSTAT_TRAP,
  CPUSTAT_INTR,
  CPUSTAT_SYSCALL,
  CPUSTAT_SYSREAD,
  CPUSTAT_SYSWRITE,
  CPUSTAT_SYSFORK,
  CPUSTAT_SYSVFORK,
  CPUSTAT_SYSEXEC,
  CPUSTAT_MSG,
  CPUSTAT_SEMA,
  CPUSTAT_NAMEI,
  CPUSTAT_INTRTHREAD,
  CPUSTAT_INTRBLK,
  CPUSTAT_IDLETHREAD,
  CPUSTAT_INV_SWTCH,
  CPUSTAT_NTHREADS,
  CPUSTAT_CPUMIGRATE,
  CPUSTAT_XCALLS,
  CPUSTAT_MUTEX_ADENTERS,
  CPUSTAT_RW_RDFAILS,
  CPUSTAT_RW_WRFAILS,
  CPUSTAT_BAWRITE,
  CPUSTAT_IOWAIT,
  CPUSTAT_PGREC,
  CPUSTAT_PGFREC,
  CPUSTAT_PGIN,
  CPUSTAT_PGPGIN,
  CPUSTAT_PGOUT,
  CPUSTAT_PGPGOUT,
  CPUSTAT_SWAPIN,
  CPUSTAT_PGSWAPIN,
  CPUSTAT_SWAPOUT,
  CPUSTAT_PGSWAPOUT,
  CPUSTAT_ZFOD,
  CPUSTAT_DFREE,
  CPUSTAT_SCAN,
  CPUSTAT_REV,
  CPUSTAT_HAT_FAULT,
  CPUSTAT_AS_FAULT,
  CPUSTAT_MAJ_FAULT,
  CPUSTAT_COW_FAULT,
  CPUSTAT_PROT_FAULT,
  CPUSTAT_SOFTLOCK,
  CPUSTAT_KERNEL_ASFLT,
  CPUSTAT_PGRRUN,
  CPUSTAT_EXECPGIN,
  CPUSTAT_EXECPGOUT,
  CPUSTAT_EXECFREE,
  CPUSTAT_ANONPGIN,
  CPUSTAT_ANONPGOUT,
  CPUSTAT_ANONFREE,
  CPUSTAT_FSPGIN,
  CPUSTAT_FSPGOUT,
  CPUSTAT_FSFREE,
  CPUSTAT_DATA_SIZE
};
static uint64_t lastCpuStats[MAX_CPU_STATS];
static uint64_t combinedCpus[CPUSTAT_DATA_SIZE];

static uint32 pageSize = 0;
static uint64_t physicalMemoryMB = 0;
static uint64_t physicalPages = 0;

static kstat_ctl_t* kc = NULL;
static kstat_t* systemkp = NULL;
static kstat_t* sysinfokp = NULL;
static kstat_t* vmkp = NULL;
/* static kstat_t* syspageskp = NULL; */
enum { MAX_CPUS = 1024 };
static int cpuCount = 0;
static kstat_t* cpukps[MAX_CPUS];


typedef uint32 namedItemFilter(uint32 x);

#define NO_FILTER ((namedItemFilter*)NULL)

typedef struct {
  const char *name;
  int srcIdx;
  const int dstIdx;
  namedItemFilter* filter;
} namedItem;

enum {
  SNET_IPACKETS,
  SNET_OPACKETS,
  SNET_DATA_SIZE
};

namedItem snetNamedData[] = {
  {"ipackets", -1, SNET_IPACKETS, NO_FILTER},
  {"opackets", -1, SNET_OPACKETS, NO_FILTER},
  {NULL, -1, -1, NO_FILTER}
};

enum { MAX_SIMPLE_NET = 8 };
static int snetCount = 0;
static kstat_t* snetkps[MAX_SIMPLE_NET];
static uint64_t combinedSnet[SNET_DATA_SIZE];

enum {
  CNET_IPACKETS,
  CNET_IERRORS,
  CNET_OPACKETS,
  CNET_OERRORS,
  CNET_COLLISIONS,
  CNET_RBYTES,
  CNET_OBYTES,
  CNET_MULTIRCV,
  CNET_MULTIXMT,
  CNET_BRDCSTRCV,
  CNET_BRDCSTXMT,
  CNET_NORCVBUF,
  CNET_NOXMTBUF,
  /*  CNET_PHY_FAILURES, */
  CNET_DATA_SIZE
};

enum { MAX_COMPLEX_NET = 128 };
static int cnetCount = 0;
static kstat_t* cnetkps[MAX_COMPLEX_NET];
static uint64_t combinedCnet[CNET_DATA_SIZE];


namedItem cnetNamedData[] = {
  {"ipackets", -1, CNET_IPACKETS, NO_FILTER},
  {"ierrors", -1, CNET_IERRORS, NO_FILTER},
  {"opackets", -1, CNET_OPACKETS, NO_FILTER},
  {"oerrors", -1, CNET_OERRORS, NO_FILTER},
  {"collisions", -1, CNET_COLLISIONS, NO_FILTER},
  {"rbytes", -1, CNET_RBYTES, NO_FILTER},
  {"obytes", -1, CNET_OBYTES, NO_FILTER},
  {"multircv", -1, CNET_MULTIRCV, NO_FILTER},
  {"multixmt", -1, CNET_MULTIXMT, NO_FILTER},
  {"brdcstrcv", -1, CNET_BRDCSTRCV, NO_FILTER},
  {"brdcstxmt", -1, CNET_BRDCSTXMT, NO_FILTER},
  {"norcvbuf", -1, CNET_NORCVBUF, NO_FILTER},
  {"noxmtbuf", -1, CNET_NOXMTBUF, NO_FILTER},
  {NULL, -1, -1, NO_FILTER}
};

enum {
  SYSTEM_PID,
  SYSTEM_ID,
  SYSTEM_CPUCOUNT,
  SYSTEM_PROCCOUNT,
  SYSTEM_LOADAV1,
  SYSTEM_LOADAV5,
  SYSTEM_LOADAV15,
  SYSTEM_PHYSMEM,
  SYSTEM_RUNCOUNT,
  SYSTEM_SWAPCOUNT,
  SYSTEM_WAITCOUNT,
  SYSTEM_FREEMEM,
  SYSTEM_RESSWAP,
  SYSTEM_ALLOCSWAP,
  SYSTEM_UNRESSWAP,
  SYSTEM_UNALLOCSWAP,
  SYSTEM_DATA_SIZE
};

static uint32 floatBits(double d)
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
  fb.f = (float)d;
  return fb.l;
}

static uint32 loadAvgFilter(uint32 v) {
  return floatBits((double)v / FSCALE);
}

namedItem systemNamedData[] = {
  {"ncpus", -1, SYSTEM_CPUCOUNT, NO_FILTER},
  {"nproc", -1, SYSTEM_PROCCOUNT, NO_FILTER},
  {"avenrun_1min", -1, SYSTEM_LOADAV1, loadAvgFilter},
  {"avenrun_5min", -1, SYSTEM_LOADAV5, loadAvgFilter},
  {"avenrun_15min", -1, SYSTEM_LOADAV15, loadAvgFilter},
  {NULL, -1, -1, NO_FILTER}
};

static void getNamedOffsets(kstat_t* ks, namedItem* items) {
  int i;
  namedItem* niPtr;
  kstat_named_t* ptr = (kstat_named_t*)ks->ks_data;
  for (i = 0; i < (int)ks->ks_ndata; i++) {
    if (ptr[i].data_type == KSTAT_DATA_UINT32 ||
	ptr[i].data_type == KSTAT_DATA_INT32) {
      niPtr = items;
      while (niPtr->name != NULL) {
	if (niPtr->srcIdx == -1) {
	  if (strcmp(niPtr->name, ptr[i].name) == 0) {
	    niPtr->srcIdx = i;
	    break;
	  }
	}
	niPtr++;
      }
    }
  }
}
static void setNamedItems(kstat_t* ks, uint32* dst, namedItem* items) {
  if (ks != NULL) { /* fix for bug 25902 */
    namedItem* niPtr = items;
    kstat_named_t* src = (kstat_named_t*)ks->ks_data;
    while (niPtr->dstIdx != -1) {
      if (niPtr->srcIdx != -1) {
	if (niPtr->filter != NO_FILTER) {
	  dst[niPtr->dstIdx] = niPtr->filter(src[niPtr->srcIdx].value.ui32);
	} else {
	  dst[niPtr->dstIdx] = src[niPtr->srcIdx].value.ui32;
	}
      }
      niPtr++;
    }
  }
}

static void updateKstatChain(BoolType force) {
  kid_t id;
  kstat_t* ks;
  if (kc == NULL) {
    return;
  }

  id = kstat_chain_update(kc);
  if (id != 0 || force) {
    if (id == -1) {
      fprintf(stderr, "kstat_chain_update failed with: %d\n", errno);
      return;
    }
    systemkp = NULL;
    sysinfokp = NULL;
    vmkp = NULL;
    cpuCount = 0;
    snetCount = 0;
    cnetCount = 0;

    ks = kc->kc_chain;
    while(ks != NULL) {
      switch (ks->ks_type) {
      case KSTAT_TYPE_RAW:
        if (strcmp(ks->ks_name, "sysinfo") == 0) {
          sysinfokp = ks;
        } else if (strcmp(ks->ks_name, "vminfo") == 0) {
          vmkp = ks;
        } else if (strcmp(ks->ks_module, "cpu_stat") == 0) {
          if (cpuCount != MAX_CPUS) {
            cpukps[cpuCount++] = ks;
          } else {
            fprintf(stderr, "Ignoring cpu stats for cpu %d.\n",
                    ks->ks_instance);
          }
        }
        break;
      case KSTAT_TYPE_NAMED:
        if (strcmp(ks->ks_name, "system_misc") == 0) {
          systemkp = ks;
          if (systemNamedData[0].srcIdx == -1) {
            if(-1 != kstat_read(kc, ks, NULL)) {
              getNamedOffsets(ks, systemNamedData);
            }
          }
        } else if (strcmp(ks->ks_class, "net") == 0) {
          if (-1 != kstat_read(kc, ks, NULL)) {
            if (ks->ks_ndata == 2) {
              if (snetCount != MAX_SIMPLE_NET) {
                snetkps[snetCount++] = ks;
                if (snetNamedData[0].srcIdx == -1) {
                  if(-1 != kstat_read(kc, ks, NULL)) {
                    getNamedOffsets(ks, snetNamedData);
                  }
                }
              }
            } else {
              if (cnetCount != MAX_SIMPLE_NET &&
                  strcmp(ks->ks_name, "zero_copy") != 0) {
                cnetkps[cnetCount++] = ks;
                if (cnetNamedData[0].srcIdx == -1) {
                  if(-1 != kstat_read(kc, ks, NULL)) {
                    getNamedOffsets(ks, cnetNamedData);
                  }
                }
              }
            }
          }
        }
        break;
      case KSTAT_TYPE_IO:
        break;
      }
      ks = ks->ks_next;
    }
  }
}

static uint32 cvtPagesToMB(uint64_t v, uint_t updates) {
  return (uint32)(((v / updates) * pageSize) / (1024 * 1024));
}


static void HostStatsFetchData(void)
{
  if (kc != NULL) {
    int i;
    updateKstatChain(FALSE);
    if (systemkp) {
      uint64_t numPages = (uint64_t)sysconf(_SC_PHYS_PAGES);
      if (numPages != physicalPages) {
        uint64_t physicalBytes = numPages * pageSize;
        physicalPages = numPages;
        physicalMemoryMB =  physicalBytes / (1024 * 1024);
      }
      kstat_read(kc, systemkp, NULL);
    }
    if (sysinfokp) {
      kstat_read(kc, sysinfokp, NULL);
      if (vmkp) {
        kstat_read(kc, vmkp, NULL);
      }
    }
    MEMSET(&combinedCpus, 0, sizeof(combinedCpus));
    for (i = 0; i < cpuCount; i++) {
      if (-1 != kstat_read(kc, cpukps[i], NULL)) {
        cpu_stat_t* ptr = (cpu_stat_t*)cpukps[i]->ks_data;
        if (cpukps[i]->ks_data_size < sizeof(cpu_stat_t)) {
#ifdef FLG_DEBUG
          fprintf(stderr,
                  "incorrect size for cpu%d. Expected %ld, got %ld.\n",
                  cpukps[i]->ks_instance,
                  sizeof(cpu_stat_t), cpukps[i]->ks_data_size);
#endif
          continue;
        }
        combinedCpus[CPUSTAT_IDLE] += ptr->cpu_sysinfo.cpu[CPU_IDLE];
        combinedCpus[CPUSTAT_USER] += ptr->cpu_sysinfo.cpu[CPU_USER];
        combinedCpus[CPUSTAT_KERNEL] += ptr->cpu_sysinfo.cpu[CPU_KERNEL];
        combinedCpus[CPUSTAT_WIO] += ptr->cpu_sysinfo.wait[W_IO];
        combinedCpus[CPUSTAT_WSWAP] += ptr->cpu_sysinfo.wait[W_SWAP];
        combinedCpus[CPUSTAT_WPIO] += ptr->cpu_sysinfo.wait[W_PIO];
        combinedCpus[CPUSTAT_BREAD] += ptr->cpu_sysinfo.bread;
        combinedCpus[CPUSTAT_BWRITE] += ptr->cpu_sysinfo.bwrite;
        combinedCpus[CPUSTAT_LREAD] += ptr->cpu_sysinfo.lread;
        combinedCpus[CPUSTAT_LWRITE] += ptr->cpu_sysinfo.lwrite;
        combinedCpus[CPUSTAT_PHREAD] += ptr->cpu_sysinfo.phread;
        combinedCpus[CPUSTAT_PHWRITE] += ptr->cpu_sysinfo.phwrite;
        combinedCpus[CPUSTAT_PSWITCH] += ptr->cpu_sysinfo.pswitch;
        combinedCpus[CPUSTAT_TRAP] += ptr->cpu_sysinfo.trap;
        combinedCpus[CPUSTAT_INTR] += ptr->cpu_sysinfo.intr;
        combinedCpus[CPUSTAT_SYSCALL] += ptr->cpu_sysinfo.syscall;
        combinedCpus[CPUSTAT_SYSREAD] += ptr->cpu_sysinfo.sysread;
        combinedCpus[CPUSTAT_SYSWRITE] += ptr->cpu_sysinfo.syswrite;
        combinedCpus[CPUSTAT_SYSFORK] += ptr->cpu_sysinfo.sysfork;
        combinedCpus[CPUSTAT_SYSVFORK] += ptr->cpu_sysinfo.sysvfork;
        combinedCpus[CPUSTAT_SYSEXEC] += ptr->cpu_sysinfo.sysexec;
        combinedCpus[CPUSTAT_MSG] += ptr->cpu_sysinfo.msg;
        combinedCpus[CPUSTAT_SEMA] += ptr->cpu_sysinfo.sema;
        combinedCpus[CPUSTAT_NAMEI] += ptr->cpu_sysinfo.namei;
        combinedCpus[CPUSTAT_INTRTHREAD] += ptr->cpu_sysinfo.intrthread;
        combinedCpus[CPUSTAT_INTRBLK] += ptr->cpu_sysinfo.intrblk;
        combinedCpus[CPUSTAT_IDLETHREAD] += ptr->cpu_sysinfo.idlethread;
        combinedCpus[CPUSTAT_INV_SWTCH] += ptr->cpu_sysinfo.inv_swtch;
        combinedCpus[CPUSTAT_NTHREADS] += ptr->cpu_sysinfo.nthreads;
        combinedCpus[CPUSTAT_CPUMIGRATE] += ptr->cpu_sysinfo.cpumigrate;
        combinedCpus[CPUSTAT_XCALLS] += ptr->cpu_sysinfo.xcalls;
        combinedCpus[CPUSTAT_MUTEX_ADENTERS] += ptr->cpu_sysinfo.mutex_adenters;
        combinedCpus[CPUSTAT_RW_RDFAILS] += ptr->cpu_sysinfo.rw_rdfails;
        combinedCpus[CPUSTAT_RW_WRFAILS] += ptr->cpu_sysinfo.rw_wrfails;
        combinedCpus[CPUSTAT_BAWRITE] += ptr->cpu_sysinfo.bawrite;
        combinedCpus[CPUSTAT_IOWAIT] += ptr->cpu_syswait.iowait;
        combinedCpus[CPUSTAT_PGREC] += ptr->cpu_vminfo.pgrec;
        combinedCpus[CPUSTAT_PGFREC] += ptr->cpu_vminfo.pgfrec;
        combinedCpus[CPUSTAT_PGIN] += ptr->cpu_vminfo.pgin;
        combinedCpus[CPUSTAT_PGPGIN] += ptr->cpu_vminfo.pgpgin;
        combinedCpus[CPUSTAT_PGOUT] += ptr->cpu_vminfo.pgout;
        combinedCpus[CPUSTAT_PGPGOUT] += ptr->cpu_vminfo.pgpgout;
        combinedCpus[CPUSTAT_SWAPIN] += ptr->cpu_vminfo.swapin;
        combinedCpus[CPUSTAT_PGSWAPIN] += ptr->cpu_vminfo.pgswapin;
        combinedCpus[CPUSTAT_SWAPOUT] += ptr->cpu_vminfo.swapout;
        combinedCpus[CPUSTAT_PGSWAPOUT] += ptr->cpu_vminfo.pgswapout;
        combinedCpus[CPUSTAT_ZFOD] += ptr->cpu_vminfo.zfod;
        combinedCpus[CPUSTAT_DFREE] += ptr->cpu_vminfo.dfree;
        combinedCpus[CPUSTAT_SCAN] += ptr->cpu_vminfo.scan;
        combinedCpus[CPUSTAT_REV] += ptr->cpu_vminfo.rev;
        combinedCpus[CPUSTAT_HAT_FAULT] += ptr->cpu_vminfo.hat_fault;
        combinedCpus[CPUSTAT_AS_FAULT] += ptr->cpu_vminfo.as_fault;
        combinedCpus[CPUSTAT_MAJ_FAULT] += ptr->cpu_vminfo.maj_fault;
        combinedCpus[CPUSTAT_COW_FAULT] += ptr->cpu_vminfo.cow_fault;
        combinedCpus[CPUSTAT_PROT_FAULT] += ptr->cpu_vminfo.prot_fault;
        combinedCpus[CPUSTAT_SOFTLOCK] += ptr->cpu_vminfo.softlock;
        combinedCpus[CPUSTAT_KERNEL_ASFLT] += ptr->cpu_vminfo.kernel_asflt;
        combinedCpus[CPUSTAT_PGRRUN] += ptr->cpu_vminfo.pgrrun;
        combinedCpus[CPUSTAT_EXECPGIN] += ptr->cpu_vminfo.execpgin;
        combinedCpus[CPUSTAT_EXECPGOUT] += ptr->cpu_vminfo.execpgout;
        combinedCpus[CPUSTAT_EXECFREE] += ptr->cpu_vminfo.execfree;
        combinedCpus[CPUSTAT_ANONPGIN] += ptr->cpu_vminfo.anonpgin;
        combinedCpus[CPUSTAT_ANONPGOUT] += ptr->cpu_vminfo.anonpgout;
        combinedCpus[CPUSTAT_ANONFREE] += ptr->cpu_vminfo.anonfree;
        combinedCpus[CPUSTAT_FSPGIN] += ptr->cpu_vminfo.fspgin;
        combinedCpus[CPUSTAT_FSPGOUT] += ptr->cpu_vminfo.fspgout;
        combinedCpus[CPUSTAT_FSFREE] += ptr->cpu_vminfo.fsfree;
      }
    }
    {
      MEMSET(&combinedSnet, 0, sizeof(combinedSnet));
      uint32 snetData[SNET_DATA_SIZE];
      int snetIdx;
      for (i = 0; i < snetCount; i++) {
        memset(snetData, 0, sizeof(snetData));
        setNamedItems(snetkps[i], snetData, snetNamedData);
        for (snetIdx = 0; snetIdx < SNET_DATA_SIZE; snetIdx++) {
          combinedSnet[snetIdx] += snetData[snetIdx];
        }
      }
    }
    {
      MEMSET(&combinedCnet, 0, sizeof(combinedCnet));
      uint32 cnetData[CNET_DATA_SIZE];
      int cnetIdx;
      for (i = 0; i < cnetCount; i++) {
        memset(cnetData, 0, sizeof(cnetData));
        setNamedItems(cnetkps[i], cnetData, cnetNamedData);
        for (cnetIdx = 0; cnetIdx < CNET_DATA_SIZE; cnetIdx++) {
          combinedCnet[cnetIdx] += cnetData[cnetIdx];
        }
      }
    }
  }
#ifdef HAS_MIB
  if (tcpStatsAvailable()) {
    fetchTcpStats();
  }
#endif
  /* do nothing */
}

static float cvtFloatBits(uint32 bits) {
  union {
    uint32 i;
    float  f;
  } u;
  u.i = bits;
  return u.f;
}

static void refreshSystem(int32* intStorage, int64* longStorage, double* doubleStorage)
{
#define INT_FIELD_STORE(field, value) \
    (intStorage[com_gemstone_gemfire_internal_SolarisSystemStats_##field##INT] = value)
#define LONG_FIELD_STORE(field, value) \
    (longStorage[com_gemstone_gemfire_internal_SolarisSystemStats_##field##LONG] = value)
#define DOUBLE_FIELD_STORE(field, value) \
    (doubleStorage[com_gemstone_gemfire_internal_SolarisSystemStats_##field##DOUBLE] = value)
#define FLOAT_FIELD_STORE(field, value) \
    DOUBLE_FIELD_STORE(field, (double)(value))

  if (kc != NULL) {
    int i;
    uint32 sd[SYSTEM_DATA_SIZE];
    memset(sd, 0, sizeof(sd));
    /* pid and id are both 0 */
    if (systemkp) {
      setNamedItems(systemkp, sd, systemNamedData);
    }
    sd[SYSTEM_PHYSMEM] = physicalMemoryMB;
    if (sysinfokp) {
      if (sysinfokp->ks_data_size < sizeof(sysinfo_t)) {
        fprintf(stderr,
                "incorrect size for sysinfo. Expected %ld, got %ld\n",
                sizeof(vminfo_t), sysinfokp->ks_data_size);
      } else {
        sysinfo_t* siptr = (sysinfo_t*)sysinfokp->ks_data;
        uint_t updates = siptr->updates - lastsysinfo.updates;
        if (updates == 0) {
          updates = 1;
        }
        sd[SYSTEM_RUNCOUNT] = siptr->runque;
        /* lfptr(siptr->runocc); */
        sd[SYSTEM_SWAPCOUNT] = siptr->swpque;
        /* lfptr(siptr->swpocc); */
        sd[SYSTEM_WAITCOUNT] = siptr->waiting;
        lastsysinfo = *siptr;
        if (vmkp) {
          if (vmkp->ks_data_size < sizeof(vminfo_t)) {
            fprintf(stderr,
                    "incorrect size for vminfo. Expected %ld, got %ld.\n",
                    sizeof(vminfo_t), vmkp->ks_data_size);
          } else {
            vminfo_t* vmptr = (vminfo_t*)vmkp->ks_data;
            sd[SYSTEM_FREEMEM] = cvtPagesToMB(vmptr->freemem - lastvminfo.freemem, updates);
            sd[SYSTEM_RESSWAP] = cvtPagesToMB(vmptr->swap_resv - lastvminfo.swap_resv, updates);
            sd[SYSTEM_ALLOCSWAP] = cvtPagesToMB(vmptr->swap_alloc - lastvminfo.swap_alloc, updates);
            sd[SYSTEM_UNRESSWAP] = cvtPagesToMB(vmptr->swap_avail - lastvminfo.swap_avail, updates);
            sd[SYSTEM_UNALLOCSWAP] = cvtPagesToMB(vmptr->swap_free - lastvminfo.swap_free, updates);
            lastvminfo = *vmptr;
          }
        }
      }
    }
    { /* compute cpu time percentages */
      uint_t total_change = 0;
      uint_t half_total;
      uint_t diffs[MAX_CPU_STATS];
      uint64_t newStats[MAX_CPU_STATS];

      newStats[CPU_STAT_IDLE] = combinedCpus[CPUSTAT_IDLE];
      newStats[CPU_STAT_USER] = combinedCpus[CPUSTAT_USER];
      newStats[CPU_STAT_SYSTEM] = combinedCpus[CPUSTAT_KERNEL];
      newStats[CPU_STAT_IOWAIT] = combinedCpus[CPUSTAT_WIO];
      newStats[CPU_STAT_SWAPWAIT] = combinedCpus[CPUSTAT_WSWAP]
        + combinedCpus[CPUSTAT_WPIO]; /* PIO is currently always 0 */
    
      /* calculate changes for each state and the overall change */
      for (i = 0; i < MAX_CPU_STATS; i++) {
        diffs[i] = (uint_t)(newStats[i] - lastCpuStats[i]);
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

      INT_FIELD_STORE(allocatedSwap, sd[SYSTEM_ALLOCSWAP]);
      LONG_FIELD_STORE(anonymousPagesFreed, combinedCpus[CPUSTAT_ANONFREE]);
      LONG_FIELD_STORE(anonymousPagesPagedIn, combinedCpus[CPUSTAT_ANONPGIN]);
      LONG_FIELD_STORE(anonymousPagesPagedOut, combinedCpus[CPUSTAT_ANONPGOUT]);
      LONG_FIELD_STORE(contextSwitches, combinedCpus[CPUSTAT_PSWITCH]);
      INT_FIELD_STORE(cpuActive, diffs[CPU_STAT_USER] + diffs[CPU_STAT_SYSTEM]);
      INT_FIELD_STORE(cpuIdle, diffs[CPU_STAT_IDLE]);
      INT_FIELD_STORE(cpuIoWait, diffs[CPU_STAT_IOWAIT]);
      INT_FIELD_STORE(cpuSwapWait, diffs[CPU_STAT_SWAPWAIT]);
      INT_FIELD_STORE(cpuSystem, diffs[CPU_STAT_SYSTEM]);
      INT_FIELD_STORE(cpuUser, diffs[CPU_STAT_USER]);
      INT_FIELD_STORE(cpuWaiting, diffs[CPU_STAT_IOWAIT] + diffs[CPU_STAT_SWAPWAIT]);
      INT_FIELD_STORE(cpus, sd[SYSTEM_CPUCOUNT]);
      LONG_FIELD_STORE(execPagesFreed, combinedCpus[CPUSTAT_EXECFREE]);
      LONG_FIELD_STORE(execPagesPagedIn, combinedCpus[CPUSTAT_EXECPGIN]);
      LONG_FIELD_STORE(execPagesPagedOut, combinedCpus[CPUSTAT_EXECPGOUT]);
      LONG_FIELD_STORE(failedMutexEnters, combinedCpus[CPUSTAT_MUTEX_ADENTERS]);
      LONG_FIELD_STORE(failedReaderLocks, combinedCpus[CPUSTAT_RW_RDFAILS]);
      LONG_FIELD_STORE(failedWriterLocks, combinedCpus[CPUSTAT_RW_WRFAILS]);
      LONG_FIELD_STORE(fileSystemPagesFreed, combinedCpus[CPUSTAT_FSFREE]);
      LONG_FIELD_STORE(fileSystemPagesPagedIn, combinedCpus[CPUSTAT_FSPGIN]);
      LONG_FIELD_STORE(fileSystemPagesPagedOut, combinedCpus[CPUSTAT_FSPGOUT]);
      INT_FIELD_STORE(freeMemory, sd[SYSTEM_FREEMEM]);
      LONG_FIELD_STORE(hatMinorFaults, combinedCpus[CPUSTAT_HAT_FAULT]);
      LONG_FIELD_STORE(interrupts, combinedCpus[CPUSTAT_INTR]);
      LONG_FIELD_STORE(involContextSwitches, combinedCpus[CPUSTAT_INV_SWTCH]);
      FLOAT_FIELD_STORE(loadAverage1, cvtFloatBits(sd[SYSTEM_LOADAV1]));
      FLOAT_FIELD_STORE(loadAverage5, cvtFloatBits(sd[SYSTEM_LOADAV5]));
      FLOAT_FIELD_STORE(loadAverage15, cvtFloatBits(sd[SYSTEM_LOADAV15]));
      LONG_FIELD_STORE(majorPageFaults, combinedCpus[CPUSTAT_MAJ_FAULT]);
      LONG_FIELD_STORE(messageCount, combinedCpus[CPUSTAT_MSG]);
      LONG_FIELD_STORE(pageDaemonCycles, combinedCpus[CPUSTAT_REV]);
      LONG_FIELD_STORE(pageIns, combinedCpus[CPUSTAT_PGIN]);
      LONG_FIELD_STORE(pageOuts, combinedCpus[CPUSTAT_PGOUT]);
      LONG_FIELD_STORE(pagerRuns, combinedCpus[CPUSTAT_PGRRUN]);
      LONG_FIELD_STORE(pagesPagedIn, combinedCpus[CPUSTAT_PGPGIN]);
      LONG_FIELD_STORE(pagesPagedOut, combinedCpus[CPUSTAT_PGPGOUT]);
      LONG_FIELD_STORE(pagesScanned, combinedCpus[CPUSTAT_SCAN]);
      INT_FIELD_STORE(physicalMemory, sd[SYSTEM_PHYSMEM]);
      INT_FIELD_STORE(processes, sd[SYSTEM_PROCCOUNT]);
      LONG_FIELD_STORE(procsInIoWait, combinedCpus[CPUSTAT_IOWAIT]);
      LONG_FIELD_STORE(protectionFaults, combinedCpus[CPUSTAT_PROT_FAULT]);
      INT_FIELD_STORE(reservedSwap, sd[SYSTEM_RESSWAP]);
      INT_FIELD_STORE(schedulerRunCount, sd[SYSTEM_RUNCOUNT]);
      INT_FIELD_STORE(schedulerSwapCount, sd[SYSTEM_SWAPCOUNT]);
      INT_FIELD_STORE(schedulerWaitCount, sd[SYSTEM_WAITCOUNT]);
      LONG_FIELD_STORE(semphoreOps, combinedCpus[CPUSTAT_SEMA]);
      LONG_FIELD_STORE(softwareLockFaults, combinedCpus[CPUSTAT_SOFTLOCK]);
      LONG_FIELD_STORE(systemCalls, combinedCpus[CPUSTAT_SYSCALL]);
      LONG_FIELD_STORE(systemMinorFaults, combinedCpus[CPUSTAT_KERNEL_ASFLT]);
      LONG_FIELD_STORE(threadCreates, combinedCpus[CPUSTAT_NTHREADS]);
      LONG_FIELD_STORE(traps, combinedCpus[CPUSTAT_TRAP]);
      INT_FIELD_STORE(unreservedSwap, sd[SYSTEM_UNRESSWAP]);
      INT_FIELD_STORE(unallocatedSwap, sd[SYSTEM_UNALLOCSWAP]);
      LONG_FIELD_STORE(userMinorFaults, combinedCpus[CPUSTAT_AS_FAULT]);
    }
    LONG_FIELD_STORE(loopbackInputPackets, combinedSnet[SNET_IPACKETS]);
    LONG_FIELD_STORE(loopbackOutputPackets, combinedSnet[SNET_OPACKETS]);
    LONG_FIELD_STORE(inputPackets, combinedCnet[CNET_IPACKETS]);
    LONG_FIELD_STORE(inputErrors, combinedCnet[CNET_IERRORS]);
    LONG_FIELD_STORE(outputPackets, combinedCnet[CNET_OPACKETS]);
    LONG_FIELD_STORE(outputErrors, combinedCnet[CNET_OERRORS]);
    LONG_FIELD_STORE(collisions, combinedCnet[CNET_COLLISIONS]);
    LONG_FIELD_STORE(inputBytes, combinedCnet[CNET_RBYTES]);
    LONG_FIELD_STORE(outputBytes, combinedCnet[CNET_OBYTES]);
    LONG_FIELD_STORE(multicastInputPackets, combinedCnet[CNET_MULTIRCV]);
    LONG_FIELD_STORE(multicastOutputPackets, combinedCnet[CNET_MULTIXMT]);
    LONG_FIELD_STORE(broadcastInputPackets, combinedCnet[CNET_BRDCSTRCV]);
    LONG_FIELD_STORE(broadcastOutputPackets, combinedCnet[CNET_BRDCSTXMT]);
    LONG_FIELD_STORE(inputPacketsDiscarded, combinedCnet[CNET_NORCVBUF]);
    LONG_FIELD_STORE(outputPacketsDiscarded, combinedCnet[CNET_NOXMTBUF]);
  }
#ifdef HAS_MIB
    if (tcpStatsAvailable()) {
	lfptr(tcpStats.tcpInSegs + tcpStats.tcpOutSegs);
	lfptr(tcpStats.tcpCurrEstab);
	lfptr(tcpStats.tcpActiveOpens);
	lfptr(tcpStats.tcpPassiveOpens);
	lfptr(tcpStats.tcpAttemptFails);
	lfptr(tcpStats.tcpEstabResets);
	lfptr(tcpStats.tcpInSegs);
	lfptr(tcpStats.tcpOutSegs);
	lfptr(tcpStats.tcpRetransSegs);

	lfptr(tcpStats.tcpOutDataBytes);
	lfptr(tcpStats.tcpRetransBytes);
	lfptr(tcpStats.tcpOutAck);
	lfptr(tcpStats.tcpOutAckDelayed);
	lfptr(tcpStats.tcpOutControl);
	lfptr(tcpStats.tcpInAckSegs);
	lfptr(tcpStats.tcpInAckBytes);
	lfptr(tcpStats.tcpInDupAck);
	lfptr(tcpStats.tcpInAckUnsent);
	lfptr(tcpStats.tcpInDataInorderBytes);
	lfptr(tcpStats.tcpInDataUnorderBytes);
	lfptr(tcpStats.tcpInDataDupBytes);
	lfptr(tcpStats.tcpInDataPartDupBytes);
	lfptr(tcpStats.tcpTimRetrans);
	lfptr(tcpStats.tcpTimRetransDrop);
	lfptr(tcpStats.tcpTimKeepalive);
	lfptr(tcpStats.tcpTimKeepaliveProbe);
	lfptr(tcpStats.tcpTimKeepaliveDrop);
	lfptr(tcpStats.tcpListenDrop);
	lfptr(tcpStats.tcpListenDropQ0);
	lfptr(tcpStats.tcpHalfOpenDrop);
	recendptr();
   }
#endif

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

  kc = kstat_open();
  if (!kc) {
    fprintf(stderr, "kstat_open failed with: %d\n", errno);
    return -1;
  }
  
  updateKstatChain(TRUE);
  pageSize = sysconf(_SC_PAGESIZE);
  if ((int)pageSize == -1) {
    pageSize = 8192; /* make a guess */
  }
  HostStatsFetchData();
  if (sysinfokp) {
    lastsysinfo = *((sysinfo_t*)sysinfokp->ks_data);
    if (vmkp) {
      lastvminfo = *((vminfo_t*)vmkp->ks_data);
    }
  }
  lastCpuStats[CPU_STAT_IDLE] = combinedCpus[CPUSTAT_IDLE];
  lastCpuStats[CPU_STAT_USER] = combinedCpus[CPUSTAT_USER];
  lastCpuStats[CPU_STAT_SYSTEM] = combinedCpus[CPUSTAT_KERNEL];
  lastCpuStats[CPU_STAT_IOWAIT] = combinedCpus[CPUSTAT_WIO];
  lastCpuStats[CPU_STAT_SWAPWAIT] = combinedCpus[CPUSTAT_WSWAP]
    + combinedCpus[CPUSTAT_WPIO]; /* PIO is currently always 0 */
#if 0
  {
    /* following code dumps kstat info to see how the kstat stuff works */
    kstat_t* ks = kc->kc_chain;
    while(ks != NULL) {
      if (strcmp(ks->ks_class, "kmem_cache") != 0
          && strcmp(ks->ks_class, "kmem") != 0
          && strcmp(ks->ks_class, "kmem_backend") != 0
          && strcmp(ks->ks_class, "nfs") != 0
          && strcmp(ks->ks_class, "ufs") != 0
          && strcmp(ks->ks_class, "rpc") != 0
          && strcmp(ks->ks_class, "streams") != 0
          && strcmp(ks->ks_class, "hat") != 0
          && strcmp(ks->ks_module, "nfs") != 0
          && strcmp(ks->ks_module, "nfs_acl") != 0
          && ks->ks_type != KSTAT_TYPE_INTR
          && ks->ks_type != KSTAT_TYPE_TIMER
          ) {
        fprintf(stderr, "%s,%d,%s id=%d class=%s type=%d\n",
                ks->ks_module, ks->ks_instance, ks->ks_name,
                ks->ks_kid, ks->ks_class, ks->ks_type);
#if 0
        if (ks->ks_type == KSTAT_TYPE_IO) {
          if(-1 == kstat_read(kc, ks, NULL)) {
            fprintf(stderr, "kstat_read failed with: %d\n", errno);
          } else {
            int i;
            kstat_io_t* ptr = (kstat_io_t*)ks->ks_data;
            fprintf(stderr, "  ndata=%d\n", ks->ks_ndata);
            fprintf(stderr, "  reads=%d writes=%d bytesread=%lld byteswritten=%lld\n  wcnt=%d rcnt=%d\n  wtime=%lld wlentime=%lld wlastupdate=%lld\n  rtime=%lld rlentime=%lld rlastupdate=%lld\n",
                    ptr->reads, ptr->writes, ptr->nread, ptr->nwritten,
                    ptr->wcnt, ptr->rcnt,
                    ptr->wtime, ptr->wlentime, ptr->wlastupdate,
                    ptr->rtime, ptr->rlentime, ptr->rlastupdate);
          }
        }
#endif
#if 0
        if (ks->ks_type == KSTAT_TYPE_NAMED && (strcmp(ks->ks_class, "device_error") == 0)) {
          if(-1 == kstat_read(kc, ks, NULL)) {
            fprintf(stderr, "kstat_read failed with: %d\n", errno);
          } else {
            int i;
            kstat_named_t* ptr = (kstat_named_t*)ks->ks_data;
            for (i = 0; i < ks->ks_ndata; i++) {
              fprintf(stderr, "  name=%s type=%d\n",
                      ptr[i].name, ptr[i].data_type);
              if (ptr[i].data_type == KSTAT_DATA_UINT64) {
                fprintf(stderr, "    value=%lld\n",
                        ptr[i].value.ull);
              } else if (ptr[i].data_type == KSTAT_DATA_CHAR) {
                fprintf(stderr, "    value=%s\n",
                        ptr[i].value.c);
              }
            }
          }
        }
#endif
      }
      ks = ks->ks_next;
    }
    kstat_close(kc);
    fflush(stderr);
    exit(0);
  }
#endif
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
  HostStatsFetchData();
}

/*
 * Class:     com_gemstone_gemfire_internal_HostStatHelper
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_gemstone_gemfire_internal_HostStatHelper_close
(JNIEnv *env, jclass unused)
{
  if (kc) {
    kstat_close(kc);
    kc = NULL;
  }
  /* do nothing */
}

#endif /*FLG_SOLARIS_UNIX */
