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
#define HOSTSTATSWIN_C TRUE
/*========================================================================
 *
 * Description:  Windows2000 Implementation of code to fetch operating system stats.
 *
 *========================================================================
 */
#include "flag.ht"
#define index work_around_gcc_bug
#include "jni.h"
#undef index


/* this file is an empty compilation on non-W2K platforms */
#if defined(FLG_MSWIN32) 
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>

/*******  GemStone constants/types defined in this and other modules ********/

#include "global.ht"
#include "host.hf"
#include "utl.hf"
#include "com_gemstone_gemfire_internal_WindowsProcessStats.h"
#include "com_gemstone_gemfire_internal_WindowsSystemStats.h"
#include "com_gemstone_gemfire_internal_HostStatHelper.h"

/* no more includes. KEEP THIS LINE */
#define FLG_DEBUG 1

static PPERF_DATA_BLOCK PerfData = NULL;
static PPERF_OBJECT_TYPE ProcessObj = NULL;
static PPERF_OBJECT_TYPE ProcessorObj = NULL;
static PPERF_OBJECT_TYPE MemoryObj = NULL;
static PPERF_OBJECT_TYPE SystemObj = NULL;
static PPERF_OBJECT_TYPE ObjectsObj = NULL;
static PPERF_OBJECT_TYPE UdpObj = NULL;
static PPERF_OBJECT_TYPE NetworkObj = NULL;

static DWORD BufferSize = 32768 + 16384; /* possible fix for event log msg with sp5 */
static int32 pidCtrOffset = -1;

/* #define NTDBG 1 */

enum { SYSTEM_OBJ_ID = 2,
       MEMORY_OBJ_ID = 4,
       PROCESS_OBJ_ID = 230,
       PROCESSOR_OBJ_ID = 238,
       OBJECTS_OBJ_ID = 260,
       NETWORK_OBJ_ID = 510,
       UDP_OBJ_ID = 658,
};
// #define LEVEL1_QUERY_STRING "2 4 230 238 260 512 658"
#define LEVEL1_QUERY_STRING "2 4 230 238 260 512 658 510"

enum { PID_ID = 784,

       PROCESS_PROCESSORTIME_ID = 6, 
       PROCESS_USERTIME_ID = 142,
       PROCESS_PRIVILEGEDTIME_ID = 144,
       VIRTUALBYTESPEAK_ID = 172,
       VIRTUALBYTES_ID = 174,

       PAGEFAULTS_ID = 28,
       WORKINGSETPEAK_ID = 178,
       WORKINGSET_ID = 180,
       PAGEFILEBYTESPEAK_ID = 182,
       PAGEFILEBYTES_ID = 184,

       PRIVATEBYTES_ID = 186,
       THREADCOUNT_ID = 680,
       PRIORITYBASE_ID = 682,
       /* ELAPSEDTIME_ID = 684, */
       POOLPAGEDBYTES_ID = 56,

       POOLNONPAGEDBYTES_ID = 58,
       HANDLECOUNT_ID = 952,
};

enum { 
       PROCESS_PROCESSORTIME_IDX = 0, 
       PROCESS_USERTIME_IDX,
       PROCESS_PRIVILEGEDTIME_IDX,
       VIRTUALBYTESPEAK_IDX,
       VIRTUALBYTES_IDX,

       PAGEFAULTS_IDX,
       WORKINGSETPEAK_IDX,
       WORKINGSET_IDX,
       PAGEFILEBYTESPEAK_IDX,
       PAGEFILEBYTES_IDX,

       PRIVATEBYTES_IDX,
       THREADCOUNT_IDX,
       PRIORITYBASE_IDX,
       /* ELAPSEDTIME omitted? */
       POOLPAGEDBYTES_IDX,

       POOLNONPAGEDBYTES_IDX,
       HANDLECOUNT_IDX,

       MAX_PROCESS_CTRS_COLLECTED
};
static PERF_COUNTER_DEFINITION processCtrCache[MAX_PROCESS_CTRS_COLLECTED];

enum {
  TOTAL_PROCESSORTIME_ID = 6,
  TOTAL_USERTIME_ID = 142,
  TOTAL_PRIVILEGEDTIME_ID = 144,
  INTERRUPTS_ID = 148,
  INTERRUPTTIME_ID = 698
};

enum {
  TOTAL_PROCESSORTIME_IDX = 0,
  TOTAL_USERTIME_IDX,
  TOTAL_PRIVILEGEDTIME_IDX,
  INTERRUPTS_IDX,
  INTERRUPTTIME_IDX,

  MAX_PROCESSOR_CTRS_COLLECTED
};

static PERF_COUNTER_DEFINITION processorCtrCache[MAX_PROCESSOR_CTRS_COLLECTED];

enum {
  TOTALFILEREADOPS_ID = 10,
  TOTALFILEWRITEOPS_ID = 12,
  TOTALFILECONTROLOPS_ID = 14,
  TOTALFILEREADKBYTES_ID = 16,
  TOTALFILEWRITEKBYTES_ID = 18,
  TOTALFILECONTROLKBYTES_ID = 20,
  TOTALCONTEXTSWITCHES_ID = 146,
  TOTALSYSTEMCALLS_ID = 150,
  TOTALFILEDATAOPS_ID = 406,
  /* SYSTEMUPTIME_ID = 674, */
  PROCESSORQUEUELENGTH_ID = 44,
  ALIGNMENTFIXUPS_ID = 686,
  EXCEPTIONDISPATCHES_ID = 688,
  FLOATINGEMULATIONS_ID = 690,
  REGISTRYQUOTAINUSE_ID = 1350,
};
enum {
  TOTALFILEREADOPS_IDX = 0,
  TOTALFILEWRITEOPS_IDX,
  TOTALFILECONTROLOPS_IDX,
  TOTALFILEREADKBYTES_IDX,
  TOTALFILEWRITEKBYTES_IDX,
  TOTALFILECONTROLKBYTES_IDX,
  TOTALCONTEXTSWITCHES_IDX,
  TOTALSYSTEMCALLS_IDX,
  TOTALFILEDATAOPS_IDX,
  PROCESSORQUEUELENGTH_IDX,
  ALIGNMENTFIXUPS_IDX,
  EXCEPTIONDISPATCHES_IDX,
  FLOATINGEMULATIONS_IDX,
  REGISTRYQUOTAINUSE_IDX,

  MAX_SYSTEM_CTRS_COLLECTED
};
static PERF_COUNTER_DEFINITION systemCtrCache[MAX_SYSTEM_CTRS_COLLECTED];


enum {
  DATAGRAMS_RECEIVED_PER_SEC_ID = 446,
  DATAGRAMS_NO_PORT_PER_SEC_ID = 664,
  DATAGRAMS_RECEIVED_ERRORS_ID = 666,
  DATAGRAMS_SENT_PER_SEC_ID = 442,
};
enum {
  DATAGRAMS_RECEIVED_PER_SEC_IDX,
  DATAGRAMS_NO_PORT_PER_SEC_IDX,
  DATAGRAMS_RECEIVED_ERRORS_IDX,
  DATAGRAMS_SENT_PER_SEC_IDX,

  MAX_UDP_CTRS_COLLECTED
};
static PERF_COUNTER_DEFINITION udpCtrCache[MAX_UDP_CTRS_COLLECTED];

enum {
  AVAILABLEBYTES_ID = 24,
  COMMITTEDBYTES_ID = 26,
  COMMITLIMIT_ID = 30,
  TOTALPAGEFAULTS_ID = 28,
  WRITECOPIES_ID = 32,
  TRANSITIONFAULTS_ID = 34,
  CACHEFAULTS_ID = 36,
  DEMANDZEROFAULTS_ID = 38,
  PAGES_ID = 40,
  PAGESINPUT_ID = 822,
  PAGEREADS_ID = 42,
  PAGESOUTPUT_ID = 48,
  PAGEWRITES_ID = 50,
  TOTALPOOLPAGEDBYTES_ID = 56,
  TOTALPOOLNONPAGEDBYTES_ID = 58,
  POOLPAGEDALLOCS_ID = 60,
  POOLNONPAGEDALLOCS_ID = 64,
  FREESYSTEMPAGETABLEENTRIES_ID = 678,
  CACHEBYTES_ID = 818,
  CACHEBYTESPEAK_ID = 820,
  POOLPAGEDRESIDENTBYTES_ID = 66,
  SYSTEMCODETOTALBYTES_ID = 68,
  SYSTEMCODERESIDENTBYTES_ID = 70,
  SYSTEMDRIVERTOTALBYTES_ID = 72,
  SYSTEMDRIVERRESIDENTBYTES_ID = 74,
  SYSTEMCACHERESIDENTBYTES_ID = 76,
  COMMITTEDBYTESINUSE_ID = 1406
};
enum {
  AVAILABLEBYTES_IDX = 0,
  COMMITTEDBYTES_IDX,
  COMMITLIMIT_IDX,
  TOTALPAGEFAULTS_IDX,
  WRITECOPIES_IDX,
  TRANSITIONFAULTS_IDX,
  CACHEFAULTS_IDX,
  DEMANDZEROFAULTS_IDX,
  PAGES_IDX,
  PAGESINPUT_IDX,
  PAGEREADS_IDX,
  PAGESOUTPUT_IDX,
  PAGEWRITES_IDX,
  TOTALPOOLPAGEDBYTES_IDX,
  TOTALPOOLNONPAGEDBYTES_IDX,
  POOLPAGEDALLOCS_IDX,
  POOLNONPAGEDALLOCS_IDX,
  FREESYSTEMPAGETABLEENTRIES_IDX,
  CACHEBYTES_IDX,
  CACHEBYTESPEAK_IDX,
  POOLPAGEDRESIDENTBYTES_IDX,
  SYSTEMCODETOTALBYTES_IDX,
  SYSTEMCODERESIDENTBYTES_IDX,
  SYSTEMDRIVERTOTALBYTES_IDX,
  SYSTEMDRIVERRESIDENTBYTES_IDX,
  SYSTEMCACHERESIDENTBYTES_IDX,
  COMMITTEDBYTESINUSE_IDX,
  MAX_MEMORY_CTRS_COLLECTED
};
static PERF_COUNTER_DEFINITION memoryCtrCache[MAX_MEMORY_CTRS_COLLECTED];

enum {
  PROCESSES_ID = 248,
  THREADS_ID = 250,
  EVENTS_ID = 252,
  SEMAPHORES_ID = 254,
  MUTEXES_ID = 256,
  SECTIONS_ID = 258
};
enum {
  PROCESSES_IDX = 0,
  THREADS_IDX,
  EVENTS_IDX,
  SEMAPHORES_IDX,
  MUTEXES_IDX,
  SECTIONS_IDX,
  MAX_OBJECTS_CTRS_COLLECTED
};
static PERF_COUNTER_DEFINITION objectsCtrCache[MAX_OBJECTS_CTRS_COLLECTED];

enum {
  PACKETS_RECEIVED_ID = 266,
  BYTES_RECEIVED_ID = 264,
  PACKETS_SENT_ID = 452,
  BYTES_SENT_ID = 506,
  };


PPERF_OBJECT_TYPE FirstObject( PPERF_DATA_BLOCK PerfData )
{
  return( (PPERF_OBJECT_TYPE)((PBYTE)PerfData + 
                              PerfData->HeaderLength) );
}

PPERF_OBJECT_TYPE NextObject( PPERF_OBJECT_TYPE PerfObj )
{
  return( (PPERF_OBJECT_TYPE)((PBYTE)PerfObj + 
                              PerfObj->TotalByteLength) );
}

PPERF_INSTANCE_DEFINITION FirstInstance( PPERF_OBJECT_TYPE PerfObj )
{
  return( (PPERF_INSTANCE_DEFINITION)((PBYTE)PerfObj + 
                                      PerfObj->DefinitionLength) );
}

PPERF_INSTANCE_DEFINITION NextInstance(PPERF_COUNTER_BLOCK PerfCntrBlk)
{
  return( (PPERF_INSTANCE_DEFINITION)((PBYTE)PerfCntrBlk + 
                                      PerfCntrBlk->ByteLength) );
}

static char* getInstIdStr(PPERF_INSTANCE_DEFINITION PerfInst, char* prefix)
{
  static char resbuff[132];
  if (PerfInst->UniqueID == PERF_NO_UNIQUE_ID) {
    short* unicodePtr = (short*)((PBYTE)PerfInst + PerfInst->NameOffset);
    sprintf(resbuff, "%S length=%d unicode[0]=%d", (char*)((PBYTE)PerfInst + PerfInst->NameOffset), PerfInst->NameLength, unicodePtr[0]);
  } else {
    sprintf(resbuff, "%s%d", prefix, PerfInst->UniqueID);
  }
  return resbuff;
}


PPERF_COUNTER_DEFINITION FirstCounter( PPERF_OBJECT_TYPE PerfObj )
{
  return( (PPERF_COUNTER_DEFINITION) ((PBYTE)PerfObj + 
                                      PerfObj->HeaderLength) );
}

PPERF_COUNTER_DEFINITION NextCounter( 
                                     PPERF_COUNTER_DEFINITION PerfCntr )
{
  return( (PPERF_COUNTER_DEFINITION)((PBYTE)PerfCntr + 
                                     PerfCntr->ByteLength) );
}

static int64 getLongValue(PPERF_COUNTER_DEFINITION PerfCntr,
                           PPERF_COUNTER_BLOCK PerfCntrBlk);


typedef struct {
  uint32 perfTimeMs;
  int64 usertime;
  int64 systime;
  int64 idletime;
  int64 inttime;
  uint32 interrupts;
} FetchDataSType;

static FetchDataSType lastFetchData;
static FetchDataSType currentFetchData;

static void loadProcessData(void) {
  DWORD c;
  PPERF_COUNTER_DEFINITION PerfCntr;

  if (pidCtrOffset != -1)
    return; /* hunh? */
  if (ProcessObj == NULL)
    return;
  PerfCntr = FirstCounter(ProcessObj);
  for (c=0; c < ProcessObj->NumCounters; c++) {
    switch (PerfCntr->CounterNameTitleIndex) {
      case PID_ID:
	pidCtrOffset = PerfCntr->CounterOffset;
	break;
      case PROCESS_PROCESSORTIME_ID:
	processCtrCache[PROCESS_PROCESSORTIME_IDX] = *PerfCntr;
	break;
      case PROCESS_USERTIME_ID:
	processCtrCache[PROCESS_USERTIME_IDX] = *PerfCntr;
	break;
      case PROCESS_PRIVILEGEDTIME_ID:
	processCtrCache[PROCESS_PRIVILEGEDTIME_IDX] = *PerfCntr;
	break;
      case VIRTUALBYTESPEAK_ID:
	processCtrCache[VIRTUALBYTESPEAK_IDX] = *PerfCntr;
	break;
      case VIRTUALBYTES_ID:
	processCtrCache[VIRTUALBYTES_IDX] = *PerfCntr;
	break;
      case PAGEFAULTS_ID:
	processCtrCache[PAGEFAULTS_IDX] = *PerfCntr;
	break;
      case WORKINGSETPEAK_ID:
	processCtrCache[WORKINGSETPEAK_IDX] = *PerfCntr;
	break;
      case WORKINGSET_ID:
	processCtrCache[WORKINGSET_IDX] = *PerfCntr;
	break;
      case PAGEFILEBYTESPEAK_ID:
	processCtrCache[PAGEFILEBYTESPEAK_IDX] = *PerfCntr;
	break;
      case PAGEFILEBYTES_ID:
	processCtrCache[PAGEFILEBYTES_IDX] = *PerfCntr;
	break;
      case PRIVATEBYTES_ID:
	processCtrCache[PRIVATEBYTES_IDX] = *PerfCntr;
	break;
      case THREADCOUNT_ID:
	processCtrCache[THREADCOUNT_IDX] = *PerfCntr;
	break;
      case PRIORITYBASE_ID:
	processCtrCache[PRIORITYBASE_IDX] = *PerfCntr;
	break;
      case POOLPAGEDBYTES_ID:
	processCtrCache[POOLPAGEDBYTES_IDX] = *PerfCntr;
	break;
      case POOLNONPAGEDBYTES_ID:
	processCtrCache[POOLNONPAGEDBYTES_IDX] = *PerfCntr;
	break;
      case HANDLECOUNT_ID:
	processCtrCache[HANDLECOUNT_IDX] = *PerfCntr;
	break;
      default:
	/* unknown counter. just skip it. */
	break;
      }
    PerfCntr = NextCounter(PerfCntr);
    } /* for */
#ifdef FLG_DEBUG
  for (c=0; c < MAX_PROCESS_CTRS_COLLECTED; c++) {
    if (processCtrCache[c].CounterNameTitleIndex == 0) {
      fprintf(stderr, "DEBUG: bad processCtr at idx=%d\n", c); fflush(stderr);
      }
    }
#endif
  }

static void loadProcessorData(void) {
  DWORD c;
  PPERF_COUNTER_DEFINITION PerfCntr;

  if (ProcessorObj == NULL)
    return;
#if 0
  if (pidCtrOffset != -1)
    return; /* hunh? */
#endif
  PerfCntr = FirstCounter(ProcessorObj);
#if 0
  fprintf(stderr, "ProcessorObj->NumCounters=%d\n", ProcessorObj->NumCounters); fflush(stderr);
#endif

  for (c=0; c < ProcessorObj->NumCounters; c++) {
#if 0
    fprintf(stderr, "PerfCntr->CounterNameTitleIndex=%d\n", 
        PerfCntr->CounterNameTitleIndex); 
    fflush(stderr);
#endif
    switch (PerfCntr->CounterNameTitleIndex) {
      case TOTAL_PROCESSORTIME_ID:
	processorCtrCache[TOTAL_PROCESSORTIME_IDX] = *PerfCntr;
	break;
      case TOTAL_USERTIME_ID:
	processorCtrCache[TOTAL_USERTIME_IDX] = *PerfCntr;
	break;
      case TOTAL_PRIVILEGEDTIME_ID:
	processorCtrCache[TOTAL_PRIVILEGEDTIME_IDX] = *PerfCntr;
	break;
      case INTERRUPTTIME_ID:
	processorCtrCache[INTERRUPTTIME_IDX] = *PerfCntr;
	break;
      case INTERRUPTS_ID:
	processorCtrCache[INTERRUPTS_IDX] = *PerfCntr;
	break;
      default:
	/* unknown counter. just skip it. */
	break;
      }
    PerfCntr = NextCounter(PerfCntr);
  }
#ifdef FLG_DEBUG
  for (c=0; c < MAX_PROCESSOR_CTRS_COLLECTED; c++) {
    if (processorCtrCache[c].CounterNameTitleIndex == 0) {
      fprintf(stderr, "DEBUG: bad processorCtr at idx=%d\n", c); fflush(stderr);
    }
  }
#endif
}

static void loadSystemData(void) {
  PPERF_COUNTER_DEFINITION PerfCntr;
  DWORD c;

  if (SystemObj == NULL)
    return;
  PerfCntr = FirstCounter(SystemObj);
  for (c=0; c < SystemObj->NumCounters; c++) {
    switch (PerfCntr->CounterNameTitleIndex) {
      case TOTALFILEREADOPS_ID:
	systemCtrCache[TOTALFILEREADOPS_IDX] = *PerfCntr;
	break;
      case TOTALFILEWRITEOPS_ID:
	systemCtrCache[TOTALFILEWRITEOPS_IDX] = *PerfCntr;
	break;
      case TOTALFILECONTROLOPS_ID:
	systemCtrCache[TOTALFILECONTROLOPS_IDX] = *PerfCntr;
	break;
      case TOTALFILEREADKBYTES_ID:
	systemCtrCache[TOTALFILEREADKBYTES_IDX] = *PerfCntr;
	break;
      case TOTALFILEWRITEKBYTES_ID:
	systemCtrCache[TOTALFILEWRITEKBYTES_IDX] = *PerfCntr;
	break;
      case TOTALFILECONTROLKBYTES_ID:
	systemCtrCache[TOTALFILECONTROLKBYTES_IDX] = *PerfCntr;
	break;
      case TOTALCONTEXTSWITCHES_ID:
	systemCtrCache[TOTALCONTEXTSWITCHES_IDX] = *PerfCntr;
	break;
      case TOTALSYSTEMCALLS_ID:
	systemCtrCache[TOTALSYSTEMCALLS_IDX] = *PerfCntr;
	break;
      case TOTALFILEDATAOPS_ID:
	systemCtrCache[TOTALFILEDATAOPS_IDX] = *PerfCntr;
	break;
      case PROCESSORQUEUELENGTH_ID:
	systemCtrCache[PROCESSORQUEUELENGTH_IDX] = *PerfCntr;
	break;
      case ALIGNMENTFIXUPS_ID:
	systemCtrCache[ALIGNMENTFIXUPS_IDX] = *PerfCntr;
	break;
      case EXCEPTIONDISPATCHES_ID:
	systemCtrCache[EXCEPTIONDISPATCHES_IDX] = *PerfCntr;
	break;
      case FLOATINGEMULATIONS_ID:
	systemCtrCache[FLOATINGEMULATIONS_IDX] = *PerfCntr;
	break;
      case REGISTRYQUOTAINUSE_ID:
	systemCtrCache[REGISTRYQUOTAINUSE_IDX] = *PerfCntr;
	break;
      default:
	/* unknown counter. just skip it. */
	break;
      }
    PerfCntr = NextCounter(PerfCntr);
  }
#ifdef FLG_DEBUG
  for (c=0; c < MAX_SYSTEM_CTRS_COLLECTED; c++) {
    if (systemCtrCache[c].CounterNameTitleIndex == 0) {
      fprintf(stderr, "DEBUG: bad systemCtr at idx=%d\n", c); fflush(stderr);
    }
  }
#endif
  }

static void loadMemoryData(void) {
  PPERF_COUNTER_DEFINITION PerfCntr;
  DWORD c;

  if (MemoryObj == NULL)
    return;
  PerfCntr = FirstCounter(MemoryObj);
  for (c=0; c < MemoryObj->NumCounters; c++) {
    switch (PerfCntr->CounterNameTitleIndex) {
    case AVAILABLEBYTES_ID:
      memoryCtrCache[AVAILABLEBYTES_IDX] = *PerfCntr;
      break;
    case COMMITTEDBYTES_ID:
      memoryCtrCache[COMMITTEDBYTES_IDX] = *PerfCntr;
      break;
    case COMMITLIMIT_ID:
      memoryCtrCache[COMMITLIMIT_IDX] = *PerfCntr;
      break;
    case TOTALPAGEFAULTS_ID:
      memoryCtrCache[TOTALPAGEFAULTS_IDX] = *PerfCntr;
      break;
    case WRITECOPIES_ID:
      memoryCtrCache[WRITECOPIES_IDX] = *PerfCntr;
      break;
    case TRANSITIONFAULTS_ID:
      memoryCtrCache[TRANSITIONFAULTS_IDX] = *PerfCntr;
      break;
    case CACHEFAULTS_ID:
      memoryCtrCache[CACHEFAULTS_IDX] = *PerfCntr;
      break;
    case DEMANDZEROFAULTS_ID:
      memoryCtrCache[DEMANDZEROFAULTS_IDX] = *PerfCntr;
      break;
    case PAGES_ID:
      memoryCtrCache[PAGES_IDX] = *PerfCntr;
      break;
    case PAGESINPUT_ID:
      memoryCtrCache[PAGESINPUT_IDX] = *PerfCntr;
      break;
    case PAGEREADS_ID:
      memoryCtrCache[PAGEREADS_IDX] = *PerfCntr;
      break;
    case PAGESOUTPUT_ID:
      memoryCtrCache[PAGESOUTPUT_IDX] = *PerfCntr;
      break;
    case PAGEWRITES_ID:
      memoryCtrCache[PAGEWRITES_IDX] = *PerfCntr;
      break;
    case TOTALPOOLPAGEDBYTES_ID:
      memoryCtrCache[TOTALPOOLPAGEDBYTES_IDX] = *PerfCntr;
      break;
    case TOTALPOOLNONPAGEDBYTES_ID:
      memoryCtrCache[TOTALPOOLNONPAGEDBYTES_IDX] = *PerfCntr;
      break;
    case POOLPAGEDALLOCS_ID:
      memoryCtrCache[POOLPAGEDALLOCS_IDX] = *PerfCntr;
      break;
    case POOLNONPAGEDALLOCS_ID:
      memoryCtrCache[POOLNONPAGEDALLOCS_IDX] = *PerfCntr;
      break;
    case FREESYSTEMPAGETABLEENTRIES_ID:
      memoryCtrCache[FREESYSTEMPAGETABLEENTRIES_IDX] = *PerfCntr;
      break;
    case CACHEBYTES_ID:
      memoryCtrCache[CACHEBYTES_IDX] = *PerfCntr;
      break;
    case CACHEBYTESPEAK_ID:
      memoryCtrCache[CACHEBYTESPEAK_IDX] = *PerfCntr;
      break;
    case POOLPAGEDRESIDENTBYTES_ID:
      memoryCtrCache[POOLPAGEDRESIDENTBYTES_IDX] = *PerfCntr;
      break;
    case SYSTEMCODETOTALBYTES_ID:
      memoryCtrCache[SYSTEMCODETOTALBYTES_IDX] = *PerfCntr;
      break;
    case SYSTEMCODERESIDENTBYTES_ID:
      memoryCtrCache[SYSTEMCODERESIDENTBYTES_IDX] = *PerfCntr;
      break;
    case SYSTEMDRIVERTOTALBYTES_ID:
      memoryCtrCache[SYSTEMDRIVERTOTALBYTES_IDX] = *PerfCntr;
      break;
    case SYSTEMDRIVERRESIDENTBYTES_ID:
      memoryCtrCache[SYSTEMDRIVERRESIDENTBYTES_IDX] = *PerfCntr;
      break;
    case SYSTEMCACHERESIDENTBYTES_ID:
      memoryCtrCache[SYSTEMCACHERESIDENTBYTES_IDX] = *PerfCntr;
      break;
    case COMMITTEDBYTESINUSE_ID:
      memoryCtrCache[COMMITTEDBYTESINUSE_IDX] = *PerfCntr;
      break;
    default:
      /* unknown counter. just skip it. */
      break;
    }
    PerfCntr = NextCounter(PerfCntr);
  }
#ifdef FLG_DEBUG
  for (c=0; c < MAX_MEMORY_CTRS_COLLECTED; c++) {
    if (memoryCtrCache[c].CounterNameTitleIndex == 0) {
      fprintf(stderr, "DEBUG: bad memoryCtr at idx=%d\n", c); fflush(stderr);
    }
  }
#endif
}

static void loadObjectData(void) {
  PPERF_COUNTER_DEFINITION PerfCntr;
  DWORD c;

  if ( ObjectsObj == NULL ) {
    return;
  }
  
  PerfCntr = FirstCounter(ObjectsObj);
  for (c=0; c < ObjectsObj->NumCounters; c++) {
    switch (PerfCntr->CounterNameTitleIndex) {
    case PROCESSES_ID:
      objectsCtrCache[PROCESSES_IDX] = *PerfCntr;
      break;
    case THREADS_ID:
      objectsCtrCache[THREADS_IDX] = *PerfCntr;
      break;
    case EVENTS_ID:
      objectsCtrCache[EVENTS_IDX] = *PerfCntr;
      break;
    case SEMAPHORES_ID:
      objectsCtrCache[SEMAPHORES_IDX] = *PerfCntr;
      break;
    case MUTEXES_ID:
      objectsCtrCache[MUTEXES_IDX] = *PerfCntr;
      break;
    case SECTIONS_ID:
      objectsCtrCache[SECTIONS_IDX] = *PerfCntr;
      break;
    default:
      /* unknown counter. just skip it. */
      break;
    }
    PerfCntr = NextCounter(PerfCntr);
  }
#ifdef FLG_DEBUG
  for (c=0; c < MAX_OBJECTS_CTRS_COLLECTED; c++) {
    if (objectsCtrCache[c].CounterNameTitleIndex == 0) {
      fprintf(stderr, "DEBUG: bad objectsCtr at idx=%d\n", c); fflush(stderr);
    }
  }
#endif
  }

static void loadUdpData(void) {
  PPERF_COUNTER_DEFINITION PerfCntr;
  DWORD c;

  if (UdpObj == NULL)
    return;
  PerfCntr = FirstCounter(UdpObj);
  for (c=0; c < UdpObj->NumCounters; c++) {
    switch (PerfCntr->CounterNameTitleIndex) {
    case DATAGRAMS_RECEIVED_PER_SEC_ID:
      udpCtrCache[DATAGRAMS_RECEIVED_PER_SEC_IDX] = *PerfCntr;
      break;
    case DATAGRAMS_NO_PORT_PER_SEC_ID:
      udpCtrCache[DATAGRAMS_NO_PORT_PER_SEC_IDX] = *PerfCntr;
      break;
    case DATAGRAMS_RECEIVED_ERRORS_ID:
      udpCtrCache[DATAGRAMS_RECEIVED_ERRORS_IDX] = *PerfCntr;
      break;
    case DATAGRAMS_SENT_PER_SEC_ID:
      udpCtrCache[DATAGRAMS_SENT_PER_SEC_IDX] = *PerfCntr;
      break;
    default:
      /* unknown counter. just skip it. */
      break;
      }
    PerfCntr = NextCounter(PerfCntr);
  }
#ifdef FLG_DEBUG
  for (c=0; c < MAX_UDP_CTRS_COLLECTED; c++) {
    if (udpCtrCache[c].CounterNameTitleIndex == 0) {
      fprintf(stderr, "DEBUG: bad udpCtr at idx=%d\n", c); fflush(stderr);
    }
  }
#endif
  }

static void loadCpuData(void) {
  int32 i;
  boolean foundIt = FALSE;
  PPERF_INSTANCE_DEFINITION PerfInst;

  lastFetchData = currentFetchData;
  currentFetchData.perfTimeMs = (uint32)(((__int64)PerfData->PerfTime100nSec.QuadPart) / 10000);

  if (ProcessorObj == NULL) {
#ifdef FLG_DEBUG
    fprintf(stderr, "DEBUG: loadCpuData, no ProcessorObj!\n");
#endif
    return;
    }
  if (ProcessObj == NULL) {
#ifdef FLG_DEBUG
    fprintf(stderr, "DEBUG: loadCpuData, no ProcessObj!\n");
#endif
    return;
    }
  if (ProcessObj->NumInstances <= 0) {
#ifdef FLG_DEBUG
    fprintf(stderr, "DEBUG: loadCpuData, ProcessObj->NumInstances <= 0!\n");
#endif
    return;
    }

  PerfInst = FirstInstance(ProcessorObj);
  for (i=0; i < (int32)ProcessorObj->NumInstances; i++) {
    PPERF_COUNTER_BLOCK PerfCntrBlk = (PPERF_COUNTER_BLOCK)((PBYTE)PerfInst + PerfInst->ByteLength);

    /* starts with "_" must be "_Total" */
    short* unicodePtr = (short*)((PBYTE)PerfInst + PerfInst->NameOffset);
    if (PerfInst->UniqueID == PERF_NO_UNIQUE_ID && unicodePtr[0] != '_') {
      PerfInst = NextInstance(PerfCntrBlk);
      continue;
      }

    currentFetchData.usertime = getLongValue(&processorCtrCache[TOTAL_USERTIME_IDX], PerfCntrBlk);
    currentFetchData.systime = getLongValue(&processorCtrCache[TOTAL_PRIVILEGEDTIME_IDX], PerfCntrBlk);
    currentFetchData.idletime = getLongValue(&processorCtrCache[TOTAL_PROCESSORTIME_IDX], PerfCntrBlk);
    currentFetchData.inttime = getLongValue(&processorCtrCache[INTERRUPTTIME_IDX], PerfCntrBlk);
    currentFetchData.interrupts = (uint32)getLongValue(&processorCtrCache[INTERRUPTS_IDX], PerfCntrBlk);
    foundIt = TRUE;
    break;
    }

#ifdef FLG_DEBUG
  if (!foundIt) {
    fprintf(stderr, "DEBUG: did not find processor named _Total!\n");
  }
#endif
}


static struct {
  uint32 loopbackPackets;
  uint32 loopbackBytes;
  uint32 packetsReceived;
  uint32 bytesReceived;
  uint32 packetsSent;
  uint32 bytesSent;
  } netStats;

static void loadNetworkData(void)
{
  int32 i;
  unsigned int c;
  int32 val;
  char nausea[512];
  PPERF_COUNTER_DEFINITION PerfCntr;
  PPERF_COUNTER_DEFINITION CurCntr;
  PPERF_INSTANCE_DEFINITION PerfInst;
  PPERF_COUNTER_BLOCK PerfCntrBlk;

  if (NetworkObj == NULL) {
#ifdef FLG_DEBUG
    fprintf(stderr, "DEBUG: loadNetworkData, no NetworkObj!\n");
#endif
    return;
    }
  if (NetworkObj->NumInstances <= 0) {
#ifdef FLG_DEBUG
    fprintf(stderr, "DEBUG: loadNetworkData, NetworkObj->NumInstances <= 0!\n");
#endif
    return;
    }

  /* We're collecting sums, so start by zeroing out */
  netStats.loopbackPackets = 0;
  netStats.loopbackBytes = 0;
  netStats.packetsReceived = 0;
  netStats.bytesReceived = 0;
  netStats.packetsSent = 0;
  netStats.bytesSent = 0;


  PerfCntr = FirstCounter(NetworkObj);

  // retrieve all instances
  PerfInst = FirstInstance(NetworkObj);

  for (i = 0; i < NetworkObj->NumInstances; i ++) {
    boolean loopBack;

    wchar_t* unicodePtr = (wchar_t*)((PBYTE)PerfInst + PerfInst->NameOffset);
#if 0
    printf( "\n\tInstance %S: \n", unicodePtr);
#endif

    sprintf(nausea, "%S", unicodePtr);
    loopBack = strcmp(nausea, "MS TCP Loopback interface") == 0;

    PerfCntrBlk = (PPERF_COUNTER_BLOCK)((PBYTE)PerfInst + PerfInst->ByteLength);
    CurCntr = PerfCntr;

    // retrieve all counters
    for (c = 0; c < NetworkObj->NumCounters; c ++) {

#if 0
      printf("\t\tCounter %ld: %s\n",
         CurCntr->CounterNameTitleIndex,
         lpNamesArray[CurCntr->CounterNameTitleIndex]);
#endif

      switch (CurCntr->CounterNameTitleIndex) {
      case PACKETS_RECEIVED_ID:
        val = (int32)getLongValue(CurCntr, PerfCntrBlk);
        if (loopBack)
          netStats.loopbackPackets = val;
        else
          netStats.packetsReceived += val;
	break;
      case BYTES_RECEIVED_ID:
        val = (int32)getLongValue(CurCntr, PerfCntrBlk);
        if (loopBack)
          netStats.loopbackBytes = val;
        else
          netStats.bytesReceived += val;
	break;
      case PACKETS_SENT_ID:
        if (!loopBack) {
          val = (int32)getLongValue(CurCntr, PerfCntrBlk);
          netStats.packetsSent += val;
          }
	break;
      case BYTES_SENT_ID:
        if (!loopBack) {
          val = (int32)getLongValue(CurCntr, PerfCntrBlk);
          netStats.bytesSent += val;
          }
	break;
      default:
	/* unknown counter. just skip it. */
	break;
	}

      CurCntr = NextCounter(CurCntr);
      }
    PerfInst = NextInstance(PerfCntrBlk);
    }
}


static int HostStatsFetchData(void)
{
  DWORD o;
  PPERF_OBJECT_TYPE objPtr = NULL;
  DWORD res;
  DWORD oldBufferSize = BufferSize;
  const char * qstr;
  qstr = LEVEL1_QUERY_STRING;

  while ((res = RegQueryValueEx(HKEY_PERFORMANCE_DATA, qstr,
				NULL, NULL,
				(LPBYTE)PerfData,
				&BufferSize)) == ERROR_MORE_DATA) {
    oldBufferSize += 4096;
    BufferSize = oldBufferSize;
    PerfData = (PPERF_DATA_BLOCK)realloc(PerfData, BufferSize);
    }
  if (res != ERROR_SUCCESS) {
    fprintf(stderr,
        "Can't get Windows performance data. RegQueryValueEx returned %ld, errorno \"%d\"\n",
        GetLastError(),
	res);
    return -1;
    }
#ifdef NTDBG
  fprintf(stderr, "buffersize is %ld\n", BufferSize);
  fflush(stderr);
#endif

  ProcessObj = NULL;
  ProcessorObj = NULL;
  MemoryObj = NULL;
  SystemObj = NULL;
  ObjectsObj = NULL;

  objPtr = FirstObject(PerfData);
  for (o=0; o < PerfData->NumObjectTypes; o++) {
#ifdef NTDBG
    fprintf(stderr, "Object %ld\n", objPtr->ObjectNameTitleIndex);
    fflush(stderr);
#endif
    switch (objPtr->ObjectNameTitleIndex) {
      case PROCESS_OBJ_ID: ProcessObj = objPtr; break;
      case PROCESSOR_OBJ_ID: ProcessorObj = objPtr; break;
      case MEMORY_OBJ_ID: MemoryObj = objPtr; break;
      case SYSTEM_OBJ_ID: SystemObj = objPtr; break;
      case OBJECTS_OBJ_ID: ObjectsObj = objPtr; break;
      case NETWORK_OBJ_ID: NetworkObj = objPtr; break;
      case UDP_OBJ_ID: UdpObj = objPtr; break;
      }
    objPtr = NextObject(objPtr);
    }
  if ( ProcessObj == NULL ||
       ProcessorObj == NULL ||
       MemoryObj == NULL ||
       SystemObj == NULL ||
       ObjectsObj == NULL ||
       NetworkObj == NULL ||
       UdpObj == NULL )
  {
    return -1;
  }
  loadProcessData();
  loadProcessorData();
  loadSystemData();
  loadMemoryData();
  loadObjectData();
  loadUdpData();
  loadCpuData();
  loadNetworkData();
  return 0;
}

static int32 getPid(int32 pidCtrOffset, PPERF_COUNTER_BLOCK PerfCntrBlk)
{
  int32* result = (int32*)((char*)PerfCntrBlk + pidCtrOffset);
  return *result;
}

static int64 getLongValue(PPERF_COUNTER_DEFINITION PerfCntr,
                           PPERF_COUNTER_BLOCK PerfCntrBlk)
{
  int64 result;
  __int64* i64ptr;
  uint32* lptr;

  if (PerfCntr->CounterOffset == 0) {
#ifdef FLG_DEBUG
    //this is being emitted in normal builds - temporarily disabling it
    //fprintf(stderr, "missing counter id=%d\n", PerfCntr->CounterNameTitleIndex);
#endif
    return 0;
  }
  switch (PerfCntr->CounterSize) {
    case 8:

      i64ptr = (__int64*)((char*)PerfCntrBlk + PerfCntr->CounterOffset);
      result = *i64ptr;

      /* #define I64_TRACE */
#ifdef I64_TRACE
      printf("%x %I64x", PerfCntr->CounterType, result);
#endif
      /* if (result == 0) return 0; */
      switch (PerfCntr->CounterType) {
	case PERF_COUNTER_LARGE_RAWCOUNT:
	case PERF_COUNTER_BULK_COUNT:
	  break;
#if 0
	case PERF_COUNTER_TIMER:
	  /* convert to milliseconds */
	  /* PerfFreq is number of times per seconds so adjust by 1000 for ms */
	  result /=  (((__int64)PerfData->PerfFreq.QuadPart) / 1000);
	  break;
#endif
	case PERF_100NSEC_TIMER_INV:
	case PERF_100NSEC_TIMER:
	  /* convert to milliseconds */
	  result /= 10000;
	  break;
	default:
	  fprintf(stderr, "HostStatHelperWin.getLongValue: unexpected 8-byte CounterType %lx, index = %ld\n",
	       PerfCntr->CounterType, PerfCntr->CounterNameTitleIndex);
	  break;
	}
      break;

    case 4:
      lptr = (uint32*)((char*)PerfCntrBlk + PerfCntr->CounterOffset);
      switch (PerfCntr->CounterType) {
        case PERF_RAW_FRACTION: {
          uint32 numerator = lptr[0];
          uint32 denominator = lptr[1];
          if (denominator != 0)
	    result = (uint32)(((double)numerator / (double)denominator) * 100.0);
          else
	    result = 0;
	  break;
          }

        case PERF_RAW_BASE: {
          uint32 denominator = lptr[0];
          uint32 numerator = lptr[-1];
          if (denominator != 0)
	    result = (uint32)(((double)numerator / (double)denominator) * 100.0);
          else
	    result = 0;
          break;
          }

	case PERF_COUNTER_COUNTER:
        case PERF_COUNTER_RATE:
          result = *lptr;
	  break;

        default:
          result = *lptr; // wild guess?
	  fprintf(stderr, "HostStatHelperWin.getLongValue: unexpected 4-byte CounterType %lx, index = %ld\n",
	       PerfCntr->CounterType, PerfCntr->CounterNameTitleIndex);
          break;
        }

      break;

    default:
      fprintf(stderr, "HostStatHelperWin.getLongValue: unexpected CounterSize of %ld, index = %ld\n",
          PerfCntr->CounterSize, PerfCntr->CounterNameTitleIndex);
      return 0;
    }

    return result;
}

static void refreshSystem(int32* intStorage, int64* longStorage, double* doubleStorage)
{
#define INT_FIELD_STORE(field, value) \
    (intStorage[com_gemstone_gemfire_internal_WindowsSystemStats_##field##INT] = (int32)(value))
#define LONG_FIELD_STORE(field, value) \
    (longStorage[com_gemstone_gemfire_internal_WindowsSystemStats_##field##LONG] = (int64)(value))
#define DOUBLE_FIELD_STORE(field, value) \
    (doubleStorage[com_gemstone_gemfire_internal_WindowsSystemStats_##field##DOUBLE] = (double)(value))
#define FLOAT_FIELD_STORE(field, value) \
    DOUBLE_FIELD_STORE(field, (double)(value))

  {
    float usertime = 0.0;
    float systime = 0.0;
    float idletime = 0.0;
    float activetime = 0.0;
    float inttime = 0.0;
    if (lastFetchData.perfTimeMs != 0) {
      uint32 timeDelta = currentFetchData.perfTimeMs - lastFetchData.perfTimeMs;
      int64 valueDelta;
      valueDelta = currentFetchData.usertime - lastFetchData.usertime;
      usertime = (float)(100.0 * ((double)valueDelta / timeDelta));
      valueDelta = currentFetchData.systime - lastFetchData.systime;
      systime = (float)(100.0 * ((double)valueDelta / timeDelta));
      valueDelta = currentFetchData.inttime - lastFetchData.inttime;
      inttime = (float)(100.0 * ((double)valueDelta / timeDelta));
      valueDelta = currentFetchData.idletime - lastFetchData.idletime;
      idletime = (float)(100.0 * ((double)valueDelta / timeDelta));
      activetime = (float)(100.0 - idletime);
    }

    FLOAT_FIELD_STORE(cpuActive, activetime);
    FLOAT_FIELD_STORE(cpuUser, usertime);
    FLOAT_FIELD_STORE(cpuSystem, systime);
    FLOAT_FIELD_STORE(cpuInterrupt, inttime);
    FLOAT_FIELD_STORE(cpuIdle, idletime);
    INT_FIELD_STORE(interrupts, currentFetchData.interrupts);
  }

  if (SystemObj) {
    if (SystemObj->NumInstances == PERF_NO_INSTANCES) {
      PPERF_COUNTER_BLOCK PerfCntrBlk;
      PerfCntrBlk = (PPERF_COUNTER_BLOCK)FirstInstance(SystemObj);

      LONG_FIELD_STORE(contextSwitches, getLongValue(&systemCtrCache[TOTALCONTEXTSWITCHES_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(systemCalls, getLongValue(&systemCtrCache[TOTALSYSTEMCALLS_IDX], PerfCntrBlk));
      INT_FIELD_STORE(processorQueueLength, getLongValue(&systemCtrCache[PROCESSORQUEUELENGTH_IDX], PerfCntrBlk));
      INT_FIELD_STORE(registryQuotaInUse, getLongValue(&systemCtrCache[REGISTRYQUOTAINUSE_IDX], PerfCntrBlk));


    } else {
#ifdef FLG_DEBUG
      fprintf(stderr, "DEBUG: unexpected >0 instances!\n");
#endif
    }
  }

  if (UdpObj) {
    if (UdpObj->NumInstances == PERF_NO_INSTANCES) {
      PPERF_COUNTER_BLOCK PerfCntrBlk;
      PerfCntrBlk = (PPERF_COUNTER_BLOCK)FirstInstance(UdpObj);

      INT_FIELD_STORE(dgramsReceived, getLongValue(&udpCtrCache[DATAGRAMS_RECEIVED_PER_SEC_IDX], PerfCntrBlk));
      INT_FIELD_STORE(dgramsNoPort, getLongValue(&udpCtrCache[DATAGRAMS_NO_PORT_PER_SEC_IDX], PerfCntrBlk));
      INT_FIELD_STORE(dgramsReceivedErrors, getLongValue(&udpCtrCache[DATAGRAMS_RECEIVED_ERRORS_IDX], PerfCntrBlk));
      INT_FIELD_STORE(dgramsSent, getLongValue(&udpCtrCache[DATAGRAMS_SENT_PER_SEC_IDX], PerfCntrBlk));
    } else {
#ifdef FLG_DEBUG
      fprintf(stderr, "DEBUG: unexpected >0 instances!\n");
#endif
    }
    
    }

  if (MemoryObj) {
    if (MemoryObj->NumInstances == PERF_NO_INSTANCES) {
      PPERF_COUNTER_BLOCK PerfCntrBlk;
      PerfCntrBlk = (PPERF_COUNTER_BLOCK)FirstInstance(MemoryObj);

      LONG_FIELD_STORE(availableMemory, getLongValue(&memoryCtrCache[AVAILABLEBYTES_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(committedMemory, getLongValue(&memoryCtrCache[COMMITTEDBYTES_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(committedMemoryLimit, getLongValue(&memoryCtrCache[COMMITLIMIT_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(pageFaults, getLongValue(&memoryCtrCache[TOTALPAGEFAULTS_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(cacheFaults, getLongValue(&memoryCtrCache[CACHEFAULTS_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(demandZeroFaults, getLongValue(&memoryCtrCache[DEMANDZEROFAULTS_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(pages, getLongValue(&memoryCtrCache[PAGES_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(pagesInput, getLongValue(&memoryCtrCache[PAGESINPUT_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(pageReads, getLongValue(&memoryCtrCache[PAGEREADS_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(pagesOutput, getLongValue(&memoryCtrCache[PAGESOUTPUT_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(pageWrites, getLongValue(&memoryCtrCache[PAGEWRITES_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(cacheSize, getLongValue(&memoryCtrCache[CACHEBYTES_IDX], PerfCntrBlk));
      LONG_FIELD_STORE(cacheSizePeak, getLongValue(&memoryCtrCache[CACHEBYTESPEAK_IDX], PerfCntrBlk));
      INT_FIELD_STORE(committedMemoryInUse, getLongValue(&memoryCtrCache[COMMITTEDBYTESINUSE_IDX], PerfCntrBlk));
    } else {
#ifdef FLG_DEBUG
      fprintf(stderr, "DEBUG: unexpected >0 instances!\n");
#endif
    }
  }

  if (ObjectsObj) {
    if (ObjectsObj->NumInstances == PERF_NO_INSTANCES) {
      PPERF_COUNTER_BLOCK PerfCntrBlk;
      PerfCntrBlk = (PPERF_COUNTER_BLOCK)FirstInstance(ObjectsObj);

      INT_FIELD_STORE(processes, getLongValue(&objectsCtrCache[PROCESSES_IDX], PerfCntrBlk));
      INT_FIELD_STORE(threads, getLongValue(&objectsCtrCache[THREADS_IDX], PerfCntrBlk));
      INT_FIELD_STORE(events, getLongValue(&objectsCtrCache[EVENTS_IDX], PerfCntrBlk));
      INT_FIELD_STORE(semaphores, getLongValue(&objectsCtrCache[SEMAPHORES_IDX], PerfCntrBlk));
      INT_FIELD_STORE(mutexes, getLongValue(&objectsCtrCache[MUTEXES_IDX], PerfCntrBlk));
      INT_FIELD_STORE(sharedMemorySections, getLongValue(&objectsCtrCache[SECTIONS_IDX], PerfCntrBlk));
    } else {
#ifdef FLG_DEBUG
      fprintf(stderr, "DEBUG: unexpected >0 instances!\n");
#endif
    }
  }

  if (NetworkObj) {
    INT_FIELD_STORE(loopbackPackets, netStats.loopbackPackets);
    INT_FIELD_STORE(loopbackBytes, netStats.loopbackBytes);
    INT_FIELD_STORE(netPacketsReceived, netStats.packetsReceived);
    INT_FIELD_STORE(netBytesReceived, netStats.bytesReceived);
    INT_FIELD_STORE(netPacketsSent, netStats.packetsSent);
    INT_FIELD_STORE(netBytesSent, netStats.bytesSent);
    }
#undef INT_FIELD_STORE
#undef LONG_FIELD_STORE
#undef FLOAT_FIELD_STORE
#undef DOUBLE_FIELD_STORE
}

static void refreshProcess(jint pid, int32* intStorage, 
    int64* longStorage, double* doubleStorage)
{
#define INT_FIELD_STORE(field, value) \
    (intStorage[com_gemstone_gemfire_internal_WindowsProcessStats_##field##INT] = value)
#define LONG_FIELD_STORE(field, value) \
    (longStorage[com_gemstone_gemfire_internal_WindowsProcessStats_##field##LONG] = value)
#define DOUBLE_FIELD_STORE(field, value) \
    (doubleStorage[com_gemstone_gemfire_internal_WindowsProcessStats_##field##DOUBLE] = value)
#define FLOAT_FIELD_STORE(field, value) \
    DOUBLE_FIELD_STORE(field, (double)(value))

  int32 i;
  if (ProcessObj) {
    PPERF_INSTANCE_DEFINITION PerfInst;

    if (ProcessObj->NumInstances > 0) {
#if 0
      static int done = 0;
      if (!done) {
	done = 1;
	PerfInst = FirstInstance(ProcessObj);
	for (i=0; i < (int32)ProcessObj->NumInstances; i++) {
	  PPERF_COUNTER_BLOCK PerfCntrBlk;
	  PerfCntrBlk = (PPERF_COUNTER_BLOCK)((PBYTE)PerfInst + 
					      PerfInst->ByteLength);
	  printf("process: %s\n", getInstIdStr(PerfInst, "proc-"));
	  PerfInst = NextInstance(PerfCntrBlk);
	}
      }
#endif
      PerfInst = FirstInstance(ProcessObj);
      for (i=0; i < (int32)ProcessObj->NumInstances; i++) {
	PPERF_COUNTER_BLOCK PerfCntrBlk;
	PerfCntrBlk = (PPERF_COUNTER_BLOCK)((PBYTE)PerfInst + 
					    PerfInst->ByteLength);
	if (pid == getPid(pidCtrOffset, PerfCntrBlk)) {
          LONG_FIELD_STORE(activeTime, getLongValue(&processCtrCache[PROCESS_PROCESSORTIME_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(userTime, getLongValue(&processCtrCache[PROCESS_USERTIME_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(systemTime, getLongValue(&processCtrCache[PROCESS_PRIVILEGEDTIME_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(virtualSizePeak, getLongValue(&processCtrCache[VIRTUALBYTESPEAK_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(virtualSize, getLongValue(&processCtrCache[VIRTUALBYTES_IDX], PerfCntrBlk));

          LONG_FIELD_STORE(pageFaults, getLongValue(&processCtrCache[PAGEFAULTS_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(workingSetSizePeak, getLongValue(&processCtrCache[WORKINGSETPEAK_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(workingSetSize, getLongValue(&processCtrCache[WORKINGSET_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(pageFileSizePeak, getLongValue(&processCtrCache[PAGEFILEBYTESPEAK_IDX], PerfCntrBlk));
          LONG_FIELD_STORE(pageFileSize, getLongValue(&processCtrCache[PAGEFILEBYTES_IDX], PerfCntrBlk));


          LONG_FIELD_STORE(privateSize, getLongValue(&processCtrCache[PRIVATEBYTES_IDX], PerfCntrBlk));
          INT_FIELD_STORE(threads, (int32)getLongValue(&processCtrCache[THREADCOUNT_IDX], PerfCntrBlk));
          INT_FIELD_STORE(priorityBase, (int32)getLongValue(&processCtrCache[PRIORITYBASE_IDX], PerfCntrBlk));
       /* ELAPSEDTIME omitted? */
       /* POOLPAGEDBYTES omitted? */

       /* POOLNONPAGEBYTES omitted? */
          INT_FIELD_STORE(handles, (int32)getLongValue(&processCtrCache[HANDLECOUNT_IDX], PerfCntrBlk));

          return;
	}
	PerfInst = NextInstance(PerfCntrBlk);
      }
    } else {
#ifdef FLG_DEBUG
      fprintf(stderr, "DEBUG: unexpected 0 instances!\n");
#endif
    }  
  }
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
JNIEXPORT void JNICALL 
    Java_com_gemstone_gemfire_internal_HostStatHelper_refreshProcess
    (JNIEnv *env, jclass unused, jint pid, jintArray intStorage, 
    jlongArray longStorage, jdoubleArray doubleStorage)
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
 * returns the exit status of HostStatsData so that the failure could be logged
 */
JNIEXPORT jint JNICALL Java_com_gemstone_gemfire_internal_HostStatHelper_init
(JNIEnv *env, jclass unused)
{
  int i;
  if (!PerfData) {
    PerfData = (PPERF_DATA_BLOCK)UtlMalloc(BufferSize, "HostStatHelper_init");
  }
  for (i=0; i < MAX_PROCESS_CTRS_COLLECTED; i++) {
    processCtrCache[i].CounterOffset = 0;
  }
  for (i=0; i < MAX_SYSTEM_CTRS_COLLECTED; i++) {
    systemCtrCache[i].CounterOffset = 0;
  }
  for (i=0; i < MAX_MEMORY_CTRS_COLLECTED; i++) {
    memoryCtrCache[i].CounterOffset = 0;
  }
  for (i=0; i < MAX_OBJECTS_CTRS_COLLECTED; i++) {
    objectsCtrCache[i].CounterOffset = 0;
  }
  lastFetchData.perfTimeMs = 0;
  currentFetchData.perfTimeMs = 0;
  return HostStatsFetchData();
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
  if (PerfData) {
    UtlFree(PerfData);
    PerfData = NULL;
  }
  RegCloseKey(HKEY_PERFORMANCE_DATA);
}

#endif
