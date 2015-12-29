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
#define HOST_C TRUE
#define GEMSTONE_DLL
/*========================================================================
 * Description: This file contains the implementation of the routines
 *              specified in host.hf on for Windows and SUN Unix system.
 *========================================================================
 */

/*******  GemStone Global constants and types  ******************************/
#include "flag.ht"
#define index work_around_gcc_issue
#include "jni.h"
#undef index
/*******  C Run-Time Library files ******************************************/

#include <time.h>

#if defined(FLG_UNIX)
#include <signal.h>
#include <sys/time.h>
#include <sys/timeb.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdlib.h>
#include <dlfcn.h>

#if defined(FLG_UNIX_POLL)   
#include <poll.h>  
#endif

#elif defined (FLG_MSWIN32)
#include <sys/stat.h>
#else
+++ unknown platform
#endif

#if defined(FLG_OSX_UNIX)
#include <sys/sysctl.h>
#endif
#include "global.ht"
#include "host.ht"
#include "host.hf"
#include "utl.hf"

/* no more includes. KEEP THIS LINE */

/* ************* External declaration of variables **************************/

#ifdef __cplusplus
extern "C" {
#endif //__cplusplus

/*======================================================================== */
BoolType HostProcessExists(int32 pid)
{
#ifdef FLG_UNIX
  if (kill((pid_t)pid, 0) == -1) {
    if (errno == EPERM) {
      /* Process exists; it probably belongs to another user. Fix for bug 27698. */
      return TRUE;
    } else {
      return FALSE;
    }
  } else {
      return TRUE;
  }
#else
  HANDLE h = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, (DWORD)pid);
  if (h == NULL) {
    return FALSE;
  } else {
    CloseHandle(h); 
    return TRUE;
  } 
#endif
}


/*======================================================================== */
BoolType HostDelayCoreFileExists(void)
{
  char path[PATH_MAX + 1];
  char* dllNamePtr;
  
  path[0] = '\0';
  DllMainGetPath(path, PATH_MAX);
#ifdef FLG_MSWIN32
  dllNamePtr = strstr(path, "gemfire.dll");
  if (dllNamePtr == NULL) {
    dllNamePtr = strstr(path, "gemfire_g.dll");
  }
#else
  dllNamePtr = strstr(path, "libgemfire");
#endif
  if (dllNamePtr == NULL) {
    fprintf(stderr, "delaycoredump.txt not found (library name not found)");
    return FALSE;
  }
  /* substitute delaycoredump file name for library name */
  strcpy(dllNamePtr, "delaycoredump.txt");
  if (HostFileExist(path)) 
    return TRUE;
  else
    return FALSE;
}

/*======================================================================== */
static int32 doWaitForDebugger = 0x01010101 ;

static void waitForDebuggerImpl(BoolType fatalErr) 
{
  int32 threadId = 0;
  int   procId;
  if (! doWaitForDebugger)
    return;

  threadId = HostGetCurrentThreadId() ;
  procId = HOST_GETPID();
  fflush(stdout);
#if defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
  { pid_t parentPid = getppid(); // helpful for finding main thread pid for attach
    fprintf(stderr, "HostWaitForDebugger, processId %d, parentPid %d , thread 0x%x (%d)\n",
          procId, parentPid, threadId, threadId );
  }
#else
  fprintf(stderr, "HostWaitForDebugger, processId %d , thread 0x%x (%d)\n",
          procId, threadId, threadId );
#endif

  if (! fatalErr || HostDelayCoreFileExists()) {
    fprintf(stderr, 
       "waiting for debugger to attach, processId %d , thread 0x%x\n",
        procId, threadId );
    fflush(stderr);

    {
      BoolType done = FALSE;
      while (!done) {
        HostSleep(2);
      }
    }  
  } else {
    fprintf(stderr, "lib/delaycoredump.txt not found, continuing\n");
    fflush(stderr);
  }
}


/*=========================================================================
 * HostWaitForDebugger will cause the current process to sleep
 * until a user attaches to it with a debugger.  It will wait forever,
 * and is not affected by the delaycoredump.txt file .  It does
 * not produce a pstack.log .
 *=========================================================================
 */
EXTERN_GS_DEC(void) 
HostWaitForDebugger(void)
{
  waitForDebuggerImpl(FALSE);
}

static BoolType noHalt = FALSE;
static int32 coreDumpEnabled = 0x01010101 ; /* zero with debugger to disable coredump*/

/* =========================================================================
 * Name -  HostCallDebugger
 *
 * Purpose -  To be called in a fatal error situation.
 *   On Solaris, forks a process to run pstack against this
 *   process.  
 *   Then if lib/delaycoredump.txt is present , waits forever
 *   for a debugger to attach, otherwise does a coredump.
 *========================================================================
 */
GFCEXTERN(int) HostCallDebugger(void) {
  return HostCallDebugger_(FALSE);
}

EXTERN_GS_DEC(int) HostCallDebugger_(BoolType fromSignalHandler)
{
  time_t seconds;  /* current time, to aid in diagnosing crashes*/
  unsigned short milliseconds;

  /* flush output so it can be seen when we reach debugger */
  fflush(stderr);
  fflush(stdout);

  if (noHalt)      /* hook to ease debugging */
    return 0;

  /* record the current time on the stack */
  HostFtime(&seconds, &milliseconds);

  /* now call the debugger or dump core */
  if (coreDumpEnabled) {
  
    waitForDebuggerImpl(TRUE);

    abort();   /* skip HostCoredump complexity and just dump to "core" */
    }

  return 0;  /* a place to jump to from a breakpoint to avoid coredump */
}

EXTERN_GS_DEC(int) HostCallDebuggerMsg(const char* msg)
{ 
  fflush(stdout);
  fflush(stderr); 
  HostPrintStderr_("severe",  "HostCallDebugger:", msg);
  return HostCallDebugger_(FALSE);
}  

/*=========================================================================
 *
 * NAME
 *   HostGetEnv
 *   
 * PURPOSE
 *   To return environmental information for this program
 *   
 * ARGUMENTS
 *
 *  INPUT
 *   inStr -- null-terminated string containing environment name
 *   outSize -- size of buffer, in characters it can hold.  Result is
 *     null-terminated (not included in outSize)
 *
 *  OUTPUT
 *   Function Result -- TRUE on success
 *   outStr -- address of result string, NULL if no translation
 *===========================================================================
 */
GFCEXTERN(BoolType) HostGetEnv(const char inStr[], char outStr[], ArraySizeType outSize)
{
#if defined(FLG_MSWIN32)
  DWORD result = GetEnvironmentVariable(inStr, outStr, outSize);
  if (0 == result || result >= outSize) {
    return FALSE;
  } else {
    return TRUE;
  }
#else
  char * result;
  int32 resultSize;
  
  result = (char *)getenv(inStr);
  if (result == NULL) {
    return FALSE;
  }
  resultSize = strlen(result);
  if ((ArraySizeType)resultSize > outSize || resultSize == 0) {
    return FALSE;
  }
  strcpy(outStr, result);
  return TRUE;
#endif
}
/*=========================================================================
 *
 * NAME
 *   HostGetCpuCount
 *   
 * PURPOSE
 *   To return the number of cpus in the host machine
 *   
 *===========================================================================
 */
GFCEXTERN(int32) HostGetCpuCount(void)
{
#ifdef FLG_MSWIN32
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return (jint)si.dwNumberOfProcessors;
#elif defined(FLG_OSX_UNIX)
  int count ;
  size_t size=sizeof(count) ;
  if (sysctlbyname("hw.ncpu",&count,&size,NULL,0)) return 1;
  return count;
#else
    return (jint)sysconf(_SC_NPROCESSORS_ONLN);
#endif
}

/*=========================================================================
 * Name - HostMilliSleep
 *   Sleep for the specified number of milliseconds 
 *=========================================================================
 */
EXTERN_GS_DEF(void) HostMilliSleep(uint32 milliseconds)
{
#if defined(FLG_MSWIN32)
    Sleep(milliseconds);
#else
  /* use select or poll to sleep. Does not interfere with setitimer */
  int result;
  uint32 seconds = 0;

  if (milliseconds >= MILLI_IN_SEC) {
    seconds = milliseconds/MILLI_IN_SEC;
    milliseconds %= MILLI_IN_SEC;
    HostSleep(seconds);
  }

  if (milliseconds == 0) {
    return;
  }

  /* do not disable interrupts with HostDisableInterrupts */

#ifdef FLG_UNIX_POLL
  result = poll(NULL, 0, (int)milliseconds);
#else
  {
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = milliseconds*MICRO_IN_MILLI;
  result = select(0, NULL,NULL,NULL, &tv);
  }
#endif
  /* do not reenable interrupts*/

  /* ignore result from poll or select; if the call was interrupted we don't care*/
#endif
}

/*=========================================================================
 * Name - HostNanoSleep
 *   On Solaris uses the rt lib nanosleep, 
 *   on other platforms it uses millisleep
 *=========================================================================
 */
#ifdef FLG_UNIX
static void doYield(void)
{
#if defined(FLG_SOLARIS_UNIX) 
  yield(); /*thr_yield(); doesn't seem to make any difference which one of these is called */
#elif defined(FLG_LINUX_UNIX)
  pthread_yield();
#elif defined(FLG_OSX_UNIX)
  pthread_yield_np();
#else
+++ port error
#endif
}
#endif

GFCEXTERN(void) HostYield(void)
{
#if defined(FLG_SOLARIS_UNIX) || defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
  doYield();
#elif defined(FLG_MSWIN32)
  /*
   * Windows offers two different ways to give up the CPU
   * for a small amount of time:
   *   1. SwitchToThread() is the most efficient. It will allow
   *      some other thread to run on the current thread's cpu.
   *      The current thread is kept ready on the current cpu.
   *   2. Sleep(0) gives up the current thread's remaining time slice.
   *      However it could cause the current thread to migrate to
   *      another cpu so it can have more overhead than SwitchToThread().
   *      Also it will not yield to lower priority threads.
   */
  SwitchToThread();
#else
  +++ port error
#endif
}

/*
 * Beware. This function sleep at least ~7ms on Solaris and Linux.
 *   On Windows it ignores the nanoseconds arg and just does a HostMilliSleep.
 */
GFCEXTERN(void) HostNanoSleep(uint32 nanoseconds)
{
#if defined(FLG_SOLARIS_UNIX) || defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
  int result;
  struct timespec rqtp;
  rqtp.tv_sec = 0;  
  rqtp.tv_nsec = nanoseconds;
  doYield();
  result = nanosleep(&rqtp, NULL);
#elif defined(FLG_MSWIN32)
  /* Windows does not support sub millisecond sleeps.
   */
  HostMilliSleep(1);
#else
  +++ port error
  HostMilliSleep(1); /* use milli sleep on other platforms */
#endif
}

/*=========================================================================
 * Name - HostSleep
 *   Sleep for the specified number of seconds 
 *=========================================================================
 */
EXTERN_GS_DEC(void) HostSleep(uint32 seconds)
{
#if defined(FLG_MSWIN32)
    Sleep(seconds*1000);
#else
  /* use select or poll to sleep. Does not interfere with setitimer */
  time_t timetowake;
  BoolType done = FALSE;
#ifdef FLG_UNIX_POLL
  uint32 millisecs;
#else
  struct timeval tv;
#endif
  if (seconds == 0)
    return;
  timetowake = time(NULL)+seconds;
#ifdef FLG_UNIX_POLL
  millisecs = seconds * MILLI_IN_SEC;
#else
  tv.tv_sec = seconds;
  tv.tv_usec = 0;
#endif
  while (!done)
    {
#ifdef FLG_UNIX_POLL
    int32 result = poll(NULL, 0, (int)millisecs);
#else    
    int32 result = select(0, NULL,NULL,NULL, &tv);
#endif
    if (result == -1) {
      if (HOST_IS_EINTR(errno)) {
	time_t now = time(NULL);
	if ((uint32)now >= (uint32)timetowake)
	  done = TRUE;
	else {
#ifdef FLG_UNIX_POLL
	  millisecs = ((uint32)timetowake - (uint32)now) * MILLI_IN_SEC;
#else
	  tv.tv_sec = (uint32)timetowake - (uint32)now;
	  tv.tv_usec = 0;
#endif
	  }
	}
      else
	{
	done = TRUE;
#ifdef FLG_DEBUG
	fprintf(stderr, "HostSleep: unexpected error %d!\n", errno);
/* 	HostCallDebugger(); */
#endif
	}
      }
    else
      {
      done = TRUE;
#ifdef FLG_DEBUG
      if (result != 0){
	fprintf(stderr, "HostSleep: unexpected result %d!\n", result);
/* 	HostCallDebugger(); */
	}
#endif
      }
    }
#endif
}

/* wait for debugger code moved */


/*=========================================================================
 * HostFtime - cover for ftime, for systems that don't have it 
 *=========================================================================
 */
EXTERN_GS_DEC(void)
HostFtime(time_t *sec, unsigned short *millitm)
{
#if defined(FLG_MSWIN32)

#if defined(FLG_MSC32) || defined(FLG_MSC)
  struct _timeb junk;
  _ftime(&junk);
  *sec = junk.time;
  *millitm = junk.millitm;
#else 
  *sec = time(NULL);
  *millitm = 0;
#endif /* FLG_MSC32 || FLG_MSC */

#else
  struct timeb junk;
  *sec = 0;
  *millitm = 0;
 
  if (ftime(&junk) == -1)
    return;
  *sec = junk.time;
  *millitm = junk.millitm;
#endif
}

/*
  FUNCTION: HostGetErrorText

  PURPOSE: copies error message text to string

  PARAMETERS:
    errcode - the error code to translate
    buf - destination buffer
    bufSize - size of buffer

  RETURN VALUE:
    destination buffer
  */
EXTERN_GS_DEC(char*) HostGetErrorText(int32 errcode, char buf[], int32 bufSize) {
#if defined(FLG_MSWIN32)
    DWORD dwRet;
    LPTSTR lpszTemp = NULL;

    dwRet = FormatMessage( FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |FORMAT_MESSAGE_ARGUMENT_ARRAY,
                           NULL,
                           errcode,
                           LANG_NEUTRAL,
                           (LPTSTR)&lpszTemp,
                           0,
                           NULL );

    /* supplied buffer is not long enough */
    if ( !dwRet || ( bufSize < (int)dwRet-2 ) ) {
      sprintf(buf, "errorCode=%d", errcode);
    } else {
      /*remove cr and newline character */
      lpszTemp[lstrlen(lpszTemp)-2] = TEXT('\0');
      strcpy(buf, lpszTemp);
    }

    if ( lpszTemp ) {
      LocalFree((HLOCAL) lpszTemp );
    }

    return buf;

#else /* unix */
  snprintf(buf, bufSize, "%s (errno=%d)", strerror(errcode), errno);
  return buf;
#endif
}

GFCEXTERN(int) HostGetCurrentThreadId(void) {
#if defined(FLG_MSWIN32)
  return GetCurrentThreadId();
#elif defined(FLG_SOLARIS_UNIX)
  return thr_self();
#elif defined(FLG_LINUX_UNIX)
  return pthread_self();
#elif defined(FLG_OSX_UNIX)
  return (int)pthread_self();
#else
+++ port error
#endif
}

GFCEXTERN(int) HostCreateThread(THREAD_FUNC threadFunction, 
                                THREAD_ARG arg, THREAD_ID *thrId) 
{
  int32 err = 0;
#ifdef _WIN32
  CreateThread(NULL, 0L, threadFunction, arg, 0, thrId);
#else
  err = pthread_create(thrId, NULL, threadFunction, arg);
#endif
  return err;
}

GFCEXTERN(void) HostJoinThread(THREAD_ID thrId)  
{  
#ifdef _WIN32  
  /* hack to get a pointer to the OpenThread function   
   * this might be cleaned up when we move past Microsoft Visual C++ 6.0  
   */  
  
typedef HANDLE (*OPEN_THR_FUNC)(DWORD dwDesiredAccess, 
                                BOOL bInheritHandle,DWORD dwThreadId);
static OPEN_THR_FUNC OpenThr = NULL;
  
  if (OpenThr == NULL){  // get a handle to the dll module 
    HINSTANCE hinstLib = LoadLibrary(TEXT("OpenThread"));
    // if the handle is valid try to get the function address
    if (hinstLib != NULL) {
      OpenThr = (OPEN_THR_FUNC) GetProcAddress(hinstLib, TEXT("OpenThread"));
      FreeLibrary(hinstLib);
    }
  }
  if (OpenThr != NULL) {
    WaitForSingleObject(OpenThr(SYNCHRONIZE, FALSE, thrId), INFINITE);
  } else {
    HostCallDebuggerMsg("OpenThread not found");
  }     
#else
  pthread_join(thrId, NULL);
#endif
} 


#ifdef FLG_UNIX
/* nothing needed */
#else
#define NANOS_PER_SEC 1000000000
#define NANOS_PER_MS 1000000
#define MS_PER_SEC 1000

/* nt stuff */
typedef int64 getTicksFuncType(void);
static int64 getTicksInit(void); /* forward ref */
static getTicksFuncType* getTicksPtr = getTicksInit;

static double ticksPerNano;

static int64 highResGetTicks(void) {
    LARGE_INTEGER l;
    QueryPerformanceCounter(&l);
    return (int64)((double)l.QuadPart / ticksPerNano);
}

static int64 lowResGetTicks(void) {
    return (int64)GetTickCount() * NANOS_PER_MS;
}

static int64 getTicksInit(void) {
    LARGE_INTEGER l;
    if (QueryPerformanceCounter(&l)) {
	int64 ticksPerSecond;
	QueryPerformanceFrequency(&l);
	ticksPerSecond = l.QuadPart;
	ticksPerNano = (double)ticksPerSecond / (double)NANOS_PER_SEC;
#if 0
        fprintf(stderr, "DEBUG: ticksPerSecond=%I64d ticksPerNano=%e\n",
                ticksPerSecond, ticksPerNano);
#endif
	getTicksPtr = highResGetTicks;
    } else {
	getTicksPtr = lowResGetTicks;
    }
    return (*getTicksPtr)();
}
#endif

EXTERN_GS_DEC(int64) HostCurrentTimeNanos(void) {
#if defined(FLG_SOLARIS_UNIX)
    return (int64)gethrtime();

#elif defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
   struct timeval tv;
   struct timezone tz;
   int64 temp, result;
   int status = gettimeofday(&tv, &tz);
   UTL_GUARANTEE(status == 0);
   result = tv.tv_sec ;
   result *= 1000000000 ; /* convert secs to nanosecs */

   temp = tv.tv_usec;
   temp *= 1000 ;        /* convert microsecs to nanosecs */
   result += temp;

   return result; 
#elif defined(FLG_MSWIN32)
    return (*getTicksPtr)();
#else
+++ port error
#endif
}

/*======================================================================== */
#if !defined(FLG_UNIX)
static int32  _has_offset = 0;
static int64  _time__offset     = 0;

int64 convert_time(FILETIME *xt) {
    int64 result = (uint64)((uint32)xt->dwHighDateTime);
    result= result << 32;
    result += (uint64)((uint32)xt->dwLowDateTime);
    return result;
}

static int64 time_offset(void) {
  SYSTEMTIME origin;
  FILETIME xt;

  if (_has_offset) return _time__offset;

  origin.wYear          = 1970;
  origin.wMonth         = 1;
  origin.wDayOfWeek     = 0; // ignored
  origin.wDay           = 1;
  origin.wHour          = 0;
  origin.wMinute        = 0;
  origin.wSecond        = 0;
  origin.wMilliseconds  = 0;
  if (!SystemTimeToFileTime(&origin, &xt)) {
    UTL_ASSERT(0);
  } else {
    _time__offset = convert_time(&xt);
    _has_offset = 1;
  }
  return _time__offset;
}

int64 adjust_time(FILETIME *wt) {
  int64 result = convert_time(wt);
  return (result - time_offset() ) / 10000;
}
#endif /* not Unix */


GFCEXTERN(int64) HostCurrentTimeMs(void) {
  /* returns a value compatible with java System.currentTimeMillis()  */
#ifdef FLG_UNIX
#if defined(FLG_SOLARIS_UNIX)
  struct timeval t;
  static const char* aNull = 0;
  int status = gettimeofday( &t, &aNull);
  UTL_GUARANTEE(status == 0);
#elif defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
  struct timeval t;
  struct timezone tz;
  int status = gettimeofday(&t, &tz);
  UTL_GUARANTEE(status == 0);
#endif
  return (int64)t.tv_sec * 1000  +  (int64)t.tv_usec / 1000;
#else
/* Win32*/
  FILETIME wt;
  GetSystemTimeAsFileTime(&wt);
  return adjust_time(&wt);
#endif
}

/*======================================================================== */
EXTERN_GS_DEC(BoolType) HostFileExist(const char *path)
{
#if defined(FLG_UNIX)
  for (;;) {
#ifdef HAVE_STRUCT_STAT64
    struct stat64 buf;
    if (stat64(path, &buf) == 0)
#else
    struct stat buf;
    if (stat(path, &buf) == 0)
#endif
      return TRUE;
    if (errno == EINTR)
      continue;
    return FALSE;
  }
#elif defined(FLG_MSWIN32)
  if (-1 == GetFileAttributes(path)) {
    return FALSE;
  } else {
    return TRUE;
  }
#else
+++ port error
#endif
}

/*======================================================================== */
EXTERN_GS_DEC(int) HostVmPageSize(void)
{
#if defined(FLG_UNIX)
  return sysconf(_SC_PAGESIZE);
#else
  SYSTEM_INFO si;
  GetSystemInfo(&si);
  return si.dwPageSize;
#endif
}

EXTERN_GS_DEC(void) HostProtectMem(char *addr, int size) 
{
#if defined(FLG_UNIX)
  int status = mprotect(addr, size, PROT_NONE);
  if (status != 0) {
    int errNum = errno;
    UTL_ASSERT(status == 0);
    errNum = errNum; // lint
  }
#else
  DWORD old_status;
  int status = VirtualProtect(addr, size, PAGE_READWRITE | PAGE_GUARD, &old_status);
  UTL_ASSERT(status == 1);
#endif
}

EXTERN_GS_DEC(void) HostUnProtectMem(char *addr, int size) 
{
#if defined(FLG_UNIX)
  int status = mprotect(addr, size, PROT_READ|PROT_WRITE|PROT_EXEC);
  if (status != 0) {
    int errNum = errno;
    UTL_ASSERT(status == 0);
    errNum = errNum; // lint
  }
#else
  DWORD old_status;
  int status = VirtualProtect(addr, size, PAGE_READWRITE, &old_status);
  UTL_ASSERT(status == 0);
#endif
}

/* HostThrKill is not implemented on Windows */
#ifdef FLG_UNIX
EXTERN_GS_DEC(int) HostThrKill(int threadId, int sigNum)
{
#if defined(FLG_SOLARIS_UNIX)
  return thr_kill(threadId, sigNum);
#elif defined(FLG_LINUX_UNIX)
  return pthread_kill(threadId, sigNum); 
#elif defined(FLG_OSX_UNIX)
  return pthread_kill((pthread_t)threadId, sigNum); 
#else
  +++ port error
#endif
}
#endif

EXTERN_GS_DEC(void) HostPrintStderr(const char* level, const char* msg1)
{
  HostPrintLogMsg(stderr, level, msg1, NULL); 
}

EXTERN_GS_DEC(void) HostPrintStderr_(const char* level, const char* msg1, const char* msg2)
{
  HostPrintLogMsg(stderr, level, msg1, msg2); 
}

static BoolType timeZoneSet = FALSE;

#if defined(FLG_MSWIN32)
  static TIME_ZONE_INFORMATION tzInfo;
  static char theTzName[32];  /* Windows doesn't have a contant for length */
#endif

static void mywctob(const wchar_t *srcPtr, char *destPtr, size_t length) {
  size_t i;
  for (i=0; i<= length; i++) {
    *destPtr++ = (char)*srcPtr++;
  }
}

GFCEXTERN(void) HostGetTimeStr(char *bufPtr)
{
  int year, month, day, hour, min, sec, milliSecs;
  const char* zoneName;
#if defined(FLG_UNIX)
  struct tm ltime;
  struct timeval clockTime;
  gettimeofday(&clockTime, NULL);
  if (! timeZoneSet) {
    localtime(&clockTime.tv_sec); // to set tzname only, ignore thread-unsafe function result
    timeZoneSet = TRUE;
  }
  localtime_r(&clockTime.tv_sec, &ltime); // thread safe 
  year = 1900 + ltime.tm_year;
  month =  1 + ltime.tm_mon;
  day =  ltime.tm_mday;
  hour = ltime.tm_hour;
  min =  ltime.tm_min;
  sec =  ltime.tm_sec;
  milliSecs = clockTime.tv_usec / 1000;
  zoneName = ltime.tm_isdst ? tzname[1] : tzname[0] ;
#else
  /* Windows */
  SYSTEMTIME ltime;
  if (! timeZoneSet) {
    DWORD tzId = GetTimeZoneInformation(&tzInfo);
    if (tzId == TIME_ZONE_ID_UNKNOWN) {
      theTzName[0] = 0; /* null string */
    } else if (tzId == TIME_ZONE_ID_STANDARD) {
      size_t tzNameLength = wcslen(tzInfo.StandardName);
      mywctob(tzInfo.StandardName, theTzName, tzNameLength);
    } else { /* must be Daylight time */
      size_t tzNameLength = wcslen(tzInfo.DaylightName);
      mywctob(tzInfo.DaylightName, theTzName, tzNameLength);
    }
    timeZoneSet = TRUE;
  }
  GetLocalTime(&ltime);
  year = ltime.wYear;
  month =  ltime.wMonth;
  day =  ltime.wDay;
  hour = ltime.wHour;
  min =  ltime.wMinute;
  sec =  ltime.wSecond;
  milliSecs = ltime.wMilliseconds;
  zoneName = theTzName;
#endif 
  sprintf(bufPtr, "%04d/%02d/%02d %02d:%02d:%02d.%03d %s ",
        year, month, day, hour, min, sec, milliSecs/*the %03d*/, 
        (zoneName == NULL ? "ZZZ" : zoneName));
}

EXTERN_GS_DEC(void) HostPrintLogMsg(FILE *file, const char* level, const char* msg1, const char* msg2)
{
#if defined(FLG_UNIX)
  const char dllName[] = "libgemfire.so";
#else
  const char dllName[] = "gemfire.dll";
#endif
  char timeBuffer[128];
  HostGetTimeStr(timeBuffer);
  fprintf(file, "[%s %s %s nid=0x%x] %s %s\n", level, timeBuffer,
	dllName, HostGetCurrentThreadId(), msg1, msg2 == NULL ? " " : msg2 );
  fflush(file);  
}

#if defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
static int linuxPid = 0;
#endif

#ifdef FLG_UNIX
EXTERN_GS_DEC(int) HostGetLinuxPid(void) {
#if defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
  if (linuxPid == 0) {
    char val[64];
    linuxPid = getpid();
    if (HostGetEnv("OSPROCESS_BGEXEC_PID", val, sizeof(val))) {
      if (strlen(val) != 0) {
        sscanf(val, "%d", &linuxPid);
        putenv("OSPROCESS_BGEXEC_PID=");
      }
    }
  }
  return linuxPid;
#elif defined(FLG_SOLARIS_UNIX)
  return 0;
#else 
+++ port error
#endif  /* FLG_LINUX_UNIX */
}
#endif  /* FLG_UNIX */

#ifdef FLG_UNIX
static BoolType call_sigaction(/*JNIEnv *env,*/ int sig, const char *sigName,
                    struct sigaction *newAct, struct sigaction *oldAct)
{
  int status = sigaction(sig, newAct, oldAct); 
  if (status) {
    /* no longer have SmUtlThrowInternal
    int saveErrno = errno;
    char msg[200];
    sprintf(msg, " sigaction(%s,,) failed, errno %d ", sigName, saveErrno);
    SmUtlThrowInternal(env, GF_EUNDEF, msg);
    */
    return FALSE;
  }
  return TRUE;
}

static int32 quitInstalled = 0;
static struct sigaction savedSigQuitHandler;

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */
/*
 * A SIGQUIT handler that prints a timestamp before passing the signal
 * on to the VM.
 *
 * @since 4.0
 */
static void gemfireSigQuitHandler(int sigNum, siginfo_t* info, void* ucArg)
{
  ucontext_t* uc = (ucontext_t*)ucArg;

  UTL_ASSERT(sigNum == SIGQUIT);
  
  if (sigNum == SIGQUIT) {
    struct sigaction *savedHandler = &savedSigQuitHandler ;
    UTL_ASSERT(savedHandler->sa_sigaction != gemfireSigQuitHandler);
    BoolType isSiginfo = (savedHandler->sa_flags & SA_SIGINFO) != 0 ;
    if ((! isSiginfo &&
	 savedHandler->sa_handler != SIG_DFL &&
         savedHandler->sa_handler != SIG_IGN) ||
        (savedHandler->sa_sigaction != NULL &&
         savedHandler->sa_sigaction != gemfireSigQuitHandler)) {
      if ((savedHandler->sa_flags & SA_NODEFER) == 0) {
	// automaticlly block the signal
	sigaddset(&(savedHandler->sa_mask), sigNum);
      }
      if ((savedHandler->sa_flags & SA_RESETHAND) != 0 ) {
	savedHandler->sa_handler = SIG_DFL;
      }

      {
        char msgBuf[300];
        snprintf(msgBuf, sizeof(msgBuf), "SIGQUIT received, dumping threads");
        HostPrintStderr("severe", msgBuf);
      }

      sigset_t oldMask;  // honor the signal mask
#if defined(FLG_SOLARIS_UNIX)
      thr_sigsetmask(SIG_SETMASK, &(savedHandler->sa_mask), &oldMask);
#else
      pthread_sigmask(SIG_SETMASK, &(savedHandler->sa_mask), &oldMask); // linux
#endif
      if ((savedHandler->sa_flags & SA_SIGINFO) != 0) {
	(*(savedHandler->sa_sigaction))(sigNum, info, uc);
      } else {
	UTL_ASSERT(0); // usually calling JVM handler, which always uses sigInfo 
	(*(savedHandler->sa_handler))(sigNum);
      }
#if defined(FLG_SOLARIS_UNIX)
      thr_sigsetmask(SIG_SETMASK, &oldMask, NULL);  // restore mask
#else
      pthread_sigmask(SIG_SETMASK, &oldMask, NULL); // restore mask // linux
#endif
      return;  /* after chaining to the saved handler */
    }

  } else {
    char msgBuf[300];
    snprintf(msgBuf, sizeof(msgBuf), "gemfireSigQuitHandler: unexpected signal number %d received \n", sigNum);
    HostPrintStderr("severe", msgBuf);
  }
}
#ifdef __cplusplus
}
#endif //__cplusplus

/*
 * Registers a signal handler for SIGQUIT that prints a time stamp
 * before passing the signal onto the VM.
 *
 * @since 4.0
 */
EXTERN_GS_DEC(BoolType) HostInstallSigQuitHandler(/*JNIEnv *env*/void)
{
  if (quitInstalled == 0) {
    quitInstalled += 1;
    struct sigaction newHandler;

// /*    UTL_ASSERT(vmVendor == com_gemstone_gemfire_internal_SmHelper_VM_VENDOR_SUN ||
//                vmVendor == com_gemstone_gemfire_internal_SmHelper_VM_VENDOR_IBM);
// */

//     sig_vmVendor = vmVendor;

    sigfillset(&newHandler.sa_mask);
    // SA_SIGINFO means use sa_sigaction function and not sa_handler function
    newHandler.sa_handler = SIG_DFL;
    newHandler.sa_sigaction = gemfireSigQuitHandler;  
    newHandler.sa_flags = SA_SIGINFO | SA_RESTART ;

//     initLibJvmBoundaries(env);
    struct sigaction oldHandler;
    // examine the JVM handler to determine if we need SA_ONSTACK
    if (! call_sigaction(/*env,*/ SIGQUIT, "SIGQUIT", NULL, &oldHandler)) {
      return FALSE;
    }
    BoolType haveOldHandler = FALSE;
    if (oldHandler.sa_flags & SA_SIGINFO) {

      haveOldHandler = oldHandler.sa_sigaction != NULL;
#ifdef FLG_DEBUG
      // printf("old QUIT handler has SA_SIGINFO, sa_sigaction = %p, haveOldHandler %d\n", 
      // oldHandler.sa_sigaction, haveOldHandler);
#endif
    } else {
      haveOldHandler = oldHandler.sa_handler != SIG_DFL &&
                       oldHandler.sa_handler != SIG_IGN ;
#ifdef FLG_DEBUG
      // printf("old QUIT not SA_SIGINFO, old.sa_handler = %p, haveOldHandler %d\n", 
      //   oldHandler.sa_handler, haveOldHandler);
#endif
    }
    if (haveOldHandler) {
      if (oldHandler.sa_flags & SA_ONSTACK) {
        newHandler.sa_flags |= SA_ONSTACK;
      } else {
// #if defined(FLG_LINUX_UNIX)
//         if (vmVendor == com_gemstone_gemfire_internal_SmHelper_VM_VENDOR_SUN) {
//           UTL_ASSERT(0); // Sun Lunix JVM, expect to always have SA_ONSTACK true
//         }
// #endif
      }
    }
    // now install the gemfire handler 
    if (! call_sigaction(/*env,*/ SIGQUIT, "SIGQUIT", &newHandler, &savedSigQuitHandler)) {
      return FALSE;
    }
  }

  return TRUE;
}
#endif

#ifdef __cplusplus
}
#endif  /* __cplusplus */
