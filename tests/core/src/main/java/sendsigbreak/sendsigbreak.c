/**
 * Copyright (C) 2003 Louis Thomas.
 * License: http://www.latenighthacking.com/projects/lnhfslicense.html
 *
 * v1.0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * of this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

#include "flag.ht"
#include "global.ht"

#include "host.hf"

#ifdef FLG_MSWIN32

#include <windows.h>
#include <stdio.h>
#include <aclapi.h>

//####################################################################

typedef unsigned int RETVAL;

#define STRINGIFY(A) #A

#define EXIT_OK 0

#define _TeardownLastError(rv, errorsource) \
    { \
        RETVAL rv2__=GetLastError(); \
        printf(errorsource " failed with 0x%08X.\n", rv2__); \
        if (EXIT_OK==rv) { \
            rv=rv2__; \
        } \
    }

#define _TeardownIfError(rv, rv2, errorsource) \
    if (EXIT_OK!=rv2) { \
        printf(errorsource " failed with 0x%08X.\n", rv2); \
        if (EXIT_OK==rv) { \
            rv=rv2; \
        } \
    }

#define _JumpLastError(rv, label, errorsource) \
    rv=GetLastError(); \
    printf(errorsource " failed with 0x%08X.\n", rv); \
    goto label;

#define _JumpLastErrorStr(rv, label, errorsource, str) \
    rv=GetLastError(); \
    printf( errorsource "(%s) failed with 0x%08X.\n", str, rv); \
    goto label;

#define _JumpIfError(rv, label, errorsource) \
    if (EXIT_OK!=rv) {\
        printf( errorsource " failed with 0x%08X.\n", rv); \
        goto label; \
    }

#define _JumpIfErrorStr(rv, label, errorsource, str) \
    if (EXIT_OK!=rv) {\
        printf( errorsource "(%s) failed with 0x%08X.\n", str, rv); \
        goto label; \
    }

#define _JumpError(rv, label, errorsource) \
    printf( errorsource " failed with 0x%08X.\n", rv); \
    goto label;

#define _JumpErrorStr(rv, label, errorsource, str) \
    printf( errorsource "(%s) failed with 0x%08X.\n", str, rv); \
    goto label;

#define _JumpIfOutOfMemory(rv, label, pointer) \
    if (NULL==(pointer)) { \
        rv=ERROR_NOT_ENOUGH_MEMORY; \
        printf("Out of memory ('" #pointer "').\n"); \
        goto label; \
    }

#define _Verify(expression, rv, label) \
    if (!(expression)) { \
        printf("Verify failed: '%s' is false.\n", #expression); \
        rv=E_UNEXPECTED; \
        goto label; \
    }


//####################################################################

//--------------------------------------------------------------------
void PrintError(DWORD dwError) {
	char * szErrorMessage=NULL;
	DWORD dwResult=FormatMessage(
		FORMAT_MESSAGE_ALLOCATE_BUFFER|FORMAT_MESSAGE_FROM_SYSTEM, 
		NULL/*ignored*/, dwError, 0/*language*/, (char *)&szErrorMessage, 0/*min-size*/, NULL/*valist*/);
	if (0==dwResult) {
		printf("(FormatMessage failed)");
	} else {
		printf("%s", szErrorMessage);
	}
	if (NULL!=szErrorMessage) {
		LocalFree(szErrorMessage);
	}
}


//--------------------------------------------------------------------
RETVAL StartRemoteThread(HANDLE hRemoteProc, PVOID dwEntryPoint,
                         BoolType sendQuit){
    RETVAL rv;

    // must be cleaned up
    HANDLE hRemoteThread=NULL;

    // inject the thread
    hRemoteThread=CreateRemoteThread(hRemoteProc, NULL, 0, (LPTHREAD_START_ROUTINE)dwEntryPoint, (void *)(sendQuit ? CTRL_BREAK_EVENT : CTRL_C_EVENT), CREATE_SUSPENDED, NULL);
    if (NULL==hRemoteThread) {
        _JumpLastError(rv, error, "CreateRemoteThread");
    }

    // wake up the thread
    if (-1==ResumeThread(hRemoteThread)) {
        _JumpLastError(rv, error, "ResumeThread");
    }

    // wait for the thread to finish
    if (WAIT_OBJECT_0!=WaitForSingleObject(hRemoteThread, INFINITE)) {
        _JumpLastError(rv, error, "WaitForSingleObject");
    }

    // find out what happened
    if (!GetExitCodeThread(hRemoteThread, (DWORD *)&rv)) {
        _JumpLastError(rv, error, "GetExitCodeThread");
    }

    if (STATUS_CONTROL_C_EXIT==rv) {
        printf("Target process was killed.\n");
        rv=EXIT_OK;
    } else if (EXIT_OK!=rv) {
        printf("(remote function) failed with 0x%08X.\n", rv);
        //if (ERROR_INVALID_HANDLE==rv) {
        //    printf("Are you sure this is a console application?\n");
        //}
    }


error:
    if (NULL!=hRemoteThread) {
        if (!CloseHandle(hRemoteThread)) {
            _TeardownLastError(rv, "CloseHandle");
        }
    }

    return rv;
}

//--------------------------------------------------------------------
void PrintHelp(void) {
    printf(
        "SendSignal <pid>\n"
        "  <pid> - send ctrl-break to process <pid> (hex ok)\n"
        );
}

//--------------------------------------------------------------------
RETVAL SetPrivilege(HANDLE hToken, char * szPrivilege, boolean bEnablePrivilege) {
    RETVAL rv;

    TOKEN_PRIVILEGES tp;
    LUID luid;

    if (!LookupPrivilegeValue(NULL, szPrivilege, &luid)) {
        _JumpLastError(rv, error, "LookupPrivilegeValue");
    }

    tp.PrivilegeCount=1;
    tp.Privileges[0].Luid=luid;
    if (bEnablePrivilege) {
        tp.Privileges[0].Attributes=SE_PRIVILEGE_ENABLED;
    } else {
        tp.Privileges[0].Attributes=0;
    }

    AdjustTokenPrivileges(hToken, FALSE, &tp, sizeof(TOKEN_PRIVILEGES), NULL, NULL); // may return true though it failed
    rv=GetLastError();
    _JumpIfError(rv, error, "AdjustTokenPrivileges");

    rv=EXIT_OK;
error:
    return rv;
}

//--------------------------------------------------------------------
RETVAL AdvancedOpenProcess(DWORD dwPid, HANDLE * phRemoteProc) {
    RETVAL rv, rv2;

    #define NEEDEDACCESS    PROCESS_QUERY_INFORMATION|PROCESS_VM_WRITE|PROCESS_VM_READ|PROCESS_VM_OPERATION|PROCESS_CREATE_THREAD|PROCESS_TERMINATE

    // must be cleaned up
    HANDLE hThisProcToken=NULL;

    boolean bDebugPriv=FALSE;

    // initialize out params
    *phRemoteProc=NULL;

    // get a process handle with the needed access
    *phRemoteProc=OpenProcess(NEEDEDACCESS, FALSE, dwPid);
    if (NULL==*phRemoteProc) {
        rv=GetLastError();
        if (ERROR_ACCESS_DENIED!=rv) {
            _JumpError(rv, error, "OpenProcess");
        }
        printf("Access denied; retrying with increased privileges.\n");

        // give ourselves god-like access over process handles
        if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &hThisProcToken)) {
            _JumpLastError(rv, error, "OpenProcessToken");
        }

        rv=SetPrivilege(hThisProcToken, SE_DEBUG_NAME, TRUE);
        if (EXIT_OK==rv) {
            bDebugPriv=TRUE;
        }
        _JumpIfErrorStr(rv, error, "SetPrivilege", SE_DEBUG_NAME);

        // get a process handle with the needed access
        *phRemoteProc=OpenProcess(NEEDEDACCESS, FALSE, dwPid);
        if (NULL==*phRemoteProc) {
            _JumpLastError(rv, error, "OpenProcess");
        }
    }

    // success
    rv=EXIT_OK;

error:
    if (ERROR_ACCESS_DENIED==rv && FALSE==bDebugPriv) {
        printf("You need administrative access (debug privilege) to access this process.\n");
    }
    if (TRUE==bDebugPriv) {
        rv2=SetPrivilege(hThisProcToken, SE_DEBUG_NAME, FALSE);
        _TeardownIfError(rv, rv2, "SetPrivilege");
    }
    if (NULL!=hThisProcToken) {
        if (!CloseHandle(hThisProcToken)) {
            _TeardownLastError(rv, "CloseHandle");
        }
    }
    return rv;
}

static PVOID g_dwCtrlRoutineAddr=NULL;
static HANDLE g_hAddrFoundEvent=NULL;

//--------------------------------------------------------------------
BOOL WINAPI MyHandler(DWORD dwCtrlType) {
    // test
    //__asm { int 3 };
    if (CTRL_BREAK_EVENT != dwCtrlType) {
        return FALSE;
    }


//     printf("Received ctrl-break event %ld\n", dwCtrlType);
//     fflush(NULL);
    if (NULL==g_dwCtrlRoutineAddr) {

        // read the stack base address from the TEB
        PVOID pStackBase = NULL;
        #if (_MSC_VER < 1300)
          #define TEB_OFFSET 4
          __asm { mov eax, fs:[TEB_OFFSET] }
          __asm { mov pStackBase, eax }
        #else 
          PNT_TIB pTib = (PNT_TIB)(NtCurrentTeb());
          if ( pTib == NULL ) {
            printf("NtCurrentTeb() returned null near line %s %d.\n",
                    __FILE__, __LINE__);
          }
          pStackBase = pTib->StackBase;
        #endif
        // read the parameter off the stack
        #if (_M_AMD64)
          #define PARAM_0_OF_BASE_THEAD_START_OFFSET -28
        #elif (_M_IX86)
          #define PARAM_0_OF_BASE_THEAD_START_OFFSET -3
        #else
          ++Need to find the correct offset, see code below, IA64 is likly -28
	#endif
        #if 0
          FILE *fp = fopen( "myStack", "w");
          fprintf(fp , "Stack Address is 0x%16X\n", pStackBase);
          //Print the top 100  groups of 32bits
          /* The pattern you are looking for is:
          1 0x00000000
          2 0x00000001
          3 0x7C875280
          4 0x00000000
          On 32bit Windows the pattern is 
          0x00 followed by 0x01 followed by the desired address.
          On 64bit Windows look for the address 3 words before that pattern   
          This is because there is an extra function all in the 64bit 
          Windows Stack.
          */
          for ( int i = 1; i< 100; i++)
            fprintf(fp , "%d 0x%08X\n", i, (PVOID) *((DWORD*)pStackBase - i));
          fclose(fp); 
        #endif
        g_dwCtrlRoutineAddr=
          (PVOID) *((DWORD*)pStackBase + PARAM_0_OF_BASE_THEAD_START_OFFSET);

        // notify that we now have the address
        if (!SetEvent(g_hAddrFoundEvent)) {
            printf("SetEvent failed with 0x08X.\n", GetLastError());
        }
    }
    return TRUE;
}


//--------------------------------------------------------------------
RETVAL GetCtrlRoutineAddress(void) {
    RETVAL rv=EXIT_OK;

    // must be cleaned up
    g_hAddrFoundEvent=NULL;

    // create an event so we know when the async callback has completed
    g_hAddrFoundEvent=CreateEvent(NULL, TRUE, FALSE, NULL); // no security, manual reset, initially unsignaled, no name
    if (NULL==g_hAddrFoundEvent) {
        _JumpLastError(rv, error, "CreateEvent");
    }

    // request that we be called on system signals
	if (!SetConsoleCtrlHandler(MyHandler, TRUE)) {
		_JumpLastError(rv, error, "SetConsoleCtrlHandler");
	}

    // generate a signal
	if (!GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, 0)) {
		_JumpLastError(rv, error, "GenerateConsoleCtrlEvent");
	}

    // wait for our handler to be called
    {
        DWORD dwWaitResult=WaitForSingleObject(g_hAddrFoundEvent, INFINITE);
        if (WAIT_FAILED==dwWaitResult) {
            _JumpLastError(rv, error, "WaitForSingleObject");
        }
    }

    _Verify(NULL!=g_dwCtrlRoutineAddr, rv, error);

error:
    if (NULL!=g_hAddrFoundEvent) {
        if (!CloseHandle(g_hAddrFoundEvent)) {
            _TeardownLastError(rv, "CloseHandle");
        }
    }
    return rv;
}

//--------------------------------------------------------------------
int main(unsigned int argc, char ** argv) {
  char *pidString = NULL;
  BoolType argsOk = FALSE;
  BoolType sendQuit = TRUE;

  RETVAL rv;

  HANDLE hRemoteProc=NULL;
  HANDLE hRemoteProcToken=NULL;
  char * szPid=NULL;
  char * szEnd=NULL;
  DWORD dwPid=0;

  if (argc == 2) {
    pidString = argv[1];
    argsOk = TRUE; 
    sendQuit = TRUE;
  } else if (argc == 3) {
    pidString = argv[2];
    if (strcmp(argv[1], "TERM") == 0) {
      sendQuit = FALSE;
      argsOk = TRUE; 
    } else if (strcmp(argv[1], "QUIT") == 0) {
      sendQuit = TRUE;
      argsOk = TRUE; 
    }
  }
  
  if (! argsOk) {
    const char* exeName = "sendsigbreak";
    printf("usage:\n");
    printf("  %s <processId> \n", exeName); 
    printf("  %s QUIT <processId> \n", exeName); 
    printf("     where <processId> is the decimal process ID of a GemStone VM process.\n");
    printf("     That VM will be sent a simulated ctl-BREAK (equivalent to Unix  kill -QUIT)\n");
    printf("     causing that VM to print its Java stacks to stdout.\n");
    printf("  %s TERM <processId> \n", exeName);
    printf("     where <processId> is the decimal process ID of a GemStone VM process.\n");
    printf("     That VM will be sent a simulated ctl-C (equivalent to Unix  kill -TERM)\n");
    printf("     causing that VM to detach safely from the shared memory cache and shutdown\n");
    return 1;
  }

    // check for the special parameter
    szPid=pidString;
    dwPid=strtoul(szPid, &szEnd, 0);
    if (szEnd==szPid || 0==dwPid) {
        printf("\"%s\" is not a valid PID.\n", szPid);
        rv=ERROR_INVALID_PARAMETER;
        goto error;
    }


    //printf("Determining address of kernel32!CtrlRoutine...\n");
    rv=GetCtrlRoutineAddress();
    _JumpIfError(rv, error, "GetCtrlRoutineAddress");
    //printf("Address is 0x%16X.\n", g_dwCtrlRoutineAddr);

//     printf("Sending signal to process %d...\n", dwPid);
//     fflush(NULL);
    rv=AdvancedOpenProcess(dwPid, &hRemoteProc);
    _JumpIfErrorStr(rv, error, "AdvancedOpenProcess", argv[1]);

    if (sendQuit) {
      printf("Dumping stacks for process %d\n", dwPid);
      fflush(NULL);
      rv=StartRemoteThread(hRemoteProc, g_dwCtrlRoutineAddr, sendQuit);
      _JumpIfError(rv, error, "StartRemoteThread");

    } else {
      printf("Terminating process %d\n", dwPid);
      fflush(NULL);
      rv=TerminateProcess(hRemoteProc, (UINT) 3);
      _JumpIfError(!rv, error, "TerminateProcess");
    }

//done:
    rv=EXIT_OK;
error:
    if (NULL!=hRemoteProc && GetCurrentProcess()!=hRemoteProc) {
        if (!CloseHandle(hRemoteProc)) {
            _TeardownLastError(rv, "CloseHandle");
        }
    }
    if (EXIT_OK!=rv) {
        printf("0x%08X == ", rv);
        PrintError(rv);
    }
    return rv;
}

#else
int main(int argc, char *argv[])
{
  fprintf(stderr, "This program not for use on Unix\n");
  return 1;
}

#endif
