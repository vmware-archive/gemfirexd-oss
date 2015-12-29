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
#include "flag.ht"
#define index work_around_gcc_bug
#include "jni.h"
#undef index 

#include <stdio.h>              // 
#include <stdlib.h>
#include <string.h>
#include <windows.h>

#define index work_around_gcc_bug
#include "hydra_WindowsPlatform.h"
#undef index 

static void throwByName(JNIEnv *env, const char *name, const char *msg)
{
  jthrowable prev = (*env)->ExceptionOccurred(env);
  if (prev == NULL) {
    jclass cls = (*env)->FindClass(env, name);
    if (cls != NULL) {
      (*env)->ThrowNew(env, cls, msg);
      (*env)->DeleteLocalRef(env, cls);  /* clean up local ref*/
    } else {
      fprintf(stderr, "INTERNAL ERROR: unable to throw %s %s \n", name, msg);
    }
  } else {
    (*env)->DeleteLocalRef(env, prev);
  }
}

static void EnableDebugPriv()
{
    HANDLE hToken;
    LUID luid;
    TOKEN_PRIVILEGES tkp;

    OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken);

    LookupPrivilegeValue(NULL, SE_DEBUG_NAME, &luid);

    tkp.PrivilegeCount = 1;
    tkp.Privileges[0].Luid = luid;
    tkp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

    AdjustTokenPrivileges(hToken, FALSE, &tkp, sizeof(tkp), NULL, NULL);

    CloseHandle(hToken);
}

  JNIEXPORT jboolean JNICALL
Java_hydra_WindowsPlatform_nativeExists(JNIEnv * env, jclass c, jint pid)
{
    EnableDebugPriv();
    
    {
      HANDLE hProc = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, (DWORD)pid);
      DWORD dwExitStatus = 0;
      if (hProc == NULL) {
          return JNI_FALSE;
      } else {
        if (GetExitCodeProcess(hProc, &dwExitStatus)) {
          if (dwExitStatus == STILL_ACTIVE) {
            CloseHandle(hProc);
            return JNI_TRUE;
          }
        }
        CloseHandle(hProc);
        return JNI_FALSE;
      }
    }
}

  JNIEXPORT jboolean JNICALL
Java_hydra_WindowsPlatform_nativeKill(JNIEnv *env, jclass c, jint pid)
{
    EnableDebugPriv();
    
    {
      jboolean result = JNI_FALSE;
    
      HANDLE hProc = OpenProcess(PROCESS_TERMINATE, FALSE, (DWORD)pid);
      
      if (hProc) {
          if (TerminateProcess(hProc, 1)) {
              result = JNI_TRUE;
          }
          CloseHandle(hProc);
      }
      return result;
    }
}

  JNIEXPORT jint JNICALL
Java_hydra_WindowsPlatform__1bgexecInternal(JNIEnv *env,
							  jclass c,
							  jobjectArray cmdarray,
							  jstring workdir,
							  jstring logfile)
{
    int result = -1;
    int k = 0;
    int argc;
    char **argv = NULL;
    const char *wdstr = NULL;
    const char *logstr = NULL;
    char *argstr = NULL; /* Win32 only */
    char errmsg[1024];

    if (cmdarray == NULL) {
	goto error_cleanup;
    }
    argc = (*env)->GetArrayLength(env, cmdarray);
    if (argc == 0) {
	throwByName(env, "java/lang/IllegalArgumentException", "empty arg array");
	goto error_cleanup;
    }
    /* calloc null terminates for us */
    argv = (char**)calloc(argc + 1, sizeof(char *));
    if (argv == NULL) {
	throwByName(env, "java/lang/OutOfMemoryError", NULL);
	goto error_cleanup;
    }
    for (k = 0; k < argc; k++) {
        jstring jstr = (jstring)(*env)->GetObjectArrayElement(env, cmdarray, k);
	argv[k] = (char*)(*env)->GetStringUTFChars(env, jstr, NULL);
	(*env)->DeleteLocalRef(env, jstr);
	if (argv[k] == NULL) {
	    goto error_cleanup;
	}
    }
    wdstr = (*env)->GetStringUTFChars(env, workdir, NULL);
    if (wdstr == NULL) {
	goto error_cleanup;
    }
    logstr = (*env)->GetStringUTFChars(env, logfile, NULL);
    if (logstr == NULL) {
	goto error_cleanup;
    }

    if (argv) {
	char *p;
	int len;
        int i;
	int argstrLength = 1;
	char* argv0 = argv[0];
	const char* shellPrefix = "cmd /c ";
	i = 0;
	while (argv[i]) {
	    argstrLength += strlen(argv[i]) + 1;
	    i++;
	}
	argstr = (char*)calloc(argstrLength, sizeof(char));
	if (argstr == NULL) {
	    throwByName(env, "java/lang/OutOfMemoryError", NULL);
	    goto error_cleanup;
	}
	i = 0;
	p = argstr;
	while (argv[i]) {
	    if (i != 0) {
		strcpy(p, " ");
		p += 1;
	    }
	    len = strlen(argv[i]);
	    strcpy(p, argv[i]);
	    p += len;
	    i++;
	}
    }

    {
	int errcode;
	BOOL childcreated;
	HANDLE hChildin, hChildout;
	SECURITY_ATTRIBUTES saAttr;
	PROCESS_INFORMATION piProcInfo;
	STARTUPINFO siStartInfo;

	saAttr.nLength = sizeof(SECURITY_ATTRIBUTES);
	saAttr.bInheritHandle = TRUE;
	saAttr.lpSecurityDescriptor = NULL;
	hChildout = CreateFile(logstr, GENERIC_READ|GENERIC_WRITE,
			       FILE_SHARE_READ|FILE_SHARE_WRITE, // bug 20179
			       &saAttr, OPEN_ALWAYS,
			       /* FILE_FLAG_WRITE_THROUGH bug 23892 */ 0,
			       NULL);
	if (hChildout == INVALID_HANDLE_VALUE) {
	    sprintf(errmsg,
		    "Could not create log file '%s' failed with error=%d",
		    logstr, GetLastError());
  	    throwByName(env, "java/io/IOException", errmsg);
	    goto error_cleanup;
	}
	SetFilePointer(hChildout, 0, 0, FILE_END);

	hChildin = CreateFile("nul", GENERIC_READ, 0,
			      &saAttr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL,
			      NULL);
	if (hChildin == INVALID_HANDLE_VALUE) {
	    sprintf(errmsg, "could not open 'nul' file. error=%d\n",
		    GetLastError());
  	    throwByName(env, "java/io/IOException", errmsg);
	    CloseHandle(hChildout);
	    goto error_cleanup;
	}

        /* make sure our child does no inherit our stdin, stdout, and stderr */
        SetHandleInformation(GetStdHandle(STD_INPUT_HANDLE), HANDLE_FLAG_INHERIT, FALSE);
        SetHandleInformation(GetStdHandle(STD_OUTPUT_HANDLE), HANDLE_FLAG_INHERIT, FALSE);
        SetHandleInformation(GetStdHandle(STD_ERROR_HANDLE), HANDLE_FLAG_INHERIT, FALSE);

	memset(&siStartInfo, 0, sizeof(STARTUPINFO));
	siStartInfo.cb = sizeof(STARTUPINFO);
	siStartInfo.lpReserved = NULL;
	siStartInfo.lpReserved2 = NULL;
	siStartInfo.cbReserved2 = 0;
	siStartInfo.lpDesktop = NULL;
	siStartInfo.dwFlags = 0;
	siStartInfo.dwFlags = STARTF_USESTDHANDLES;
	siStartInfo.hStdInput = hChildin;
	siStartInfo.hStdOutput = hChildout;
	siStartInfo.hStdError = hChildout;
	childcreated = CreateProcess(NULL, (char*)argstr, NULL, NULL, TRUE /*inheritLogfile*/,
				     CREATE_NEW_PROCESS_GROUP|CREATE_DEFAULT_ERROR_MODE|CREATE_NO_WINDOW|DETACHED_PROCESS,
				     NULL, wdstr, &siStartInfo, &piProcInfo);
	errcode = GetLastError();
	CloseHandle(hChildin);
	CloseHandle(hChildout);
	if (!childcreated) {
	    sprintf(errmsg, "CreateProcess failed with errcode=%d\n",
		    errcode);
  	    throwByName(env, "java/io/IOException", errmsg);
	    goto error_cleanup;
	} else {
	    result = piProcInfo.dwProcessId;
	    CloseHandle(piProcInfo.hThread);
	    CloseHandle(piProcInfo.hProcess);
	}
    }

 error_cleanup:
    if (wdstr) {
	(*env)->ReleaseStringUTFChars(env, workdir, wdstr);
    }
    if (logstr) {
	(*env)->ReleaseStringUTFChars(env, logfile, logstr);
    }
    if (argv) {
	int len = (*env)->GetArrayLength(env, cmdarray);
        int i;
	for (i = 0; i < len; i++) {
	    if (argv[i] != NULL) {
		jstring jstr = (jstring)(*env)->GetObjectArrayElement(env, cmdarray, i);
		(*env)->ReleaseStringUTFChars(env, jstr, argv[i]);
		(*env)->DeleteLocalRef(env, jstr);
	    }
	}
	free(argv);
	if (argstr) {
	    free(argstr);
	}
    }
    return (jint)result;
}
