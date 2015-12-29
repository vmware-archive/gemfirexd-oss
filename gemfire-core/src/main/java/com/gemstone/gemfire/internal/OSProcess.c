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


#ifdef FLG_UNIX
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef FLG_OSX_UNIX
#else
#include <wait.h>
#endif
#include <poll.h>
#else /* NT */
#include <windows.h>
#endif

#include "host.hf"
#include "utl.hf"
#define index work_around_gcc_bug
#include "com_gemstone_gemfire_internal_OSProcess.h"
#undef index 

static void checkAsciiMessage(const char* message)
{
  int j;
  // some checks to try to catch uninitialized message buffers
  UTL_ASSERT(message != NULL);
  for (j = 0; j < 100; j++) {
    int aChar = message[j] ;
    if (aChar == '\0')
      break;
    UTL_ASSERT(aChar > 0 && aChar <= 0x7F);
  }
}

static void throwByName(JNIEnv *env, const char *name, const char *msg)
{
  jthrowable prev;
  UTL_ASSERT(env != NULL);
  checkAsciiMessage(msg);
  prev = (*env)->ExceptionOccurred(env);
  if (prev == NULL) {
    jclass cls = (*env)->FindClass(env, name);

    if (cls != NULL) { /* Otherwise an exception has already been thrown */
      (*env)->ThrowNew(env, cls, msg);
      (*env)->DeleteLocalRef(env, cls);  /* clean up local ref*/
    } else {
      char msgBuf[300];
      snprintf(msgBuf, sizeof(msgBuf), "unable to throw %s %s \n", name, msg);
      HostPrintStderr("severe", msgBuf);
#ifdef FLG_DEBUG
      HostCallDebugger();
#endif
    }
  } else {
    (*env)->DeleteLocalRef(env, prev);
  }
}

  JNIEXPORT jint JNICALL
Java_com_gemstone_gemfire_internal_OSProcess_bgexecInternal(JNIEnv *env,
							  jclass c,
							  jobjectArray cmdarray,
							  jstring workdir,
							  jstring logfile,
                                                          jboolean inheritLogfile)
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
    argv = (char**)UtlCalloc(argc + 1, sizeof(char *));
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

#ifdef FLG_UNIX
    do { /* loop because SIGCHLD can cause fork() to fail */
	errno = 0;
#ifdef FLG_SOLARIS_UNIX
	result = fork1();
#elif defined(FLG_LINUX_UNIX)
	result = fork();  
#elif defined(FLG_OSX_UNIX)
        result = fork();  
#else
+++ sorry, we only want _this_ thread forked.  How do you do it?
        result = fork();
#endif
#ifdef FLG_OSX_UNIX
    } while (result == -1 && (errno == EINTR));
#else
    } while (result == -1 && (errno == EINTR || errno == ERESTART));
#endif

    if (result == 0) {
	/* Child process */
	int max_fd;
	int logfd;
	FILE* theLogFile;
	char logbuff[4096];
	const char* logptr;
	int skipfd = 0;
	char * envhack = getenv("GEMSTONE_SERVER_PIPE");
	if (envhack != NULL) {
	    sscanf(envhack, "%d", &skipfd);
	}

#if defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
        {
          /* Record our pid in an env var. We use this to get the main pid
           * to fix bug 28770.
           */
          char* inStr = (char*)UtlMalloc(100, "OSPROCESS_BGEXEC_PID");
          snprintf(inStr, 99, "OSPROCESS_BGEXEC_PID=%d", getpid());
          putenv(inStr);
          inStr = (char*)UtlMalloc(100, "TEST_OSPROCESS_BGEXEC_PID");
          snprintf(inStr, 99, "TEST_OSPROCESS_BGEXEC_PID=%d", getpid());
          putenv(inStr);
        }
#endif        
	/* close everything */
	max_fd = sysconf(_SC_OPEN_MAX);
        int j;
	for (j = 3; j < max_fd; j++) {
	    if (j != skipfd) {
		close(j);
	    }
	}

	if (strstr(logstr, "bgexec") != NULL) {
	  char *endptr;

	  /* It contains "bgexec" so tack on the pid.
	   * But first remove the empty file created in OSProcess.java.
	   */
	  unlink(logstr);
	  
	  strncpy(logbuff, logstr, sizeof(logbuff) - 10);
	  logbuff[sizeof(logbuff) - 10] = '\0';
	  endptr = strrchr(logbuff, '.');
	  if (endptr != NULL) {
	    endptr[0] = '\0';
	  } else {
	    endptr = logbuff + strlen(logbuff);
	  }
	  sprintf(endptr, "_%d.log", getpid());
	  logptr = logbuff;
	} else {
	  logptr = logstr;
	}

	logfd = open(logptr, O_WRONLY|O_APPEND|O_CREAT, 0644);
	if (logfd == -1) {
	    /* Bummer. Can't do much since have no place to write error */
	    _exit(-1);
	}
	
	close(0);
	open("/dev/null", O_RDONLY); /* makes stdin point at /dev/null */
	fcntl(0, F_SETFD, 0);	/* unset close-on-exec for good measure */

	if (-1 == dup2(logfd, 1)) {
	    const char* msg = "dup2 of stdout failed\n";
	    char* msg2 = strerror(errno);
	    size_t num = write(logfd, msg, strlen(msg));
	    num = write(logfd, msg2, strlen(msg2));
        if (num < 0) {
          _exit(num);
        }
	    _exit(-1);
	}
	if (-1 == dup2(logfd, 2)) {
	    const char* msg = "dup2 of stderr failed: ";
	    char* msg2 = strerror(errno);
	    size_t num = write(logfd, msg, strlen(msg));
	    num = write(logfd, msg2, strlen(msg2));
        if (num < 0) {
          _exit(num);
        }
	    _exit(-1);
	}
	/* don't want this file hanging around after exec */
	fcntl(logfd, F_SETFD, 1);
	/* unset close-on-exec for good measure */
	fcntl(1, F_SETFD, 0);
	fcntl(2, F_SETFD, 0);

	errno = 0;
	theLogFile = fdopen(1, "a");
	/* Note we use '1' instead of 'logfd'.
	 * Due to the dup2 call above we can do this.
	 * And since logfd might be greater than
	 * 255 this fixes bug 14707.
	 */
	if (!theLogFile) {
	    const char* msg = "fdopen(1, \"a\") failed: ";
	    char* msg2 = strerror(errno);
	    size_t num = write(logfd, msg, strlen(msg));
	    num = write(logfd, msg2, strlen(msg2));
        if (num < 0) {
          _exit(num);
        }
	    _exit(-1);
	}

	/* Now we can write messages to user's theLogFile. */
#if 0
	fprintf(theLogFile, "trace messages\n");
	fflush(theLogFile);
	i = 0;
	while (argv[i]) {
	    fprintf(theLogFile, "argv[%d] = '%s'\n", i, argv[i]);
	    i++;
	}
	fprintf(theLogFile, "workdir = '%s'\n", wdstr);
	fprintf(theLogFile, "logfile = '%s'\n", logptr);
	fflush(theLogFile);
#endif

	errno = 0;
	if (chdir(wdstr) == -1) {
	    fprintf(theLogFile, "chdir(%s) failed with errno=%d (i.e. %s)\n",
		    wdstr, errno, strerror(errno));
	    fflush(theLogFile);
	    _exit(-1);
	}
	/* Make sure this process not part of parent's session */
	errno = 0;
	if (setsid() == -1) {
	    fprintf(theLogFile, "setsid() failed with errno=%d (i.e. %s)\n",
		    errno, strerror(errno));
	    fflush(theLogFile);
	    _exit(-1);
	}

	errno = 0;
	execv(argv[0], argv);

	fprintf(theLogFile, "execv() failed with errno=%d (i.e. %s)\n",
		errno, strerror(errno));
	fprintf(theLogFile, "The command and its args follow:\n");
	fprintf(theLogFile, "%s\n", argv[0]);
	int i2 = 1;
	while (argv[i2]) {
	    fprintf(theLogFile, "  %s\n", argv[i2]);
	    i2++;
	}
	fflush(theLogFile);
	_exit(-1);
    } else {
	/* parent process */
  	if (result < 0) {
	    sprintf(errmsg, "fork failed with result = %d, errno = %d, %s\n",
		    result, errno, strerror(errno));
  	    throwByName(env, "java/io/IOException", errmsg);
  	    goto error_cleanup;
	}
    }
    
#else /* NT Win32 */
    if (argv) {
	char *p;
	size_t len;
        int i;
	size_t argstrLength = 1;
	int needsShell = 0;
	char* argv0 = argv[0];
	const char* shellPrefix = "cmd /c ";
#if 0
	if (strlen(argv0) > 4) {
	    char* extPtr = argv0 + strlen(argv0) - 4;
	    if (strcmp(extPtr, ".bat") == 0 || strcmp(extPtr, ".BAT") == 0) {
		needsShell = 1;
	    }
	}
#endif
	if (needsShell) {
	    argstrLength += strlen(shellPrefix);
	}
	i = 0;
	while (argv[i]) {
	    argstrLength += strlen(argv[i]) + 1;
	    i++;
	}
	argstr = (char*)UtlCalloc(argstrLength, sizeof(char));
	if (argstr == NULL) {
	    throwByName(env, "java/lang/OutOfMemoryError", NULL);
	    goto error_cleanup;
	}
	i = 0;
	p = argstr;
	if (needsShell) {
	    strcpy(p, shellPrefix);
	    p += strlen(shellPrefix);
	}
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

#if 0
	fprintf(stdout, "trace messages\n");
	fflush(stdout);
	i = 0;
	while (argv[i]) {
	    fprintf(stdout, "argv[%d] = '%s'\n", i, argv[i]);
	    i++;
	}
	fprintf(stdout, "workdir = '%s'\n", wdstr);
	fprintf(stdout, "logfile = '%s'\n", logstr);
	fprintf(stdout, "argstr = '%s'\n", argstr);
	fflush(stdout);
#endif

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
#endif

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
	UtlFree(argv);
	if (argstr) {
	    UtlFree(argstr);
	}
    }
    return (jint)result;
}

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    nativeExists
 * Signature: (I)Z
 */
  JNIEXPORT jboolean JNICALL 
Java_com_gemstone_gemfire_internal_OSProcess_nativeExists(JNIEnv * env,
							jclass c,
							jint pid)
{
#ifdef FLG_UNIX
    if (kill((pid_t)pid, 0) == -1) {
      if (errno == EPERM) {
        /* Process exists; it probably belongs to another user. Fix for bug 27698. */
        return JNI_TRUE;
      } else {
	return JNI_FALSE;
      }
    } else {
	return JNI_TRUE;
    }
#else
    HANDLE h = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, (DWORD)pid);
    if (h == NULL) {
	return JNI_FALSE;
    } else {
	CloseHandle(h);
	return JNI_TRUE;
    }
#endif
}

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    kill
 * Signature: (I)Z
 */
  JNIEXPORT jboolean JNICALL
Java_com_gemstone_gemfire_internal_OSProcess__1kill(JNIEnv *env,
						jclass c,
						jint pid)
{
#ifdef FLG_UNIX
    if (kill((pid_t)pid, SIGKILL) == -1) {
	return JNI_FALSE;
    } else {
	return JNI_TRUE;
    }
#else
    jboolean result = JNI_FALSE;
    HANDLE h = OpenProcess(PROCESS_TERMINATE, FALSE, pid);
    if (h) {
	if (TerminateProcess(h, 1)) {
	    result = JNI_TRUE;
	}
	CloseHandle(h);
    }
    return result;
#endif
}

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    shutdown
 * Signature: (I)Z
 */
  JNIEXPORT jboolean JNICALL
Java_com_gemstone_gemfire_internal_OSProcess__1shutdown(JNIEnv * env,
						    jclass c,
						    jint pid)
{
#ifdef FLG_UNIX
    if (kill((pid_t)pid, SIGTERM) == -1) {
	return JNI_FALSE;
    } else {
	return JNI_TRUE;
    }
#else
    /* no nice way on nt; so just do a kill */
    return Java_com_gemstone_gemfire_internal_OSProcess__1kill(env, c, pid);
#endif
}

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    printStacks
 * Signature: (I)Z
 */
  JNIEXPORT jboolean JNICALL
Java_com_gemstone_gemfire_internal_OSProcess__1printStacks(JNIEnv * env,
						    jclass c,
						    jint pid)
{
#ifdef FLG_UNIX
    if (pid == 0) {
      pid = getpid();
    }
    if (kill((pid_t)pid, SIGQUIT) == -1) {
	return JNI_FALSE;
    } else {
	return JNI_TRUE;
    }
#else
  jboolean result = JNI_FALSE;
  char nameBuf[128];
  HANDLE targetEvent = NULL;

  sprintf(nameBuf, "gsjvm_sigbreakevent%d"/*must agree with os_win32.cpp*/, pid);
  targetEvent = OpenEvent(EVENT_MODIFY_STATE, FALSE, nameBuf);
  if (targetEvent != NULL) {
    if (SetEvent(targetEvent)) {
      result = JNI_TRUE;
    }
    CloseHandle(targetEvent);
  }
  return result;
#endif
}

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    waitForPid
 * Signature: (I)V
 */
  JNIEXPORT void JNICALL
Java_com_gemstone_gemfire_internal_OSProcess_waitForPid(JNIEnv *env,
						      jclass c,
						      jint pid)
{
#ifdef FLG_UNIX
    pid_t resultPid;
    int   status;
    do {
      resultPid = waitpid(pid, &status, 0);
    } while (resultPid == -1 && errno == EINTR);
#else /* NT */
    HANDLE h = OpenProcess(SYNCHRONIZE, FALSE, (DWORD)pid);
    if (h != NULL) {
        WaitForSingleObject(h, INFINITE);
	CloseHandle(h);
    }
#endif
}

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    reapPid
 * Signature: (I)Z
 */
    /**
     * Reaps a child process if it has died.
     * Does not wait for the child.
     * @param pid the id of the process to reap
     * @return true if it was reaped or lost (someone else reaped it);
     * false if the child still exists.
     * HACK: If pid is -1 then returns true if this platform needs reaping.
     */
  JNIEXPORT jboolean JNICALL 
Java_com_gemstone_gemfire_internal_OSProcess_reapPid(JNIEnv *env,
						   jclass c,
						   jint pid)
{
#ifdef FLG_UNIX
    pid_t resultPid;
    int status;
    if (pid < 0) {  // initialization call , just return true on unix
      return JNI_TRUE;
    }
    do {
      resultPid = waitpid(pid, &status, WNOHANG);
    } while (resultPid == -1 && errno == EINTR);
    if (resultPid != pid) {
	if (resultPid == -1) {
	    /* we get an error if someone else reaped the pid 
	     *
	     * printf("waitpid for pid %d failed with result_pid = %d errno %d\n",
             *        pid, resultPid, errno);
	     */
	    return JNI_TRUE;
	} else {
	    /* this means that the child process still exists 
	     *
	     * printf("waitpid for pid %d returned %d and status = %d\n",
	     *   	pid, resultPid, status);
	     */
	    return JNI_FALSE;
	}
    } else {
	/* the child died and we reaped it */
	/*
	 * printf("pid %d exited with status code %d\n", pid, info.si_status);
	 */
	return JNI_TRUE;
    }
#else
    /* nothing needed on NT */
    return JNI_FALSE;
#endif
}

/* deleted reaperRun */

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    jniSetCurDir
 * Signature: (Ljava/lang/String;)Z
 */
  JNIEXPORT jboolean JNICALL 
Java_com_gemstone_gemfire_internal_OSProcess_jniSetCurDir(JNIEnv * env,
							jclass c,
							jstring dir)
{
    const char *dirStr = (*env)->GetStringUTFChars(env, dir, NULL);
    jboolean result;
    if (dirStr == NULL) {
      return JNI_FALSE;
    }
#ifdef FLG_UNIX
    if (chdir(dirStr) != -1) {
      result = JNI_TRUE;
    } else {
      /* what to do with errno? */
      result = JNI_FALSE;
    }
#else
    if (SetCurrentDirectory(dirStr)) {
      result = JNI_TRUE;
    } else {
      /* what to do with GetLastError() ? */
      result = JNI_FALSE;
    }
#endif
    (*env)->ReleaseStringUTFChars(env, dir, dirStr);
    return result;
}

  JNIEXPORT jint JNICALL
Java_com_gemstone_gemfire_internal_OSProcess_getProcessId(JNIEnv *env, jclass c)
{
  // note on Linux, getpid() may return the "process id" of the current
  //  thread, which may be different from the process id of the Java VM main thread .
  return HOST_GETPID();
}
#if defined(FLG_LINUX_UNIX) 
#define STDERR_ALIAS 2 
static void redirectFd(int target, const char* filename) { 
  int fd = open(filename, O_WRONLY|O_CREAT|O_APPEND|O_LARGEFILE, 0666); 
  int duplicate = fcntl(target, F_DUPFD, 10); 
  const char msg[] = "Failed to redirect line %d, fd %d, with errono %d"; 
  if(duplicate == -1) { 
    if(target == STDERR_ALIAS) { 
      fprintf(stdout, msg, __LINE__, target, errno); 
    } else { 
      fprintf(stderr, msg, __LINE__, target, errno); 
    } 
  } 
  int ret = fcntl(duplicate, F_SETFD, FD_CLOEXEC); 
  if(ret == -1) { 
    if(target == STDERR_ALIAS) { 
      fprintf(stdout, msg, __LINE__, target, errno); 
    } else { 
      fprintf(stderr, msg, __LINE__, target, errno); 
    } 
  } 
  ret = dup2(fd, target); 
  if(ret == -1) { 
    if(target == STDERR_ALIAS) { 
      fprintf(stdout, msg, __LINE__, target, errno); 
    } else { 
      fprintf(stderr, msg, __LINE__, target, errno); 
    } 
  } 
  close(fd); 
  close(duplicate); 
} 
#undef STDERR_ALIAS 
#endif 
	 
/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    redirectCOutput
 */
  JNIEXPORT void JNICALL 
Java_com_gemstone_gemfire_internal_OSProcess_redirectCOutput(JNIEnv * env,
                                                             jclass c,
                                                             jstring fname)
{
  jclass booleanClazz = (*env)->FindClass(env, "java/lang/Boolean"); 
  jmethodID getBooleanMethod = (*env)->GetStaticMethodID(env,  
                                                         booleanClazz, 
                                                         "getBoolean", 
                                                         "(Ljava/lang/String;)Z"); 
  jstring property = (*env)->NewStringUTF(env,  
                                          "gemfire.disableLowLevelIORedirection"); 
  jboolean disableIORedirect = (*env)->CallStaticBooleanMethod(env,  
                                                               booleanClazz, 
                                                               getBooleanMethod, 
                                                               property); 
  (*env)->DeleteLocalRef(env, property);   
  (*env)->DeleteLocalRef(env, booleanClazz);   
  if( disableIORedirect == JNI_FALSE ) { 
    const char *fnameStr = (*env)->GetStringUTFChars(env, fname, NULL);
    if (fnameStr != NULL) {
#if defined(FLG_LINUX_UNIX) 
      redirectFd(1, fnameStr); 
      redirectFd(2, fnameStr); 
#else
      freopen(fnameStr, "a+", stdout);
      freopen(fnameStr, "a+", stderr);
#endif
      (*env)->ReleaseStringUTFChars(env, fname, fnameStr);
    }
  }
}

/*
 * Class:     com_gemstone_gemfire_internal_OSProcess
 * Method:    registerSigQuitHandler
 */
  JNIEXPORT void JNICALL 
Java_com_gemstone_gemfire_internal_OSProcess_registerSigQuitHandler(JNIEnv * env,
                                                             jclass c)
{
#ifdef FLG_UNIX
  HostInstallSigQuitHandler(/*env*/);
#endif
}
