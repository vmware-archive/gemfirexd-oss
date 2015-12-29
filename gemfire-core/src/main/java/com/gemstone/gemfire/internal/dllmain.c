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
#define DLL_MAIN_C

#include "flag.ht"
#if defined(FLG_UNIX)
#include <stdlib.h>
#include <dlfcn.h>
#endif

#define index work_around_gcc_bug
#include "jni.h"
#undef index

#include "host.hf"
#include "gemfire.h"

#ifdef _WIN32
#include <windows.h>
#include <process.h>
HINSTANCE libgemfire_handle;
BOOL WINAPI _CRT_INIT( HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpReserved);
BOOL WINAPI DLLMain( HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpReserved)
{
  /*
   * This comment was taken from process.h and explains the use of _CRT_INIT
   *
   * Declare DLL notification (initialization/termination) routines
   *      The preferred method is for the user to provide DllMain() which will
   *      be called automatically by the DLL entry point defined by the C run-
   *      time library code.  If the user wants to define the DLL entry point
   *      routine, the user's entry point must call _CRT_INIT on all types of
   *      notifications, as the very first thing on attach notifications and
   *      as the very last thing on detach notifications.
   */

  switch (fdwReason) {
    case DLL_PROCESS_ATTACH:
      /* puts("DLL_PROCESS_ATTACH"); */
      if ( !_CRT_INIT( hinstDLL, fdwReason, lpReserved) ) {
        return FALSE;
      }
      libgemfire_handle = hinstDLL ;
      break;
    case DLL_PROCESS_DETACH:
      /* should the tls key be freed here? */
      return _CRT_INIT( hinstDLL, fdwReason, lpReserved);
      break;
    case DLL_THREAD_ATTACH:
      /* puts("DLL_THREAD_ATTACH"); */
      if ( !_CRT_INIT( hinstDLL, fdwReason, lpReserved) ) {
        return FALSE;
      }
      break;
    case DLL_THREAD_DETACH:
      /* puts("DLL_THREAD_DETACH"); */
      return _CRT_INIT( hinstDLL, fdwReason, lpReserved);
      break;
    default:
      break;
  }
  return TRUE;
}

EXTERN_GS_DEC(void) DllMainGetPath(char *result, int maxLen)
{
  GetModuleFileName( libgemfire_handle, result, maxLen);
}

#elif defined(FLG_UNIX)
#include "utl.hf"

EXTERN_GS_DEC(void) DllMainGetPath(char *result, int maxLen)
{
  UTL_ASSERT(maxLen >= PATH_MAX);
  { Dl_info dlInfo;

#ifdef FLG_SOLARIS_UNIX
    dladdr( (void*) DllMainGetPath, &dlInfo);
#elif defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
    dladdr( (const void*) DllMainGetPath, &dlInfo);
#else
#error port error
+++ port error
#endif

    char * vResult = realpath(dlInfo.dli_fname, result);

#ifdef FLG_SOLARIS_UNIX
 ; 
#elif defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
    free(vResult);
#else
#error port error
+++ port error
#endif
  }
}


#else
+++ port error
#endif
