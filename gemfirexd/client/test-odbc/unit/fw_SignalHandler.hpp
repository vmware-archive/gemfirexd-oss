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
#ifndef _GEMFIRE_SIGNALHANDLER_HPP_
#define _GEMFIRE_SIGNALHANDLER_HPP_

//#include "gfcpp_globals.hpp"
//#include "Exception.hpp"

/** @file
*/

#ifdef _WIN32
struct _EXCEPTION_POINTERS;
typedef _EXCEPTION_POINTERS EXCEPTION_POINTERS;
#endif

//namespace gemfire
//{

 // class DistributedSystem;

  /** Represents a signal handler used for dumping stacks and 
   * attaching a debugger */
  class  SignalHandler
  {
  private:
    static int s_waitSeconds;

    static void init(bool crashDumpEnabled, const char* crashDumpLocation,
        const char* crashDumpPrefix);

  //  friend class DistributedSystem;

  public:
    /**
     * Register the GemFire backtrace signal handler for signals
     * that would otherwise core dump.
     */
    static void installBacktraceHandler();

    /**
     * Remove the GemFire backtrace signal handler for signals that were
     * previously registered.
     */
    static void removeBacktraceHandler();
    /**
    * Returns whether CrashDump is enabled or not.
    */
    static bool getCrashDumpEnabled();
    /**
    * Returns CrashDump location.
    */
    static const char* getCrashDumpLocation();
    /**
    * Returns CrashDump Prefix.
    */
    static const char* getCrashDumpPrefix();

    /**
     * Dump an image of the current process (core in UNIX, minidump in Windows)
     * and filling the path of dump in provided buffer of given max size.
     */
    static void dumpStack(char* dumpFile, unsigned int maxLen);

#ifdef _WIN32

    /**
     * Dump an image of the current process (core in UNIX, minidump in Windows)
     * and filling the path of dump in provided buffer of given max size.
     */
    static void dumpStack(unsigned int expCode, EXCEPTION_POINTERS* pExp,
        char* dumpFile, size_t maxLen);

    static void * s_pOldHandler;

#endif

    /** wait in a loop to allow a debugger to be manuallly attached 
     * and done is set to 1. */
    static void waitForDebugger();
  };

//}

#endif

