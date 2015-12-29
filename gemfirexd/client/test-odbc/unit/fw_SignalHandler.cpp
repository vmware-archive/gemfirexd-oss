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

//#include "gfcpp_globals.hpp"

#include "fw_SignalHandler.hpp"
//#include "ExceptionTypes.hpp"

#include <ace/OS.h>
#include <string>


namespace
{
  // crash dump related properties
  bool g_crashDumpEnabled = true;
  std::string g_crashDumpLocation = "";
  std::string g_crashDumpPrefix = "gemfire_nativeclient";
}

//namespace gemfire {

int SignalHandler::s_waitSeconds = 345600; // 4 days
#ifdef _WIN32
  void * SignalHandler::s_pOldHandler = NULL;
#endif

void SignalHandler::waitForDebugger( )
{
  const char dbg[128] =    "***         WAITING FOR DEBUGGER          ***\n";
  const char stars[128] =  "*********************************************\n";

  fprintf(stdout, "%s\n%s\n%s\n   Waiting %d seconds for debugger in Process Id %d\n",
      stars, dbg, stars, s_waitSeconds, ACE_OS::getpid());
  fflush( stdout );
  int max = s_waitSeconds / 10;
  while( max-- > 0 ) {
    ACE_OS::sleep( 10 );
  }
  fprintf( stdout, "%s   Waited %d seconds for debugger, Process Id %d exiting.\n%s",
    stars, s_waitSeconds, ACE_OS::getpid(), stars );
  fflush( stdout );
  exit( -1 );
}

void SignalHandler::init(bool crashDumpEnabled, const char* crashDumpLocation,
    const char* crashDumpPrefix)
{
  g_crashDumpEnabled = crashDumpEnabled;
  if (crashDumpLocation != NULL) {
    g_crashDumpLocation = crashDumpLocation;
  }
  g_crashDumpPrefix = crashDumpPrefix;
  if (!g_crashDumpEnabled) {
    SignalHandler::removeBacktraceHandler();
  }
}

bool SignalHandler::getCrashDumpEnabled()
{
  return g_crashDumpEnabled;
}

const char* SignalHandler::getCrashDumpLocation()
{
  return g_crashDumpLocation.c_str();
}

const char* SignalHandler::getCrashDumpPrefix()
{
  return g_crashDumpPrefix.c_str();
}

#ifdef _WIN32
  #include "WindowsSignalHandler.hpp"
#else
  #include "UnixSignalHandler.hpp"
#endif
