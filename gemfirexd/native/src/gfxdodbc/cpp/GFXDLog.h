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

#ifndef GFXDLOG_H_
#define GFXDLOG_H_

#undef TRACE_ON
#undef TRACE_OFF

#include <LogWriter.h>
#include <Utils.h>

#include "OdbcBase.h"
#include "GFXDDefaults.h"

using namespace com::pivotal::gemfirexd::client;

#define DEBUG_PRINT_STR(x) \
  LogWriter::TRACE(TraceFlag::ClientHA) << x << _GFXD_NEWLINE;

#define DEBUG_PRINT_STR2(x, y) \
  LogWriter::TRACE(TraceFlag::ClientHA) << x << y << _GFXD_NEWLINE;

#ifndef MAX_PATH
#define MAX_PATH 260
#endif

class GFXDLog {
public:
  static char logFile[MAX_PATH];
  static int logLevel;

  static void init() {
    if (!GFXDGlobals::g_loggingEnabled && GFXDGlobals::g_initialized
        && ::strlen(GFXDLog::logFile) > 0) {
      // TODO: SW: set properties for C++ client logging below
      //java::lang::System::setProperty(JvNewStringLatin1("gemfirexd.debug.true"),
      //    JvNewStringLatin1("TraceClientHA"));
      GFXDGlobals::g_loggingEnabled = true;
    }
  }

  inline static void Log(SQLCHAR* str) {
    if (GFXDGlobals::g_loggingEnabled) {
      DEBUG_PRINT_STR(str);
    }
  }

  inline static void Log(const char* str) {
    if (GFXDGlobals::g_loggingEnabled) {
      DEBUG_PRINT_STR(str);
    }
  }

  inline static void Log(const std::string& str) {
    if (GFXDGlobals::g_loggingEnabled) {
      DEBUG_PRINT_STR(str);
    }
  }

  inline static void Log(const wchar_t* wstr) {
    if (GFXDGlobals::g_loggingEnabled) {
      DEBUG_PRINT_STR(wstr);
    }
  }

  inline static void Log(const std::wstring& str) {
    if (GFXDGlobals::g_loggingEnabled) {
      DEBUG_PRINT_STR(str.c_str());
    }
  }
};

class FunctionTracer {
private:
  const char* m_funcName;

public:
  FunctionTracer(const char* funcName) {
    m_funcName = NULL;
    if (GFXDGlobals::g_loggingEnabled) {
      m_funcName = funcName;
      DEBUG_PRINT_STR2("ENTER ", m_funcName);
    }
  }

  ~FunctionTracer() {
    if (GFXDGlobals::g_loggingEnabled && m_funcName != NULL) {
      DEBUG_PRINT_STR2("EXIT ", m_funcName);
    }
  }
};

#define FUNCTION_TRACER \
    FunctionTracer ft(__PRETTY_FUNCTION__);

#endif /* GFXDLOG_H_ */
