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
#include <eh.h>
#include <exception>

#include <windows.h>
#include <dbghelp.h>
#include <tchar.h>
#include <time.h>


typedef BOOL (WINAPI *MINIDUMP_WRITE_DUMP)(
  HANDLE hProcess,
  DWORD dwPid,
  HANDLE hFile,
  MINIDUMP_TYPE DumpType,
  CONST PMINIDUMP_EXCEPTION_INFORMATION ExceptionParam,
  CONST PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
  CONST PMINIDUMP_CALLBACK_INFORMATION CallbackParam
);
LONG handleDebugEvent(LPEXCEPTION_POINTERS lpEP);
