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

/**
 * Library.cpp
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#include "Library.h"

extern "C"
{
#include <stdio.h>
}

using namespace com::pivotal::gemfirexd::native;

#ifdef _WINDOWS
void g_getLastErrorMessage(std::string& errMsg, const char* genericMsg)
{
  const DWORD err = ::GetLastError();
  LPTSTR errBuf = NULL;
  DWORD res = ::FormatMessage(
      FORMAT_MESSAGE_ALLOCATE_BUFFER |
      FORMAT_MESSAGE_FROM_SYSTEM |
      FORMAT_MESSAGE_IGNORE_INSERTS,
      NULL,
      err,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPTSTR)&errBuf,
      0, NULL);
  if (res > 0) {
    errMsg.append(errBuf, res);
    LocalFree(errBuf);
  }
  else {
    char buf[512];
    ::snprintf(buf, 511, "%s with error code %ld", genericMsg, err);
    errMsg.append(buf);
  }
}
}
#endif

Library::Library(const char* path) {
  std::string errMsg;
#ifdef _WINDOWS
  m_libraryHandle = ::LoadLibrary(path);
  if (m_libraryHandle == NULL) {
    ::g_getLastErrorMessage(errMsg, "failed to load library");
  }
#else // !_WINDOWS
  m_libraryHandle = ::dlopen(path, RTLD_NOW);
  if (m_libraryHandle == NULL) {
    errMsg.append(::dlerror());
  }
#endif
  if (errMsg.size() > 0) {
    throw GET_SQLEXCEPTION2(
        SQLStateMessage::TRANSACTION_LIBRARY_LOAD_FAILED_MSG, path,
        errMsg.c_str());
  }
}

Library::~Library() {
  if (m_libraryHandle != NULL) {
#ifdef _WINDOWS
    ::FreeLibrary((HMODULE)m_libraryHandle);
#else // !_WINDOWS
    ::dlclose(m_libraryHandle);
#endif
    m_libraryHandle = NULL;
  }
}

void* Library::getFunction(const char* name) {
  void* procHandle = NULL;
  std::string errMsg;
  if (m_libraryHandle != NULL) {
#ifdef _WINDOWS
    procHandle = (void*)::GetProcAddress((HMODULE)m_libraryHandle, (LPCSTR)name);
    if (procHandle == NULL) {
      errMsg.append("'").append(name).append("': ");
      ::g_getLastErrorMessage(errMsg, "failed to get function from library");
    }
#else // !_WINDOWS
    ::dlerror(); // clear the error first
    procHandle = ::dlsym(m_libraryHandle, name);
    const char* err;
    if ((err = ::dlerror()) != NULL) {
      errMsg.append("'").append(name).append("': ");
      errMsg.append(err);
    }
#endif
  }
  if (errMsg.size() == 0 && procHandle != NULL) {
    return procHandle;
  } else {
    throw GET_SQLEXCEPTION2(
        SQLStateMessage::TRANSACTION_LIBRARY_LOAD_FAILED_MSG, name,
        (errMsg.size() > 0 ? errMsg.c_str() : "got null function handle"));
  }
}
