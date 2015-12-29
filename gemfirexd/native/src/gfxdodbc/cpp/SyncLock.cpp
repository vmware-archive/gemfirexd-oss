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
 * SyncLock.cpp
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#include "SyncLock.h"

extern "C"
{
#include <pthread.h>
}

using namespace com::pivotal::gemfirexd;

native::SyncLock::SyncLock() {
  m_error = 0;
#ifdef _WINDOWS
  m_lock = ::CreateMutex(NULL, 0, NULL);
  if (m_lock == NULL) {
    m_error = ::GetLastError();
  }
#else // !_WINDOWS
  m_error = ::pthread_mutex_init(&m_lock, NULL);
#endif
  if (m_error != 0) {
    GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
        SQLStateMessage::NATIVE_ERROR, "Initialization", "mutex", m_error));
  }
}

native::SyncLock::~SyncLock() {
  int err = 0;
  if (m_error == 0) {
#ifdef _WINDOWS
    if (::CloseHandle(m_lock) == 0) {
      err = ::GetLastError();
    }
#else // !_WINDOWS
    err = ::pthread_mutex_destroy(&m_lock);
#endif
  }
  if (err != 0) {
    GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
        SQLStateMessage::NATIVE_ERROR, "Destruction", "mutex", err));
  }
}

SQLRETURN native::SyncLock::Guard::acquire() {
#ifdef _WINDOWS
  m_state = ::WaitForSingleObject(m_sync.m_lock, INFINITE);
#else // !_WINDOWS
  m_state = ::pthread_mutex_lock(&m_sync.m_lock);
#endif
  if (m_state == 0) {
    return SQL_SUCCESS;
  } else {
    GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
        SQLStateMessage::NATIVE_ERROR, "Locking", "mutex", m_state));
    return SQL_ERROR;
  }
}

native::SyncLock::Guard::~Guard() {
  int err = 0;
  if (m_state == 0) {
#ifdef _WINDOWS
    if (::ReleaseMutex(m_sync.m_lock) == 0) {
      err = ::GetLastError();
    }
#else // !_WINDOWS
    err = ::pthread_mutex_unlock(&m_sync.m_lock);
#endif
  }
  if (err != 0) {
    GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
        SQLStateMessage::NATIVE_ERROR, "Destruction", "mutex", err));
  }
}
