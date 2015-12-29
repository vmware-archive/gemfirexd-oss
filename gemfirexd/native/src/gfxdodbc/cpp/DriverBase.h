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
 * DriverBase.h
 *
 *  Contains declarations common to all parts of the driver.
 *
 *      Author: swale
 */

#ifndef DRIVERBASE_H_
#define DRIVERBASE_H_

#include "OdbcBase.h"

//#define NDEBUG // TODO: enable this in production version

#include <string>
#include <vector>

extern "C"
{
#include <stdio.h>
}

#include <Types.h>
#include <SQLException.h>
#include <Utils.h>
#include <ClientBase.h>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

template<typename K, typename V>
struct hash_map {
  typedef boost::unordered_map<K, V> type;
};

template<typename K>
struct hash_set {
  typedef boost::unordered_set<K> type;
};

using namespace com::pivotal::gemfirexd::client;

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {

      class GFXDHandleBase
      {
      private:
        AutoPtr<SQLException> m_lastError;
        static AutoPtr<SQLException> s_globalError;

        static SQLException* getUnknownException(const char* file, int line,
            const std::exception& ex) {
          // check for SQLException itself
          const SQLException* sqle = dynamic_cast<const SQLException*>(&ex);
          if (sqle != NULL) {
            return sqle->clone();
          }
          // check for out of memory separately
          if (dynamic_cast<const std::bad_alloc*>(&ex) != NULL) {
            return new SQLException(file, line, SQLState::OUT_OF_MEMORY,
                SQLStateMessage::OUT_OF_MEMORY_MSG.format(ex.what()));
          }

          std::string reason;
          Utils::demangleTypeName(typeid(ex).name(), reason);
          reason.append(": ").append(ex.what());
          return new SQLException(file, line, reason,
              SQLState::UNKNOWN_EXCEPTION, NULL);
        }

      protected:
        inline GFXDHandleBase() :
            m_lastError() {
        }

      public:
        inline static void setGlobalException(const SQLException& ex) {
          s_globalError.reset(ex.clone());
        }

        inline static void setGlobalException(const char* file, int line,
            const std::exception& ex) {
          s_globalError.reset(getUnknownException(file, line, ex));
        }

        inline static SQLException* getLastGlobalError() {
          return s_globalError.get();
        }

        inline static void clearLastGlobalError() {
          s_globalError.reset();
        }

        inline void setException(const SQLException& ex) {
          m_lastError.reset(ex.clone());
        }

        inline void setException(const char* file, int line,
            const std::exception& ex) {
          m_lastError.reset(getUnknownException(file, line, ex));
        }

        inline void setSQLWarning(const SQLWarning& warning) {
          m_lastError.reset(warning.clone());
        }

        inline SQLException* getLastError() {
          return m_lastError.get();
        }

        inline void clearLastError() {
          if (!m_lastError.isNull()) {
            m_lastError.reset();
          }
          clearLastGlobalError();
        }

        /** Common utility to handle a NULL passed in handle. */
        static SQLRETURN errorNullHandle(SQLSMALLINT handleType) {
          GFXDHandleBase::setGlobalException(
              GET_SQLEXCEPTION2(SQLStateMessage::NULL_HANDLE_MSG, handleType));
          return SQL_INVALID_HANDLE;
        }

        /** Common utility to handle a NULL passed in handle. */
        static SQLRETURN errorNullHandle(SQLSMALLINT handleType,
            GFXDHandleBase* handle) {
          if (handle != NULL) {
            handle->setException(GET_SQLEXCEPTION2(
                SQLStateMessage::NULL_HANDLE_MSG, handleType));
            return SQL_INVALID_HANDLE;
          } else {
            return errorNullHandle(handleType);
          }
        }

        /** Return error condition for an API function not implemented. */
        static SQLRETURN errorNotImplemented(const char* function) {
          GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
              SQLStateMessage::NOT_IMPLEMENTED_MSG, function));
          return SQL_ERROR;
        }

        /** Return error condition for an API function not implemented. */
        static SQLRETURN errorNotImplemented(const char* function,
            GFXDHandleBase* handle) {
          if (handle != NULL) {
            handle->setException(GET_SQLEXCEPTION2(
                SQLStateMessage::NOT_IMPLEMENTED_MSG, function));
            return SQL_ERROR;
          } else {
            return errorNotImplemented(function);
          }
        }

        /** Return error condition for an invalid input string/buffer length */
        static SQLRETURN errorInvalidBufferLength(int length, const char* name,
            GFXDHandleBase* handle) {
          handle->setException(GET_SQLEXCEPTION2(
              SQLStateMessage::INVALID_BUFFER_LENGTH_MSG, length, name));
          return SQL_ERROR;
        }
      };
    }
  }
}

#endif /* DRIVERBASE_H_ */
