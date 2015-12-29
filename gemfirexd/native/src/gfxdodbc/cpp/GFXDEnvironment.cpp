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
 * GFXDEnvironment.cpp
 *
 * Implementation of general allocation functions and execution environment.
 *
 *      Author: swale
 */

#include "GFXDEnvironment.h"
#include "StringFunctions.h"
#include "GFXDConnection.h"
#include "GFXDStatement.h"
#include "OdbcIniKeys.h"
#include "GFXDDefaults.h"
#include "Library.h"
#include "GFXDLog.h"

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::native;

namespace
{
  SyncLock g_sync;
}

std::vector<GFXDEnvironment*> GFXDEnvironment::m_envHandles;

template<typename CHAR_TYPE>
SQLRETURN GFXDEnvironment::handleError_(const SQLException& err,
    SQLSMALLINT callNumber, SQLSMALLINT recNumber, CHAR_TYPE* sqlState,
    SQLINTEGER* nativeError, CHAR_TYPE* messageText, SQLSMALLINT bufferLength,
    SQLSMALLINT* textLength) {
  SQLRETURN result = SQL_SUCCESS;

  const SQLException* sqlEx;
  for (int rec = 1; rec < recNumber; rec++) {
    if ((sqlEx = err.getNextException()) == NULL) {
      return SQL_NO_DATA;
    }
  }
  if (sqlState != NULL) {
    const std::string& state = sqlEx->getSQLState();
    StringFunctions::copyString((SQLCHAR*)state.c_str(), state.length(),
        sqlState, SQL_NTS, NULL);
  }
  if (nativeError != NULL) {
    *nativeError = sqlEx->getSeverity();
  }
  if (messageText != NULL) {
    if (bufferLength < 0) {
      return SQL_ERROR;
    }
    if (GFXDGlobals::g_loggingEnabled) {
      const std::string stack = err.getStackTrace();
      GFXDLog::Log(stack);
    }
    const std::string message = err.toString();
    ::memset(messageText, 0, bufferLength);
    SQLLEN len = 0;
    if (StringFunctions::copyString((SQLCHAR*)message.c_str(), message.length(),
        messageText, bufferLength, &len)) {
      result = SQL_SUCCESS_WITH_INFO;
    }
    if (textLength != NULL) {
      *textLength = (len > 0x7FFF ? 0x7FFF : len);
    }
  }
  return result;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDEnvironment::handleErrorT(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, CHAR_TYPE* sqlState,
    SQLINTEGER* nativeError, CHAR_TYPE* messageText, SQLSMALLINT bufferLength,
    SQLSMALLINT* textLength) {
  // return the last error, if any, else the last warning
  SQLException* err = NULL;

  if (recNumber <= 0) {
    return SQL_ERROR;
  }
  // try the global error first since it will override everything else
  err = GFXDHandleBase::getLastGlobalError();

  if (err == NULL) {
    GFXDHandleBase* handleBase;
    if (handle == NULL) {
      return SQL_INVALID_HANDLE;
    }
    switch (handleType) {
      case SQL_HANDLE_ENV:
        handleBase = (GFXDEnvironment*)handle;
        break;
      case SQL_HANDLE_DBC:
        handleBase = (GFXDConnection*)handle;
        break;
      case SQL_HANDLE_STMT:
        handleBase = (GFXDStatement*)handle;
        break;
      case SQL_HANDLE_DESC:
        // TODO: handleBase = (GFXDDescription*)handle;
        // break;
      default:
        return SQL_INVALID_HANDLE;
    }
    err = handleBase->getLastError();
  }
  return handleError_(*err, 1, recNumber, sqlState, nativeError, messageText,
      bufferLength, textLength);
}

SQLRETURN GFXDEnvironment::handleDiagFieldT(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
    SQLPOINTER diagInfo, SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  SQLRETURN ret = SQL_SUCCESS;
  SQLLEN intRes;
  bool hasIntRes = false;
  switch (diagId) {
    case SQL_DIAG_CURSOR_ROW_COUNT: {
      if (handleType != SQL_HANDLE_STMT) {
        return SQL_ERROR;
      }
      hasIntRes = true;
      intRes = 0;
      // TODO: SW: need to revisit
      /*
      GFXDStatement* stmt = (GFXDStatement*)handle;
      if (stmt->m_resultSet.get() != NULL) {
        const int32_t currentRow = stmt->m_resultSet->getRow();
        if (stmt->m_resultSet->last()) {
          intRes = stmt->m_resultSet->getRow();
          if (currentRow == 0) {
            stmt->m_resultSet->beforeFirst();
          }
          else {
            stmt->m_resultSet->absolute(currentRow);
          }
        }
      }
      */
      break;
    }
    case SQL_DIAG_NUMBER: {
      hasIntRes = true;
      intRes = 0;
      GFXDHandleBase* handleBase = (GFXDHandleBase*)handle;
      const SQLException* sqlEx = handleBase->getLastError();
      if (sqlEx != NULL) {
        intRes++;
        while ((sqlEx = sqlEx->getNextException()) != NULL) {
          intRes++;
        }
      }
      break;
    }
    case SQL_DIAG_ROW_COUNT: {
      if (handleType != SQL_HANDLE_STMT) {
        return SQL_ERROR;
      }
      hasIntRes = true;
      intRes = 0;
      GFXDStatement* stmt = (GFXDStatement*)handle;
      if ((ret = stmt->getUpdateCount(&intRes)) != SQL_SUCCESS) {
        return ret;
      }
      break;
    }
    default:
      // TODO: implement other field IDs
      return SQL_ERROR;
  }
  if (hasIntRes) {
    switch (bufferLength) {
      case SQL_IS_INTEGER:
        *((SQLINTEGER*)diagInfo) = intRes;
        break;
      case SQL_IS_UINTEGER:
        *((SQLINTEGER*)diagInfo) = intRes;
        break;
      case SQL_IS_SMALLINT:
        *((SQLSMALLINT*)diagInfo) = (SQLSMALLINT)intRes;
        break;
      case SQL_IS_USMALLINT:
        *((SQLUSMALLINT*)diagInfo) = (SQLUSMALLINT)intRes;
        break;
      default:
        return SQL_ERROR;
    }
  }
  return ret;
}

SQLRETURN GFXDEnvironment::newEnvironment(GFXDEnvironment*& envRef) {
  SyncLock::Guard lock(::g_sync);
  SQLRETURN lockResult;
  if ((lockResult = lock.acquire()) != SQL_SUCCESS) {
    envRef = SQL_NULL_HENV;
    return lockResult;
  }
  GFXDEnvironment* env = new GFXDEnvironment(false);
  m_envHandles.push_back(env);

  envRef = env;
  if (GFXDGlobals::g_initialized) {
    return SQL_SUCCESS;
  } else {
    if (OdbcIniKeys::init() == SQL_ERROR) {
      delete env;
      envRef = SQL_NULL_HENV;
      return SQL_ERROR;
    }

    GFXDGlobals::g_initialized = true;
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDEnvironment::freeEnvironment(GFXDEnvironment* env) {
  if (env != NULL) {
    int numConnections = 0;
    {
      SyncLock::Guard lock(env->m_connLock);
      SQLRETURN lockResult;
      if ((lockResult = lock.acquire()) != SQL_SUCCESS) {
        return lockResult;
      }

      numConnections = env->m_connections.size();
    }

    // if there is any active connection then return error
    if (numConnections > 0) {
      GFXDHandleBase::setGlobalException(
          GET_SQLEXCEPTION2(SQLStateMessage::FUNCTION_SEQUENCE_ERROR_MSG,
              "active connections when freeing HENV handle"));
      return SQL_ERROR;
    }

    {
      SyncLock::Guard lock(::g_sync);
      SQLRETURN lockResult;
      if ((lockResult = lock.acquire()) != SQL_SUCCESS) {
        return lockResult;
      }

      //remove this env handle
      for (std::vector<GFXDEnvironment*>::iterator iter = m_envHandles.begin();
          iter != m_envHandles.end(); ++iter) {
        if (env == (*iter)) {
          m_envHandles.erase(iter);
          break;
        }
      }
    }

    delete env;
    GFXDGlobals::g_loggingEnabled = false;
    return SQL_SUCCESS;
  } else {
    return GFXDHandleBase::errorNullHandle(SQL_HANDLE_ENV);
  }
}

void GFXDEnvironment::addNewActiveConnection(GFXDConnection* conn) {
  SQLRETURN lockResult;

  SyncLock::Guard lock(m_connLock);
  if ((lockResult = lock.acquire()) != SQL_SUCCESS) {
    throw GET_SQLEXCEPTION(SQLState::UNKNOWN_EXCEPTION,
        "Failed to acquire connection lock");
  }

  m_connections.push_back(conn);
}

int GFXDEnvironment::getActiveConnectionsCount() {
  SQLRETURN lockResult;

  SyncLock::Guard lock(m_connLock);
  if ((lockResult = lock.acquire()) != SQL_SUCCESS) {
    throw GET_SQLEXCEPTION(SQLState::UNKNOWN_EXCEPTION,
        "Failed to acquire connection lock");
  }

  return m_connections.size();
}

SQLRETURN GFXDEnvironment::forEachActiveConnection(
    SQLRETURN (*connOperation)(GFXDConnection*)) {
  SQLRETURN result = SQL_SUCCESS, res, lockResult;
  GFXDConnection* conn;

  SyncLock::Guard lock(m_connLock);
  if ((lockResult = lock.acquire()) != SQL_SUCCESS) {
    return lockResult;
  }

  for (std::vector<GFXDConnection*>::iterator iter = m_connections.begin();
      iter != m_connections.end(); ++iter) {
    conn = (*iter);
    if (conn->isActive()) {
      res = connOperation(conn);
      switch (res) {
        case SQL_SUCCESS:
          // no change to existing result
          break;
        case SQL_ERROR:
          result = SQL_ERROR;
          break;
        case SQL_SUCCESS_WITH_INFO:
          // change in existing result only if it is SQL_SUCCESS
          if (result == SQL_SUCCESS) {
            result = res;
          }
          break;
        default:
          if (result != SQL_ERROR) {
            result = res;
          }
          break;
      }
    }
  }
  return result;
}

SQLRETURN GFXDEnvironment::removeActiveConnection(GFXDConnection* conn) {
  SQLRETURN lockResult;

  SyncLock::Guard lock(m_connLock);
  if ((lockResult = lock.acquire()) != SQL_SUCCESS) {
    return lockResult;
  }

  for (std::vector<GFXDConnection*>::iterator iter = m_connections.begin();
      iter != m_connections.end(); ++iter) {
    if (conn == (*iter)) {
      m_connections.erase(iter);
      return SQL_SUCCESS;
    }
  }
  return SQL_NO_DATA;
}

SQLRETURN GFXDEnvironment::setAttribute(SQLINTEGER attribute, SQLPOINTER value,
    SQLINTEGER stringLength) {
  const SQLINTEGER intValue = (SQLINTEGER)(SQLBIGINT)value;
  switch (attribute) {
    case SQL_ATTR_ODBC_VERSION: {
      switch (intValue) {
        case SQL_OV_ODBC3:
          m_appIsVersion2x = false;
          break;
        case SQL_OV_ODBC2:
          m_appIsVersion2x = true;
          break;
        default:
          char buf[64];
          ::snprintf(buf, sizeof(buf) - 1,
              "setAttribute(SQL_ATTR_ODBC_VERSION=%d)", intValue);
          setException(
              GET_SQLEXCEPTION2(SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
                  buf));
          return SQL_ERROR;
      }
      break;
    }
    case SQL_ATTR_OUTPUT_NTS:
      if (intValue != SQL_TRUE) {
        setException(
            GET_SQLEXCEPTION2(SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
                "setAttribute(SQL_ATTR_OUTPUT_NTS==SQL_FALSE)"));
        return SQL_ERROR;
      }
      break;
    case SQL_ATTR_CONNECTION_POOLING:
      // should be handled by DriverManager
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
              "setAttribute(SQL_ATTR_CONNECTION_POOLING)"));
      return SQL_ERROR;
    case SQL_ATTR_CP_MATCH:
      // should be handled by DriverManager
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
              "setAttribute(SQL_ATTR_CP_MATCH)"));
      return SQL_ERROR;
    case SQL_ATTR_ANSI_APP:  //just to make sure ANSI APIs are supported
      break;
    default:
      // should be handled by DriverManager
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::UNKNOWN_ATTRIBUTE_MSG, attribute));
      return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDEnvironment::getAttribute(SQLINTEGER attribute,
    SQLPOINTER resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr) {
  SQLINTEGER* result = (SQLINTEGER*)resultValue;
  switch (attribute) {
    case SQL_ATTR_ODBC_VERSION:
      if (result != NULL) {
        *result = !m_appIsVersion2x ? SQL_OV_ODBC3 : SQL_OV_ODBC2;
        //*result = SQL_OV_ODBC3;
      }
      break;
    case SQL_ATTR_OUTPUT_NTS:
      if (result != NULL) {
        *result = SQL_TRUE;
      }
      break;
    case SQL_ATTR_CONNECTION_POOLING:
      // should be handled by DriverManager
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
              "getAttribute(SQL_ATTR_CONNECTION_POOLING)"));
      return SQL_ERROR;
    case SQL_ATTR_CP_MATCH:
      // should be handled by DriverManager
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
              "getAttribute(SQL_ATTR_CP_MATCH)"));
      return SQL_ERROR;
    default:
      // should be handled by DriverManager
      char buf[32];
      ::snprintf(buf, sizeof(buf) - 1, "getAttribute(%d)", attribute);
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
              buf));
      return SQL_ERROR;
  }
  if (stringLengthPtr != NULL) {
    *stringLengthPtr = sizeof(SQLINTEGER);
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDEnvironment::handleError(SQLSMALLINT handleType, SQLHANDLE handle,
    SQLSMALLINT recNumber, SQLCHAR* sqlState, SQLINTEGER* nativeError,
    SQLCHAR* messageText, SQLSMALLINT bufferLength, SQLSMALLINT* textLength) {
  return handleErrorT(handleType, handle, recNumber, sqlState, nativeError,
      messageText, bufferLength, textLength);
}

SQLRETURN GFXDEnvironment::handleError(SQLSMALLINT handleType, SQLHANDLE handle,
    SQLSMALLINT recNumber, SQLWCHAR* sqlState, SQLINTEGER* nativeError,
    SQLWCHAR* messageText, SQLSMALLINT bufferLength, SQLSMALLINT* textLength) {
  return handleErrorT(handleType, handle, recNumber, sqlState, nativeError,
      messageText, bufferLength, textLength);
}

SQLRETURN GFXDEnvironment::handleDiagField(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
    SQLPOINTER diagInfo, SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  return handleDiagFieldT(handleType, handle, recNumber, diagId, diagInfo,
      bufferLength, stringLength);
}

SQLRETURN GFXDEnvironment::handleDiagFieldW(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
    SQLPOINTER diagInfo, SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  return handleDiagFieldT(handleType, handle, recNumber, diagId, diagInfo,
      bufferLength, stringLength);
}
