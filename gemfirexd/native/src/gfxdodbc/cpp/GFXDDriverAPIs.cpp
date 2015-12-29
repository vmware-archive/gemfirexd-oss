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
 * GFXDDriverAPIs.cpp
 *
 *  Created on: Mar 26, 2013
 *      Author: shankarh
 */

#include "GFXDEnvironment.h"
#include "GFXDConnection.h"
#include "GFXDStatement.h"
#include "ConnStringPropertyReader.h"
#include "IniPropertyReader.h"
#include "GFXDLog.h"

char GFXDLog::logFile[MAX_PATH] = { };

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::impl;

/*
 * List of functions supported by the gemfirexd ODBC driver used
 * in SqlGetfunctions API
 */
SQLUSMALLINT gfxdSupportedfunctions[] = { SQL_API_SQLALLOCHANDLE,
                                          SQL_API_SQLALLOCSTMT,
                                          SQL_API_SQLBINDCOL,
                                          SQL_API_SQLCANCEL,
                                          SQL_API_SQLCLOSECURSOR,
                                          SQL_API_SQLCOLATTRIBUTE,
                                          SQL_API_SQLCOLUMNS,
                                          SQL_API_SQLCONNECT,
                                          SQL_API_SQLDISCONNECT,
                                          SQL_API_SQLENDTRAN,
                                          SQL_API_SQLEXECDIRECT,
                                          SQL_API_SQLEXECUTE,
                                          SQL_API_SQLFETCH,
                                          SQL_API_SQLFETCHSCROLL,
                                          SQL_API_SQLFREEHANDLE,
                                          SQL_API_SQLFREESTMT,
                                          SQL_API_SQLGETCONNECTATTR,
                                          SQL_API_SQLGETCURSORNAME,
                                          SQL_API_SQLGETDATA,
                                          SQL_API_SQLGETDIAGFIELD,
                                          SQL_API_SQLGETDIAGREC,
                                          SQL_API_SQLGETENVATTR,
                                          SQL_API_SQLGETFUNCTIONS,
                                          SQL_API_SQLGETINFO,
                                          SQL_API_SQLGETSTMTATTR,
                                          SQL_API_SQLGETTYPEINFO,
                                          SQL_API_SQLNUMRESULTCOLS,
                                          SQL_API_SQLPARAMDATA,
                                          SQL_API_SQLPREPARE,
                                          SQL_API_SQLPUTDATA,
                                          SQL_API_SQLROWCOUNT,
                                          SQL_API_SQLSETCONNECTATTR,
                                          SQL_API_SQLSETCURSORNAME,
                                          SQL_API_SQLSETENVATTR,
                                          SQL_API_SQLSETSTMTATTR,
                                          SQL_API_SQLSPECIALCOLUMNS,
                                          SQL_API_SQLSTATISTICS,
                                          SQL_API_SQLTABLES,
                                          SQL_API_SQLBULKOPERATIONS,
                                          SQL_API_SQLBINDPARAMETER,
                                          SQL_API_SQLBROWSECONNECT,
                                          SQL_API_SQLCOLUMNPRIVILEGES,
                                          SQL_API_SQLDRIVERCONNECT,
                                          SQL_API_SQLFOREIGNKEYS,
                                          SQL_API_SQLMORERESULTS,
                                          SQL_API_SQLNATIVESQL,
                                          SQL_API_SQLNUMPARAMS,
                                          SQL_API_SQLPRIMARYKEYS,
                                          SQL_API_SQLPROCEDURECOLUMNS,
                                          SQL_API_SQLPROCEDURES,
                                          SQL_API_SQLSETPOS,
                                          SQL_API_SQLTABLEPRIVILEGES,
                                          SQL_API_SQLDESCRIBECOL,
                                          SQL_API_SQLDESCRIBEPARAM };

///////////////////////////////////////////////////////////////////////////////
////               Environment APIS
///////////////////////////////////////////////////////////////////////////////

SQLRETURN DLLPUBLIC SQL_API SQLAllocHandle(SQLSMALLINT handleType,
    SQLHANDLE inputHandle, SQLHANDLE* outputHandle) {
  FUNCTION_TRACER
  SQLRETURN result;
  switch (handleType) {
    case SQL_HANDLE_ENV: {
      if (outputHandle != NULL && inputHandle == NULL) {
        GFXDEnvironment* env;
        result = GFXDEnvironment::newEnvironment(env);
        *outputHandle = env;
        return result;
      }
      return GFXDHandleBase::errorNullHandle(SQL_HANDLE_ENV);
    }
    case SQL_HANDLE_DBC: {
      GFXDEnvironment* env = NULL;
      if (outputHandle != NULL && inputHandle != NULL) {
        GFXDEnvironment* env = (GFXDEnvironment*)inputHandle;
        GFXDConnection* conn;
        result = GFXDConnection::newConnection(env, conn);
        *outputHandle = conn;
        return result;
      }
      return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC, env);
    }
    case SQL_HANDLE_STMT: {
      GFXDConnection* conn = NULL;
      if (outputHandle != NULL && inputHandle != NULL) {
        GFXDConnection* conn = (GFXDConnection*)inputHandle;
        if (conn != NULL && conn->isActive()) {
          GFXDStatement* stmt;
          result = GFXDStatement::newStatement(conn, stmt);
          *outputHandle = stmt;
          return result;
        } else {
          GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
              SQLStateMessage::NO_CURRENT_CONNECTION_MSG));
          return SQL_ERROR;
        }
      }
      return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT, conn);
    }
    case SQL_HANDLE_DESC:
      //return impl::gfxdAllocDesc(inputHandle, outputHandle);

    default:
      GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_HANDLE_TYPE_MSG, handleType));
      return SQL_ERROR;
  }
}

SQLRETURN DLLPUBLIC SQL_API SQLFreeHandle(SQLSMALLINT handleType,
    SQLHANDLE handle) {
  FUNCTION_TRACER
  if (handle != NULL) {
    switch (handleType) {
      case SQL_HANDLE_ENV:
        return GFXDEnvironment::freeEnvironment((GFXDEnvironment*)handle);

      case SQL_HANDLE_DBC:
        return GFXDConnection::freeConnection((GFXDConnection*)handle);

      case SQL_HANDLE_STMT:
        return GFXDStatement::freeStatement((GFXDStatement*)handle, SQL_DROP);

      case SQL_HANDLE_DESC:
        //return impl::gfxdFreeDesc(handle);

      default:
        GFXDHandleBase::setGlobalException(GET_SQLEXCEPTION2(
            SQLStateMessage::INVALID_HANDLE_TYPE_MSG, handleType));
        return SQL_ERROR;
    }
  } else {
    return GFXDHandleBase::errorNullHandle(handleType);
  }
}

SQLRETURN DLLPUBLIC SQL_API SQLGetDiagRec(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, SQLCHAR* sqlState,
    SQLINTEGER* nativeError, SQLCHAR* messageText, SQLSMALLINT bufferLength,
    SQLSMALLINT* textLength) {
  FUNCTION_TRACER
  if (handle != NULL) {
    return GFXDEnvironment::handleError(handleType, handle, recNumber, sqlState,
        nativeError, messageText, bufferLength, textLength);
  }
  return SQL_INVALID_HANDLE;
}

SQLRETURN DLLPUBLIC SQL_API SQLGetDiagRecW(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, SQLWCHAR* sqlState,
    SQLINTEGER* nativeError, SQLWCHAR* messageText, SQLSMALLINT bufferLength,
    SQLSMALLINT* textLength) {
  FUNCTION_TRACER
  if (handle != NULL) {
    return GFXDEnvironment::handleError(handleType, handle, recNumber, sqlState,
        nativeError, messageText, bufferLength, textLength);
  }
  return SQL_INVALID_HANDLE;
}

SQLRETURN DLLPUBLIC SQL_API SQLGetDiagField(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
    SQLPOINTER diagInfo, SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  FUNCTION_TRACER
  if (handle != NULL) {
    return GFXDEnvironment::handleDiagField(handleType, handle, recNumber,
        diagId, diagInfo, bufferLength, stringLength);
  }
  return SQL_INVALID_HANDLE;
}

SQLRETURN DLLPUBLIC SQL_API SQLGetDiagFieldW(SQLSMALLINT handleType,
    SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
    SQLPOINTER diagInfo, SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  FUNCTION_TRACER
  if (handle != NULL) {
    return GFXDEnvironment::handleDiagFieldW(handleType, handle, recNumber,
        diagId, diagInfo, bufferLength, stringLength);
  }
  return SQL_INVALID_HANDLE;
}

SQLRETURN DLLPUBLIC SQL_API SQLSetEnvAttr(SQLHENV envHandle,
    SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER stringLength) {
  FUNCTION_TRACER
  if (envHandle != NULL) {
    return ((GFXDEnvironment*)envHandle)->setAttribute(attribute, value,
        stringLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_ENV);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetEnvAttr(SQLHENV envHandle,
    SQLINTEGER attribute, SQLPOINTER resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr) {
  FUNCTION_TRACER
  if (envHandle != NULL) {
    return ((GFXDEnvironment*)envHandle)->getAttribute(attribute, resultValue,
        bufferLength, stringLengthPtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_ENV);
}

///////////////////////////////////////////////////////////////////////////////
////               Connection APIS
///////////////////////////////////////////////////////////////////////////////

SQLRETURN DLLPUBLIC SQL_API SQLGetFunctions(SQLHDBC hdbc,
    SQLUSMALLINT fFunction, SQLUSMALLINT *pfExists) {
  FUNCTION_TRACER
  SQLUSMALLINT supported_func_size;
  int index;

  supported_func_size = sizeof(gfxdSupportedfunctions)
      / sizeof(gfxdSupportedfunctions[0]);

  if (fFunction == SQL_API_ODBC3_ALL_FUNCTIONS) {
    // Clear and set bits in the 4000 bit vector
    memset(pfExists, 0,
        sizeof(SQLUSMALLINT) * SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);
    for (index = 0; index < supported_func_size; ++index) {
      SQLUSMALLINT id = gfxdSupportedfunctions[index];
      pfExists[id >> 4] |= (1 << (id & 0x000F));
    }
    return SQL_SUCCESS;
  }

  if (fFunction == SQL_API_ALL_FUNCTIONS) {
    // Clear and set elements in the SQLUSMALLINT 100 element array
    memset(pfExists, 0, sizeof(SQLUSMALLINT) * 100);
    for (index = 0; index < supported_func_size; ++index) {
      if (gfxdSupportedfunctions[index] < 100) {
        pfExists[gfxdSupportedfunctions[index]] = SQL_TRUE;
      }
    }
    return SQL_SUCCESS;
  }

  *pfExists = SQL_FALSE;
  for (index = 0; index < supported_func_size; ++index) {
    if (gfxdSupportedfunctions[index] == fFunction) {
      *pfExists = SQL_TRUE;
      break;
    }
  }
  return SQL_SUCCESS;
}

SQLRETURN DLLPUBLIC SQL_API SQLConnect(SQLHDBC connHandle, SQLCHAR* dsn,
    SQLSMALLINT dsnLen, SQLCHAR* userName, SQLSMALLINT userNameLen,
    SQLCHAR* passwd, SQLSMALLINT passwdLen) {
  FUNCTION_TRACER
  GFXDConnection* conn = (GFXDConnection*)connHandle;
  if (conn != NULL) {
    std::string server;
    int port;
    // the Properties object to be passed to the underlying Connection
    Properties connProps;

    // lookup the data source name and get all the properties'
    if (dsn == NULL || *dsn == 0 || dsnLen == 0) {
      dsn = (SQLCHAR*)GFXDDefaults::DEFAULT_DSN;
      dsnLen = SQL_NTS;
    }
    ArrayIterator<std::string> allPropNames(OdbcIniKeys::ALL_PROPERTIES,
        OdbcIniKeys::NUM_ALL_PROPERTIES);
    IniPropertyReader<SQLCHAR> propReader;
    const SQLRETURN result = readProperties<SQLCHAR>(&propReader, dsn, dsnLen,
        &allPropNames, userName, userNameLen, passwd, passwdLen, server, port,
        connProps, conn);
    if (result != SQL_ERROR) {
      return conn->connect(server, port, connProps, (SQLCHAR*)NULL, -1, NULL);
    }
    return result;
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLConnectW(SQLHDBC connHandle, SQLWCHAR* dsn,
    SQLSMALLINT dsnLen, SQLWCHAR* userName, SQLSMALLINT userNameLen,
    SQLWCHAR* passwd, SQLSMALLINT passwdLen) {
  FUNCTION_TRACER
  GFXDConnection* conn = (GFXDConnection*)connHandle;
  if (conn != NULL) {
    std::string server;
    int port;
    // the Properties object to be passed to the underlying Connection
    Properties connProps;

    // lookup the data source name and get all the properties'
    if (dsn == NULL || *dsn == 0 || dsnLen == 0) {
      dsn = (SQLWCHAR*)GFXDDefaults::DEFAULT_DSNW;
      dsnLen = SQL_NTS;
    }
    ArrayIterator<std::string> allPropNames(OdbcIniKeys::ALL_PROPERTIES,
        OdbcIniKeys::NUM_ALL_PROPERTIES);
    IniPropertyReader<SQLWCHAR> propReader;
    const SQLRETURN result = readProperties(&propReader, dsn, dsnLen,
        &allPropNames, userName, userNameLen, passwd, passwdLen, server, port,
        connProps, conn);
    if (result != SQL_ERROR) {
      return conn->connect(server, port, connProps, (SQLCHAR*)NULL, -1, NULL);
    }
    return result;
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLDriverConnect(SQLHDBC connHandle,
    SQLHWND windowHandle, SQLCHAR* connStr, SQLSMALLINT connStrLen,
    SQLCHAR* outConnStr, SQLSMALLINT outConnStrSize, SQLSMALLINT* outConnStrLen,
    SQLUSMALLINT completionFlag) {
  FUNCTION_TRACER
  if (completionFlag != SQL_DRIVER_PROMPT) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    if (conn != NULL) {
      std::string server;
      int port;
      // the Properties object to be passed to the underlying Connection
      Properties connProps;
      if (connStrLen <= 0 && connStrLen != SQL_NTS) {
        return GFXDHandleBase::errorInvalidBufferLength(connStrLen,
            "Connection string length", conn);
      }
      // split the given connection string to obtain the server, port and other
      // attributes
      ArrayIterator<std::string> allPropNames(OdbcIniKeys::ALL_PROPERTIES,
          OdbcIniKeys::NUM_ALL_PROPERTIES);
      ConnStringPropertyReader<SQLCHAR> connStrReader;
      SQLRETURN result = readProperties<SQLCHAR>(&connStrReader, connStr,
          connStrLen, &allPropNames, NULL, -1, NULL, -1, server, port,
          connProps, conn);
      if (result != SQL_ERROR) {
        result = conn->connect(server, port, connProps, outConnStr,
            outConnStrSize, outConnStrLen);
      }
      return result;
    }
    return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
  }
  return GFXDHandleBase::errorNotImplemented("SQLDriverConnect with prompt");
}

SQLRETURN DLLPUBLIC SQL_API SQLDriverConnectW(SQLHDBC connHandle,
    SQLHWND windowHandle, SQLWCHAR* connStr, SQLSMALLINT connStrLen,
    SQLWCHAR* outConnStr, SQLSMALLINT outConnStrSize,
    SQLSMALLINT* outConnStrLen, SQLUSMALLINT completionFlag) {
  FUNCTION_TRACER
  if (completionFlag != SQL_DRIVER_PROMPT) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    if (conn != NULL) {
      std::string server;
      int port;
      // the Properties object to be passed to the underlying Connection
      Properties connProps;

      if (connStrLen <= 0 && connStrLen != SQL_NTS) {
        return GFXDHandleBase::errorInvalidBufferLength(connStrLen,
            "Connection string length", conn);
      }
      // split the given connection string to obtain the server, port and other
      // attributes
      ArrayIterator<std::string> allPropNames(OdbcIniKeys::ALL_PROPERTIES,
          OdbcIniKeys::NUM_ALL_PROPERTIES);
      ConnStringPropertyReader<SQLWCHAR> connStrReader;
      SQLRETURN result = readProperties(&connStrReader, connStr, connStrLen,
          &allPropNames, (const SQLWCHAR*)NULL, -1, (const SQLWCHAR*)NULL, -1,
          server, port, connProps, conn);
      if (result != SQL_ERROR) {
        return conn->connect(server, port, connProps, outConnStr,
            outConnStrSize, outConnStrLen);
      }
      return result;
    }
    return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
  }
  return GFXDHandleBase::errorNotImplemented("SQLDriverConnect with prompt");
}

SQLRETURN DLLPUBLIC SQL_API SQLBrowseConnect(SQLHDBC connHandle,
    SQLCHAR* connStr, SQLSMALLINT connStrLen, SQLCHAR* outConnStr,
    SQLSMALLINT outConnStrMaxLen, SQLSMALLINT* outConnStrLen) {
  return GFXDHandleBase::errorNotImplemented("SQLBrowseConnect");
}

SQLRETURN DLLPUBLIC SQL_API SQLDisconnect(SQLHDBC connHandle) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->disconnect();
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLEndTran(SQLSMALLINT handleType, SQLHANDLE handle,
    SQLSMALLINT completionType) {
  FUNCTION_TRACER
  if (handle != NULL) {
    if (handleType == SQL_HANDLE_DBC) {
      GFXDConnection* conn = (GFXDConnection*)handle;
      if (!conn->getEnvironment()->isShared()) {
        switch (completionType) {
          case SQL_COMMIT:
            return conn->commit();
          case SQL_ROLLBACK:
            return conn->rollback();
          default:
            ((GFXDConnection*)handle)->setException(GET_SQLEXCEPTION2(
                SQLStateMessage::INVALID_TRANSACTION_OPERATION_MSG1,
                "completionType"));
            return SQL_ERROR;
        }
      } else {
        ((GFXDConnection*)handle)->setException(GET_SQLEXCEPTION2(
            SQLStateMessage::INVALID_TRANSACTION_OPERATION_MSG2));
        return SQL_ERROR;
      }
    } else if (handleType == SQL_HANDLE_ENV) {
      GFXDEnvironment* env = (GFXDEnvironment*)handle;
      if (!env->isShared()) {
        SQLRETURN (*connOperation)(GFXDConnection* conn);
        switch (completionType) {
          case SQL_COMMIT:
            connOperation = ::gfxdCommit;
            break;
          case SQL_ROLLBACK:
            connOperation = ::gfxdRollback;
            break;
          default:
            ((GFXDConnection*)handle)->setException(
                GET_SQLEXCEPTION2(
                    SQLStateMessage::INVALID_TRANSACTION_OPERATION_MSG1,
                    "completionType"));
            return SQL_ERROR;
        }
        return env->forEachActiveConnection(connOperation);
      } else {
        ((GFXDConnection*)handle)->setException(GET_SQLEXCEPTION2(
            SQLStateMessage::INVALID_TRANSACTION_OPERATION_MSG2));
        return SQL_ERROR;
      }
    } else {
      ((GFXDHandleBase*)handle)->setException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_HANDLE_TYPE_MSG, handleType));
      return SQL_ERROR;
    }
  }
  return GFXDHandleBase::errorNullHandle(handleType);
}

SQLRETURN DLLPUBLIC SQL_API SQLSetConnectAttr(SQLHDBC connHandle,
    SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER stringLength) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->setAttribute(attribute, value, stringLength, true);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLSetConnectAttrW(SQLHDBC connHandle,
    SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER stringLength) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->setAttribute(attribute, value, stringLength, false);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetConnectAttr(SQLHDBC connHandle,
    SQLINTEGER attribute, SQLPOINTER resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->getAttribute(attribute, resultValue, bufferLength,
        stringLengthPtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetConnectAttrW(SQLHDBC connHandle,
    SQLINTEGER attribute, SQLPOINTER resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->getAttributeW(attribute, resultValue, bufferLength,
        stringLengthPtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetInfo(SQLHDBC connHandle,
    SQLUSMALLINT infoType, SQLPOINTER infoValue, SQLSMALLINT bufferLength,
    SQLSMALLINT* stringLength) {
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->getInfo(infoType, infoValue, bufferLength, stringLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetInfoW(SQLHDBC connHandle,
    SQLUSMALLINT infoType, SQLPOINTER infoValue, SQLSMALLINT bufferLength,
    SQLSMALLINT* stringLength) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->getInfoW(infoType, infoValue, bufferLength, stringLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLNativeSql(SQLHDBC connHandle,
    SQLCHAR* inStatementText, SQLINTEGER textLength1, SQLCHAR* outStatementText,
    SQLINTEGER bufferLength, SQLINTEGER* textLength2Ptr) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->nativeSQL(inStatementText, textLength1, outStatementText,
        bufferLength, textLength2Ptr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

SQLRETURN DLLPUBLIC SQL_API SQLNativeSqlW(SQLHDBC connHandle,
    SQLWCHAR* inStatementText, SQLINTEGER textLength1,
    SQLWCHAR* outStatementText, SQLINTEGER bufferLength,
    SQLINTEGER* textLength2Ptr) {
  FUNCTION_TRACER
  if (connHandle != NULL) {
    GFXDConnection* conn = (GFXDConnection*)connHandle;
    return conn->nativeSQLW(inStatementText, textLength1, outStatementText,
        bufferLength, textLength2Ptr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
}

///////////////////////////////////////////////////////////////////////////////
////               Statement APIs
///////////////////////////////////////////////////////////////////////////////

SQLRETURN DLLPUBLIC SQL_API SQLBindParameter(SQLHSTMT stmtHandle,
    SQLUSMALLINT paramNum, SQLSMALLINT inputOutputType, SQLSMALLINT valueType,
    SQLSMALLINT paramType, SQLULEN precision, SQLSMALLINT scale,
    SQLPOINTER paramValue, SQLLEN valueSize, SQLLEN *lenOrIndPtr) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->addParameter(paramNum, inputOutputType,
        valueType, paramType, precision, scale, paramValue, valueSize,
        lenOrIndPtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLPrepare(SQLHSTMT stmtHandle, SQLCHAR *stmtText,
    SQLINTEGER textLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->prepare(stmtText, textLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLPrepareW(SQLHSTMT stmtHandle, SQLWCHAR *stmtText,
    SQLINTEGER textLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->prepare(stmtText, textLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLExecDirect(SQLHSTMT stmtHandle,
    SQLCHAR* stmtText, SQLINTEGER textLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->execute(stmtText, textLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLExecDirectW(SQLHSTMT stmtHandle,
    SQLWCHAR* stmtText, SQLINTEGER textLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->execute(stmtText, textLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLExecute(SQLHSTMT stmtHandle) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->execute();
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLBindCol(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNum, SQLSMALLINT targetType, SQLPOINTER targetValue,
    SQLLEN valueSize, SQLLEN *lenOrIndPtr) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->bindOutputField(columnNum, targetType,
        targetValue, valueSize, lenOrIndPtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLRowCount(SQLHSTMT stmtHandle, SQLLEN* count) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    if (count != NULL) {
      return ((GFXDStatement*)stmtHandle)->getUpdateCount(count);
    }
    return GFXDHandleBase::errorNullHandle(SQL_PARAM_ERROR);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLFetch(SQLHSTMT stmtHandle) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    // TODO: what do docs say about multiple rowsets?
    return ((GFXDStatement*)stmtHandle)->next();
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLFetchScroll(SQLHSTMT stmtHandle,
    SQLSMALLINT fetchOrientation, SQLLEN fetchOffset) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->fetchScroll(fetchOrientation,
        fetchOffset);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLSetPos(SQLHSTMT stmtHandle,
    SQLSETPOSIROW rowNumber, SQLUSMALLINT operation, SQLUSMALLINT lockType) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->setPos(rowNumber, operation, lockType);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetData(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNum, SQLSMALLINT targetType, SQLPOINTER targetValue,
    SQLLEN valueSize, SQLLEN *lenOrIndPtr) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getData(columnNum, targetType,
        targetValue, valueSize, lenOrIndPtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLCloseCursor(SQLHSTMT stmtHandle) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->closeResultSet();
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLCancel(SQLHSTMT stmtHandle) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->cancel();
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

// ODBC 3.8
extern "C"
{
  SQLRETURN SQL_API SQLCancelHandle(SQLSMALLINT handleType, SQLHANDLE handle);
}

SQLRETURN DLLPUBLIC SQL_API SQLCancelHandle(SQLSMALLINT handleType,
    SQLHANDLE handle) {
  FUNCTION_TRACER
  if (handle != NULL) {
    if (handleType == SQL_HANDLE_STMT) {
      return ((GFXDStatement*)handle)->cancel();
    } else if (handleType == SQL_HANDLE_DBC) {
      ((GFXDConnection*)handle)->setException(GET_SQLEXCEPTION2(
          SQLStateMessage::FUNCTION_NOT_SUPPORTED_MSG,
          "SQLCancelHandle(SQL_HANDLE_DBC)"));
      return SQL_ERROR;
    } else {
      ((GFXDHandleBase*)handle)->setException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_HANDLE_TYPE_MSG, handleType));
      return SQL_ERROR;
    }
  }
  return GFXDHandleBase::errorNullHandle(handleType);
}

SQLRETURN DLLPUBLIC SQL_API SQLFreeStmt(SQLHSTMT stmtHandle,
    SQLUSMALLINT option) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return GFXDStatement::freeStatement((GFXDStatement*)stmtHandle, option);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetStmtAttr(SQLHSTMT stmtHandle,
    SQLINTEGER attribute, SQLPOINTER valueBuffer, SQLINTEGER bufferLen,
    SQLINTEGER* valueLen) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getAttribute(attribute, valueBuffer,
        bufferLen, valueLen);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetStmtAttrW(SQLHSTMT stmtHandle,
    SQLINTEGER attribute, SQLPOINTER valueBuffer, SQLINTEGER bufferLen,
    SQLINTEGER* valueLen) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getAttributeW(attribute, valueBuffer,
        bufferLen, valueLen);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLSetStmtAttr(SQLHSTMT stmtHandle,
    SQLINTEGER attribute, SQLPOINTER valueBuffer, SQLINTEGER valueLen) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->setAttribute(attribute, valueBuffer,
        valueLen);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLSetStmtAttrW(SQLHSTMT stmtHandle,
    SQLINTEGER attribute, SQLPOINTER valueBuffer, SQLINTEGER valueLen) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->setAttributeW(attribute, valueBuffer,
        valueLen);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLDescribeCol(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNumber, SQLCHAR* columnName, SQLSMALLINT bufferLength,
    SQLSMALLINT* nameLength, SQLSMALLINT* dataType, SQLULEN* columnSize,
    SQLSMALLINT* decimalDigits, SQLSMALLINT* nullable) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getResultColumnDescriptor(columnNumber,
        columnName, bufferLength, nameLength, dataType, columnSize,
        decimalDigits, nullable);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLDescribeColW(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNumber, SQLWCHAR* columnName, SQLSMALLINT bufferLength,
    SQLSMALLINT* nameLength, SQLSMALLINT* dataType, SQLULEN* columnSize,
    SQLSMALLINT* decimalDigits, SQLSMALLINT* nullable) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getResultColumnDescriptor(columnNumber,
        columnName, bufferLength, nameLength, dataType, columnSize,
        decimalDigits, nullable);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLDescribeParam(SQLHSTMT stmtHandle,
    SQLUSMALLINT paramNumber, SQLSMALLINT * patamDataTypePtr,
    SQLULEN * paramSizePtr, SQLSMALLINT * decimalDigitsPtr,
    SQLSMALLINT * nullablePtr) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getParamMetadata(paramNumber,
        patamDataTypePtr, paramSizePtr, decimalDigitsPtr, nullablePtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

#if defined(WIN32) || defined(_WIN32)
SQLRETURN DLLPUBLIC SQL_API SQLColAttribute(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNumber, SQLUSMALLINT fieldId, SQLPOINTER charAttribute,
    SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
    SQLPOINTER numericAttribute) {
#else
SQLRETURN DLLPUBLIC SQL_API SQLColAttribute(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNumber, SQLUSMALLINT fieldId, SQLPOINTER charAttribute,
    SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
    SQLLEN* numericAttribute) {
#endif
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getColumnAttribute(columnNumber,
        fieldId, charAttribute, bufferLength, stringLength,
        (SQLLEN*)numericAttribute);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

#if defined(WIN32) || defined(_WIN32)
SQLRETURN DLLPUBLIC SQL_API SQLColAttributeW(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNumber, SQLUSMALLINT fieldId, SQLPOINTER charAttribute,
    SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
    SQLPOINTER numericAttribute) {
#else
SQLRETURN DLLPUBLIC SQL_API SQLColAttributeW(SQLHSTMT stmtHandle,
    SQLUSMALLINT columnNumber, SQLUSMALLINT fieldId, SQLPOINTER charAttribute,
    SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
    SQLLEN* numericAttribute) {
#endif
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getColumnAttributeW(columnNumber,
        fieldId, charAttribute, bufferLength, stringLength,
        (SQLLEN*)numericAttribute);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetCursorName(SQLHSTMT stmtHandle,
    SQLCHAR* cursorName, SQLSMALLINT bufferLength, SQLSMALLINT* nameLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getCursorName(cursorName, bufferLength,
        nameLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetCursorNameW(SQLHSTMT stmtHandle,
    SQLWCHAR* cursorName, SQLSMALLINT bufferLength, SQLSMALLINT* nameLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getCursorName(cursorName, bufferLength,
        nameLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLSetCursorName(SQLHSTMT stmtHandle,
    SQLCHAR* cursorName, SQLSMALLINT nameLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->setCursorName(cursorName, nameLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLSetCursorNameW(SQLHSTMT stmtHandle,
    SQLWCHAR* cursorName, SQLSMALLINT nameLength) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->setCursorName(cursorName, nameLength);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLNumResultCols(SQLHSTMT stmtHandle,
    SQLSMALLINT* columnCount) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getNumResultColumns(columnCount);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLNumParams(SQLHSTMT stmtHandle,
    SQLSMALLINT* parameterCount) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getNumParameters(parameterCount);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLTables(SQLHSTMT stmtHandle, SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3, SQLCHAR* tableTypes,
    SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getTables(catalogName, nameLength1,
        schemaName, nameLength2, tableName, nameLength3, tableTypes,
        nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLTablesW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* tableName, SQLSMALLINT nameLength3,
    SQLWCHAR* tableTypes, SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getTables(catalogName, nameLength1,
        schemaName, nameLength2, tableName, nameLength3, tableTypes,
        nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLTablePrivileges(SQLHSTMT stmtHandle,
    SQLCHAR* catalogName, SQLSMALLINT nameLength1, SQLCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLCHAR* tableName, SQLSMALLINT nameLength3) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getTablePrivileges(catalogName,
        nameLength1, schemaName, nameLength2, tableName, nameLength3);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLTablePrivilegesW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* tableName, SQLSMALLINT nameLength3) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getTablePrivileges(catalogName,
        nameLength1, schemaName, nameLength2, tableName, nameLength3);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLColumns(SQLHSTMT stmtHandle,
    SQLCHAR* catalogName, SQLSMALLINT nameLength1, SQLCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLCHAR* tableName, SQLSMALLINT nameLength3,
    SQLCHAR* columnName, SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getColumns(catalogName, nameLength1,
        schemaName, nameLength2, tableName, nameLength3, columnName,
        nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLColumnsW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* tableName, SQLSMALLINT nameLength3,
    SQLWCHAR* columnName, SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getColumns(catalogName, nameLength1,
        schemaName, nameLength2, tableName, nameLength3, columnName,
        nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLColumnPrivileges(SQLHSTMT stmtHandle,
    SQLCHAR* catalogName, SQLSMALLINT nameLength1, SQLCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLCHAR* tableName, SQLSMALLINT nameLength3,
    SQLCHAR* columnName, SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getColumnPrivileges(catalogName,
        nameLength1, schemaName, nameLength2, tableName, nameLength3,
        columnName, nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLColumnPrivilegesW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* tableName, SQLSMALLINT nameLength3,
    SQLWCHAR* columnName, SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getColumnPrivileges(catalogName,
        nameLength1, schemaName, nameLength2, tableName, nameLength3,
        columnName, nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLStatistics(SQLHSTMT stmtHandle,
    SQLCHAR* catalogName, SQLSMALLINT nameLength1, SQLCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLCHAR* tableName, SQLSMALLINT nameLength3,
    SQLUSMALLINT unique, SQLUSMALLINT reserved) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getIndexInfo(catalogName, nameLength1,
        schemaName, nameLength2, tableName, nameLength3,
        unique == SQL_INDEX_UNIQUE, reserved == SQL_QUICK);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLStatisticsW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* tableName, SQLSMALLINT nameLength3,
    SQLUSMALLINT unique, SQLUSMALLINT reserved) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getIndexInfo(catalogName, nameLength1,
        schemaName, nameLength2, tableName, nameLength3,
        unique == SQL_INDEX_UNIQUE, reserved == SQL_QUICK);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLPrimaryKeys(SQLHSTMT stmtHandle,
    SQLCHAR* catalogName, SQLSMALLINT nameLength1, SQLCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLCHAR* tableName, SQLSMALLINT nameLength3) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getPrimaryKeys(catalogName,
        nameLength1, schemaName, nameLength2, tableName, nameLength3);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLPrimaryKeysW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* tableName, SQLSMALLINT nameLength3) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getPrimaryKeys(catalogName,
        nameLength1, schemaName, nameLength2, tableName, nameLength3);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLForeignKeys(SQLHSTMT stmtHandle,
    SQLCHAR* pkCatalogName, SQLSMALLINT nameLength1, SQLCHAR* pkSchemaName,
    SQLSMALLINT nameLength2, SQLCHAR* pkTableName, SQLSMALLINT nameLength3,
    SQLCHAR* fkCatalogName, SQLSMALLINT nameLength4, SQLCHAR* fkSchemaName,
    SQLSMALLINT nameLength5, SQLCHAR* fkTableName, SQLSMALLINT nameLength6) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    GFXDStatement* stmt = (GFXDStatement*)stmtHandle;
    if (pkTableName != NULL) {
      if (fkTableName != NULL) {
        return stmt->getCrossReference(pkCatalogName, nameLength1, pkSchemaName,
            nameLength2, pkTableName, nameLength3, fkCatalogName, nameLength4,
            fkSchemaName, nameLength5, fkTableName, nameLength6);
      } else {
        return stmt->getExportedKeys(pkCatalogName, nameLength1, pkSchemaName,
            nameLength2, pkTableName, nameLength3);
      }
    } else {
      // both NULL should never happen since DriverManager is supposed to
      // handle it
      return stmt->getImportedKeys(fkCatalogName, nameLength1, fkSchemaName,
          nameLength2, fkTableName, nameLength3);
    }
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLForeignKeysW(SQLHSTMT stmtHandle,
    SQLWCHAR* pkCatalogName, SQLSMALLINT nameLength1, SQLWCHAR* pkSchemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* pkTableName, SQLSMALLINT nameLength3,
    SQLWCHAR* fkCatalogName, SQLSMALLINT nameLength4, SQLWCHAR* fkSchemaName,
    SQLSMALLINT nameLength5, SQLWCHAR* fkTableName, SQLSMALLINT nameLength6) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    GFXDStatement* stmt = (GFXDStatement*)stmtHandle;
    if (pkTableName != NULL) {
      if (fkTableName != NULL) {
        return stmt->getCrossReference(pkCatalogName, nameLength1, pkSchemaName,
            nameLength2, pkTableName, nameLength3, fkCatalogName, nameLength4,
            fkSchemaName, nameLength5, fkTableName, nameLength6);
      } else {
        return stmt->getExportedKeys(pkCatalogName, nameLength1, pkSchemaName,
            nameLength2, pkTableName, nameLength3);
      }
    } else {
      // both NULL should never happen since DriverManager is supposed to
      // handle it
      return stmt->getImportedKeys(fkCatalogName, nameLength1, fkSchemaName,
          nameLength2, fkTableName, nameLength3);
    }
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLProcedures(SQLHSTMT stmtHandle,
    SQLCHAR* catalogName, SQLSMALLINT nameLength1, SQLCHAR* schemaPattern,
    SQLSMALLINT nameLength2, SQLCHAR* procedureNamePattern,
    SQLSMALLINT nameLength3) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getProcedures(catalogName, nameLength1,
        schemaPattern, nameLength2, procedureNamePattern, nameLength3);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLProceduresW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaPattern,
    SQLSMALLINT nameLength2, SQLWCHAR* procedureNamePattern,
    SQLSMALLINT nameLength3) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getProcedures(catalogName, nameLength1,
        schemaPattern, nameLength2, procedureNamePattern, nameLength3);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLProcedureColumns(SQLHSTMT stmtHandle,
    SQLCHAR* catalogName, SQLSMALLINT nameLength1, SQLCHAR* schemaPattern,
    SQLSMALLINT nameLength2, SQLCHAR* procedureNamePattern,
    SQLSMALLINT nameLength3, SQLCHAR* columnNamePattern,
    SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getProcedureColumns(catalogName,
        nameLength1, schemaPattern, nameLength2, procedureNamePattern,
        nameLength3, columnNamePattern, nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLProcedureColumnsW(SQLHSTMT stmtHandle,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaPattern,
    SQLSMALLINT nameLength2, SQLWCHAR* procedureNamePattern,
    SQLSMALLINT nameLength3, SQLWCHAR* columnNamePattern,
    SQLSMALLINT nameLength4) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getProcedureColumns(catalogName,
        nameLength1, schemaPattern, nameLength2, procedureNamePattern,
        nameLength3, columnNamePattern, nameLength4);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetTypeInfo(SQLHSTMT stmtHandle,
    SQLSMALLINT dataType) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getTypeInfo(dataType);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLGetTypeInfoW(SQLHSTMT stmtHandle,
    SQLSMALLINT dataType) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getTypeInfoW(dataType);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLBulkOperations(SQLHSTMT stmtHandle,
    SQLSMALLINT operation) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->bulkOperations(operation);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLPutData(SQLHSTMT stmtHandle, SQLPOINTER dataPtr,
    SQLLEN strLen) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->putData(dataPtr, strLen);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLMoreResults(SQLHSTMT stmtHandle) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getMoreResults();
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLParamData(SQLHSTMT stmtHandle,
    SQLPOINTER* valuePtr) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getParamData(valuePtr);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLSpecialColumns(SQLHSTMT stmtHandle,
    SQLUSMALLINT identifierType, SQLCHAR *catlogName, SQLSMALLINT nameLength1,
    SQLCHAR *schemaName, SQLSMALLINT nameLength2, SQLCHAR *tableName,
    SQLSMALLINT nameLength3, SQLUSMALLINT scope, SQLUSMALLINT nullable) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getSpecialColumns(identifierType,
        catlogName, nameLength1, schemaName, nameLength2, tableName,
        nameLength3, scope, nullable);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}

SQLRETURN DLLPUBLIC SQL_API SQLSpecialColumnsW(SQLHSTMT stmtHandle,
    SQLUSMALLINT identifierType, SQLWCHAR* catalogName, SQLSMALLINT nameLength1,
    SQLWCHAR* schemaName, SQLSMALLINT nameLength2, SQLWCHAR* tableName,
    SQLSMALLINT nameLength3, SQLUSMALLINT scope, SQLUSMALLINT nullable) {
  FUNCTION_TRACER
  if (stmtHandle != NULL) {
    return ((GFXDStatement*)stmtHandle)->getSpecialColumns(identifierType,
        catalogName, nameLength1, schemaName, nameLength2, tableName,
        nameLength3, scope, nullable);
  }
  return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
}
