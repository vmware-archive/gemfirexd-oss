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
 * GFXDConnection.cpp
 *
 *  Contains implementation of setup and connection ODBC API.
 *
 *      Author: swale
 */

#include "GFXDEnvironment.h"
#include "GFXDConnection.h"
#include "StringFunctions.h"
#include "ArrayIterator.h"
#include "Library.h"
#include "ConnStringPropertyReader.h"
#include "OdbcIniKeys.h"
#include "GFXDLog.h"

#include <boost/lexical_cast.hpp>

using namespace com::pivotal::gemfirexd;

SQLRETURN GFXDConnection::newConnection(GFXDEnvironment *env,
    GFXDConnection*& connRef) {
  if (env != NULL) {
    try {
      connRef = new GFXDConnection(env);
      return SQL_SUCCESS;
    } catch (const SQLException& sqle) {
      GFXDHandleBase::setGlobalException(sqle);
      return SQL_ERROR;
    }
  } else {
    return GFXDHandleBase::errorNullHandle(SQL_HANDLE_ENV);
  }
}

SQLRETURN GFXDConnection::freeConnection(GFXDConnection* conn) {
  if (conn != NULL) {
    if (conn->isActive()) {
      SQLSMALLINT result = conn->m_env->removeActiveConnection(conn);
      if (result == SQL_SUCCESS || result == SQL_NO_DATA) {
        delete conn;
        return SQL_SUCCESS;
      } else {
        return result;
      }
    } else {
      conn->setException(GET_SQLEXCEPTION2(
          SQLStateMessage::FUNCTION_SEQUENCE_ERROR_MSG,
          "SQLFreeHandle(SQL_HANDLE_DBC) invoked before SQLDisconnect"));
      return SQL_ERROR;
    }
  } else {
    return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
  }
}

GFXDConnection::GFXDConnection(GFXDEnvironment* env) :
    m_conn(), m_env(env), m_attributes(), m_argsAsIdentifiers(false),
    m_hwnd( NULL), m_translateOption(0), m_translationLibrary(NULL),
    m_dataSourceToDriver(NULL), m_driverToDataSource(NULL) {
  env->addNewActiveConnection(this);
}

GFXDConnection::~GFXDConnection() {
  // delete any string attributes
  for (AttributeMap::const_iterator iter = m_attributes.begin();
      iter != m_attributes.end(); ++iter) {
    switch (iter->first) {
      case SQL_ATTR_CURRENT_CATALOG:
      case SQL_ATTR_TRANSLATE_LIB:
        if (iter->second.m_ascii) {
          delete[] (SQLCHAR*)iter->second.m_val.m_refv;
        } else {
          delete[] (SQLWCHAR*)iter->second.m_val.m_refv;
        }
        break;
    }
  }
  if (m_translationLibrary != NULL) {
    delete m_translationLibrary;
    m_translationLibrary = NULL;
  }
  m_conn.close();
}

template<typename CHAR_TYPE, typename CHAR_TYPE2>
SQLRETURN GFXDConnection::getStringValue(const CHAR_TYPE* str,
    const SQLINTEGER len, CHAR_TYPE2* resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr, const char* op) {
  SQLRETURN result = SQL_SUCCESS;
  if (resultValue != NULL) {
    SQLLEN stringLen;
    SQLLEN* stringLenP = stringLengthPtr != NULL ? &stringLen : NULL;
    if (StringFunctions::copyString(str, len, resultValue, bufferLength,
        stringLenP)) {
      result = SQL_SUCCESS_WITH_INFO;
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::STRING_TRUNCATED_MSG, op, bufferLength - 1));
    }
  } else if (stringLengthPtr != NULL) {
    *stringLengthPtr = StringFunctions::strlen(str, resultValue);
  }
  return result;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDConnection::connectT(const std::string& server, const int port,
    const Properties& connProps, CHAR_TYPE* outConnStr,
    const SQLINTEGER outConnStrLen, SQLSMALLINT* connStrLen) {
  SQLRETURN result = SQL_SUCCESS;
  if (!m_conn.isOpen()) {
    m_conn.open(server, port, connProps);

    if (outConnStr != NULL) {
      std::string connStr;
      connStr.append("server=").append(server).append(";port=").append(
          boost::lexical_cast<std::string>(port));
      for (Properties::const_iterator iter = connProps.begin();
          iter != connProps.end(); ++iter) {
        connStr.append(";").append(iter->first).append("=").append(
            iter->second);
      }
      SQLINTEGER connLen = 0;
      result = getStringValue((const SQLCHAR*)connStr.data(), connStr.size(),
          outConnStr, outConnStrLen, &connLen, "connection string");
      if (connStrLen != NULL) {
        *connStrLen = (connLen > 0x7FFF ? 0x7FFF : connLen);
      }
    }
    // now set all the attributes on the connection
    for (AttributeMap::const_iterator iter = m_attributes.begin();
        iter != m_attributes.end(); ++iter) {
      const SQLINTEGER attrKey = iter->first;
      if (attrKey != SQL_ATTR_LOGIN_TIMEOUT) {
        result = setConnectionAttribute(attrKey, iter->second);
        if (result == SQL_ERROR) return result;
      }
    }
    return result;
  } else {
    setException(GET_SQLEXCEPTION2(SQLStateMessage::CONNECTION_IN_USE_MSG));
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::connect(const std::string& server, const int port,
    const Properties& connProps, SQLCHAR* outConnStr,
    const SQLINTEGER outConnStrLen, SQLSMALLINT* connStrLen) {
  try {
    return connectT(server, port, connProps, outConnStr, outConnStrLen,
        connStrLen);
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::connect(const std::string& server, const int port,
    const Properties& connProps, SQLWCHAR* outConnStr,
    const SQLINTEGER outConnStrLen, SQLSMALLINT* connStrLen) {
  try {
    return connectT(server, port, connProps, outConnStr, outConnStrLen,
        connStrLen);
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::disconnect() {
  if (m_conn.isOpen()) {
    m_conn.close();
    return SQL_SUCCESS;
  } else {
    setException(GET_SQLEXCEPTION2(SQLStateMessage::NO_CURRENT_CONNECTION_MSG));
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::setConnectionAttribute(SQLINTEGER attribute,
    const AttributeValue& attrValue) {
  switch (attribute) {
    case SQL_ATTR_ACCESS_MODE:
      switch (attrValue.m_val.m_intv) {
        case SQL_MODE_READ_ONLY:
          m_conn.setTransactionAttribute(
              TransactionAttribute::READ_ONLY_CONNECTION, true);
          break;
        case SQL_MODE_READ_WRITE:
          m_conn.setTransactionAttribute(
              TransactionAttribute::READ_ONLY_CONNECTION, false);
          break;
        default:
          // should be handled by DriverManager
          setException(GET_SQLEXCEPTION2(
              SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG,
              attrValue.m_val.m_intv, "SQL_ATTR_ACCESS_MODE"));
          return SQL_ERROR;
      }
      break;
    case SQL_ATTR_AUTOCOMMIT:
      switch (attrValue.m_val.m_intv) {
        case SQL_AUTOCOMMIT_OFF:
          m_conn.setTransactionAttribute(TransactionAttribute::AUTOCOMMIT,
              false);
          break;
        case SQL_AUTOCOMMIT_ON:
          m_conn.setTransactionAttribute(TransactionAttribute::AUTOCOMMIT,
              true);
          break;
        default:
          // should be handled by DriverManager
          setException(GET_SQLEXCEPTION2(
              SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG,
              attrValue.m_val.m_intv, "SQL_ATTR_AUTOCOMMIT"));
          return SQL_ERROR;
      }
      break;
    case SQL_ATTR_CONNECTION_TIMEOUT:
      m_conn.setReceiveTimeout(attrValue.m_val.m_intv);
      break;
    case SQL_ATTR_CURRENT_CATALOG: {
      // catalogs not relevant in GemFireXD; silently ignore
      m_conn.checkAndGetService();
      break;
    }
    case SQL_ATTR_METADATA_ID:
      switch (attrValue.m_val.m_intv) {
        case SQL_TRUE:
          m_argsAsIdentifiers = true;
          break;
        case SQL_FALSE:
          m_argsAsIdentifiers = false;
          break;
        default:
          // should be handled by DriverManager
          setException(GET_SQLEXCEPTION2(
              SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG,
              attrValue.m_val.m_intv, "SQL_ATTR_METADATA_ID"));
          return SQL_ERROR;
      }
      break;
    case SQL_ATTR_PACKET_SIZE:
      m_conn.setSendBufferSize(attrValue.m_val.m_intv);
      break;
    case SQL_ATTR_QUIET_MODE:
      m_hwnd = attrValue.m_val.m_refv;
      break;
    case SQL_ATTR_TRANSLATE_LIB: {
      if (m_translationLibrary != NULL) {
        delete m_translationLibrary;
        m_translationLibrary = NULL;
      }
      // for unicode version convert to UTF8
      if (attrValue.m_ascii) {
        m_translationLibrary = new native::Library(
            (const char*)attrValue.m_val.m_refv);
      } else {
        const SQLWCHAR* wchars = (const SQLWCHAR*)attrValue.m_val.m_refv;
        const int wlen = StringFunctions::strlen(wchars);
        // UTF8 can take upto 3 bytes
        SQLCHAR* chars = new SQLCHAR[wlen * 3 + 1];
        // delete the chars at the end of this block
        DestroyArray<SQLCHAR> destroyChars(chars);
        StringFunctions::copyString(wchars, wlen, chars, wlen * 3, NULL);
        m_translationLibrary = new native::Library((const char*)chars);
      }
      m_driverToDataSource =
          (DriverToDataSource)m_translationLibrary->getFunction(
              "SQLDriverToDataSource");
      m_dataSourceToDriver =
          (DataSourceToDriver)m_translationLibrary->getFunction(
              "SQLDataSourceToDriver");
      break;
    }
    case SQL_ATTR_TRANSLATE_OPTION:
      m_translateOption = attrValue.m_val.m_intv;
      break;
    case SQL_ATTR_TXN_ISOLATION:
      switch (attrValue.m_val.m_intv) {
        case SQL_TXN_READ_COMMITTED:
          m_conn.beginTransaction(IsolationLevel::READ_COMMITTED);
          break;
        case SQL_TXN_REPEATABLE_READ:
          m_conn.beginTransaction(IsolationLevel::REPEATABLE_READ);
          break;
        case 0:
          m_conn.beginTransaction(IsolationLevel::NONE);
          break;
        case SQL_TXN_READ_UNCOMMITTED:
          m_conn.beginTransaction(IsolationLevel::READ_UNCOMMITTED);
          break;
        case SQL_TXN_SERIALIZABLE:
          m_conn.beginTransaction(IsolationLevel::SERIALIZABLE);
          break;
        default:
          setException(GET_SQLEXCEPTION2(
              SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG,
              attrValue.m_val.m_intv, "SQL_ATTR_TXN_ISOLATION"));
          return SQL_ERROR;
      }
      break;
    default:
      // should be handled by DriverManager/ODBC API
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::UNKNOWN_ATTRIBUTE_MSG, attribute));
      return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDConnection::setAttribute(SQLINTEGER attribute, SQLPOINTER value,
    SQLINTEGER stringLength, bool isAscii) {
  AttributeValue attrValue(isAscii);
  switch (attribute) {
    case SQL_ATTR_ACCESS_MODE:
    case SQL_ATTR_AUTOCOMMIT:
    case SQL_ATTR_CONNECTION_TIMEOUT:
    case SQL_ATTR_METADATA_ID:
    case SQL_ATTR_PACKET_SIZE:
    case SQL_ATTR_TRANSLATE_OPTION:
    case SQL_ATTR_TXN_ISOLATION:
      // integer values
      attrValue.m_val.m_intv = (SQLUINTEGER)(SQLBIGINT)value;
      m_attributes[attribute] = attrValue;
      break;
    case SQL_ATTR_LOGIN_TIMEOUT:
      if (m_conn.isOpen()) {
        setException(GET_SQLEXCEPTION2(
            SQLStateMessage::ATTRIBUTE_CANNOT_BE_SET_NOW_MSG,
            "SQL_ATTR_LOGIN_TIMEOUT ", "Connection is open."));
        return SQL_ERROR;
      } else {
        attrValue.m_val.m_intv = (SQLUINTEGER)(SQLBIGINT)value;
        m_attributes[attribute] = attrValue;
      }
      break;
    case SQL_ATTR_QUIET_MODE:
      attrValue.m_val.m_refv = value;
      m_attributes[SQL_ATTR_QUIET_MODE] = attrValue;
      break;
    case SQL_ATTR_CURRENT_CATALOG:
    case SQL_ATTR_TRANSLATE_LIB: {
      // string values
      if (isAscii) {
        if (stringLength == SQL_NTS) {
          stringLength = ::strlen((const char*)value);
        }
        SQLCHAR* strv = new SQLCHAR[stringLength + 1];
        ::memcpy(strv, value, stringLength + 1);
        attrValue.m_val.m_refv = strv;
      } else {
        if (stringLength == SQL_NTS) {
          stringLength = StringFunctions::strlen((const SQLWCHAR*)value);
        }
        SQLWCHAR* strv = new SQLWCHAR[stringLength + 1];
        ::memcpy(strv, value, (stringLength + 1) * sizeof(SQLWCHAR));
        attrValue.m_val.m_refv = strv;
      }
      // free up any old value
      AttributeMap::const_iterator oldValue = m_attributes.find(attribute);
      if (oldValue != m_attributes.end()) {
        if (oldValue->second.m_ascii) {
          delete[] (SQLCHAR*)oldValue->second.m_val.m_refv;
        } else {
          delete[] (SQLWCHAR*)oldValue->second.m_val.m_refv;
        }
      }
      m_attributes[attribute] = attrValue;
      break;
    }

    case SQL_ATTR_ASYNC_ENABLE:
      // TODO: async execution by just invoking send_* & recv_ in background?
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
          "setAttribute(SQL_ATTR_ASYNC_ENABLE)"));
      return SQL_ERROR;
    case SQL_ATTR_AUTO_IPD:
      // TODO: descriptors not yet implemented
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
          "setAttribute(SQL_ATTR_AUTO_IPD)"));
      return SQL_ERROR;
    case SQL_ATTR_ENLIST_IN_DTC:
      // TODO: XA transactions support not yet added to ODBC driver
      // see IDtcToXaHelperSinglePipe, ITransactionResourceAsync etc.
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
          "setAttribute(SQL_ATTR_ENLIST_IN_DTC)"));
      return SQL_ERROR;

    case SQL_ATTR_CONNECTION_DEAD:
    case SQL_ATTR_ODBC_CURSORS:
    case SQL_ATTR_TRACE:
    case SQL_ATTR_TRACEFILE:
    default:
      // should be handled by DriverManager
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::UNKNOWN_ATTRIBUTE_MSG, attribute));
      return SQL_ERROR;
  }
  if (m_conn.isOpen()) {
    if (attribute != SQL_ATTR_LOGIN_TIMEOUT) {
      try {
        return setConnectionAttribute(attribute, attrValue);
      } catch (const SQLException& sqle) {
        setException(sqle);
        return SQL_ERROR;
      }
    }
  }

  return SQL_SUCCESS;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDConnection::getAttributeT(SQLINTEGER attribute,
    SQLPOINTER resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr) {
  if (m_conn.isOpen()) {
    SQLUINTEGER intResult = SQL_NTS;
    switch (attribute) {
      case SQL_ATTR_CONNECTION_DEAD:
        intResult = m_conn.isOpen() ? SQL_FALSE : SQL_TRUE;
        break;
      case SQL_ATTR_ACCESS_MODE:
        intResult = !m_conn.getTransactionAttribute(
            TransactionAttribute::READ_ONLY_CONNECTION)
            ? SQL_MODE_READ_WRITE : SQL_MODE_READ_ONLY;
        break;
      case SQL_ATTR_AUTOCOMMIT:
        intResult = !m_conn.getTransactionAttribute(
            TransactionAttribute::AUTOCOMMIT)
            ? SQL_MODE_READ_WRITE : SQL_MODE_READ_ONLY;
        break;
      case SQL_ATTR_CONNECTION_TIMEOUT:
        intResult = m_conn.getReceiveTimeout();
        break;
      case SQL_ATTR_METADATA_ID:
        intResult = !m_argsAsIdentifiers ? SQL_FALSE : SQL_TRUE;
        break;
      case SQL_ATTR_PACKET_SIZE:
        intResult = m_conn.getSendBufferSize();
        break;
      case SQL_ATTR_TRANSLATE_OPTION:
        intResult = m_translateOption;
        break;
      case SQL_ATTR_LOGIN_TIMEOUT:
        intResult = m_conn.getConnectTimeout();
        break;
      case SQL_ATTR_TXN_ISOLATION: {
        const IsolationLevel::type isolationLevel =
            m_conn.getCurrentIsolationLevel();
        switch (isolationLevel) {
          case IsolationLevel::READ_COMMITTED:
            intResult = SQL_TXN_READ_COMMITTED;
            break;
          case IsolationLevel::REPEATABLE_READ:
            intResult = SQL_TXN_REPEATABLE_READ;
            break;
          case IsolationLevel::NONE:
            intResult = 0;
            break;
          case IsolationLevel::READ_UNCOMMITTED:
            intResult = SQL_TXN_READ_UNCOMMITTED;
            break;
          case IsolationLevel::SERIALIZABLE:
            intResult = SQL_TXN_SERIALIZABLE;
            break;
          default:
            // unexpected value from driver?
            setException(GET_SQLEXCEPTION2(
                SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG,
                isolationLevel, "SQL_ATTR_TXN_ISOLATION"));
            return SQL_ERROR;
        }
        break;
      }
      case SQL_ATTR_CURRENT_CATALOG: {
        // catalogs not valid for GFXD
        return SQL_NO_DATA;
      }
      case SQL_ATTR_TRANSLATE_LIB: {
        // translation library name is in the attributes map
        AttributeMap::const_iterator attrSearch = m_attributes.find(
            SQL_ATTR_TRANSLATE_LIB);
        if (attrSearch != m_attributes.end()) {
          if (attrSearch->second.m_ascii) {
            const SQLCHAR* str =
                (const SQLCHAR*)attrSearch->second.m_val.m_refv;
            return getStringValue(str, SQL_NTS, (CHAR_TYPE*)resultValue,
                bufferLength, stringLengthPtr, "attribute");
          } else {
            const SQLWCHAR* str =
                (const SQLWCHAR*)attrSearch->second.m_val.m_refv;
            return getStringValue(str, SQL_NTS, (CHAR_TYPE*)resultValue,
                bufferLength, stringLengthPtr, "attribute");
          }
        } else {
          return SQL_NO_DATA;
        }
      }

      case SQL_ATTR_QUIET_MODE:
        if (m_hwnd != NULL) {
          if (resultValue != NULL) {
            *((SQLPOINTER*)resultValue) = m_hwnd;
            if (stringLengthPtr != NULL) *stringLengthPtr =
                sizeof(*((SQLPOINTER*)resultValue));
          }
          return SQL_SUCCESS;
        } else {
          return SQL_NO_DATA;
        }
        //Below attributes are handles by the driver manager
      case SQL_ATTR_TRACE:
      case SQL_ATTR_TRACEFILE:
      case SQL_ATTR_ODBC_CURSORS:
      default:
        // should be handled by DriverManager/ODBC API
        setException(
            GET_SQLEXCEPTION2(SQLStateMessage::UNKNOWN_ATTRIBUTE_MSG,
                attribute));
        return SQL_ERROR;
    }
    // if control reaches here then it is an integer result
    if (resultValue != NULL) {
      *((SQLUINTEGER*)resultValue) = intResult;
      if (stringLengthPtr != NULL) *stringLengthPtr =
          sizeof(*((SQLUINTEGER*)resultValue));
    }

    return SQL_SUCCESS;
  } else {
    AttributeMap::const_iterator attrSearch = m_attributes.find(attribute);

    switch (attribute) {
      case SQL_ATTR_CONNECTION_DEAD:
        if (resultValue != NULL) *((SQLUINTEGER*)resultValue) = SQL_TRUE;
        if (stringLengthPtr != NULL) *stringLengthPtr =
            sizeof(*((SQLUINTEGER*)resultValue));
        return SQL_SUCCESS;
        break;
      case SQL_ATTR_METADATA_ID:
        *((SQLUINTEGER*)resultValue) =
            !m_argsAsIdentifiers ? SQL_FALSE : SQL_TRUE;
        if (stringLengthPtr != NULL) *stringLengthPtr =
            sizeof(*((SQLUINTEGER*)resultValue));
        return SQL_SUCCESS;
      case SQL_ATTR_ACCESS_MODE:
      case SQL_ATTR_AUTOCOMMIT:
      case SQL_ATTR_CONNECTION_TIMEOUT:
      case SQL_ATTR_LOGIN_TIMEOUT:
      case SQL_ATTR_PACKET_SIZE:
      case SQL_ATTR_TRANSLATE_OPTION:
      case SQL_ATTR_TXN_ISOLATION: {
        // integer value
        if (attrSearch != m_attributes.end()) {
          if (resultValue != NULL) {
            *((SQLUINTEGER*)resultValue) = attrSearch->second.m_val.m_intv;
            if (stringLengthPtr != NULL) *stringLengthPtr =
                sizeof(*((SQLUINTEGER*)resultValue));
          }

          return SQL_SUCCESS;
        } else {
          return SQL_NO_DATA;
        }
      }

      case SQL_ATTR_CURRENT_CATALOG:
      case SQL_ATTR_TRANSLATE_LIB: {
        // string value
        if (attrSearch != m_attributes.end()) {
          if (attrSearch->second.m_ascii) {
            const SQLCHAR* str =
                (const SQLCHAR*)attrSearch->second.m_val.m_refv;
            return getStringValue(str, SQL_NTS, (CHAR_TYPE*)resultValue,
                bufferLength, stringLengthPtr, "attribute");
          } else {
            const SQLWCHAR* str =
                (const SQLWCHAR*)attrSearch->second.m_val.m_refv;
            return getStringValue(str, SQL_NTS, (CHAR_TYPE*)resultValue,
                bufferLength, stringLengthPtr, "attribute");
          }
        } else {
          return SQL_NO_DATA;
        }
      }

      case SQL_ATTR_QUIET_MODE:
        if (attrSearch != m_attributes.end()) {
          if (resultValue != NULL) {
            *((SQLPOINTER*)resultValue) = attrSearch->second.m_val.m_refv;
            if (stringLengthPtr != NULL) *stringLengthPtr =
                sizeof(*((SQLPOINTER*)resultValue));
          }
          return SQL_SUCCESS;
        } else {
          return SQL_NO_DATA;
        }

      case SQL_ATTR_ASYNC_ENABLE:
        // TODO: async execution?
        setException(GET_SQLEXCEPTION2(
            SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
            "getAttribute(SQL_ATTR_ASYNC_ENABLE)"));
        return SQL_ERROR;
      case SQL_ATTR_AUTO_IPD:
        // TODO: descriptors not yet implemented
        setException(GET_SQLEXCEPTION2(
            SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
            "getAttribute(SQL_ATTR_AUTO_IPD)"));
        return SQL_ERROR;
      case SQL_ATTR_ENLIST_IN_DTC:
        // TODO: XA transactions support not yet added to ODBC driver
        // see IDtcToXaHelperSinglePipe, ITransactionResourceAsync etc.
        setException(GET_SQLEXCEPTION2(
            SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
            "getAttribute(SQL_ATTR_ENLIST_IN_DTC)"));
        return SQL_ERROR;

        //these attributes are handled by the Driver manager
      case SQL_ATTR_ODBC_CURSORS:
      case SQL_ATTR_TRACE:
      case SQL_ATTR_TRACEFILE:
      default:
        // should be handled by DriverManager
        setException(GET_SQLEXCEPTION2(
            SQLStateMessage::UNKNOWN_ATTRIBUTE_MSG, attribute));
        return SQL_ERROR;
    }
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDConnection::getInfoT(SQLUSMALLINT infoType, SQLPOINTER infoValue,
    SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  SQLRETURN ret = SQL_SUCCESS;
  const SQLCHAR* resStr = NULL;
  const char* resInfo = NULL;
  SQLINTEGER resStrLen = 0;
  switch (infoType) {
    case SQL_DRIVER_NAME: {
      const SQLCHAR* driverName =
          (const SQLCHAR*)"Pivotal GemFire XD ODBC Driver";
      resStr = driverName;
      resStrLen = SQL_NTS;
      resInfo = "SQL_DRIVER_NAME";
      break;
    }
    case PRODUCT_NAME: {
      const SQLCHAR* productName = (const SQLCHAR*)"Pivotal GemFire XD";
      resStr = productName;
      resStrLen = SQL_NTS;
      resInfo = "PRODUCT_NAME";
      break;
    }
    case SQL_DRIVER_VER: {
      const SQLCHAR* driverVers = (const SQLCHAR*)"01.00.02.0001";
      resStr = driverVers;
      resStrLen = SQL_NTS;
      resInfo = "SQL_DRIVER_VER";
      break;
    }
    case SQL_DRIVER_ODBC_VER: {
      const SQLCHAR* driverODBCVers = (const SQLCHAR*)"03.51";
      resStr = driverODBCVers;
      resStrLen = SQL_NTS;
      resInfo = "SQL_DRIVER_ODBC_VER";
      break;
    }
    case SQL_ODBC_VER: {
      const SQLCHAR* odbcVers = (const SQLCHAR*)"03.51.0000";
      resStr = odbcVers;
      resStrLen = SQL_NTS;
      resInfo = "SQL_ODBC_VER";
      break;
    }
    case SQL_MAX_DRIVER_CONNECTIONS:
      *(SQLUINTEGER*)infoValue = this->m_env->getActiveConnectionsCount();
      break;
    case SQL_SCROLL_OPTIONS:
      // not available in API so hardcoding here: no support for
      // scroll-sensitive ResultSets so SQL_SO_STATIC, and keys are always
      // part of rows if required, so SQL_SO_DYNAMIC
      *(SQLUINTEGER*)infoValue = (SQL_SO_DYNAMIC | SQL_SO_STATIC);
      break;
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES1: {
      // no lock/unlock of current row
      SQLUINTEGER flags = SQL_CA1_LOCK_NO_CHANGE;
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      if (dbmd->isFeatureSupported(DatabaseFeature::POSITIONED_UPDATE)) {
        flags |= SQL_CA1_POSITIONED_UPDATE;
      }
      if (dbmd->isFeatureSupported(DatabaseFeature::POSITIONED_DELETE)) {
        flags |= SQL_CA1_POSITIONED_DELETE;
      }
      if (dbmd->isFeatureSupported(DatabaseFeature::SELECT_FOR_UPDATE)) {
        flags |= SQL_CA1_SELECT_FOR_UPDATE;
      }
      if (dbmd->isFeatureSupported(
          DatabaseFeature::SQL_GRAMMAR_ANSI92_INTERMEDIATE)) {
        flags |= SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
      }
      *(SQLUINTEGER*)infoValue = flags;
      break;
    }
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES2: {
      // by default guarantee single row changes
      SQLUINTEGER flags = SQL_CA2_SIMULATE_UNIQUE;
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      const ResultSetType::type dynamicCursorType =
          ResultSetType::TYPE_FORWARD_ONLY;
      if (dbmd->supportsResultSetReadOnly(dynamicCursorType)) {
        flags |= SQL_CA2_READ_ONLY_CONCURRENCY;
      }
      if (dbmd->othersInsertsVisible(dynamicCursorType)) {
        flags |= SQL_CA2_SENSITIVITY_ADDITIONS;
      }
      if (dbmd->othersDeletesVisible(dynamicCursorType)) {
        flags |= SQL_CA2_SENSITIVITY_DELETIONS;
      }
      if (dbmd->othersUpdatesVisible(dynamicCursorType)) {
        flags |= SQL_CA2_SENSITIVITY_UPDATES;
      }
      *(SQLUINTEGER*)infoValue = flags;
      break;
    }
    case SQL_STATIC_CURSOR_ATTRIBUTES1: {
      // no lock/unlock of current row
      SQLUINTEGER flags = SQL_CA1_LOCK_NO_CHANGE;
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      if (dbmd->isFeatureSupported(DatabaseFeature::POSITIONED_UPDATE)) {
        flags |= SQL_CA1_POSITIONED_UPDATE;
      }
      if (dbmd->isFeatureSupported(DatabaseFeature::POSITIONED_DELETE)) {
        flags |= SQL_CA1_POSITIONED_DELETE;
      }
      if (dbmd->isFeatureSupported(DatabaseFeature::SELECT_FOR_UPDATE)) {
        flags |= SQL_CA1_SELECT_FOR_UPDATE;
      }
      if (dbmd->isFeatureSupported(
          DatabaseFeature::SQL_GRAMMAR_ANSI92_INTERMEDIATE)) {
        flags |= SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
      }
      *(SQLUINTEGER*)infoValue = flags;
      break;
    }
    case SQL_STATIC_CURSOR_ATTRIBUTES2: {
      // by default guarantee single row changes
      SQLUINTEGER flags = SQL_CA2_SIMULATE_UNIQUE;
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      const ResultSetType::type staticCursorType =
          ResultSetType::TYPE_INSENSITIVE;
      if (dbmd->supportsResultSetReadOnly(staticCursorType)) {
        flags |= SQL_CA2_READ_ONLY_CONCURRENCY;
      }
      if (dbmd->othersInsertsVisible(staticCursorType)) {
        flags |= SQL_CA2_SENSITIVITY_ADDITIONS;
      }
      if (dbmd->othersDeletesVisible(staticCursorType)) {
        flags |= SQL_CA2_SENSITIVITY_DELETIONS;
      }
      if (dbmd->othersUpdatesVisible(staticCursorType)) {
        flags |= SQL_CA2_SENSITIVITY_UPDATES;
      }
      *(SQLUINTEGER*)infoValue = flags;
      break;
    }
    case SQL_BATCH_SUPPORT: {
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      *(SQLUINTEGER*)infoValue = dbmd->isFeatureSupported(
          DatabaseFeature::BATCH_UPDATES)
          ? SQL_BS_ROW_COUNT_EXPLICIT : 0;
      break;
    }
    case SQL_BATCH_ROW_COUNT:
      *(SQLUINTEGER*)infoValue = SQL_BRC_EXPLICIT;
      break;
    case SQL_PARAM_ARRAY_ROW_COUNTS:
      *(SQLUINTEGER*)infoValue = SQL_PARC_BATCH;
      break;
    case SQL_CURSOR_COMMIT_BEHAVIOR: {
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      *(SQLSMALLINT*)infoValue =
          dbmd->isFeatureSupported(
              DatabaseFeature::OPEN_CURSORS_ACROSS_COMMIT)
              ? SQL_CB_PRESERVE : (dbmd->isFeatureSupported(
                  DatabaseFeature::OPEN_STATEMENTS_ACROSS_COMMIT)
                  ? SQL_CB_CLOSE : SQL_CB_DELETE);
      break;
    }
    case SQL_CURSOR_ROLLBACK_BEHAVIOR: {
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      *(SQLSMALLINT*)infoValue =
          dbmd->isFeatureSupported(
              DatabaseFeature::OPEN_CURSORS_ACROSS_ROLLBACK)
              ? SQL_CB_PRESERVE : (dbmd->isFeatureSupported(
                  DatabaseFeature::OPEN_STATEMENTS_ACROSS_ROLLBACK)
                  ? SQL_CB_CLOSE : SQL_CB_DELETE);
      break;
    }
    case SQL_EXPRESSIONS_IN_ORDERBY: {
      AutoPtr<DatabaseMetaData> dbmd = m_conn.getServiceMetaData();
      resStr = (const SQLCHAR*)(dbmd->isFeatureSupported(
          DatabaseFeature::ORDER_BY_EXPRESSIONS) ? "Y" : "N");
      resStrLen = SQL_NTS;
      resInfo = "SQL_EXPRESSIONS_IN_ORDERBY";
      break;
    }

    case SQL_GETDATA_EXTENSIONS:
      *((SQLUINTEGER *)infoValue) = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER
          | SQL_GD_BLOCK | SQL_GD_BOUND;
      break;

    default:
      // TODO: implement remaining info types
      char buf[32];
      ::snprintf(buf, sizeof(buf) - 1, "getInfo(%d)", infoType);
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1, buf));
      return SQL_ERROR;
  }
  if (resStr != NULL) {
    SQLINTEGER totalLen = 0;
    ret = getStringValue(resStr, resStrLen, (CHAR_TYPE*)infoValue, bufferLength,
        &totalLen, resInfo);
    if (stringLength != NULL) {
      *stringLength = (SQLSMALLINT)totalLen * sizeof(CHAR_TYPE);
    }
  }
  return ret;
}

SQLRETURN GFXDConnection::getAttribute(SQLINTEGER attribute,
    SQLPOINTER resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr) {
  try {
    return getAttributeT<SQLCHAR>(attribute, resultValue, bufferLength,
        stringLengthPtr);
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::getAttributeW(SQLINTEGER attribute,
    SQLPOINTER resultValue, SQLINTEGER bufferLength,
    SQLINTEGER* stringLengthPtr) {
  try {
    return getAttributeT<SQLWCHAR>(attribute, resultValue, bufferLength,
        stringLengthPtr);
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::getInfo(SQLUSMALLINT infoType, SQLPOINTER infoValue,
    SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  try {
    return getInfoT<SQLCHAR>(infoType, infoValue, bufferLength, stringLength);
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::getInfoW(SQLUSMALLINT infoType, SQLPOINTER infoValue,
    SQLSMALLINT bufferLength, SQLSMALLINT* stringLength) {
  try {
    return getInfoT<SQLWCHAR>(infoType, infoValue, bufferLength, stringLength);
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDConnection::nativeSQLT(CHAR_TYPE* inStatementText,
    SQLINTEGER textLength1, CHAR_TYPE* outStatementText,
    SQLINTEGER bufferLength, SQLINTEGER* textLength2Ptr) {
  if (outStatementText == NULL
      || (bufferLength <= 0 && bufferLength != SQL_NTS)) {
    return GFXDHandleBase::errorInvalidBufferLength(bufferLength,
        "outStatementText string length", this);
  }
  std::string stmt;
  StringFunctions::getString(inStatementText, textLength1, stmt);
  std::string nativeSQL = m_conn.getNativeSQL(stmt);
  return getStringValue((const SQLCHAR*)nativeSQL.data(), nativeSQL.size(),
      outStatementText, bufferLength, textLength2Ptr, "nativeSQL");
}

SQLRETURN GFXDConnection::nativeSQL(SQLCHAR* inStatementText,
    SQLINTEGER textLength1, SQLCHAR* outStatementText, SQLINTEGER bufferLength,
    SQLINTEGER* textLength2Ptr) {
  return nativeSQLT<SQLCHAR>(inStatementText, textLength1, outStatementText,
      bufferLength, textLength2Ptr);
}

SQLRETURN GFXDConnection::nativeSQLW(SQLWCHAR* inStatementText,
    SQLINTEGER textLength1, SQLWCHAR* outStatementText, SQLINTEGER bufferLength,
    SQLINTEGER* textLength2Ptr) {
  return nativeSQLT<SQLWCHAR>(inStatementText, textLength1, outStatementText,
      bufferLength, textLength2Ptr);
}

SQLRETURN GFXDConnection::commit() {
  try {
    m_conn.commitTransaction(true);
    return SQL_SUCCESS;
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDConnection::rollback() {
  try {
    m_conn.rollbackTransaction(true);
    return SQL_SUCCESS;
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}
