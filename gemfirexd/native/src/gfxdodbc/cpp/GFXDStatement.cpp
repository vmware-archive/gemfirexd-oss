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
 * GFXDStatement.cpp
 *
 *  Contains implementation of statement creation and execution ODBC API.
 *
 *      Author: swale
 */

#include "GFXDEnvironment.h"
#include "GFXDStatement.h"
#include "GFXDLog.h"

#include <Row.h>
#include <ParametersBatch.h>
#include <limits>

using namespace com::pivotal::gemfirexd;

const char* GFXDStatement::s_GUID_FORMAT =
    "%08lx-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x";

SQLRETURN GFXDStatement::bindParameter(Parameters& paramValues,
    Parameter& param, std::map<int32_t, OutputParameter>* outParams) {
  //check if data at exec param and return
  if (param.m_isDataAtExecParam) {
    return SQL_NEED_DATA;
  }
  // TODO: handle other values of m_o_lenOrIndp
  if (param.m_o_value != NULL && param.m_inputOutputType != SQL_PARAM_OUTPUT) {
    const SQLSMALLINT ctype = param.m_o_valueType;
    SQLLEN len;

    switch (ctype) {
      case SQL_C_CHAR:
        if (param.m_o_lenOrIndp != NULL) {
          len = *param.m_o_lenOrIndp;
        } else {
          len = param.m_o_valueSize;
        }
        paramValues.setString(param.m_paramNum, (const char*)param.m_o_value,
            len);
        param.m_paramType = SQLType::VARCHAR;
        break;
      case SQL_C_WCHAR: {
        if (param.m_o_lenOrIndp != NULL) {
          len = *param.m_o_lenOrIndp;
        } else {
          len = param.m_o_valueSize;
        }
        // use the string object in paramValues itself
        paramValues.setString(param.m_paramNum, "");
        AutoPtr<std::string> s = paramValues.getString(param.m_paramNum, 0);
        StringFunctions::getString((const SQLWCHAR*)param.m_o_value, len, *s);
        param.m_paramType = SQLType::VARCHAR;
        break;
      }
      case SQL_C_SSHORT:
      case SQL_C_SHORT:
        paramValues.setShort(param.m_paramNum, *(SQLSMALLINT*)param.m_o_value);
        param.m_paramType = SQLType::SMALLINT;
        break;
      case SQL_C_USHORT:
        paramValues.setUnsignedShort(param.m_paramNum,
            *(SQLUSMALLINT*)param.m_o_value);
        param.m_paramType = SQLType::SMALLINT;
        break;
      case SQL_C_SLONG:
        paramValues.setInt(param.m_paramNum, *(SQLINTEGER*)param.m_o_value);
        param.m_paramType = SQLType::INTEGER;
        break;
      case SQL_C_ULONG:
        paramValues.setUnsignedInt(param.m_paramNum,
            *(SQLUINTEGER*)param.m_o_value);
        param.m_paramType = SQLType::INTEGER;
        break;
      case SQL_C_LONG:
        paramValues.setInt(param.m_paramNum, *(SQLLEN*)param.m_o_value);
        param.m_paramType = SQLType::INTEGER;
        break;
      case SQL_C_FLOAT:
        paramValues.setFloat(param.m_paramNum, *(SQLREAL*)param.m_o_value);
        param.m_paramType = SQLType::FLOAT;
        break;
      case SQL_C_DOUBLE:
        paramValues.setDouble(param.m_paramNum, *(SQLDOUBLE*)param.m_o_value);
        param.m_paramType = SQLType::DOUBLE;
        break;
      case SQL_C_BIT:
      case SQL_C_UTINYINT:
      case SQL_C_TINYINT:
        paramValues.setUnsignedByte(param.m_paramNum,
            *(SQLCHAR*)param.m_o_value);
        param.m_paramType = SQLType::TINYINT;
        break;
      case SQL_C_STINYINT:
        paramValues.setByte(param.m_paramNum, *(SQLSCHAR*)param.m_o_value);
        param.m_paramType = SQLType::TINYINT;
        break;
      case SQL_C_SBIGINT:
        paramValues.setLong(param.m_paramNum, *(SQLBIGINT*)param.m_o_value);
        param.m_paramType = SQLType::BIGINT;
        break;
      case SQL_C_UBIGINT:
        paramValues.setUnsignedLong(param.m_paramNum,
            *(SQLUBIGINT*)param.m_o_value);
        param.m_paramType = SQLType::BIGINT;
        break;
      case SQL_C_BINARY: {
        if (param.m_o_lenOrIndp != NULL) {
          len = *param.m_o_lenOrIndp;
        } else {
          len = param.m_o_valueSize;
        }
        const int8_t* bytes = (const int8_t*)param.m_o_value;
        if (len == SQL_NTS) {
          // assume NULL terminated data
          len = ::strlen((const char*)bytes);
        }
        paramValues.setBinary(param.m_paramNum, bytes, len);
        param.m_paramType = SQLType::VARBINARY;
        break;
      }
      case SQL_C_TYPE_DATE: {
        const SQL_DATE_STRUCT* date = (const SQL_DATE_STRUCT*)param.m_o_value;
        DateTime dt(date->year, date->month, date->day);
        paramValues.setDate(param.m_paramNum, dt);
        param.m_paramType = SQLType::DATE;
        break;
      }
      case SQL_C_TYPE_TIME: {
        const SQL_TIME_STRUCT* time = (const SQL_TIME_STRUCT*)param.m_o_value;
        DateTime tm(1970, 1, 1, time->hour, time->minute, time->second);
        paramValues.setTime(param.m_paramNum, tm);
        param.m_paramType = SQLType::TIME;
        break;
      }
      case SQL_C_TYPE_TIMESTAMP: {
        const SQL_TIMESTAMP_STRUCT* timestamp =
            (const SQL_TIMESTAMP_STRUCT*)param.m_o_value;
        Timestamp ts(timestamp->year, timestamp->month, timestamp->day,
            timestamp->hour, timestamp->minute, timestamp->second,
            timestamp->fraction);
        paramValues.setTimestamp(param.m_paramNum, ts);
        param.m_paramType = SQLType::TIMESTAMP;
        break;
      }
      case SQL_C_NUMERIC: {
        const SQL_NUMERIC_STRUCT* numeric =
            (const SQL_NUMERIC_STRUCT*)param.m_o_value;

        const int precision = numeric->precision;
        if (precision == 0) {
          paramValues.setDecimal(param.m_paramNum, Decimal::ZERO);
        } else {
          const SQLCHAR* mag = numeric->val;
          // skip trailing zeros to get the length
          size_t maglen = SQL_MAX_NUMERIC_LEN;
          const SQLCHAR* magp = (mag + maglen - 1);
          while (*magp == 0) {
            maglen--;
            magp--;
          }
          paramValues.setDecimal(param.m_paramNum, numeric->sign == 1 ? 1 : -1,
              numeric->scale, (const int8_t*)mag, maglen, false);
        }
        param.m_paramType = SQLType::DECIMAL;
        break;
      }
      case SQL_C_INTERVAL_YEAR: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const int years = interval->intval.year_month.year;
        paramValues.setInt(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? years : -years);
        param.m_paramType = SQLType::INTEGER;
        break;
      }
      case SQL_C_INTERVAL_MONTH: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const int months = interval->intval.year_month.month;
        paramValues.setInt(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? months : -months);
        param.m_paramType = SQLType::INTEGER;
        break;
      }
      case SQL_C_INTERVAL_DAY: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const int days = interval->intval.day_second.day;
        paramValues.setInt(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? days : -days);
        param.m_paramType = SQLType::INTEGER;
        break;
      }
      case SQL_C_INTERVAL_HOUR: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const int hours = interval->intval.day_second.hour;
        paramValues.setInt(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? hours : -hours);
        param.m_paramType = SQLType::INTEGER;
        break;
      }
      case SQL_C_INTERVAL_MINUTE: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const int minutes = interval->intval.day_second.minute;
        paramValues.setInt(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? minutes : -minutes);
        param.m_paramType = SQLType::INTEGER;
        break;
      }
      case SQL_C_INTERVAL_SECOND: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const int seconds = interval->intval.day_second.second;
        paramValues.setInt(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? seconds : -seconds);
        param.m_paramType = SQLType::INTEGER;
        break;
      }
      case SQL_C_INTERVAL_YEAR_TO_MONTH: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const int months = interval->intval.year_month.year * 12
            + interval->intval.year_month.month;
        paramValues.setInt(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? months : -months);
        param.m_paramType = SQLType::INTEGER;
        break;
      }
      case SQL_C_INTERVAL_DAY_TO_HOUR: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const SQLBIGINT hours = ((SQLBIGINT)interval->intval.day_second.day)
            * 24 + interval->intval.day_second.hour;
        paramValues.setLong(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? hours : -hours);
        param.m_paramType = SQLType::BIGINT;
        break;
      }
      case SQL_C_INTERVAL_DAY_TO_MINUTE: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const SQLBIGINT hours = ((SQLBIGINT)interval->intval.day_second.day)
            * 24 + interval->intval.day_second.hour;
        const SQLBIGINT minutes = hours * 60
            + interval->intval.day_second.minute;
        paramValues.setLong(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? minutes : -minutes);
        param.m_paramType = SQLType::BIGINT;
        break;
      }
      case SQL_C_INTERVAL_DAY_TO_SECOND: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const SQLBIGINT hours = ((SQLBIGINT)interval->intval.day_second.day)
            * 24 + interval->intval.day_second.hour;
        const SQLBIGINT minutes = hours * 60
            + interval->intval.day_second.minute;
        const SQLBIGINT seconds = minutes * 60
            + interval->intval.day_second.second;
        paramValues.setLong(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? seconds : -seconds);
        param.m_paramType = SQLType::BIGINT;
        break;
      }
      case SQL_C_INTERVAL_HOUR_TO_MINUTE: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const SQLBIGINT minutes = ((SQLBIGINT)interval->intval.day_second.hour)
            * 60 + interval->intval.day_second.minute;
        paramValues.setLong(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? minutes : -minutes);
        param.m_paramType = SQLType::BIGINT;
        break;
      }
      case SQL_C_INTERVAL_HOUR_TO_SECOND: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const SQLBIGINT minutes = ((SQLBIGINT)interval->intval.day_second.hour)
            * 60 + interval->intval.day_second.minute;
        const SQLBIGINT seconds = minutes * 60
            + interval->intval.day_second.second;
        paramValues.setLong(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? seconds : -seconds);
        param.m_paramType = SQLType::BIGINT;
        break;
      }
      case SQL_C_INTERVAL_MINUTE_TO_SECOND: {
        const SQL_INTERVAL_STRUCT* interval =
            (const SQL_INTERVAL_STRUCT*)param.m_o_value;
        const SQLBIGINT seconds =
            ((SQLBIGINT)interval->intval.day_second.minute) * 60
                + interval->intval.day_second.second;
        paramValues.setLong(param.m_paramNum,
            (interval->interval_sign == SQL_FALSE) ? seconds : -seconds);
        param.m_paramType = SQLType::BIGINT;
        break;
      }
      case SQL_C_GUID: {
        const SQLGUID* guid = (const SQLGUID*)param.m_o_value;
        char guidChars[40];
        const int maxLen = sizeof(guidChars) - 1;
        // convert GUID to string representation
        const int guidLen = ::snprintf(guidChars, maxLen, s_GUID_FORMAT,
            guid->Data1, guid->Data2, guid->Data3, guid->Data4[0],
            guid->Data4[1], guid->Data4[2], guid->Data4[3], guid->Data4[4],
            guid->Data4[5], guid->Data4[6], guid->Data4[7]);
        paramValues.setString(param.m_paramNum, guidChars, guidLen);
        param.m_paramType = SQLType::VARCHAR;
        break;
      }
      default:
        setException(
            GET_SQLEXCEPTION2(SQLStateMessage::INVALID_CTYPE_MSG, ctype,
                (int )param.m_paramNum));
        return SQL_ERROR;
    }
  } else {
    paramValues.setNull(param.m_paramNum);
    param.m_paramType = getSQLType(param.m_o_paramType, param.m_paramNum);
  }
  if ((param.m_inputOutputType == SQL_PARAM_OUTPUT
      || param.m_inputOutputType == SQL_PARAM_INPUT_OUTPUT)) {
    if (outParams != NULL) {
      OutputParameter& outParam = outParams->operator [](param.m_paramNum);
      outParam.setType(param.m_paramType);
      outParam.setScale(param.m_o_scale);
    } else {
      m_pstmt->registerOutParameter(param.m_paramNum, param.m_paramType,
          param.m_o_scale);
    }
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDStatement::newStatement(GFXDConnection* conn,
    GFXDStatement*& stmtRef) {
  try {
    if (conn != NULL) {
      stmtRef = new GFXDStatement(conn);
      return SQL_SUCCESS;
    } else {
      return GFXDHandleBase::errorNullHandle(SQL_HANDLE_DBC);
    }
  } catch (const SQLException& sqle) {
    if (conn != NULL) {
      conn->setException(sqle);
    } else {
      GFXDHandleBase::setGlobalException(sqle);
    }
    return SQL_ERROR;
  }
}

SQLRETURN GFXDStatement::freeStatement(GFXDStatement* stmt,
    SQLUSMALLINT option) {
  if (stmt != NULL) {
    SQLRETURN result = SQL_SUCCESS;
    try {
      switch (option) {
        case SQL_CLOSE:
          if (!stmt->m_resultSet.isNull()) {
            stmt->m_resultSet->close();
            stmt->m_resultSet.reset();
          }
          break;
        case SQL_UNBIND:
          stmt->m_outputFields.clear();
          break;
        case SQL_DROP:
          delete stmt;
          break;
        case SQL_RESET_PARAMS:
          result = stmt->resetParameters();
          break;
        default:
          stmt->setException(
              GET_SQLEXCEPTION2(SQLStateMessage::OPTION_TYPE_OUT_OF_RANGE,
                  option, "freeStatement"));
          result = SQL_ERROR;
          break;
      }
    } catch (const SQLException& sqle) {
      GFXDHandleBase::setGlobalException(sqle);
      return SQL_ERROR;
    }
    return result;
  } else {
    return GFXDHandleBase::errorNullHandle(SQL_HANDLE_STMT);
  }
}

SQLRETURN GFXDStatement::bindParameters(Parameters& paramValues,
    std::map<int32_t, OutputParameter>* outParams) {
  SQLRETURN retVal = SQL_SUCCESS;
  for (ArrayList<Parameter>::const_iterator iter = m_params.cbegin();
      iter != m_params.cend(); ++iter) {
    retVal = bindParameter(paramValues, *iter, outParams);
    if (retVal != SQL_SUCCESS) {
      break;
    }
  }
  //m_params.clear();
  return retVal;
}

SQLRETURN GFXDStatement::fillOutput(const Row& outputRow,
    const uint32_t outputColumn, SQLPOINTER value, const SQLLEN valueSize,
    SQLSMALLINT ctype, const SQLULEN precision, SQLLEN* lenOrIndp) {
  SQLRETURN res = SQL_SUCCESS;

  if (ctype == SQL_C_DEFAULT) {
    ctype = getCTypeFromSQLType(outputRow.getType(outputColumn));
  }
  switch (ctype) {
    case SQL_C_CHAR: {
      if (valueSize != SQL_NTS) {
        int32_t actualLen = outputRow.getString(outputColumn, (char*)value,
            valueSize, true, precision);
        if (actualLen > valueSize) {
          res = SQL_SUCCESS_WITH_INFO;
        }
        if (lenOrIndp != NULL) {
          if (actualLen == 0 && outputRow.isNull(outputColumn)) {
            *lenOrIndp = SQL_NULL_DATA;
          } else {
            *lenOrIndp = actualLen;
          }
        }
      } else {
        AutoPtr<std::string> outStr = outputRow.getString(outputColumn,
            precision);
        std::string* outStrp = outStr.get();
        if (outStrp != NULL) {
          if (StringFunctions::copyString(outStrp->c_str(),
              (SQLLEN)outStrp->size(), (SQLCHAR*)value, valueSize, lenOrIndp)) {
            res = SQL_SUCCESS_WITH_INFO;
          }
        } else {
          if (valueSize != 0) {
            *((SQLCHAR*)value) = '\0';
          }
          if (lenOrIndp != NULL) {
            *lenOrIndp = SQL_NULL_DATA;
          }
        }
      }
      break;
    }
    case SQL_C_WCHAR: {
      AutoPtr<std::string> outStr = outputRow.getString(outputColumn,
          precision);
      std::string* outStrp = outStr.get();
      if (outStrp != NULL) {
        if (StringFunctions::copyString((const SQLCHAR*)outStrp->c_str(),
            (SQLLEN)outStrp->size(), (SQLWCHAR*)value, valueSize, lenOrIndp)) {
          res = SQL_SUCCESS_WITH_INFO;
        }
      } else {
        if (valueSize != 0) {
          *((SQLWCHAR*)value) = 0;
        }
        if (lenOrIndp != NULL) {
          *lenOrIndp = SQL_NULL_DATA;
        }
      }
      break;
    }
    case SQL_C_SSHORT:
    case SQL_C_SHORT: {
      const int16_t v = outputRow.getShort(outputColumn);
      *(SQLSMALLINT*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLSMALLINT);
        }
      }
      break;
    }
    case SQL_C_USHORT: {
      const uint16_t v = outputRow.getUnsignedShort(outputColumn);
      *(SQLUSMALLINT*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLUSMALLINT);
        }
      }
      break;
    }
    case SQL_C_SLONG: {
      const int32_t v = outputRow.getInt(outputColumn);
      *(SQLINTEGER*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLINTEGER);
        }
      }
      break;
    }
    case SQL_C_ULONG: {
      const uint32_t v = outputRow.getUnsignedInt(outputColumn);
      *(SQLUINTEGER*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLUINTEGER);
        }
      }
      break;
    }
    case SQL_C_LONG: {
      const int32_t v = outputRow.getInt(outputColumn);
      *(SQLLEN*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLLEN);
        }
      }
      break;
    }
    case SQL_C_FLOAT: {
      const float v = outputRow.getFloat(outputColumn);
      *(SQLREAL*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLREAL);
        }
      }
      break;
    }
    case SQL_C_DOUBLE: {
      const double v = outputRow.getDouble(outputColumn);
      *(SQLDOUBLE*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLDOUBLE);
        }
      }
      break;
    }
    case SQL_C_BIT: {
      const int8_t v = outputRow.getByte(outputColumn);
      *(SQLCHAR*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLCHAR);
        }
      }
      break;
    }
    case SQL_C_UTINYINT:
    case SQL_C_TINYINT: {
      const uint8_t v = outputRow.getUnsignedByte(outputColumn);
      *(SQLCHAR*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLCHAR);
        }
      }
      break;
    }
    case SQL_C_STINYINT: {
      const int8_t v = outputRow.getByte(outputColumn);
      *(SQLSCHAR*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLSCHAR);
        }
      }
      break;
    }
    case SQL_C_SBIGINT: {
      const int64_t v = outputRow.getLong(outputColumn);
      *(SQLBIGINT*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLBIGINT);
        }
      }
      break;
    }
    case SQL_C_UBIGINT: {
      const uint64_t v = outputRow.getUnsignedLong(outputColumn);
      *(SQLUBIGINT*)value = v;
      if (lenOrIndp != NULL) {
        if (v == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQLUBIGINT);
        }
      }
      break;
    }
    case SQL_C_BINARY: {
      AutoPtr<std::string> byteArr = outputRow.getBinary(outputColumn);
      const std::string* bytes = byteArr.get();
      if (bytes != NULL) {
        const size_t bytesLen = bytes->size();
        SQLLEN outLen = valueSize;
        if (outLen < 0 || (size_t)outLen >= bytesLen) {
          outLen = bytesLen;
        }
        ::memcpy(value, bytes->c_str(), outLen);
        if (lenOrIndp != NULL) {
          *lenOrIndp = outLen;
        }
      } else if (lenOrIndp != NULL) {
        *lenOrIndp = SQL_NULL_DATA;
      }
      break;
    }
    case SQL_C_TYPE_DATE: {
      DateTime date = outputRow.getDate(outputColumn);
      if (date.getEpochTime() != 0 || !outputRow.isNull(outputColumn)) {
        SQL_DATE_STRUCT* outDate = (SQL_DATE_STRUCT*)value;
        struct tm t = date.toDateTime(false);
        outDate->year = t.tm_year + 1900;
        // MONTH in calender are zero numbered 0 = Jan
        outDate->month = t.tm_mon + 1;
        outDate->day = t.tm_mday;
        if (lenOrIndp != NULL) {
          *lenOrIndp = sizeof(SQL_DATE_STRUCT);
        }
      } else {
        ::memset(value, 0, sizeof(SQL_DATE_STRUCT));
        if (lenOrIndp != NULL) {
          *lenOrIndp = SQL_NULL_DATA;
        }
      }
      break;
    }
    case SQL_C_TYPE_TIME: {
      DateTime time = outputRow.getTime(outputColumn);
      if (time.getEpochTime() != 0 || !outputRow.isNull(outputColumn)) {
        SQL_TIME_STRUCT* outTime = (SQL_TIME_STRUCT*)value;
        struct tm t = time.toDateTime(false);
        outTime->hour = t.tm_hour;
        outTime->minute = t.tm_min;
        outTime->second = t.tm_sec;
        if (lenOrIndp != NULL) {
          *lenOrIndp = sizeof(SQL_TIME_STRUCT);
        }
      } else {
        ::memset(value, 0, sizeof(SQL_TIME_STRUCT));
        if (lenOrIndp != NULL) {
          *lenOrIndp = SQL_NULL_DATA;
        }
      }
      break;
    }
    case SQL_C_TYPE_TIMESTAMP: {
      Timestamp ts = outputRow.getTimestamp(outputColumn);
      if (ts.getEpochTime() != 0 || !outputRow.isNull(outputColumn)) {
        SQL_TIMESTAMP_STRUCT* outTs = (SQL_TIMESTAMP_STRUCT*)value;
        struct tm t = ts.toDateTime(false);
        outTs->year = t.tm_year;
        // MONTH in calender are zero numbered 0 = Jan
        outTs->month = t.tm_mon + 1;
        outTs->day = t.tm_mday;
        outTs->hour = t.tm_hour;
        outTs->minute = t.tm_min;
        outTs->second = t.tm_sec;
        outTs->fraction = ts.getNanos();
        if (lenOrIndp != NULL) {
          *lenOrIndp = sizeof(SQL_TIMESTAMP_STRUCT);
        }
      } else {
        ::memset(value, 0, sizeof(SQL_TIMESTAMP_STRUCT));
        if (lenOrIndp != NULL) {
          *lenOrIndp = SQL_NULL_DATA;
        }
      }
      break;
    }
    case SQL_C_NUMERIC: {
      AutoPtr<thrift::Decimal> tdec = outputRow.getTDecimal(outputColumn,
          SQL_MAX_NUMERIC_LEN);
      thrift::Decimal* pdec = tdec.get();
      if (pdec != NULL) {
        SQL_NUMERIC_STRUCT* numeric = (SQL_NUMERIC_STRUCT*)value;
        const size_t magLen = pdec->magnitude.size();
        if (magLen <= SQL_MAX_NUMERIC_LEN) {
          numeric->precision = magLen;
          numeric->scale = pdec->scale;
          numeric->sign = pdec->signum;
          ::memcpy(numeric->val, pdec->magnitude.c_str(), magLen);
        } else {
          // need to truncate fractional portion (result may still overflow)
          Decimal dec(*pdec);
          uint32_t wholeLen;
          if (dec.wholeDigits(numeric->val, SQL_MAX_NUMERIC_LEN, wholeLen)) {
            numeric->precision = SQL_MAX_NUMERIC_LEN;
            numeric->scale = 0;
            numeric->sign = dec.signum();
            setException(
                GET_SQLEXCEPTION2(SQLStateMessage::NUMERIC_TRUNCATED_MSG,
                    "output column", magLen - wholeLen));
            res = SQL_SUCCESS_WITH_INFO;
          } else {
            setException(GET_SQLEXCEPTION2(
                SQLStateMessage::LANG_OUTSIDE_RANGE_FOR_NUMERIC_MSG,
                SQL_MAX_NUMERIC_LEN, magLen));
            res = SQL_ERROR;
          }
        }
        if (lenOrIndp != NULL) {
          *lenOrIndp = magLen;
        }
      } else {
        ::memset(value, 0, sizeof(SQL_NUMERIC_STRUCT));
        if (lenOrIndp != NULL) {
          *lenOrIndp = SQL_NULL_DATA;
        }
      }
      break;
    }
    case SQL_C_INTERVAL_YEAR: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      interval->interval_type = SQL_IS_YEAR;
      const int32_t year = outputRow.getInt(outputColumn);
      if (year >= 0) {
        interval->intval.year_month.year = year;
        interval->interval_sign = SQL_FALSE;
      } else {
        interval->intval.year_month.year = -year;
        interval->interval_sign = SQL_TRUE;
      }
      interval->intval.year_month.month = 0;
      if (lenOrIndp != NULL) {
        if (year == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_MONTH: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      interval->interval_type = SQL_IS_MONTH;
      const int32_t month = outputRow.getInt(outputColumn);
      if (month >= 0) {
        interval->intval.year_month.month = month;
        interval->interval_sign = SQL_FALSE;
      } else {
        interval->intval.year_month.month = -month;
        interval->interval_sign = SQL_TRUE;
      }
      interval->intval.year_month.year = 0;
      if (lenOrIndp != NULL) {
        if (month == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_DAY: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_DAY;
      const int32_t day = outputRow.getInt(outputColumn);
      if (day >= 0) {
        interval->intval.day_second.day = day;
        interval->interval_sign = SQL_FALSE;
      } else {
        interval->intval.day_second.day = -day;
        interval->interval_sign = SQL_TRUE;
      }
      if (lenOrIndp != NULL) {
        if (day == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_HOUR: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_HOUR;
      const int32_t hour = outputRow.getInt(outputColumn);
      if (hour >= 0) {
        interval->intval.day_second.hour = hour;
        interval->interval_sign = SQL_FALSE;
      } else {
        interval->intval.day_second.hour = -hour;
        interval->interval_sign = SQL_TRUE;
      }
      if (lenOrIndp != NULL) {
        if (hour == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_MINUTE: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_MINUTE;
      const int32_t minute = outputRow.getInt(outputColumn);
      if (minute >= 0) {
        interval->intval.day_second.minute = minute;
        interval->interval_sign = SQL_FALSE;
      } else {
        interval->intval.day_second.minute = -minute;
        interval->interval_sign = SQL_TRUE;
      }
      if (lenOrIndp != NULL) {
        if (minute == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_SECOND: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_SECOND;
      const int32_t second = outputRow.getInt(outputColumn);
      if (second >= 0) {
        interval->intval.day_second.second = second;
        interval->interval_sign = SQL_FALSE;
      } else {
        interval->intval.day_second.second = -second;
        interval->interval_sign = SQL_TRUE;
      }
      if (lenOrIndp != NULL) {
        if (second == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_YEAR_TO_MONTH: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      interval->interval_type = SQL_IS_YEAR_TO_MONTH;
      const int32_t month = outputRow.getInt(outputColumn);
      SQLUINTEGER months;
      if (month >= 0) {
        months = month;
        interval->interval_sign = SQL_FALSE;
      } else {
        months = -month;
        interval->interval_sign = SQL_TRUE;
      }
      interval->intval.year_month.year = months / 12;
      interval->intval.year_month.month = months % 12;
      if (lenOrIndp != NULL) {
        if (month == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_DAY_TO_HOUR: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_DAY_TO_HOUR;
      const int32_t hour = outputRow.getInt(outputColumn);
      SQLUBIGINT hours;
      if (hour >= 0) {
        hours = hour;
        interval->interval_sign = SQL_FALSE;
      } else {
        hours = -hour;
        interval->interval_sign = SQL_TRUE;
      }
      interval->intval.day_second.day = hours / 24;
      interval->intval.day_second.hour = hours % 24;
      if (lenOrIndp != NULL) {
        if (hour == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_DAY_TO_MINUTE: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_DAY_TO_MINUTE;
      const int64_t minute = outputRow.getLong(outputColumn);
      SQLUBIGINT minutes;
      if (minute >= 0) {
        minutes = minute;
        interval->interval_sign = SQL_FALSE;
      } else {
        minutes = -minute;
        interval->interval_sign = SQL_TRUE;
      }
      const SQLUBIGINT hours = minutes / 60;
      interval->intval.day_second.day = hours / 24;
      interval->intval.day_second.hour = hours % 24;
      interval->intval.day_second.minute = minutes % 60;
      if (lenOrIndp != NULL) {
        if (minute == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_DAY_TO_SECOND: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_DAY_TO_SECOND;
      const int64_t second = outputRow.getLong(outputColumn);
      SQLUBIGINT seconds;
      if (second >= 0) {
        seconds = second;
        interval->interval_sign = SQL_FALSE;
      } else {
        seconds = -second;
        interval->interval_sign = SQL_TRUE;
      }
      const SQLUBIGINT minutes = seconds / 60;
      const SQLUBIGINT hours = minutes / 60;
      interval->intval.day_second.day = hours / 24;
      interval->intval.day_second.hour = hours % 24;
      interval->intval.day_second.minute = minutes % 60;
      interval->intval.day_second.second = seconds % 60;
      if (lenOrIndp != NULL) {
        if (second == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_HOUR_TO_MINUTE: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_HOUR_TO_MINUTE;
      const int64_t minute = outputRow.getLong(outputColumn);
      SQLUBIGINT minutes;
      if (minute >= 0) {
        minutes = minute;
        interval->interval_sign = SQL_FALSE;
      } else {
        minutes = -minute;
        interval->interval_sign = SQL_TRUE;
      }
      interval->intval.day_second.hour = minutes / 60;
      interval->intval.day_second.minute = minutes % 60;
      if (lenOrIndp != NULL) {
        if (minute == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_HOUR_TO_SECOND: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_HOUR_TO_SECOND;
      const int64_t second = outputRow.getLong(outputColumn);
      SQLUBIGINT seconds;
      if (second >= 0) {
        seconds = second;
        interval->interval_sign = SQL_FALSE;
      } else {
        seconds = -second;
        interval->interval_sign = SQL_TRUE;
      }
      const SQLUBIGINT minutes = seconds / 60;
      interval->intval.day_second.hour = minutes / 60;
      interval->intval.day_second.minute = minutes % 60;
      interval->intval.day_second.second = seconds % 60;
      if (lenOrIndp != NULL) {
        if (second == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_INTERVAL_MINUTE_TO_SECOND: {
      SQL_INTERVAL_STRUCT* interval = (SQL_INTERVAL_STRUCT*)value;
      ::memset(&interval->intval.day_second, 0, sizeof(SQL_DAY_SECOND_STRUCT));
      interval->interval_type = SQL_IS_MINUTE_TO_SECOND;
      const int64_t second = outputRow.getLong(outputColumn);
      SQLUBIGINT seconds;
      if (second >= 0) {
        seconds = second;
        interval->interval_sign = SQL_FALSE;
      } else {
        seconds = -second;
        interval->interval_sign = SQL_TRUE;
      }
      interval->intval.day_second.minute = seconds / 60;
      interval->intval.day_second.second = seconds % 60;
      if (lenOrIndp != NULL) {
        if (second == 0 && outputRow.isNull(outputColumn)) {
          *lenOrIndp = SQL_NULL_DATA;
        } else {
          *lenOrIndp = sizeof(SQL_INTERVAL_STRUCT);
        }
      }
      break;
    }
    case SQL_C_GUID: {
      AutoPtr<std::string> str = outputRow.getString(outputColumn, precision);
      std::string* strp = str.get();
      if (strp != NULL) {
        SQLGUID* guid = (SQLGUID*)value;
        // convert from string representation to GUID
        unsigned int c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11;
        ::sscanf(strp->c_str(), s_GUID_FORMAT, &c1, &c2, &c3, &c4, &c5, &c6,
            &c7, &c8, &c9, &c10, &c11);
        guid->Data1 = c1;
        guid->Data2 = c2;
        guid->Data3 = c3;
        guid->Data4[0] = c4;
        guid->Data4[1] = c5;
        guid->Data4[2] = c6;
        guid->Data4[3] = c7;
        guid->Data4[4] = c8;
        guid->Data4[5] = c9;
        guid->Data4[6] = c10;
        guid->Data4[7] = c11;

        if (lenOrIndp != NULL) {
          *lenOrIndp = sizeof(SQLGUID);
        }
      } else {
        ::memset(value, 0, sizeof(SQLGUID));
        if (lenOrIndp != NULL) {
          *lenOrIndp = SQL_NULL_DATA;
        }
      }
      break;
    }
    default:
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::INVALID_CTYPE_MSG, ctype,
              outputColumn));
      res = SQL_ERROR;
      break;
  }

  if (res == SQL_SUCCESS_WITH_INFO
      && (ctype == SQL_CHAR || ctype == SQL_WCHAR)) {
    setException(
        GET_SQLEXCEPTION2(SQLStateMessage::STRING_TRUNCATED_MSG,
            "output column", valueSize));
  }
  return res;
}

SQLRETURN GFXDStatement::fillOutParameters(const Result& result) {
  AutoPtr<const Row> outputParams = result.getOutputParameters();
  if (outputParams.isNull()) {
    return SQL_SUCCESS;
  }
  SQLRETURN retVal = SQL_SUCCESS;
  uint32_t outParamIndex = 0;
  const Row& outParams = *outputParams;
  for (ArrayList<Parameter>::const_iterator iter = m_params.cbegin();
      iter != m_params.cend(); ++iter) {
    const short inoutType = iter->m_inputOutputType;
    if (inoutType == SQL_PARAM_OUTPUT || inoutType == SQL_PARAM_INPUT_OUTPUT) {
      retVal = fillOutput(outParams, ++outParamIndex, iter->m_o_value,
          iter->m_o_valueSize, iter->m_o_valueType, iter->m_o_precision,
          iter->m_o_lenOrIndp);
      if (retVal != SQL_SUCCESS) {
        break;
      }
    }
  }
  return retVal;
}

SQLRETURN GFXDStatement::fillOutputFields() {
  SQLRETURN result = SQL_SUCCESS, result2;

  const Row* currentRow = m_cursor.get();
  if (currentRow == NULL) {
    setException(GET_SQLEXCEPTION2(SQLStateMessage::INVALID_CURSOR_STATE_MSG2));
    return SQL_ERROR;
  }
  if (m_rowArraySize > 1) {
    return fillOutputFieldsWithArrays(currentRow);
  }

  // now bind the output fields
  uint32_t columnIndex = 0;
  for (ArrayList<OutputField>::const_iterator iter = m_outputFields.cbegin();
      iter != m_outputFields.cend(); ++iter) {
    result2 = fillOutput(*currentRow, ++columnIndex, iter->m_targetValue,
        iter->m_valueSize, iter->m_targetType, DEFAULT_REAL_PRECISION,
        iter->m_lenOrIndPtr);
    if (result2 != SQL_SUCCESS) result = result2;
  }
  return result;
}

SQLRETURN GFXDStatement::fillOutputFieldsWithArrays(const Row* currentRow) {
  SQLRETURN result = SQL_SUCCESS;
  int bindOffset = 0;
  int rowsFetched = 0;
  //int rowsIgnored = 0;
  if (m_bindOffsetPtr != NULL) {
    bindOffset = (int)*m_bindOffsetPtr;
  }

  for (int i = 0; i < m_rowArraySize; i++) {
    if (m_bindingOrientation == SQL_BIND_BY_COLUMN) {
      for (int j = 1; j <= (int)m_outputFields.size(); j++) {
        OutputField& outputField = m_outputFields[j];
        const SQLLEN valueSize = outputField.m_valueSize;
        SQLPOINTER targetValue = (((char*)outputField.m_targetValue)
            + (i * valueSize) + bindOffset);
        SQLLEN* lenOrIndPtr = (SQLLEN*)((char*)(outputField.m_lenOrIndPtr + i)
            + bindOffset);

        result = fillOutput(*currentRow, j, targetValue, valueSize,
            outputField.m_targetType, DEFAULT_REAL_PRECISION, lenOrIndPtr);

        if (result == SQL_ERROR) {
          break;
        }
      }
    } else {/*ROW_WISE_BINDING*/
      SQLLEN structSize = m_bindingOrientation;
      for (int j = 1; j <= (int)m_outputFields.size(); j++) {
        OutputField& outputField = m_outputFields[j];
        SQLPOINTER targetValue = (((char*)outputField.m_targetValue)
            + (i * structSize) + bindOffset);
        SQLLEN* lenOrIndPtr = (SQLLEN*)(((char*)outputField.m_lenOrIndPtr)
            + ((i * structSize) + bindOffset));
        result = fillOutput(*currentRow, j, targetValue,
            outputField.m_valueSize, outputField.m_targetType,
            DEFAULT_REAL_PRECISION, lenOrIndPtr);
        if (result == SQL_ERROR) {
          break;
        }
      }
    }
    if (result == SQL_ERROR) {
      break;
    }
    rowsFetched++;
    if (m_rowStatusPtr != NULL) {
      m_rowStatusPtr[i] = SQL_ROW_SUCCESS;
    }

    ++m_cursor;
    currentRow = m_cursor.get();
    if (currentRow == NULL) {
      setException(
          GET_SQLEXCEPTION2(SQLStateMessage::INVALID_CURSOR_STATE_MSG2));
      return SQL_ERROR;
    }
  }
  if (m_fetchedRowsPtr != NULL) {
    *m_fetchedRowsPtr = rowsFetched;
  }
  return result;
}

SQLRETURN GFXDStatement::setRowStatus() {
  SQLRETURN result = SQL_SUCCESS;
  if (m_rowStatusPtr == NULL || m_fetchedRowsPtr == NULL
      || m_resultSet.isNull()) {
    return result;
  }
  m_resultSet->getColumnCount()
  *m_fetchedRowsPtr = m_resultSet->getFetchSize();
  if (m_resultSet->rowDeleted()) {
    m_rowStatusPtr[m_resultSet->getRow()] = SQL_ROW_DELETED;
  } else if (m_resultSet->rowUpdated()) {
    m_rowStatusPtr[m_resultSet->getRow()] = SQL_ROW_UPDATED;
  } else if (m_resultSet->rowInserted()) {
    m_rowStatusPtr[m_resultSet->getRow()] = SQL_ROW_ADDED;
  } else {
    m_rowStatusPtr[m_resultSet->getRow()] = SQL_ROW_SUCCESS;
  }
  return result;
}

SQLType::type GFXDStatement::getSQLType(const SQLSMALLINT odbcType,
    const uint32_t paramNum) {
  switch (odbcType) {
    case SQL_CHAR:
      return SQLType::CHAR;
    case SQL_VARCHAR:
      return SQLType::VARCHAR;
    case SQL_LONGVARCHAR:
      return SQLType::LONGVARCHAR;
      // GemXD support wide-character types as non-wide UTF-8 encoded strings
    case SQL_WCHAR:
      return SQLType::CHAR;
    case SQL_WVARCHAR:
      return SQLType::VARCHAR;
    case SQL_WLONGVARCHAR:
      return SQLType::LONGVARCHAR;
    case SQL_DECIMAL:
    case SQL_NUMERIC:
      return SQLType::DECIMAL;
    case SQL_SMALLINT:
      return SQLType::SMALLINT;
    case SQL_INTEGER:
      return SQLType::INTEGER;
    case SQL_REAL:
      return SQLType::REAL;
    case SQL_FLOAT:
      return SQLType::FLOAT;
    case SQL_DOUBLE:
      return SQLType::DOUBLE;
    case SQL_BIT:
      return SQLType::BOOLEAN;
    case SQL_TINYINT:
      return SQLType::TINYINT;
    case SQL_BIGINT:
      return SQLType::BIGINT;
    case SQL_BINARY:
      return SQLType::BINARY;
    case SQL_VARBINARY:
      return SQLType::VARBINARY;
    case SQL_LONGVARBINARY:
      return SQLType::LONGVARBINARY;
    case SQL_TYPE_DATE:
    case SQL_DATE:
      return SQLType::DATE;
    case SQL_TYPE_TIME:
    case SQL_TIME:
      return SQLType::TIME;
    case SQL_TYPE_TIMESTAMP:
    case SQL_TIMESTAMP:
      return SQLType::TIMESTAMP;
      // TODO: there are no interval data types in JDBC or GemFireXD
      // below mapped according to closest precision data type but will it work?
    case SQL_INTERVAL_MONTH:
      return SQLType::INTEGER;
    case SQL_INTERVAL_YEAR:
      return SQLType::INTEGER;
    case SQL_INTERVAL_YEAR_TO_MONTH:
      return SQLType::DATE;
    case SQL_INTERVAL_DAY:
      return SQLType::INTEGER;
    case SQL_INTERVAL_HOUR:
      return SQLType::INTEGER;
    case SQL_INTERVAL_MINUTE:
      return SQLType::INTEGER;
    case SQL_INTERVAL_SECOND:
      return SQLType::INTEGER;
    case SQL_INTERVAL_DAY_TO_HOUR:
      return SQLType::TIMESTAMP;
    case SQL_INTERVAL_DAY_TO_MINUTE:
      return SQLType::TIMESTAMP;
    case SQL_INTERVAL_DAY_TO_SECOND:
      return SQLType::TIMESTAMP;
    case SQL_INTERVAL_HOUR_TO_MINUTE:
      return SQLType::TIME;
    case SQL_INTERVAL_HOUR_TO_SECOND:
      return SQLType::TIME;
    case SQL_INTERVAL_MINUTE_TO_SECOND:
      return SQLType::TIME;
    case SQL_GUID:
      return SQLType::CHAR;
    default:
      throw GET_SQLEXCEPTION2(SQLStateMessage::INVALID_PARAMETER_TYPE_MSG,
          odbcType, paramNum);
  }
}

SQLSMALLINT GFXDStatement::getCType(const SQLSMALLINT odbcType,
    const uint32_t paramNum) {
  switch (odbcType) {
    case SQL_CHAR:
      return SQL_C_CHAR;
    case SQL_VARCHAR:
      return SQL_C_CHAR;
    case SQL_LONGVARCHAR:
      return SQL_C_CHAR;
    case SQL_WCHAR:
      return SQL_C_WCHAR;
    case SQL_WVARCHAR:
      return SQL_C_WCHAR;
    case SQL_WLONGVARCHAR:
      return SQL_C_WCHAR;
    case SQL_DECIMAL:
      return SQL_C_NUMERIC;
    case SQL_NUMERIC:
      return SQL_C_NUMERIC;
    case SQL_SMALLINT:
      return SQL_C_SSHORT;
    case SQL_INTEGER:
      return SQL_C_SLONG;
    case SQL_REAL:
      return SQL_C_FLOAT;
    case SQL_FLOAT:
      return SQL_C_FLOAT;
    case SQL_DOUBLE:
      return SQL_C_DOUBLE;
    case SQL_BIT:
      return SQL_C_BIT;
    case SQL_TINYINT:
      return SQL_C_TINYINT;
    case SQL_BIGINT:
      return SQL_C_SBIGINT;
    case SQL_BINARY:
      return SQL_C_BINARY;
    case SQL_VARBINARY:
      return SQL_C_BINARY;
    case SQL_LONGVARBINARY:
      return SQL_C_BINARY;
    case SQL_DATE:
    case SQL_TYPE_DATE:
      return SQL_C_TYPE_DATE;
    case SQL_TIME:
    case SQL_TYPE_TIME:
      return SQL_C_TYPE_TIME;
    case SQL_TIMESTAMP:
    case SQL_TYPE_TIMESTAMP:
      return SQL_C_TYPE_TIMESTAMP;
    case SQL_INTERVAL_MONTH:
      return SQL_C_INTERVAL_MONTH;
    case SQL_INTERVAL_YEAR:
      return SQL_C_INTERVAL_YEAR;
    case SQL_INTERVAL_YEAR_TO_MONTH:
      return SQL_C_INTERVAL_YEAR_TO_MONTH;
    case SQL_INTERVAL_DAY:
      return SQL_C_INTERVAL_DAY;
    case SQL_INTERVAL_HOUR:
      return SQL_C_INTERVAL_HOUR;
    case SQL_INTERVAL_MINUTE:
      return SQL_C_INTERVAL_MINUTE;
    case SQL_INTERVAL_SECOND:
      return SQL_C_INTERVAL_SECOND;
    case SQL_INTERVAL_DAY_TO_HOUR:
      return SQL_C_INTERVAL_DAY_TO_HOUR;
    case SQL_INTERVAL_DAY_TO_MINUTE:
      return SQL_C_INTERVAL_DAY_TO_MINUTE;
    case SQL_INTERVAL_DAY_TO_SECOND:
      return SQL_C_INTERVAL_DAY_TO_SECOND;
    case SQL_INTERVAL_HOUR_TO_MINUTE:
      return SQL_C_INTERVAL_HOUR_TO_MINUTE;
    case SQL_INTERVAL_HOUR_TO_SECOND:
      return SQL_C_INTERVAL_HOUR_TO_SECOND;
    case SQL_INTERVAL_MINUTE_TO_SECOND:
      return SQL_C_INTERVAL_MINUTE_TO_SECOND;
    case SQL_GUID:
      return SQL_C_GUID;
    default:
      throw GET_SQLEXCEPTION2(SQLStateMessage::INVALID_PARAMETER_TYPE_MSG,
          odbcType, paramNum);
  }
}

SQLSMALLINT GFXDStatement::getCTypeFromSQLType(const SQLType::type sqlType) {
  switch (sqlType) {
    case SQLType::BIGINT:
      return SQL_C_SBIGINT;
    case SQLType::BINARY:
    case SQLType::BLOB:
    case SQLType::LONGVARBINARY:
    case SQLType::VARBINARY:
      return SQL_C_BINARY;
    case SQLType::BOOLEAN:
      return SQL_C_BIT;
    case SQLType::CHAR:
    case SQLType::CLOB:
    case SQLType::LONGVARCHAR:
    case SQLType::VARCHAR:
      return SQL_C_CHAR;
    case SQLType::NCHAR:
    case SQLType::NCLOB:
    case SQLType::LONGNVARCHAR:
    case SQLType::NVARCHAR:
      return SQL_C_WCHAR;
    case SQLType::DATE:
      return SQL_C_TYPE_DATE;
    case SQLType::DECIMAL:
      return SQL_C_NUMERIC;
    case SQLType::DOUBLE:
      return SQL_C_DOUBLE;
    case SQLType::FLOAT:
    case SQLType::REAL:
      return SQL_C_FLOAT;
    case SQLType::INTEGER:
      return SQL_C_LONG;
    case SQLType::SMALLINT:
      return SQL_C_SHORT;
    case SQLType::TIME:
      return SQL_C_TYPE_TIME;
    case SQLType::TIMESTAMP:
      return SQL_C_TYPE_TIMESTAMP;
    case SQLType::TINYINT:
      return SQL_C_TINYINT;
    default:
      return SQL_C_CHAR;
  }
}

SQLLEN GFXDStatement::getTypeFromSQLType(const SQLType::type sqlType) {
  switch (sqlType) {
    case SQLType::BIGINT:
      return SQL_BIGINT;
    case SQLType::BINARY:
      return SQL_BINARY;
    case SQLType::BLOB:
    case SQLType::LONGVARBINARY:
      return SQL_LONGVARBINARY;
    case SQLType::VARBINARY:
      return SQL_VARBINARY;
    case SQLType::BOOLEAN:
      return SQL_BIT;
    case SQLType::CHAR:
      return SQL_CHAR;
    case SQLType::NCHAR:
      return SQL_WCHAR;
    case SQLType::CLOB:
    case SQLType::LONGVARCHAR:
      return SQL_LONGVARCHAR;
    case SQLType::NCLOB:
    case SQLType::LONGNVARCHAR:
      return SQL_WLONGVARCHAR;
    case SQLType::VARCHAR:
      return SQL_VARCHAR;
    case SQLType::NVARCHAR:
      return SQL_WVARCHAR;
    case SQLType::DATE:
      return SQL_TYPE_DATE;
    case SQLType::DECIMAL:
      return SQL_DECIMAL;
    case SQLType::DOUBLE:
      return SQL_DOUBLE;
    case SQLType::FLOAT:
      return SQL_FLOAT;
    case SQLType::REAL:
      return SQL_REAL;
    case SQLType::INTEGER:
      return SQL_INTEGER;
    case SQLType::SMALLINT:
      return SQL_SMALLINT;
    case SQLType::TIME:
      return SQL_TYPE_TIME;
    case SQLType::TIMESTAMP:
      return SQL_TYPE_TIMESTAMP;
    case SQLType::TINYINT:
      return SQL_TINYINT;
    default:
      return sqlType;
  }
}

// Check for the conversions supported
bool GFXDStatement::checkSupportedCtypeToSQLTypeConversion(SQLSMALLINT cType,
    SQLSMALLINT sqlType) {
  bool result = true;
  switch (cType) {
    case SQL_C_SSHORT:
    case SQL_C_SHORT:
    case SQL_C_USHORT:
    case SQL_C_SLONG:
    case SQL_C_ULONG:
    case SQL_C_LONG:
    case SQL_C_BIT:
    case SQL_C_UTINYINT:
    case SQL_C_TINYINT:
    case SQL_C_STINYINT:
    case SQL_C_SBIGINT:
    case SQL_C_UBIGINT:
    case SQL_C_NUMERIC:
      switch (sqlType) {
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
        case SQL_DATE:
        case SQL_TIME:
        case SQL_TIMESTAMP:
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_TYPE_TIMESTAMP:
        case SQL_GUID:
          result = false;
          break;
        default:
          break;
      }
      break;
    case SQL_C_FLOAT:
    case SQL_C_DOUBLE:
      if (sqlType == SQL_BINARY || sqlType == SQL_VARBINARY
          || sqlType == SQL_LONGVARBINARY || sqlType == SQL_TYPE_DATE
          || sqlType == SQL_TYPE_TIME || sqlType == SQL_TYPE_TIMESTAMP
          || sqlType == SQL_GUID) {
        result = false;
      }
      break;
    case SQL_C_TYPE_DATE: {
      if (!(sqlType == SQL_CHAR || sqlType == SQL_VARCHAR
          || sqlType == SQL_LONGVARCHAR || sqlType == SQL_WCHAR
          || sqlType == SQL_WVARCHAR || sqlType == SQL_WLONGVARCHAR
          || sqlType == SQL_TYPE_DATE || sqlType == SQL_TYPE_TIMESTAMP)) {
        result = false;
      }
      break;
    }
    case SQL_C_TYPE_TIME: {
      if (!(sqlType == SQL_CHAR || sqlType == SQL_VARCHAR
          || sqlType == SQL_LONGVARCHAR || sqlType == SQL_WCHAR
          || sqlType == SQL_WVARCHAR || sqlType == SQL_WLONGVARCHAR
          || sqlType == SQL_TYPE_TIME)) {
        result = false;
      }
      break;
    }
    case SQL_C_TYPE_TIMESTAMP: {
      if (!(sqlType == SQL_CHAR || sqlType == SQL_VARCHAR
          || sqlType == SQL_LONGVARCHAR || sqlType == SQL_WCHAR
          || sqlType == SQL_WVARCHAR || sqlType == SQL_WLONGVARCHAR
          || sqlType == SQL_TYPE_DATE || sqlType == SQL_TYPE_TIME
          || sqlType == SQL_TYPE_TIMESTAMP)) {
        result = false;
      }
      break;
    }
    case SQL_C_GUID: {
      if (sqlType != SQL_GUID) {
        result = false;
      }
      break;
    }
  }
  return result;
}

SQLRETURN GFXDStatement::addParameter(SQLUSMALLINT paramNum,
    SQLSMALLINT inputOutputType, SQLSMALLINT valueType, SQLSMALLINT paramType,
    SQLULEN precision, SQLSMALLINT scale, SQLPOINTER paramValue,
    SQLLEN valueSize, SQLLEN* lenOrIndPtr) {
  const size_t sz = m_params.size();
  if (sz == 0) {
    m_params.reserve(std::max<SQLUSMALLINT>(4, paramNum));
  } else if (sz < paramNum) {
    m_params.resize(paramNum);
  }
  m_params[paramNum - 1].set(paramNum, inputOutputType, valueType, paramType,
      precision, scale, paramValue, valueSize, lenOrIndPtr,
      m_useWideStringDefault);
  if (checkSupportedCtypeToSQLTypeConversion(valueType, paramType)) {
    return SQL_SUCCESS;
  } else {
    setException(GET_SQLEXCEPTION2(
        SQLStateMessage::TYPE_ATTRIBUTE_VIOLATION_MSG,
        valueType, paramType));
    return SQL_ERROR;
  }
}

SQLRETURN GFXDStatement::prepare(const std::string& sqlText,
    const bool isWideString) {
  clearLastError();
  m_useWideStringDefault = isWideString;
  try {
    // need to prepare the statement and bind the parameters

    // clear any old parameters
    m_params.clear();
    m_pstmt = m_conn.m_conn.prepareStatement(sqlText, EMPTY_OUTPUT_PARAMS,
        m_stmtAttrs);

    return handleWarnings(m_pstmt);
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDStatement::executeWthArrayOfParams(const std::string& sqlText,
    const bool isWideString) {
  try {
    if (!isPrepared()) {
      m_pstmt = m_conn.m_conn.prepareStatement(sqlText);
    }

    int paramSetProcessed = 0;
    int bindOffset = 0;
    //int paramsSetIgnored = 0;

    if (m_paramBindOffsetPtr != NULL) {
      bindOffset = (int)(*m_paramBindOffsetPtr);
    }

    ParametersBatch paramsBatch(*m_pstmt);
    for (int i = 0; i < m_paramSetSize; i++) {
      Parameters& params = paramsBatch.createParameters();
      if (m_paramBindingOrientation == SQL_PARAM_BIND_BY_COLUMN) {
        for (int j = 1; j <= (int)m_params.size(); j++) {
          Parameter param(m_params[j]);
          param.m_o_value = (((char*)m_params[j].m_o_value)
              + (i * m_params[j].m_o_precision) + bindOffset);
          if (m_params[j].m_o_lenOrIndp != NULL) {
            if (*m_params[j].m_o_lenOrIndp != SQL_NTS)
              param.m_o_lenOrIndp = m_params[j].m_o_lenOrIndp + i + bindOffset;
            else
              *param.m_o_lenOrIndp = SQL_NTS;
          }
          bindParameter(params, param, NULL);
        }
      } else {/*ROW_WISE_BINDING */
        SQLLEN structSize = m_paramBindingOrientation;
        for (int j = 1; j <= (int)m_params.size(); j++) {
          Parameter param(m_params[j]);
          param.m_o_value = (((char*)m_params[j].m_o_value) + (i * structSize)
              + bindOffset);
          if (m_params[j].m_o_lenOrIndp != NULL) {
            if (*m_params[j].m_o_lenOrIndp != SQL_NTS)
              param.m_o_lenOrIndp = m_params[j].m_o_lenOrIndp + (i * structSize)
                  + bindOffset;
            else
              *param.m_o_lenOrIndp = SQL_NTS;
          }
          bindParameter(params, param, NULL);
        }
      }
      paramSetProcessed++;
      if (m_paramStatusArr != NULL) {
        m_paramsProcessedPtr[i] = SQL_PARAM_SUCCESS;
      }
    }
    m_pstmt->executeBatch(paramsBatch);
    if (m_paramsProcessedPtr != NULL) {
      *m_paramsProcessedPtr = (SQLLEN)(paramSetProcessed);
    }
    return SQL_SUCCESS;
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
}

SQLRETURN GFXDStatement::execute(const std::string& sqlText,
    const bool isWideString) {
  clearLastError();
  SQLRETURN result = SQL_SUCCESS;
  m_useWideStringDefault = isWideString;
  if (m_resultSet.isNull()) {
    try {
      if (m_paramSetSize > 1) {
        return executeWthArrayOfParams(sqlText, isWideString);
      }

      const size_t numParams = m_params.size();
      if (isPrepared()) {
        Parameters paramValues;
        result = bindParameters(paramValues, NULL);
        if (result != SQL_SUCCESS && result != SQL_SUCCESS_WITH_INFO) {
          return result;
        }
        m_result = m_pstmt->execute(paramValues);
      } else if (numParams > 0) {
        Parameters paramValues;
        std::map<int32_t, OutputParameter> outParams;
        result = bindParameters(paramValues, &outParams);
        if (result != SQL_SUCCESS && result != SQL_SUCCESS_WITH_INFO) {
          return result;
        }
        // need to prepare too, so use prepareAndExecute
        m_result = m_conn.m_conn.prepareAndExecute(sqlText, paramValues,
            outParams, m_stmtAttrs);
        m_pstmt = m_result->getPreparedStatement();
      } else {
        m_result = m_conn.m_conn.execute(sqlText, EMPTY_OUTPUT_PARAMS,
            m_stmtAttrs);
      }

      setResultSet(m_result->getResultSet());
      if (numParams > 0) {
        fillOutParameters(*m_result);
      }

      return handleWarnings(m_result);
    } catch (const SQLException& sqle) {
      setException(sqle);
      return SQL_ERROR;
    }
  } else {
    // old cursor still open
    setException(GET_SQLEXCEPTION2(SQLStateMessage::INVALID_CURSOR_STATE_MSG1));
    return SQL_ERROR;
  }
}

SQLRETURN GFXDStatement::execute() {
  clearLastError();
  SQLRETURN result = SQL_SUCCESS;
  if (m_resultSet.isNull()) {
    try {
      if (m_paramSetSize > 1) {
        return executeWthArrayOfParams(NULL, false);
      }

      if (isPrepared()) {
        Parameters paramValues;
        result = bindParameters(paramValues, NULL);
        if (result != SQL_SUCCESS && result != SQL_SUCCESS_WITH_INFO) {
          return result;
        }
        m_result = m_pstmt->execute(paramValues);
      } else {
        // should be handled by DriverManager
        setException(
            GET_SQLEXCEPTION2(SQLStateMessage::STATEMENT_NOT_PREPARED_MSG));
        return SQL_ERROR;
      }
      setResultSet(m_result->getResultSet());
      fillOutParameters(*m_result);

      return handleWarnings(m_result);
    } catch (const SQLException& sqle) {
      setException(sqle);
      return SQL_ERROR;
    }
  } else {
    // old cursor still open
    setException(GET_SQLEXCEPTION2(SQLStateMessage::INVALID_CURSOR_STATE_MSG1));
    return SQL_ERROR;
  }
}

SQLRETURN GFXDStatement::bulkOperations(SQLUSMALLINT operation) {
  if (operation == SQL_ADD) {
    try {
      std::string batchQueryString("INSERT INTO ");
      if (m_resultSet.isNull()) {
        if (m_pstmt.isNull()) {
          setException(GET_SQLEXCEPTION2(
              SQLStateMessage::STATEMENT_NOT_PREPARED_MSG));
          return SQL_ERROR;
        } else {
          batchQueryString.append(m_pstmt->getColumnDescriptor(1).getTable());
        }
      } else {
        batchQueryString.append(m_resultSet->getColumnDescriptor(1).getTable());
      }
      batchQueryString.append(" VALUES (");

      ArrayList<OutputField>::iterator iter = m_outputFields.begin();
      SQLUSMALLINT columnIndex = 1;
      while (iter != m_outputFields.end()) {
        if (columnIndex == 1) {
          batchQueryString.push_back('?');
        } else {
          batchQueryString.append(",?");
        }
        SQLType::type sqlType = (m_resultSet.isNull()
            ? m_pstmt->getColumnDescriptor(columnIndex).getSQLType()
            : m_resultSet->getColumnDescriptor(columnIndex).getSQLType());
        addParameter(columnIndex, SQL_PARAM_INPUT, iter->m_targetType,
            getTypeFromSQLType(sqlType), 0, 0, iter->m_targetValue,
            iter->m_valueSize, iter->m_lenOrIndPtr);
        ++iter;
        ++columnIndex;
      }
      batchQueryString.push_back(')');
      // need to prepare the statement and bind the parameters
      AutoPtr<PreparedStatement> batchStmt = m_conn.m_conn.prepareStatement(
          batchQueryString, EMPTY_OUTPUT_PARAMS, m_stmtAttrs);
      ParametersBatch paramsBatch(*batchStmt);
      //TODO: 1. its row-wise binding
      //TODO: 2. its column-wise binding
      //TODO: 3. array wise binding
      //TODO: 4. check binding offset
      std::map<int32_t, OutputParameter> outParams;
      for (int i = 0; i < m_rowArraySize; i++) {
        Parameters& params = paramsBatch.createParameters();
        if (m_bindingOrientation == SQL_BIND_BY_COLUMN) {
          /* Column wise binding : When using column-wise binding, an application binds
           * one or two, or in some cases three, arrays to each column for
           * which data is to be returned. The first array holds the data values,
           * and the second array holds length/indicator buffers.*/
          for (size_t j = 1; j <= m_params.size(); j++) {
            Parameter param = m_params[j];
            if (i == 0) {
              param.m_o_value = m_outputFields[j].m_targetValue;
            } else {
              param.m_o_value = ((char*)m_outputFields[j].m_targetValue)
                  + (i * m_outputFields[j].m_valueSize);
            }
            param.m_o_valueSize = 0;
            param.m_o_lenOrIndp = NULL;
            param.m_isDataAtExecParam = false;
            bindParameter(params, param, &outParams);
          }
        } else {
          /*ROW_WISE_BINDING : When using row-wise binding, an application defines a
           * structure containing one or two, or in some cases three, elements for
           * each column for which data is to be returned. The first element holds
           * the data value, and the second element holds the length/indicator buffer*/
          SQLLEN structSize = m_bindingOrientation;
          for (int j = 1; j <= (int)m_params.size(); j++) {
            Parameter param = m_params[j];
            param.m_o_value = (((char*)m_outputFields[j].m_targetValue)
                + (i * structSize));
            param.m_o_valueSize = 0;
            param.m_o_lenOrIndp = NULL;
            bindParameter(params, param, &outParams);
          }
        }
      }
      batchStmt->executeBatch(paramsBatch);
      return SQL_SUCCESS;
    } catch (const SQLException& sqle) {
      setException(sqle);
      return SQL_ERROR;
    }
  } else {
    // not supported operation
    std::ostringstream ostr;
    ostr << "BulkOperations for operation=" << operation;
    setException(GET_SQLEXCEPTION2(
        SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1, ostr.str().c_str()));
    return SQL_ERROR;
  }
}

SQLRETURN GFXDStatement::bindOutputField(SQLUSMALLINT columnNum,
    SQLSMALLINT targetType, SQLPOINTER targetValue, SQLLEN valueSize,
    SQLLEN *lenOrIndPtr) {
  if (targetValue != NULL) {
    m_outputFields[columnNum].set(targetType, targetValue, valueSize,
        lenOrIndPtr);
  } else {
    m_outputFields.erase(columnNum);
  }
  return SQL_SUCCESS;
}

/* SW:
SQLRETURN GFXDStatement::setPos(SQLUSMALLINT rowNumber, SQLUSMALLINT operation,
    SQLUSMALLINT lockType) {
  clearLastError();
  // TODO: need to verify other lock types
  if (lockType != SQL_LOCK_NO_CHANGE || rowNumber < 0) {
    // not supported lock type
    if (lockType != SQL_LOCK_NO_CHANGE) {
      std::ostringstream ostr;
      ostr << "setPos with lock type = " << lockType;
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::NOT_IMPLEMENTED_MSG, ostr.str().c_str()));
    } else {
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG, rowNumber,
          "ROW NUMBER"));
    }
    return SQL_ERROR;
  }

  try {
    if (!m_resultSet.isNull()) {
      SQLRETURN result = SQL_SUCCESS, result2;
      switch (operation) {
        case SQL_POSITION:
          if (rowNumber <= 0) {
            setException(GET_SQLEXCEPTION2(
                SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG, rowNumber,
                "ROW NUMBER"));
            return SQL_ERROR;
          }
          // position the cursor to the row number
          m_cursor = m_resultSet->begin(rowNumber);
          setRowStatus();
          break;
        case SQL_REFRESH:
          if (rowNumber > 0) {
            // position the cursor to the row number and refresh the row
            m_cursor = m_resultSet->begin(rowNumber);
            m_cursor.clearInsertRow();
            setRowStatus();
          } else if (m_resultSet->first()) {
            do {
              m_resultSet->refreshRow();
              setRowStatus();
            } while (m_resultSet->next());
          }
          break;
        case SQL_UPDATE:
          if (rowNumber > 0) {
            // position the cursor to the row number and refresh the row
            m_resultSet->absolute(rowNumber);
            m_resultSet->updateRow();
            setRowStatus();
          } else if (m_resultSet->first()) {
            do {
              m_resultSet->updateRow();
              setRowStatus();
            } while (m_resultSet->next());
          }
          break;
        case SQL_DELETE:
          if (rowNumber > 0) {
            // position the cursor to the row number and refresh the row
            m_resultSet->absolute(rowNumber);
            m_resultSet->deleteRow();
            setRowStatus();
          } else if (m_resultSet->first()) {
            do {
              m_resultSet->deleteRow();
              setRowStatus();
            } while (m_resultSet->next());
          }
          break;
        case SQL_ADD:
          bulkOperations(SQL_ADD);
          break;
        default:
          // unknown operation
          setException(GET_SQLEXCEPTION2(
              SQLStateMessage::INVALID_ATTRIBUTE_VALUE_MSG, operation,
              "operation type in setPos"));
          return SQL_ERROR;
      }
      result = fillOutputFields();
      result2 = handleWarnings(m_resultSet);
      return result == SQL_SUCCESS ? result2 : result;
    } else {
      // no open cursor
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_CURSOR_STATE_MSG2));
      return SQL_ERROR;
    }
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
  return SQL_NO_DATA;
}

SQLRETURN GFXDStatement::fetchScroll(SQLSMALLINT fetchOrientation,
    SQLINTEGER fetchOffset)
{
  clearLastError();

  try {
    if (m_resultSet != NULL) {
      SQLRETURN result = SQL_SUCCESS, result2;
      bool bRetVal = false;
      switch (fetchOrientation) {
        case SQL_FETCH_NEXT:
          bRetVal = m_resultSet->next();
          break;
        case SQL_FETCH_PRIOR:
          bRetVal = m_resultSet->previous();
          break;
        case SQL_FETCH_RELATIVE:
          bRetVal = m_resultSet->relative(fetchOffset);
          break;
        case SQL_FETCH_ABSOLUTE:
          bRetVal = m_resultSet->absolute(fetchOffset);
          break;
        case SQL_FETCH_FIRST:
          bRetVal = m_resultSet->first();
          break;
        case SQL_FETCH_LAST:
          bRetVal = m_resultSet->last();
          break;
        case SQL_FETCH_BOOKMARK:
        default:
          // not supported fetch orientation
          setException(GET_SQLEXCEPTION2(
              SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1,
              "SQL_FETCH_BOOKMARK in fetchScroll"));
          return SQL_ERROR;
      }
      if (bRetVal) {
        setRowStatus();
        // now bind the output fields
        result = fillOutputFields();
      }
      else {
        result = SQL_NO_DATA;
      }
      result2 = handleWarnings(m_resultSet);
      return result == SQL_SUCCESS ? result2 : result;
    }
    else {
      // no open cursor
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_CURSOR_STATE_MSG2));
      return SQL_ERROR;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return SQL_NO_DATA;
}

SQLRETURN GFXDStatement::next()
{
  clearLastError();
  try {
    if (m_resultSet != NULL) {
      SQLRETURN result = SQL_SUCCESS, r;
      if (m_resultSet->next()) {
        setRowStatus();
        // now bind the output fields
        result = fillOutputFields();
        r = handleWarnings(m_resultSet);
        return result == SQL_SUCCESS ? r : result;
      }
    }
    else {
      // no open cursor
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_CURSOR_STATE_MSG2));
      return SQL_ERROR;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return SQL_NO_DATA;
}

SQLRETURN GFXDStatement::getUpdateCount(SQLLEN* count) {
  // also called from handleDiagField so don't clear last error
  if (count != NULL) {
    // TODO: also handle for UPDATE/DELETE in SQLSetPos
    if (!m_result.isNull()) {
      *count = m_result->getUpdateCount();
    } else {
      *count = -1;
    }
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDStatement::getData(SQLUSMALLINT columnNum, SQLSMALLINT targetType,
    SQLPOINTER targetValue, SQLLEN valueSize, SQLLEN *lenOrIndPtr) {
  clearLastError();
  try {
    if (m_resultSet != NULL) {
      if (targetValue != NULL) {
        OutputField output;
        output.set(targetType, targetValue, valueSize, lenOrIndPtr);
        const SQLRETURN result = fillOutputField(columnNum, output);
        const SQLRETURN ret = handleWarnings(m_resultSet);
        return result == SQL_SUCCESS ? ret : result;
      }
    }
    else {
      // no open cursor
      setException(GET_SQLEXCEPTION2(
          SQLStateMessage::INVALID_CURSOR_STATE_MSG2));
      return SQL_ERROR;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return SQL_NO_DATA;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getAttributeT(SQLINTEGER attribute,
    SQLPOINTER valueBuffer, SQLINTEGER bufferLen, SQLINTEGER* valueLen)
{
  clearLastError();
  SQLRETURN ret = SQL_SUCCESS;
  SQLUINTEGER intResult = 0;
  try {
    switch (attribute) {
      case SQL_ATTR_APP_ROW_DESC:
        *(SQLPOINTER*)valueBuffer = m_ardDesc;
        if (valueLen == NULL) {
          valueLen = (SQLINTEGER*)malloc(sizeof(SQLINTEGER));
          *valueLen = sizeof(GFXDDescriptor);
        }
        else {
          *valueLen = sizeof(GFXDDescriptor);
        }
        bufferLen = SQL_IS_POINTER;
        break;

      case SQL_ATTR_IMP_ROW_DESC:
        *(SQLPOINTER*)valueBuffer = m_irdDesc;
        if (valueLen == NULL) {
          valueLen = (SQLINTEGER*)malloc(sizeof(SQLINTEGER));
          *valueLen = sizeof(GFXDDescriptor);
        }
        else {
          *valueLen = sizeof(GFXDDescriptor);
        }
        bufferLen = SQL_IS_POINTER;
        break;

      case SQL_ATTR_APP_PARAM_DESC:
        *(SQLPOINTER*)valueBuffer = m_apdDesc;
        if (valueLen == NULL) {
          valueLen = (SQLINTEGER*)malloc(sizeof(SQLINTEGER));
          *valueLen = sizeof(GFXDDescriptor);
        }
        else {
          *valueLen = sizeof(GFXDDescriptor);
        }
        bufferLen = SQL_IS_POINTER;
        break;

      case SQL_ATTR_IMP_PARAM_DESC:
        *(SQLPOINTER*)valueBuffer = m_ipdDesc;
        if (valueLen == NULL) {
          valueLen = (SQLINTEGER*)malloc(sizeof(SQLINTEGER));
          *valueLen = sizeof(GFXDDescriptor);
        }
        else {
          *valueLen = sizeof(GFXDDescriptor);
        }
        bufferLen = SQL_IS_POINTER;
        break;

      case SQL_ATTR_CONCURRENCY:
        if (m_resultSetConcurrency == -1) {
          intResult = SQL_CONCUR_READ_ONLY;
        }
        else {
          switch (m_resultSetConcurrency) {
            case java::sql::ResultSet::CONCUR_READ_ONLY:
              intResult = SQL_CONCUR_READ_ONLY;
              break;
            default:
              intResult = SQL_CONCUR_LOCK;
              break;
          }
        }
        *((SQLINTEGER*)valueBuffer) = intResult;
        if (valueLen != NULL) *valueLen = sizeof(intResult);
        break;
      case SQL_ATTR_CURSOR_SCROLLABLE:
        switch (m_resultSetType) {
          case java::sql::ResultSet::TYPE_FORWARD_ONLY:
            intResult = SQL_NONSCROLLABLE;
            break;
          case java::sql::ResultSet::TYPE_SCROLL_SENSITIVE:
          case java::sql::ResultSet::TYPE_SCROLL_INSENSITIVE:
            intResult = SQL_SCROLLABLE;
            break;
          default:
            intResult = SQL_NONSCROLLABLE;
            break;
        }
        *((SQLINTEGER*)valueBuffer) = intResult;
        if (valueLen != NULL) *valueLen = sizeof(intResult);
        break;
      case SQL_ATTR_CURSOR_SENSITIVITY:
        *((SQLINTEGER*)valueBuffer) =
            m_resultSetCursorSensitive ? SQL_INSENSITIVE : SQL_SENSITIVE;
        break;
      case SQL_ATTR_CURSOR_TYPE:
        switch (m_resultSetType) {
          case java::sql::ResultSet::TYPE_FORWARD_ONLY:
            intResult = SQL_CURSOR_FORWARD_ONLY;
            break;
          case java::sql::ResultSet::TYPE_SCROLL_SENSITIVE:
            intResult = SQL_CURSOR_DYNAMIC;
            break;
          case java::sql::ResultSet::TYPE_SCROLL_INSENSITIVE:
            intResult = SQL_CURSOR_STATIC;
            break;
          default:
            intResult = SQL_CURSOR_FORWARD_ONLY;
            break;
        }
        *((SQLINTEGER*)valueBuffer) = intResult;
        if (valueLen != NULL) *valueLen = sizeof(intResult);
        break;
      case SQL_ATTR_ENABLE_AUTO_IPD:
        *((SQLINTEGER*)valueBuffer) = SQL_FALSE;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_FETCH_BOOKMARK_PTR:
        *((SQLINTEGER*)valueBuffer) = m_bookmark;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_MAX_LENGTH:
        *((SQLINTEGER*)valueBuffer) = m_stmtAttrs.getMaxFieldSize();
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_MAX_ROWS:
        *((SQLINTEGER*)valueBuffer) = m_stmtAttrs.getMaxRows();
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_ROW_ARRAY_SIZE:
        *((SQLINTEGER*)valueBuffer) = m_rowArraySize;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_ROW_BIND_TYPE:
        *((SQLINTEGER*)valueBuffer) = m_bindingOrientation;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_ROW_STATUS_PTR:
        *(SQLPOINTER*)valueBuffer = m_rowStatusPtr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;
      case SQL_ATTR_ROWS_FETCHED_PTR:
        *(SQLPOINTER*)valueBuffer = m_fetchedRowsPtr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;
      case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        *(SQLPOINTER*)valueBuffer = m_bindOffsetPtr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;
      case SQL_ATTR_METADATA_ID:
        *((SQLINTEGER*)valueBuffer) =
            !m_argsAsIdentifiers ? SQL_FALSE : SQL_TRUE;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;

      case SQL_ATTR_NOSCAN:
        *((SQLINTEGER*)valueBuffer) =
            m_escapeProcessing ? SQL_NOSCAN_OFF : SQL_NOSCAN_ON;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;

      case SQL_ATTR_PARAM_BIND_TYPE:
        *((SQLINTEGER*)valueBuffer) = m_paramBindingOrientation;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_PARAMSET_SIZE:
        *((SQLINTEGER*)valueBuffer) = m_paramSetSize;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;
      case SQL_ATTR_PARAM_STATUS_PTR:
        *(SQLPOINTER*)valueBuffer = m_paramStatusArr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;
      case SQL_ATTR_PARAMS_PROCESSED_PTR:
        *(SQLPOINTER*)valueBuffer = m_paramsProcessedPtr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;

      case SQL_ATTR_QUERY_TIMEOUT:
        *((SQLLEN*)valueBuffer) = m_stmtAttrs.getTimeout();
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;

      case SQL_ATTR_KEYSET_SIZE:
        *((SQLLEN*)valueBuffer) = m_KeySetSize;
        if (valueLen != NULL) *valueLen = sizeof(*((SQLINTEGER*)valueBuffer));
        break;

      case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        *(SQLPOINTER*)valueBuffer = m_paramBindOffsetPtr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;

      case SQL_ATTR_PARAM_OPERATION_PTR:
        *(SQLPOINTER*)valueBuffer = m_paramOperationPtr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;

      case SQL_ATTR_ROW_OPERATION_PTR:
        *(SQLPOINTER*)valueBuffer = m_rowOperationPtr;
        bufferLen = SQL_IS_POINTER;
        if (valueLen != NULL) *valueLen = sizeof(*(SQLPOINTER*)valueBuffer);
        break;

        //TODO: Need to implement below attribs
      case SQL_ATTR_ASYNC_ENABLE:
      case SQL_ATTR_RETRIEVE_DATA:
      default:
        std::ostringstream ostr;
        ostr << "getAttribute for " << attribute;
        setException(GET_SQLEXCEPTION2(
            SQLStateMessage::FEATURE_NOT_IMPLEMENTED_MSG1, ostr.str().c_str()));
        ret = SQL_ERROR;
        break;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return ret;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::setAttributeT(SQLINTEGER attribute,
    SQLPOINTER valueBuffer, SQLINTEGER valueLen)
{
  SQLRETURN ret = SQL_SUCCESS;
  clearLastError();
  try {
    switch (attribute) {
      case SQL_ATTR_CONCURRENCY:
        if (isUnprepared()) {
          const SQLULEN intValue = (SQLULEN)(SQLBIGINT)valueBuffer;
          switch (intValue) {
            case SQL_CONCUR_READ_ONLY:
              m_resultSetConcurrency = java::sql::ResultSet::CONCUR_READ_ONLY;
              break;
            case SQL_CONCUR_LOCK:
              // mapping CONCUR_LOCK to JDBC CONCUR_UPDATABLE
              m_resultSetConcurrency = java::sql::ResultSet::CONCUR_UPDATABLE;
              break;
            case SQL_CONCUR_ROWVER:
            case SQL_CONCUR_VALUES:
              m_resultSetConcurrency = java::sql::ResultSet::CONCUR_UPDATABLE;
              setJavaException(
                  SQLState::newSQLException(SQLState::OptionValueChanged,
                      ExceptionSeverity::STATEMENT_SEVERITY,
                      SQLState::OptionValueChangedMessage,
                      "SQL_ATTR_CONCURRENCY", intValue, SQL_CONCUR_LOCK));
              ret = SQL_SUCCESS_WITH_INFO;
              break;
            default:
              setJavaException(
                  SQLState::newSQLException(SQLState::InvalidAttributeValue,
                      ExceptionSeverity::STATEMENT_SEVERITY,
                      SQLState::InvalidAttributeValueMessage, intValue,
                      "SQL_ATTR_CONCURRENCY"));
              ret = SQL_ERROR;
              break;
          }
        } else {
          setJavaException(
              SQLState::newSQLException(SQLState::OptionCannotBeSet,
                  ExceptionSeverity::STATEMENT_SEVERITY,
                  SQLState::OptionCannotBeSetForStatementMessage,
                  "SQL_ATTR_CONCURRENCY"));
          ret = SQL_ERROR;
        }
        break;

      case SQL_ATTR_CURSOR_SCROLLABLE:
        if (isUnprepared()) {
          const SQLULEN intValue = (SQLULEN)(SQLBIGINT)valueBuffer;
          switch (intValue) {
            case SQL_NONSCROLLABLE:
              m_resultSetType = java::sql::ResultSet::TYPE_FORWARD_ONLY;
              break;
            case SQL_SCROLLABLE:
              m_resultSetType =
                  m_resultSetCursorSensitive ? java::sql::ResultSet::TYPE_SCROLL_SENSITIVE :
                      java::sql::ResultSet::TYPE_SCROLL_INSENSITIVE;
              break;
            default:
              setJavaException(
                  SQLState::newSQLException(SQLState::InvalidAttributeValue,
                      ExceptionSeverity::STATEMENT_SEVERITY,
                      SQLState::InvalidAttributeValueMessage, intValue,
                      "SQL_ATTR_CURSOR_SCROLLABLE"));
              ret = SQL_ERROR;
              break;
          }
        }
        else {
          setJavaException(
              SQLState::newSQLException(SQLState::OptionCannotBeSet,
                  ExceptionSeverity::STATEMENT_SEVERITY,
                  SQLState::OptionCannotBeSetForStatementMessage,
                  "SQL_ATTR_CURSOR_SCROLLABLE"));
          ret = SQL_ERROR;
        }
        break;

      case SQL_ATTR_CURSOR_SENSITIVITY:
        if (isUnprepared()) {
          const SQLULEN intValue = (SQLULEN)(SQLBIGINT)valueBuffer;
          switch (intValue) {
            case SQL_UNSPECIFIED:
              // nothing to be done
              break;
            case SQL_INSENSITIVE:
              m_resultSetType = java::sql::ResultSet::TYPE_SCROLL_INSENSITIVE;
              m_resultSetCursorSensitive = false;
              break;
            case SQL_SENSITIVE:
              // forward-only cursors are already sensitive to changes
              if (m_resultSetType != java::sql::ResultSet::TYPE_FORWARD_ONLY) {
                m_resultSetType = java::sql::ResultSet::TYPE_SCROLL_SENSITIVE;
              }
              m_resultSetCursorSensitive = true;
              break;
            default:
              setJavaException(
                  SQLState::newSQLException(SQLState::InvalidAttributeValue,
                      ExceptionSeverity::STATEMENT_SEVERITY,
                      SQLState::InvalidAttributeValueMessage, intValue,
                      "SQL_ATTR_CURSOR_SENSITIVITY"));
              ret = SQL_ERROR;
              break;
          }
        }
        else {
          setJavaException(
              SQLState::newSQLException(SQLState::OptionCannotBeSet,
                  ExceptionSeverity::STATEMENT_SEVERITY,
                  SQLState::OptionCannotBeSetForStatementMessage,
                  "SQL_ATTR_CURSOR_SENSITIVITY"));
          ret = SQL_ERROR;
        }
        break;

      case SQL_ATTR_CURSOR_TYPE:
        if (isUnprepared()) {
          const SQLULEN intValue = (SQLULEN)(SQLBIGINT)valueBuffer;
          switch (intValue) {
            case SQL_CURSOR_FORWARD_ONLY:
              m_resultSetType = java::sql::ResultSet::TYPE_FORWARD_ONLY;
              break;
            case SQL_CURSOR_STATIC:
              m_resultSetType = java::sql::ResultSet::TYPE_SCROLL_INSENSITIVE;
              break;
            case SQL_CURSOR_DYNAMIC:
              m_resultSetType = java::sql::ResultSet::TYPE_SCROLL_SENSITIVE;
              break;
            case SQL_CURSOR_KEYSET_DRIVEN:
              m_resultSetType = java::sql::ResultSet::TYPE_SCROLL_INSENSITIVE;
              setJavaException(
                  SQLState::newSQLException(SQLState::OptionValueChanged,
                      ExceptionSeverity::STATEMENT_SEVERITY,
                      SQLState::OptionValueChangedMessage,
                      "SQL_ATTR_CURSOR_TYPE", intValue, SQL_CURSOR_STATIC));
              ret = SQL_SUCCESS_WITH_INFO;
              break;
            default:
              setJavaException(
                  SQLState::newSQLException(SQLState::InvalidAttributeValue,
                      ExceptionSeverity::STATEMENT_SEVERITY,
                      SQLState::InvalidAttributeValueMessage, intValue,
                      "SQL_ATTR_CURSOR_TYPE"));
              ret = SQL_ERROR;
              break;
          }
        }
        else {
          setJavaException(
              SQLState::newSQLException(SQLState::OptionCannotBeSet,
                  ExceptionSeverity::STATEMENT_SEVERITY,
                  SQLState::OptionCannotBeSetForStatementMessage,
                  "SQL_ATTR_CURSOR_TYPE"));
          ret = SQL_ERROR;
        }
        break;

      case SQL_ATTR_ENABLE_AUTO_IPD: {
        // ResultSetMetadata is always available in GemFireXD after prepare
        const SQLULEN intValue = (SQLULEN)(SQLBIGINT)valueBuffer;
        switch (intValue) {
          case SQL_TRUE:
          case SQL_FALSE:
            // nothing to be done
            break;
          default:
            setJavaException(
                SQLState::newSQLException(SQLState::InvalidAttributeValue,
                    ExceptionSeverity::STATEMENT_SEVERITY,
                    SQLState::InvalidAttributeValueMessage, intValue,
                    "SQL_ATTR_ENABLE_AUTO_IPD"));
            ret = SQL_ERROR;
            break;
        }
        break;
      }

      case SQL_ATTR_FETCH_BOOKMARK_PTR:
        // our bookmark is just an offset of current row
        m_bookmark = (SQLLEN)(SQLBIGINT)valueBuffer;
        break;

      case SQL_ATTR_MAX_LENGTH:
        if (isPrepared()) {
          const SQLLEN intValue = (SQLLEN)(SQLBIGINT)valueBuffer;
          if (m_resultSet == NULL) {
            switch (m_statementType) {
              case PREPARED_STATEMENT:
                m_stmt.pstmt->setMaxFieldSize(intValue);
                break;
              case NORMAL_STATEMENT:
                m_stmt.stmt->setMaxFieldSize(intValue);
                break;
              case CALLABLE_STATEMENT:
                m_stmt.cstmt->setMaxFieldSize(intValue);
                break;
            }
            m_stmtAttrs.setMaxFieldSize(intValue);
          }
          else {
            setJavaException(
                SQLState::newSQLException(SQLState::OptionValueChanged,
                    ExceptionSeverity::STATEMENT_SEVERITY,
                    SQLState::OptionValueChangedMessage, "SQL_ATTR_MAX_LENGTH",
                    intValue, -1));
            ret = SQL_SUCCESS_WITH_INFO;
          }
        }
        else {
          m_stmtAttrs.setMaxFieldSize((SQLLEN)(SQLBIGINT)valueBuffer);
        }
        break;

      case SQL_ATTR_MAX_ROWS:
        if (isPrepared()) {
          const SQLLEN intValue = (SQLLEN)(SQLBIGINT)valueBuffer;
          if (m_resultSet == NULL) {
            switch (m_statementType) {
              case PREPARED_STATEMENT:
                m_stmt.pstmt->setMaxRows(intValue);
                break;
              case NORMAL_STATEMENT:
                m_stmt.stmt->setMaxRows(intValue);
                break;
              case CALLABLE_STATEMENT:
                m_stmt.cstmt->setMaxRows(intValue);
                break;
            }
            m_stmtAttrs.setMaxRows(intValue);
          }
          else {
            setJavaException(
                SQLState::newSQLException(SQLState::OptionValueChanged,
                    ExceptionSeverity::STATEMENT_SEVERITY,
                    SQLState::OptionValueChangedMessage, "SQL_ATTR_MAX_ROWS",
                    intValue, -1));
            ret = SQL_SUCCESS_WITH_INFO;
          }
        }
        else {
          m_stmtAttrs.setMaxRows((SQLLEN)(SQLBIGINT)valueBuffer);
        }
        break;
      case SQL_ATTR_METADATA_ID:
        m_argsAsIdentifiers =
            ((SQLULEN)(SQLBIGINT)valueBuffer) == SQL_FALSE ? false : true;
        break;
      case SQL_ATTR_ROW_ARRAY_SIZE:
        m_rowArraySize = (SQLLEN)(SQLBIGINT)valueBuffer;
        break;
      case SQL_ATTR_ROW_BIND_TYPE:
        m_bindingOrientation = (SQLLEN)(SQLBIGINT)valueBuffer;
        break;
      case SQL_ATTR_ROW_STATUS_PTR:
        m_rowStatusPtr = (SQLUSMALLINT*)valueBuffer;
        break;
      case SQL_ATTR_ROWS_FETCHED_PTR:
        m_fetchedRowsPtr = (SQLLEN*)valueBuffer;
        break;
      case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        m_bindOffsetPtr = (SQLUSMALLINT*)valueBuffer;
        break;
      case SQL_ATTR_NOSCAN:
        m_stmtAttrs.setEscapeProcessing(
            ((SQLULEN)(SQLBIGINT)valueBuffer) == SQL_NOSCAN_OFF);
        break;
      case SQL_ATTR_PARAM_BIND_TYPE:
        m_paramBindingOrientation = (SQLLEN)(SQLBIGINT)valueBuffer;
        break;
      case SQL_ATTR_PARAMSET_SIZE:
        m_paramSetSize = (SQLLEN)(SQLBIGINT)valueBuffer;
        break;
      case SQL_ATTR_PARAM_STATUS_PTR:
        m_paramStatusArr = (SQLUSMALLINT*)valueBuffer;
        break;
      case SQL_ATTR_PARAMS_PROCESSED_PTR:
        m_paramsProcessedPtr = (SQLLEN*)valueBuffer;
        break;
      case SQL_ATTR_QUERY_TIMEOUT:
        m_stmtAttrs.setTimeout((uint32_t)(SQLBIGINT)valueBuffer);
        break;

      case SQL_ATTR_KEYSET_SIZE:
        m_KeySetSize = (SQLLEN)(SQLBIGINT)valueBuffer;
        break;

      case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        m_paramBindOffsetPtr = (SQLLEN*)valueBuffer;
        break;

      case SQL_ATTR_PARAM_OPERATION_PTR:
        m_paramOperationPtr = (SQLUSMALLINT*)valueBuffer;
        break;

      case SQL_ATTR_ROW_OPERATION_PTR:
        m_rowOperationPtr = (SQLUSMALLINT*)valueBuffer;
        break;

      case SQL_ATTR_APP_PARAM_DESC:
      case SQL_ATTR_APP_ROW_DESC:
      case SQL_ATTR_ASYNC_ENABLE:
      case SQL_ATTR_RETRIEVE_DATA:

      default:
        setJavaException(
            SQLState::newSQLException(SQLState::FeatureNotImplemented,
                ExceptionSeverity::STATEMENT_SEVERITY,
                SQLState::FeatureNotImplementedMessage));
        ret = SQL_ERROR;
        break;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return ret;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getResultColumnDescriptorT(SQLUSMALLINT columnNumber,
    CHAR_TYPE* columnName, SQLSMALLINT bufferLength, SQLSMALLINT* nameLength,
    SQLSMALLINT* dataType, SQLULEN* columnSize, SQLSMALLINT* decimalDigits,
    SQLSMALLINT* nullable)
{
  clearLastError();
  try {
    java::sql::ResultSetMetaData* rsmd;
    if (m_resultSet != NULL) {
      if (m_resultSetMetaData != NULL) {
        rsmd = m_resultSetMetaData;
      }
      else {
        rsmd = m_resultSetMetaData = m_resultSet->getMetaData();
      }
    }
    else {
      switch (m_statementType) {
        case PREPARED_STATEMENT:
          rsmd = m_stmt.pstmt->getMetaData();
          break;
        case CALLABLE_STATEMENT:
          rsmd = m_stmt.cstmt->getMetaData();
          break;
        default:
          // no open cursor
          setJavaException(
              SQLState::newSQLException(SQLState::InvalidCursorState,
                  ExceptionSeverity::STATEMENT_SEVERITY,
                  SQLState::InvalidCursorStateMessage2));
          return SQL_ERROR;
      }
    }
    if (columnNumber >= 1) {
      SQLRETURN result = SQL_SUCCESS;
      const int numColumns = rsmd->getColumnCount();
      if (columnNumber <= numColumns) {
        if (columnName != NULL) {
          if (bufferLength >= 0) {
            jstring colName = rsmd->getColumnName(columnNumber);
            if (colName != NULL) {
              ::memset(columnName, 0, bufferLength);
              if (StringFunctions::getNativeString(colName, columnName,
                  bufferLength, nameLength)) {
                result = SQL_SUCCESS_WITH_INFO;
                setJavaException(
                    SQLState::newSQLException(SQLState::StringTruncated,
                        ExceptionSeverity::STATEMENT_SEVERITY,
                        SQLState::StringTruncatedMessage, "Column Name",
                        bufferLength - 1));
              }
            }
            else {
              *columnName = 0;
              if (nameLength != NULL) {
                *nameLength = 0;
              }
            }
          }
          else {
            result = GFXDHandleBase::errorInvalidBufferLength(bufferLength,
                "Column Name", this);
          }
        }
        if (dataType != NULL) {
          *dataType = rsmd->getColumnType(columnNumber);
        }
        if (columnSize != NULL) {
          *columnSize = rsmd->getColumnDisplaySize(columnNumber);
        }
        if (decimalDigits != NULL) {
          *decimalDigits = rsmd->getPrecision(columnNumber);
        }
        if (nullable != NULL) {
          *nullable = rsmd->isNullable(columnNumber);
        }
        return result;
      }
      setJavaException(
          SQLState::newSQLException(SQLState::InvalidDescriptorIndex,
              ExceptionSeverity::STATEMENT_SEVERITY,
              SQLState::InvalidDescriptorIndexMessage, columnNumber,
              "column number in result set"));
      return SQL_ERROR;
    }
    // TODO: handle SQL_ATTR_USE_BOOKMARKS below
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidDescriptorIndex,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidDescriptorIndexMessage, columnNumber,
            "result set column number"));
    return SQL_ERROR;
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  setJavaException(
      SQLState::newSQLException(SQLState::NoResultSet,
          ExceptionSeverity::STATEMENT_SEVERITY,
          SQLState::NoResultSetMessage));
  return SQL_ERROR;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getColumnAttributeT(SQLUSMALLINT columnNumber,
    SQLUSMALLINT fieldId, SQLPOINTER charAttribute, SQLSMALLINT bufferLength,
    SQLSMALLINT* stringLength, SQLLEN* numericAttribute)
{
  clearLastError();
  try {
    if (m_resultSet != NULL) {
      SQLRETURN result = SQL_SUCCESS;
      if (m_resultSetMetaData == NULL) {
        m_resultSetMetaData = m_resultSet->getMetaData();
      }
      java::sql::ResultSetMetaData* rsmd = m_resultSetMetaData;
      const int numColumns = rsmd->getColumnCount();
      // TODO: handle SQL_ATTR_USE_BOOKMARKS below for columnNumber == 0
      if (columnNumber >= 1 && columnNumber <= numColumns) {
        jstring jstringAttribute = NULL;
        const char* stringAttribute = NULL;
        const char* descType = NULL;
        switch (fieldId) {
          case SQL_DESC_AUTO_UNIQUE_VALUE:
            if (numericAttribute != NULL) {
              *numericAttribute =
                  rsmd->isAutoIncrement(columnNumber) ? SQL_TRUE : SQL_FALSE;
            }
            break;
          case SQL_DESC_BASE_COLUMN_NAME:
            descType = "Column Name";
          case SQL_DESC_NAME:
          case SQL_COLUMN_NAME:
            if (descType == NULL) {
              descType = "Column Name or Alias";
            }
            if (charAttribute != NULL) {
              jstringAttribute = rsmd->getColumnName(columnNumber);
            }
            break;
          case SQL_DESC_BASE_TABLE_NAME:
            if (charAttribute != NULL) {
              jstringAttribute = rsmd->getTableName(columnNumber);
              descType = "Table Name";
            }
            break;
          case SQL_DESC_CASE_SENSITIVE:
            if (numericAttribute != NULL) {
              *numericAttribute =
                  rsmd->isCaseSensitive(columnNumber) ? SQL_TRUE : SQL_FALSE;
            }
            break;
          case SQL_DESC_CATALOG_NAME:
            if (charAttribute != NULL) {
              jstringAttribute = rsmd->getCatalogName(columnNumber);
              descType = "Catalog Name";
            }
            break;
          case SQL_DESC_CONCISE_TYPE:
            if (numericAttribute != NULL) {
              *numericAttribute = getTypeFromJDBCType(
                  rsmd->getColumnType(columnNumber), sizeof(CHAR_TYPE) == 1);
            }
            break;
          case SQL_DESC_COUNT:
          case SQL_COLUMN_COUNT:
            if (numericAttribute != NULL) {
              *numericAttribute = rsmd->getColumnCount();
            }
            break;
          case SQL_DESC_DISPLAY_SIZE:
            if (numericAttribute != NULL) {
              *numericAttribute = rsmd->getColumnDisplaySize(columnNumber);
            }
            break;
          case SQL_DESC_FIXED_PREC_SCALE:
            if (numericAttribute != NULL) {
              switch (rsmd->getColumnType(columnNumber)) {
                case java::sql::Types::DECIMAL:
                case java::sql::Types::NUMERIC:
                  *numericAttribute =
                      rsmd->getScale(columnNumber) > 0 ? SQL_TRUE : SQL_FALSE;
                  break;
                default:
                  *numericAttribute = SQL_FALSE;
                  break;
              }
            }
            break;
          case SQL_DESC_LABEL:
            if (charAttribute != NULL) {
              jstringAttribute = rsmd->getColumnLabel(columnNumber);
              descType = "Column Label";
            }
            break;
          case SQL_DESC_LENGTH:
          case SQL_DESC_PRECISION:
          case SQL_COLUMN_LENGTH:
          case SQL_COLUMN_PRECISION:
            if (numericAttribute != NULL) {
              *numericAttribute = rsmd->getPrecision(columnNumber);
            }
            break;
          case SQL_DESC_LITERAL_PREFIX:
            descType = "Literal Prefix";
          case SQL_DESC_LITERAL_SUFFIX:
            if (descType == NULL) {
              descType = "Literal Suffix";
            }
            if (charAttribute != NULL) {
              switch (rsmd->getColumnType(columnNumber)) {
                case java::sql::Types::CHAR:
                case java::sql::Types::VARCHAR:
                case java::sql::Types::LONGVARCHAR:
                case java::sql::Types::CLOB:
                  stringAttribute = "'";
                  break;
                case java::sql::Types::BINARY:
                case java::sql::Types::VARBINARY:
                case java::sql::Types::LONGVARBINARY:
                  stringAttribute =
                      (fieldId == SQL_DESC_LITERAL_PREFIX) ? "X'" : "'";
                  break;
                case java::sql::Types::DATE:
                  stringAttribute =
                      (fieldId == SQL_DESC_LITERAL_PREFIX) ? "DATE'" : "'";
                  break;
                case java::sql::Types::TIME:
                  stringAttribute =
                      (fieldId == SQL_DESC_LITERAL_PREFIX) ? "TIME'" : "'";
                  break;
                case java::sql::Types::TIMESTAMP:
                  stringAttribute =
                      (fieldId == SQL_DESC_LITERAL_PREFIX) ? "TIMESTAMP'" :
                          "'";
                  break;
                case JDBC_TYPE_SQLXML:
                  stringAttribute =
                      (fieldId == SQL_DESC_LITERAL_PREFIX) ? "XMLPARSE (DOCUMENT '" :
                          "' PRESERVE WHITESPACE)";
                  break;
                default:
                  stringAttribute = "";
                  break;
              }
            }
            break;
          case SQL_DESC_LOCAL_TYPE_NAME:
            descType = "Column Local Type Name";
          case SQL_DESC_TYPE_NAME:
            if (descType == NULL) {
              descType = "Column Type Name";
            }
            if (charAttribute != NULL) {
              jstringAttribute = rsmd->getColumnTypeName(columnNumber);
            }
            break;
          case SQL_DESC_NULLABLE:
          case SQL_COLUMN_NULLABLE:
            if (numericAttribute != NULL) {
              switch (rsmd->isNullable(columnNumber)) {
                case java::sql::ResultSetMetaData::columnNoNulls:
                  *numericAttribute = SQL_NO_NULLS;
                  break;
                case java::sql::ResultSetMetaData::columnNullable:
                  *numericAttribute = SQL_NULLABLE;
                  break;
                default:
                  *numericAttribute = SQL_NULLABLE_UNKNOWN;
                  break;
              }
            }
            break;
          case SQL_DESC_NUM_PREC_RADIX:
            if (numericAttribute != NULL) {
              switch (rsmd->getColumnType(columnNumber)) {
                case java::sql::Types::BIGINT:
                case java::sql::Types::BOOLEAN:
                case java::sql::Types::DATE:
                case java::sql::Types::DECIMAL:
                case java::sql::Types::INTEGER:
                case java::sql::Types::NUMERIC:
                case java::sql::Types::SMALLINT:
                case java::sql::Types::TIME:
                case java::sql::Types::TIMESTAMP:
                case java::sql::Types::TINYINT:
                  *numericAttribute = 10;
                  break;
                case java::sql::Types::DOUBLE:
                case java::sql::Types::FLOAT:
                case java::sql::Types::REAL:
                  *numericAttribute = 2;
                  break;
                default:
                  *numericAttribute = 0;
                  break;
              }
            }
            break;
          case SQL_DESC_OCTET_LENGTH:
            if (numericAttribute != NULL) {
              SQLLEN precision = rsmd->getPrecision(columnNumber);
              switch (rsmd->getColumnType(columnNumber)) {
                case java::sql::Types::CHAR:
                case java::sql::Types::VARCHAR:
                case java::sql::Types::LONGVARCHAR:
                case java::sql::Types::CLOB:
                case JDBC_TYPE_SQLXML:
                  if (precision <= (0x7fffffff >> 1)) {
                    precision <<= 1;
                  }
                  break;
              }
              *numericAttribute = precision;
            }
            break;
          case SQL_DESC_SCALE:
          case SQL_COLUMN_SCALE:
            if (numericAttribute != NULL) {
              *numericAttribute = rsmd->getScale(columnNumber);
            }
            break;
          case SQL_DESC_SCHEMA_NAME:
            if (charAttribute != NULL) {
              jstringAttribute = rsmd->getSchemaName(columnNumber);
              descType = "Schema Name";
            }
            break;
          case SQL_DESC_TABLE_NAME:
            if (charAttribute != NULL) {
              jstringAttribute = rsmd->getTableName(columnNumber);
              descType = "Table Name";
            }
            break;
          case SQL_DESC_TYPE: {
            if (numericAttribute != NULL) {
              const jint jdbcType = rsmd->getColumnType(columnNumber);
              switch (jdbcType) {
                case java::sql::Types::DATE:
                case java::sql::Types::TIME:
                case java::sql::Types::TIMESTAMP:
                  *numericAttribute = SQL_DATETIME;
                  break;
                default:
                  *numericAttribute = getTypeFromJDBCType(jdbcType,
                      sizeof(CHAR_TYPE) == 1);
                  break;
              }
            }
            break;
          }
          case SQL_DESC_SEARCHABLE:
            if (numericAttribute != NULL) {
              switch (rsmd->getColumnType(columnNumber)) {
                case java::sql::Types::CHAR:
                case java::sql::Types::VARCHAR:
                case java::sql::Types::LONGVARCHAR:
                  *numericAttribute = SQL_PRED_SEARCHABLE;
                  break;
                case java::sql::Types::CLOB:
                case java::sql::Types::BLOB:
                case JDBC_TYPE_SQLXML:
                  *numericAttribute = SQL_PRED_CHAR;
                  break;
                case java::sql::Types::JAVA_OBJECT:
                  *numericAttribute = SQL_PRED_NONE;
                  break;
                default:
                  *numericAttribute = SQL_PRED_BASIC;
                  break;
              }
            }
            break;
          case SQL_DESC_UNNAMED:
            if (numericAttribute != NULL) {
              jstring columnName = rsmd->getColumnName(columnNumber);
              *numericAttribute =
                  columnName != NULL && columnName->length() > 0 ? SQL_NAMED :
                      SQL_UNNAMED;
            }
            break;
          case SQL_DESC_UNSIGNED:
            if (numericAttribute != NULL) {
              *numericAttribute =
                  rsmd->isSigned(columnNumber) ? SQL_FALSE : SQL_TRUE;
            }
            break;
          case SQL_DESC_UPDATABLE:
            if (numericAttribute != NULL) {
              if (rsmd->isReadOnly(columnNumber)) {
                *numericAttribute = SQL_ATTR_READONLY;
              }
              else if (rsmd->isDefinitelyWritable(columnNumber)) {
                *numericAttribute = SQL_ATTR_WRITE;
              }
              else if (rsmd->isWritable(columnNumber)) {
                *numericAttribute = SQL_DESC_UPDATABLE;
              }
              else {
                *numericAttribute = SQL_ATTR_READWRITE_UNKNOWN;
              }
            }
            break;
          default:
            setJavaException(
                SQLState::newSQLException(SQLState::InvalidDescriptorFieldId,
                    ExceptionSeverity::STATEMENT_SEVERITY,
                    SQLState::InvalidDescriptorFieldIdMessage, fieldId));
            return SQL_ERROR;
        }
        if (jstringAttribute != NULL || stringAttribute != NULL) {
          bool truncated;
          SQLINTEGER totalLen = 0;
          SQLINTEGER* totalLenp = stringLength != NULL ? &totalLen : NULL;
          if (jstringAttribute != NULL) {
            if (sizeof(CHAR_TYPE) == 1) {
              truncated = StringFunctions::getNativeString(jstringAttribute,
                  (SQLCHAR*)charAttribute, bufferLength, totalLenp);
            }
            else {
              truncated = StringFunctions::getNativeString(jstringAttribute,
                  (SQLWCHAR*)charAttribute, bufferLength, totalLenp);
            }
          }
          else {
            if (sizeof(CHAR_TYPE) == 1) {
              truncated = StringFunctions::copyString(
                  (const SQLCHAR*)stringAttribute, SQL_NTS,
                  (SQLCHAR*)charAttribute, bufferLength, totalLenp);
            }
            else {
              truncated = StringFunctions::copyString(
                  (const SQLCHAR*)stringAttribute, SQL_NTS,
                  (SQLWCHAR*)charAttribute, bufferLength, totalLenp);
            }
          }

          if (stringLength != NULL) {
              *stringLength = totalLen * sizeof(CHAR_TYPE);
            }
          if (truncated) {
            result = SQL_SUCCESS_WITH_INFO;
            setJavaException(
                SQLState::newSQLException(SQLState::StringTruncated,
                    ExceptionSeverity::STATEMENT_SEVERITY,
                    SQLState::StringTruncatedMessage, descType,
                    bufferLength - 1));
          }
        }
        return result;
      }
      else {
        setJavaException(
            SQLState::newSQLException(SQLState::InvalidDescriptorIndex,
                ExceptionSeverity::STATEMENT_SEVERITY,
                SQLState::InvalidDescriptorIndexMessage, columnNumber,
                "result set column number"));
        return SQL_ERROR;
      }
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  setJavaException(
      SQLState::newSQLException(SQLState::NoResultSet,
          ExceptionSeverity::STATEMENT_SEVERITY,
          SQLState::NoResultSetMessage));
  return SQL_ERROR;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getCursorNameT(CHAR_TYPE* cursorName,
    SQLSMALLINT bufferLength, SQLSMALLINT* nameLength)
{
  clearLastError();
  SQLRETURN result = SQL_SUCCESS;
  try {
    if (cursorName != NULL) {
      if (bufferLength >= 0) {
        jstring jcursorName =
            m_resultSet != NULL ? m_resultSet->getCursorName() : NULL;
        if (jcursorName != NULL) {
          if (StringFunctions::getNativeString(jcursorName, cursorName,
              bufferLength, nameLength)) {
            result = SQL_SUCCESS_WITH_INFO;
            setJavaException(
                SQLState::newSQLException(SQLState::StringTruncated,
                    ExceptionSeverity::STATEMENT_SEVERITY,
                    SQLState::StringTruncatedMessage, "Cursor Name",
                    bufferLength - 1));
          }
        }
        else {
          *cursorName = 0;
          if (nameLength != NULL) {
            *nameLength = 0;
          }
        }
      }
      else {
        result = GFXDHandleBase::errorInvalidBufferLength(bufferLength,
            "Cursor Name", this);
      }
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return result;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::setCursorNameT(CHAR_TYPE* cursorName,
    SQLSMALLINT nameLength)
{
  clearLastError();
  SQLRETURN result = SQL_SUCCESS;
  try {
    if (cursorName != NULL) {
      jstring jcursorName = StringFunctions::getJavaString(cursorName,
          nameLength);
      if (jcursorName != NULL) {
        switch (m_statementType) {
          case PREPARED_STATEMENT:
            if (m_stmt.pstmt != NULL) {
              m_stmt.pstmt->setCursorName(jcursorName);
            }
            break;
          case NORMAL_STATEMENT:
            if (m_stmt.stmt != NULL) {
              m_stmt.stmt->setCursorName(jcursorName);
            }
            break;
          case CALLABLE_STATEMENT:
            if (m_stmt.cstmt != NULL) {
              m_stmt.cstmt->setCursorName(jcursorName);
            }
            break;
        }
      }
      else {
        result = GFXDHandleBase::errorInvalidBufferLength(nameLength, "Cursor Name",
            this);
      }
    }
    else {
      setJavaException(
          SQLState::newSQLException(SQLState::NullHandle,
              ExceptionSeverity::STATEMENT_SEVERITY,
              SQLState::NullCursorMessage));
      return SQL_ERROR;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return result;
}
SW: */

/**
 * State constants used by the FSM inside getStatementToken.
 * @see GFXDStatement#getStatementToken
 */
const int OUTSIDE = 0;
const int INSIDE_SIMPLECOMMENT = 1;
const int INSIDE_BRACKETED_COMMENT = 2;

/**
 * Ported from Derby client driver's Statement.getStatementToken()
 */
/* SW:
std::string GFXDStatement::getStatementToken(const std::string& sqlText) {
  int bracketNesting = 0;
  int state = OUTSIDE;
  int idx = 0;
  std::string tokenFound = NULL;
  char next;

  const size_t len = sqlText.length();
  const char* sqlChars = sqlText.c_str();
  while (idx < len && tokenFound == NULL) {
    next = sqlChars[idx];

    switch (state) {
      case OUTSIDE:
        switch (next) {
          case '\n':
          case '\t':
          case '\r':
          case '\f':
          case ' ':
          case '(':
          case '{': // JDBC escape characters
          case '=': //
          case '?': //
            idx++;
            break;
          case '/':
            if (idx == len - 1) {
              // no more characters, so this is the token
              tokenFound = '/';
            }
            else if (sqlChars[idx + 1] == '*') {
              state = INSIDE_BRACKETED_COMMENT;
              bracketNesting++;
              idx++; // step two chars
            }
            idx++;
            break;
          case '-':
            if (idx == len - 1) {
              // no more characters, so this is the token
              tokenFound = '/';
            }
            else if (sqlChars[idx + 1] == '-') {
              state = INSIDE_SIMPLECOMMENT;
              idx++;
            }
            idx++;
            break;
          default: {
            // a token found
            int j;
            for (j = idx; j < len; j++) {
              char ch = sqlChars[j];
              if (!::isalpha(ch)) {
                // first non-token char found
                break;
              }
            }
            // return initial token if one is found, or the entire string
            // otherwise
            tokenFound =
                (j > idx) ? sqlText.substr(idx, j - idx) : sqlText.substr(idx);
            break;
          }
        }
        break;
      case INSIDE_SIMPLECOMMENT:
        switch (next) {
          case '\n':
          case '\r':
          case '\f':
            state = OUTSIDE;
            idx++;
            break;
          default:
            // anything else inside a simple comment is ignored
            idx++;
            break;
        }
        break;
      case INSIDE_BRACKETED_COMMENT:
        switch (next) {
          case '/':
            if (idx != len - 1 && sqlChars[idx + 1] == '*') {
              bracketNesting++;
              idx++; // step two chars
            }
            idx++;
            break;
          case '*':
            if (idx != len - 1 && sqlChars[idx + 1] == '/') {
              bracketNesting--;
              if (bracketNesting == 0) {
                state = OUTSIDE;
                idx++; // step two chars
              }
            }
            idx++;
            break;
          default:
            idx++;
            break;
        }

        break;
      default:
        break;
    }
  }
  return tokenFound;
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getTablesT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3, CHAR_TYPE* tableTypes,
    SQLSMALLINT nameLength4)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    jstringArray jtableTypes = NULL;
    if (tableTypes != NULL) {
      const int allTypesLen = sizeof(SQL_ALL_TABLE_TYPES) - 1;
      // check for "%" pattern for all types that corresponding to NULL
      // in JDBC
      if ((nameLength4 == SQL_NTS || nameLength4 == allTypesLen)
          && StringFunctions::strncmp(SQL_ALL_TABLE_TYPES, tableTypes,
              allTypesLen) == 0) {
        jtableTypes = NULL;
      }
      else {
        jstring jstr = StringFunctions::getJavaString(tableTypes, nameLength4);
        if (!jstr->isEmpty()) {
          // split the table types on "," then remove surrounding single quotes
          jtableTypes = jstr->split(GFXDDefaults::COMMA);
          jstring* ttypes = elements(jtableTypes);
          for (int index = 0; index < jtableTypes->length; index++) {
            jstr = ttypes[index];
            if (jstr->charAt(0) == '\''
                && jstr->charAt(jstr->length() - 1) == '\'') {
              ttypes[index] = jstr->substring(1, jstr->length() - 1);
            }
          }
        }
      }
    }
    else {
      jtableTypes = NULL;
    }
    setResultSet(m_conn->getMetaData()->getTables(jcatalogName, jschemaName,
        jtableName, jtableTypes));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getTablePrivilegesT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    setResultSet(m_conn->getMetaData()->getTablePrivileges(jcatalogName,
        jschemaName, jtableName));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getColumnsT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3, CHAR_TYPE* columnName,
    SQLSMALLINT nameLength4)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    jstring jcolumnName =
        columnName != NULL ? StringFunctions::getJavaString(columnName,
            nameLength4) :
            NULL;
    setResultSet(m_conn->getMetaData()->getColumns(jcatalogName, jschemaName,
        jtableName, jcolumnName));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getSpecialColumnsT(SQLUSMALLINT idType,
    CHAR_TYPE *catalogName, SQLSMALLINT nameLength1, CHAR_TYPE *schemaName,
    SQLSMALLINT nameLength2, CHAR_TYPE *tableName, SQLSMALLINT nameLength3,
    SQLUSMALLINT scope, SQLUSMALLINT nullable)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;
    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    if (idType == SQL_BEST_ROWID) {
      jboolean jNullable = false;
      if (nullable == SQL_NULLABLE) jNullable = true;
      setResultSet(m_conn->getMetaData()->getBestRowIdentifier(jcatalogName,
          jschemaName, jtableName, (jint)scope, jNullable));
    }
    else if (idType == SQL_ROWVER) {
      setResultSet(m_conn->getMetaData()->getVersionColumns(jcatalogName,
          jschemaName, jtableName));
    }
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getColumnPrivilegesT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3, CHAR_TYPE* columnName,
    SQLSMALLINT nameLength4)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    jstring jcolumnName =
        columnName != NULL ? StringFunctions::getJavaString(columnName,
            nameLength4) :
            NULL;
    setResultSet(m_conn->getMetaData()->getColumnPrivileges(jcatalogName,
        jschemaName, jtableName, jcolumnName));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getIndexInfoT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3, bool unique,
    bool approximate)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    setResultSet(m_conn->getMetaData()->getIndexInfo(jcatalogName,
        jschemaName, jtableName, (jboolean)unique, (jboolean)approximate));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getPrimaryKeysT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    setResultSet(m_conn->getMetaData()->getPrimaryKeys(jcatalogName,
        jschemaName, jtableName));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getImportedKeysT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    setResultSet(m_conn->getMetaData()->getImportedKeys(jcatalogName,
        jschemaName, jtableName));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getExportedKeysT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
    CHAR_TYPE* tableName, SQLSMALLINT nameLength3)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaName =
        schemaName != NULL ? StringFunctions::getJavaString(schemaName,
            nameLength2) :
            NULL;
    jstring jtableName =
        tableName != NULL ? StringFunctions::getJavaString(tableName,
            nameLength3) :
            NULL;
    setResultSet(m_conn->getMetaData()->getExportedKeys(jcatalogName,
        jschemaName, jtableName));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getCrossReferenceT(CHAR_TYPE* parentCatalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* parentSchemaName,
    SQLSMALLINT nameLength2, CHAR_TYPE* parentTableName,
    SQLSMALLINT nameLength3, CHAR_TYPE* foreignCatalogName,
    SQLSMALLINT nameLength4, CHAR_TYPE* foreignSchemaName,
    SQLSMALLINT nameLength5, CHAR_TYPE* foreignTableName,
    SQLSMALLINT nameLength6)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jparentCatalogName =
        parentCatalogName != NULL ? StringFunctions::getJavaString(
            parentCatalogName, nameLength1) :
            NULL;
    jstring jparentSchemaName =
        parentSchemaName != NULL ? StringFunctions::getJavaString(
            parentSchemaName, nameLength2) :
            NULL;
    jstring jparentTableName =
        parentTableName != NULL ? StringFunctions::getJavaString(
            parentTableName, nameLength3) :
            NULL;
    jstring jforeignCatalogName =
        foreignCatalogName != NULL ? StringFunctions::getJavaString(
            foreignCatalogName, nameLength4) :
            NULL;
    jstring jforeignSchemaName =
        foreignSchemaName != NULL ? StringFunctions::getJavaString(
            foreignSchemaName, nameLength5) :
            NULL;
    jstring jforeignTableName =
        foreignTableName != NULL ? StringFunctions::getJavaString(
            foreignTableName, nameLength6) :
            NULL;
    setResultSet(m_conn->getMetaData()->getCrossReference(jparentCatalogName,
        jparentSchemaName, jparentTableName, jforeignCatalogName,
        jforeignSchemaName, jforeignTableName));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getProceduresT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaPattern, SQLSMALLINT nameLength2,
    CHAR_TYPE* procedureNamePattern, SQLSMALLINT nameLength3)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaPattern =
        schemaPattern != NULL ? StringFunctions::getJavaString(schemaPattern,
            nameLength2) :
            NULL;
    jstring jprocedureNamePattern =
        procedureNamePattern != NULL ? StringFunctions::getJavaString(
            procedureNamePattern, nameLength3) :
            NULL;
    setResultSet(m_conn->getMetaData()->getProcedures(jcatalogName,
        jschemaPattern, jprocedureNamePattern));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getProcedureColumnsT(CHAR_TYPE* catalogName,
    SQLSMALLINT nameLength1, CHAR_TYPE* schemaPattern, SQLSMALLINT nameLength2,
    CHAR_TYPE* procedureNamePattern, SQLSMALLINT nameLength3,
    CHAR_TYPE* columnNamePattern, SQLSMALLINT nameLength4)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    jstring jcatalogName =
        catalogName != NULL ? StringFunctions::getJavaString(catalogName,
            nameLength1) :
            NULL;
    jstring jschemaPattern =
        schemaPattern != NULL ? StringFunctions::getJavaString(schemaPattern,
            nameLength2) :
            NULL;
    jstring jprocedureNamePattern =
        procedureNamePattern != NULL ? StringFunctions::getJavaString(
            procedureNamePattern, nameLength3) :
            NULL;
    jstring jcolumnNamePattern =
        columnNamePattern != NULL ? StringFunctions::getJavaString(
            columnNamePattern, nameLength4) :
            NULL;
    setResultSet(m_conn->getMetaData()->getProcedureColumns(jcatalogName,
        jschemaPattern, jprocedureNamePattern, jcolumnNamePattern));
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

template<typename CHAR_TYPE>
SQLRETURN GFXDStatement::getTypeInfoT(SQLSMALLINT dataType)
{
  clearLastError();
  if (m_resultSet != NULL) {
    // old cursor still open
    setJavaException(
        SQLState::newSQLException(SQLState::InvalidCursorState,
            ExceptionSeverity::STATEMENT_SEVERITY,
            SQLState::InvalidCursorStateMessage));
    return SQL_ERROR;
  }
  try {
    m_statementType = NORMAL_STATEMENT;
    m_params.clear();
    m_stmt.stmt = NULL;

    if (dataType == SQL_ALL_TYPES) {
      setResultSet(m_conn->getMetaData()->getTypeInfo());
    }
    else {
      m_resultSet =
          ((com::pivotal::gemfirexd::internal::client::am::DatabaseMetaData*)m_conn->getMetaData())->getTypeInfo(
              dataType);
    }
    m_resultSetMetaData = NULL;
    m_useWideStringDefault = (sizeof(CHAR_TYPE) > 1);
    return setSQLWarning(m_resultSet->getWarnings());
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

SQLRETURN GFXDStatement::getAttribute(SQLINTEGER attribute,
    SQLPOINTER valueBuffer, SQLINTEGER bufferLen, SQLINTEGER* valueLen)
{
  return getAttributeT<SQLCHAR>(attribute, valueBuffer, bufferLen, valueLen);
}

SQLRETURN GFXDStatement::getAttributeW(SQLINTEGER attribute,
    SQLPOINTER valueBuffer, SQLINTEGER bufferLen, SQLINTEGER* valueLen)
{
  return getAttributeT<SQLWCHAR>(attribute, valueBuffer, bufferLen, valueLen);
}

SQLRETURN GFXDStatement::setAttribute(SQLINTEGER attribute,
    SQLPOINTER valueBuffer, SQLINTEGER valueLen)
{
  return setAttributeT<SQLCHAR>(attribute, valueBuffer, valueLen);
}

SQLRETURN GFXDStatement::setAttributeW(SQLINTEGER attribute,
    SQLPOINTER valueBuffer, SQLINTEGER valueLen)
{
  return setAttributeT<SQLWCHAR>(attribute, valueBuffer, valueLen);
}

SQLRETURN GFXDStatement::getResultColumnDescriptor(SQLUSMALLINT columnNumber,
    SQLCHAR* columnName, SQLSMALLINT bufferLength, SQLSMALLINT* nameLength,
    SQLSMALLINT* dataType, SQLULEN* columnSize, SQLSMALLINT* decimalDigits,
    SQLSMALLINT* nullable)
{
  return getResultColumnDescriptorT(columnNumber, columnName, bufferLength,
      nameLength, dataType, columnSize, decimalDigits, nullable);
}

SQLRETURN GFXDStatement::getResultColumnDescriptor(SQLUSMALLINT columnNumber,
    SQLWCHAR* columnName, SQLSMALLINT bufferLength, SQLSMALLINT* nameLength,
    SQLSMALLINT* dataType, SQLULEN* columnSize, SQLSMALLINT* decimalDigits,
    SQLSMALLINT* nullable)
{
  return getResultColumnDescriptorT(columnNumber, columnName, bufferLength,
      nameLength, dataType, columnSize, decimalDigits, nullable);
}

SQLRETURN GFXDStatement::getColumnAttribute(SQLUSMALLINT columnNumber,
    SQLUSMALLINT fieldId, SQLPOINTER charAttribute, SQLSMALLINT bufferLength,
    SQLSMALLINT* stringLength, SQLLEN* numericAttribute)
{
  return getColumnAttributeT<SQLCHAR>(columnNumber, fieldId, charAttribute,
      bufferLength, stringLength, numericAttribute);
}

SQLRETURN GFXDStatement::getColumnAttributeW(SQLUSMALLINT columnNumber,
    SQLUSMALLINT fieldId, SQLPOINTER charAttribute, SQLSMALLINT bufferLength,
    SQLSMALLINT* stringLength, SQLLEN* numericAttribute)
{
  return getColumnAttributeT<SQLWCHAR>(columnNumber, fieldId, charAttribute,
      bufferLength, stringLength, numericAttribute);
}

SQLRETURN GFXDStatement::getNumResultColumns(SQLSMALLINT* columnCount)
{
//::printf("SW: invoking getNumResCols\n");
//::fflush(stdout);
  clearLastError();
  try {
    if (m_resultSet != NULL) {
      if (columnCount != NULL) {
        if (m_resultSetMetaData == NULL) {
          m_resultSetMetaData = m_resultSet->getMetaData();
        }
        *columnCount = m_resultSetMetaData->getColumnCount();
        //::printf("SW: for getNumResCols returning %d\n", *columnCount);
        //::fflush(stdout);
      }
      return SQL_SUCCESS;
    }
    else if (isPrepared()) {
      // set the result to zero as per ODBC spec
      if (columnCount != NULL) {
        *columnCount = 0;
      }
      return SQL_SUCCESS;
    }
    else {
      // no execution done
      setJavaException(
          SQLState::newSQLException(SQLState::InvalidCursorState,
              ExceptionSeverity::STATEMENT_SEVERITY,
              SQLState::InvalidCursorStateMessage2));
      return SQL_ERROR;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  setJavaException(
      SQLState::newSQLException(SQLState::NoResultSet,
          ExceptionSeverity::STATEMENT_SEVERITY,
          SQLState::NoResultSetMessage));
  return SQL_ERROR;
}

SQLRETURN GFXDStatement::getParamMetadata(SQLUSMALLINT paramNumber,
    SQLSMALLINT * patamDataTypePtr, SQLULEN * paramSizePtr,
    SQLSMALLINT * decimalDigitsPtr, SQLSMALLINT * nullablePtr)
{
  clearLastError();
  try {
    java::sql::ParameterMetaData* pmd = NULL;
    switch (m_statementType) {
      case PREPARED_STATEMENT:
        pmd = m_stmt.pstmt->getParameterMetaData();
        break;
      case CALLABLE_STATEMENT:
        pmd = m_stmt.cstmt->getParameterMetaData();
        break;
      default:
        // no open cursor
        setJavaException(
            SQLState::newSQLException(SQLState::InvalidCursorState,
                ExceptionSeverity::STATEMENT_SEVERITY,
                SQLState::InvalidCursorStateMessage2));
        return SQL_ERROR;
    }

    const int numParams = pmd->getParameterCount();
    if (paramNumber >= 1 && paramNumber <= numParams) {
      *patamDataTypePtr = pmd->getParameterType(paramNumber);
      *paramSizePtr = pmd->getPrecision(paramNumber);
      *decimalDigitsPtr = pmd->getScale(paramNumber);
      *nullablePtr =
          pmd->isNullable(paramNumber) ? SQL_NULLABLE : SQL_NULLABLE_UNKNOWN;
      return SQL_SUCCESS;
    }
    else {
      // no open cursor
      setJavaException(
          SQLState::newSQLException(SQLState::InvalidDescriptorIndex,
              ExceptionSeverity::STATEMENT_SEVERITY,
              SQLState::InvalidDescriptorIndexMessage, paramNumber,
              "Parameter number"));
      return SQL_ERROR;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

SQLRETURN GFXDStatement::getNumParameters(SQLSMALLINT* parameterCount)
{
  clearLastError();
  try {
    if (isPrepared()) {
      if (parameterCount != NULL) {
        java::sql::ParameterMetaData* pmd;
        switch (m_statementType) {
          case PREPARED_STATEMENT:
            pmd = m_stmt.pstmt->getParameterMetaData();
            break;
          case CALLABLE_STATEMENT:
            pmd = m_stmt.pstmt->getParameterMetaData();
            break;
          default:
            *parameterCount = 0;
            return SQL_SUCCESS;
        }
        *parameterCount = pmd->getParameterCount();
      }
      return SQL_SUCCESS;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  setJavaException(
      SQLState::newSQLException(SQLState::StatementNotPrepared,
          ExceptionSeverity::STATEMENT_SEVERITY,
          SQLState::StatementNotPreparedMessage));
  return SQL_ERROR;
}

SQLRETURN GFXDStatement::getCursorName(SQLCHAR* cursorName,
    SQLSMALLINT bufferLength, SQLSMALLINT* nameLength)
{
  return getCursorNameT(cursorName, bufferLength, nameLength);
}

SQLRETURN GFXDStatement::getCursorName(SQLWCHAR* cursorName,
    SQLSMALLINT bufferLength, SQLSMALLINT* nameLength)
{
  return getCursorNameT(cursorName, bufferLength, nameLength);
}

SQLRETURN GFXDStatement::setCursorName(SQLCHAR* cursorName,
    SQLSMALLINT nameLength)
{
  return setCursorNameT(cursorName, nameLength);
}

SQLRETURN GFXDStatement::setCursorName(SQLWCHAR* cursorName,
    SQLSMALLINT nameLength)
{
  return setCursorNameT(cursorName, nameLength);
}

SQLRETURN GFXDStatement::getTables(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3, SQLCHAR* tableTypes,
    SQLSMALLINT nameLength4)
{
  return getTablesT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3, tableTypes, nameLength4);
}

SQLRETURN GFXDStatement::getTables(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3, SQLWCHAR* tableTypes,
    SQLSMALLINT nameLength4)
{
  return getTablesT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3, tableTypes, nameLength4);
}

SQLRETURN GFXDStatement::getTablePrivileges(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getTablePrivilegesT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getTablePrivileges(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getTablePrivilegesT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getColumns(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3, SQLCHAR* columnName,
    SQLSMALLINT nameLength4)
{
  return getColumnsT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3, columnName, nameLength4);
}

SQLRETURN GFXDStatement::getColumns(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3, SQLWCHAR* columnName,
    SQLSMALLINT nameLength4)
{
  return getColumnsT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3, columnName, nameLength4);
}

SQLRETURN GFXDStatement::getSpecialColumns(SQLUSMALLINT identifierType,
    SQLCHAR *catalogName, SQLSMALLINT nameLength1, SQLCHAR *schemaName,
    SQLSMALLINT nameLength2, SQLCHAR *tableName, SQLSMALLINT nameLength3,
    SQLUSMALLINT scope, SQLUSMALLINT nullable)
{
  return getSpecialColumnsT(identifierType, catalogName, nameLength1,
      schemaName, nameLength2, tableName, nameLength3, scope, nullable);
}

SQLRETURN GFXDStatement::getSpecialColumns(SQLUSMALLINT identifierType,
    SQLWCHAR* catalogName, SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* tableName, SQLSMALLINT nameLength3,
    SQLUSMALLINT scope, SQLUSMALLINT nullable)
{
  return getSpecialColumnsT(identifierType, catalogName, nameLength1,
      schemaName, nameLength2, tableName, nameLength3, scope, nullable);
}

SQLRETURN GFXDStatement::getColumnPrivileges(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3, SQLCHAR* columnName,
    SQLSMALLINT nameLength4)
{
  return getColumnPrivilegesT(catalogName, nameLength1, schemaName,
      nameLength2, tableName, nameLength3, columnName, nameLength4);
}

SQLRETURN GFXDStatement::getColumnPrivileges(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3, SQLWCHAR* columnName,
    SQLSMALLINT nameLength4)
{
  return getColumnPrivilegesT(catalogName, nameLength1, schemaName,
      nameLength2, tableName, nameLength3, columnName, nameLength4);
}

SQLRETURN GFXDStatement::getIndexInfo(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3, bool unique, bool approximate)
{
  return getIndexInfoT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3, unique, approximate);
}

SQLRETURN GFXDStatement::getIndexInfo(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3, bool unique,
    bool approximate)
{
  return getIndexInfoT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3, unique, approximate);
}

SQLRETURN GFXDStatement::getPrimaryKeys(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getPrimaryKeysT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getPrimaryKeys(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getPrimaryKeysT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getImportedKeys(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getImportedKeysT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getImportedKeys(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getImportedKeysT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getExportedKeys(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getExportedKeysT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getExportedKeys(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaName, SQLSMALLINT nameLength2,
    SQLWCHAR* tableName, SQLSMALLINT nameLength3)
{
  return getExportedKeysT(catalogName, nameLength1, schemaName, nameLength2,
      tableName, nameLength3);
}

SQLRETURN GFXDStatement::getCrossReference(SQLCHAR* parentCatalogName,
    SQLSMALLINT nameLength1, SQLCHAR* parentSchemaName,
    SQLSMALLINT nameLength2, SQLCHAR* parentTableName, SQLSMALLINT nameLength3,
    SQLCHAR* foreignCatalogName, SQLSMALLINT nameLength4,
    SQLCHAR* foreignSchemaName, SQLSMALLINT nameLength5,
    SQLCHAR* foreignTableName, SQLSMALLINT nameLength6)
{
  return getCrossReferenceT(parentCatalogName, nameLength1, parentSchemaName,
      nameLength2, parentTableName, nameLength3, foreignCatalogName,
      nameLength4, foreignSchemaName, nameLength5, foreignTableName,
      nameLength6);
}

SQLRETURN GFXDStatement::getCrossReference(SQLWCHAR* parentCatalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* parentSchemaName,
    SQLSMALLINT nameLength2, SQLWCHAR* parentTableName,
    SQLSMALLINT nameLength3, SQLWCHAR* foreignCatalogName,
    SQLSMALLINT nameLength4, SQLWCHAR* foreignSchemaName,
    SQLSMALLINT nameLength5, SQLWCHAR* foreignTableName,
    SQLSMALLINT nameLength6)
{
  return getCrossReferenceT(parentCatalogName, nameLength1, parentSchemaName,
      nameLength2, parentTableName, nameLength3, foreignCatalogName,
      nameLength4, foreignSchemaName, nameLength5, foreignTableName,
      nameLength6);
}

SQLRETURN GFXDStatement::getProcedures(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaPattern, SQLSMALLINT nameLength2,
    SQLCHAR* procedureNamePattern, SQLSMALLINT nameLength3)
{
  return getProceduresT(catalogName, nameLength1, schemaPattern, nameLength2,
      procedureNamePattern, nameLength3);
}

SQLRETURN GFXDStatement::getProcedures(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaPattern, SQLSMALLINT nameLength2,
    SQLWCHAR* procedureNamePattern, SQLSMALLINT nameLength3)
{
  return getProceduresT(catalogName, nameLength1, schemaPattern, nameLength2,
      procedureNamePattern, nameLength3);
}

SQLRETURN GFXDStatement::getProcedureColumns(SQLCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLCHAR* schemaPattern, SQLSMALLINT nameLength2,
    SQLCHAR* procedureNamePattern, SQLSMALLINT nameLength3,
    SQLCHAR* columnNamePattern, SQLSMALLINT nameLength4)
{
  return getProcedureColumnsT(catalogName, nameLength1, schemaPattern,
      nameLength2, procedureNamePattern, nameLength3, columnNamePattern,
      nameLength4);
}

SQLRETURN GFXDStatement::getProcedureColumns(SQLWCHAR* catalogName,
    SQLSMALLINT nameLength1, SQLWCHAR* schemaPattern, SQLSMALLINT nameLength2,
    SQLWCHAR* procedureNamePattern, SQLSMALLINT nameLength3,
    SQLWCHAR* columnNamePattern, SQLSMALLINT nameLength4)
{
  return getProcedureColumnsT(catalogName, nameLength1, schemaPattern,
      nameLength2, procedureNamePattern, nameLength3, columnNamePattern,
      nameLength4);
}

SQLRETURN GFXDStatement::getTypeInfo(SQLSMALLINT dataType)
{
  return getTypeInfoT<SQLCHAR>(dataType);
}

SQLRETURN GFXDStatement::getTypeInfoW(SQLSMALLINT dataType)
{
  return getTypeInfoT<SQLWCHAR>(dataType);
}

SQLRETURN GFXDStatement::getMoreResults()
{
  clearLastError();
  bool bRetVal = false;

  try {
    switch (m_statementType) {
      case PREPARED_STATEMENT:
        if (m_stmt.pstmt != NULL) {
          bRetVal = m_stmt.pstmt->getMoreResults();
          if (m_stmt.pstmt->getMoreResults()) {
            bRetVal = true;
            setResultSet(m_stmt.pstmt->getResultSet());
          }
        }
        break;
      case NORMAL_STATEMENT:
        if (m_stmt.stmt != NULL) {
          if (m_stmt.stmt->getMoreResults()) {
            bRetVal = true;
            setResultSet(m_stmt.stmt->getResultSet());
          }
        }
        break;
      case CALLABLE_STATEMENT:
        if (m_stmt.cstmt != NULL) {
          if (m_stmt.cstmt->getMoreResults()) {
            bRetVal = true;
            setResultSet(m_stmt.cstmt->getResultSet());
          }
        }
        break;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return bRetVal ? SQL_SUCCESS : SQL_NO_DATA;
}

//TODO: do we need to check the statement type
SQLRETURN GFXDStatement::getParamData(SQLPOINTER* valuePtr)
{
  clearLastError();
  int totalParamCount = m_params.size();
  try {
    for (int i = m_currentParameterIndex + 1; i <= totalParamCount; i++) {
      if (m_params[i].m_isDataAtExecParam) {
        valuePtr = &(m_params[i].m_o_value);
        m_currentParameterIndex = i;
        m_params[i].m_o_value = NULL;
        m_params[i].m_o_valueSize = 0;
        m_params[i].m_isAllocated = false;
        m_params[i].m_isDataAtExecParam = false;
        return SQL_NEED_DATA;
      }
    }
    // No more data at exec parameters so execute the statement
    return execute();
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
}

SQLRETURN GFXDStatement::putData(SQLPOINTER dataPtr, SQLINTEGER dataLength)
{
  clearLastError();
  try {
    Parameter& currentParam = m_params[m_currentParameterIndex];
    dataLength = (dataLength == SQL_NTS) ? strlen((char*)dataPtr) : dataLength;

    if (currentParam.m_o_value) { // Append to old value
      currentParam.m_o_value = ::realloc(currentParam.m_o_value,
          currentParam.m_o_valueSize + dataLength + 1);
      ::memcpy((char*)currentParam.m_o_value + currentParam.m_o_valueSize,
          dataPtr, dataLength);
      currentParam.m_o_valueSize += dataLength;
      ((char*)currentParam.m_o_value)[currentParam.m_o_valueSize] = 0;
      currentParam.m_isAllocated = true;
    }
    else {      // New value
      currentParam.m_o_value = ::malloc(dataLength + 1);
      ::memcpy((char*)currentParam.m_o_value, dataPtr, dataLength);
      currentParam.m_o_valueSize = dataLength;
      ((char*)currentParam.m_o_value)[currentParam.m_o_valueSize] = 0;
      currentParam.m_isAllocated = true;
    }
    currentParam.m_isDataAtExecParam = false;
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDStatement::closeResultSet()
{
  clearLastError();
  try {
    if (m_resultSet != NULL) {
      m_resultSet->close();
      m_resultSet = NULL;
      m_cursor.clear();
      m_resultSetMetaData = NULL;
    }
    else {
      // no open cursor
      setJavaException(
          SQLState::newSQLException(SQLState::InvalidCursorState,
              ExceptionSeverity::STATEMENT_SEVERITY,
              SQLState::InvalidCursorStateMessage2));
      return SQL_ERROR;
    }
  } catch (java::lang::Throwable* t) {
    return GFXDHandleBase::errorJavaException(t, this);
  }
  return SQL_SUCCESS;
}

SQLRETURN GFXDStatement::resetParameters() {
  clearLastError();
  m_params.clear();
  return SQL_SUCCESS;
  return SQL_SUCCESS;
}

SQLRETURN GFXDStatement::cancel() {
  clearLastError();
  // cancel is not yet supported by GemFireXD, so ignore any exceptions
  // indicating not-implemented, but if implemented in future then
  // above will start working
  //if (!java::sql::SQLException::class$.isInstance(t) || !JvNewStringLatin1(
  //    "0A000")->equals(((java::sql::SQLException*)t)->getSQLState())) {
  return SQL_SUCCESS;
}

SQLRETURN GFXDStatement::close()
{
  clearLastError();
  try {
    if (m_resultSet.get() != NULL) {
      m_resultSet->close();
      m_resultSet.reset();
    }
    if (m_pstmt.get() != NULL) {
      m_pstmt->close();
    }
    m_params.clear();
    m_outputFields.clear();
    m_useWideStringDefault = false;
  } catch (const SQLException& sqle) {
    setException(sqle);
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}
SW: */
