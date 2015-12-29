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
 * Row.cpp
 *
 *      Author: swale
 */

#include "Row.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <limits>

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::client;

bool Row::convertBoolean(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  switch (cv.getType()) {
    case thrift::GFXDType::SMALLINT:
      return cv.getI16() != 0;
    case thrift::GFXDType::INTEGER:
      return cv.getI32() != 0;
    case thrift::GFXDType::BIGINT:
      return cv.getI64() != 0;
    case thrift::GFXDType::NULLTYPE:
      return false;
    case thrift::GFXDType::TINYINT:
      return cv.getByte() != 0;
    case thrift::GFXDType::DOUBLE:
      return cv.getDouble() != 0.0;
    case thrift::GFXDType::FLOAT:
      return cv.getFloat() != 0.0f;
    case thrift::GFXDType::DECIMAL:
      return Decimal(*cv.getDecimal()) != Decimal::ZERO;
    case thrift::GFXDType::VARCHAR: {
      std::string& v = *cv.getString();
      if (v == "true" || v == "1") {
        return true;
      }
      else {
        std::string s = boost::algorithm::trim_copy(*cv.getString());
        boost::algorithm::to_lower(s);
        return !(s == "0" || s == "false");
      }
    }
    default:
      throw GET_DATACONVERSION_ERROR(cv, "BOOLEAN", columnIndex);
  }
}

int8_t Row::convertByte(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return boost::numeric_cast<int8_t>(cv.getI16());
      case thrift::GFXDType::INTEGER:
        return boost::numeric_cast<int8_t>(cv.getI32());
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<int8_t>(cv.getI64());
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<int8_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<int8_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        int64_t result;
        if (d.toLong(result, false)
            && result >= std::numeric_limits<int8_t>::min()
            && result <= std::numeric_limits<int8_t>::max()) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("TINYINT", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<int8_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("TINYINT", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "TINYINT", columnIndex);
}

uint8_t Row::convertUnsignedByte(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return boost::numeric_cast<uint8_t>(cv.getI16());
      case thrift::GFXDType::INTEGER:
        return boost::numeric_cast<uint8_t>(cv.getI32());
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<uint8_t>(cv.getI64());
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<uint8_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<uint8_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        uint64_t result;
        if (d.toULong(result, false)
            && result >= std::numeric_limits<uint8_t>::min()
            && result <= std::numeric_limits<uint8_t>::max()) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("TINYINT", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<uint8_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("TINYINT", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "TINYINT", columnIndex);
}

int16_t Row::convertShort(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::INTEGER:
        return boost::numeric_cast<int16_t>(cv.getI32());
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<int16_t>(cv.getI64());
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<int16_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<int16_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        int64_t result;
        if (d.toLong(result, false)
            && result >= std::numeric_limits<int16_t>::min()
            && result <= std::numeric_limits<int16_t>::max()) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("TINYINT", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<int16_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("SMALLINT", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "SMALLINT", columnIndex);
}

uint16_t Row::convertUnsignedShort(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::INTEGER:
        return boost::numeric_cast<uint16_t>(cv.getI32());
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<uint16_t>(cv.getI64());
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<uint16_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<uint16_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        uint64_t result;
        if (d.toULong(result, false)
            && result >= std::numeric_limits<uint16_t>::min()
            && result <= std::numeric_limits<uint16_t>::max()) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("TINYINT", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<uint16_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("SMALLINT", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "SMALLINT", columnIndex);
}

int32_t Row::convertInt(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return cv.getI16();
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<int32_t>(cv.getI64());
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<int32_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<int32_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        int64_t result;
        if (d.toLong(result, false)
            && result >= std::numeric_limits<int32_t>::min()
            && result <= std::numeric_limits<int32_t>::max()) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("INTEGER", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<int32_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("INTEGER", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "INTEGER", columnIndex);
}

uint32_t Row::convertUnsignedInt(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return cv.getI16();
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<uint32_t>(cv.getI64());
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<uint32_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<uint32_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        uint64_t result;
        if (d.toULong(result, false)
            && result >= std::numeric_limits<uint32_t>::min()
            && result <= std::numeric_limits<uint32_t>::max()) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("INTEGER", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<uint32_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("INTEGER", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "INTEGER", columnIndex);
}

int64_t Row::convertLong(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return cv.getI16();
      case thrift::GFXDType::INTEGER:
        return cv.getI32();
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<int64_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<int64_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        int64_t result;
        if (d.toLong(result, false)) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("BIGINT", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<int64_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("BIGINT", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "BIGINT", columnIndex);
}

uint64_t Row::convertUnsignedLong(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return cv.getI16();
      case thrift::GFXDType::INTEGER:
        return cv.getI32();
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<uint64_t>(cv.getDouble());
      case thrift::GFXDType::FLOAT:
        return boost::numeric_cast<uint64_t>(cv.getFloat());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        uint64_t result;
        if (d.toULong(result, false)) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("BIGINT", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<uint64_t>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("BIGINT", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "BIGINT", columnIndex);
}

float Row::convertFloat(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return cv.getI16();
      case thrift::GFXDType::INTEGER:
        return cv.getI32();
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<float>(cv.getI64());
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0.0f;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::DOUBLE:
        return boost::numeric_cast<float>(cv.getDouble());
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        double result;
        if (d.toDouble(result) && result >= std::numeric_limits<float>::min()
            && result <= std::numeric_limits<float>::max()) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("FLOAT", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<float>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("FLOAT", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "FLOAT", columnIndex);
}

double Row::convertDouble(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  try {
    switch (cv.getType()) {
      case thrift::GFXDType::SMALLINT:
        return cv.getI16();
      case thrift::GFXDType::INTEGER:
        return cv.getI32();
      case thrift::GFXDType::BIGINT:
        return boost::numeric_cast<double>(cv.getI64());
      case thrift::GFXDType::TINYINT:
        return cv.getByte();
      case thrift::GFXDType::NULLTYPE:
        return 0.0;
      case thrift::GFXDType::BOOLEAN:
        return cv.getBool() ? 1 : 0;
      case thrift::GFXDType::FLOAT:
        return cv.getFloat();
      case thrift::GFXDType::DECIMAL: {
        Decimal d(*cv.getDecimal());
        double result;
        if (d.toDouble(result)) {
          return result;
        }
        else {
          std::string s;
          d.toString(s);
          Utils::throwDataOutsideRangeError("DOUBLE", columnIndex, s.c_str());
        }
        break;
      }
      case thrift::GFXDType::VARCHAR:
        return boost::lexical_cast<double>(*cv.getString());
      default:
        break;
    }
  } catch (const std::exception& ex) {
    Utils::throwDataFormatError("DOUBLE", columnIndex, ex.what());
  }
  throw GET_DATACONVERSION_ERROR(cv, "DOUBLE", columnIndex);
}

AutoPtr<std::string> Row::convertString(const thrift::ColumnValue& cv,
    const uint32_t columnIndex, const uint32_t realPrecision) const {
  switch (cv.getType()) {
    // karma::generate is about the fastest converter (beats sprintf easily)
    case thrift::GFXDType::INTEGER: {
      AutoPtr<std::string> result(new std::string());
      Utils::convertIntToString(cv.getI32(), *result);
      return result;
    }
    case thrift::GFXDType::BIGINT: {
      AutoPtr<std::string> result(new std::string());
      Utils::convertLongToString(cv.getI64(), *result);
      return result;
    }
    case thrift::GFXDType::SMALLINT: {
      AutoPtr<std::string> result(new std::string());
      Utils::convertShortToString(cv.getI16(), *result);
      return result;
    }
    case thrift::GFXDType::BOOLEAN:
      return AutoPtr<std::string>(
          new std::string(cv.getBool() ? "true" : "false"));
    case thrift::GFXDType::TINYINT: {
      AutoPtr<std::string> result(new std::string());
      Utils::convertByteToString(cv.getByte(), *result);
      return result;
    }
    case thrift::GFXDType::NULLTYPE:
      return AutoPtr<std::string>(NULL);
    case thrift::GFXDType::FLOAT: {
      AutoPtr<std::string> result(new std::string());
      Utils::convertFloatToString(cv.getFloat(), *result, realPrecision);
      return result;
    }
    case thrift::GFXDType::DOUBLE: {
      AutoPtr<std::string> result(new std::string());
      Utils::convertDoubleToString(cv.getDouble(), *result, realPrecision);
      return result;
    }
    case thrift::GFXDType::DECIMAL: {
      Decimal d(*cv.getDecimal());
      std::string* str = new std::string();
      AutoPtr<std::string> res(str);
      d.toString(*str);
      return res;
    }
    case thrift::GFXDType::DATE: {
      std::string* str = new std::string();
      AutoPtr<std::string> res(str);
      DateTime dt(cv.getDate());
      dt.toDate(*str);
      return res;
    }
    case thrift::GFXDType::TIME: {
      std::string* str = new std::string();
      AutoPtr<std::string> res(str);
      DateTime dt(cv.getTime());
      dt.toTime(*str);
      return res;
    }
    case thrift::GFXDType::TIMESTAMP: {
      std::string* str = new std::string();
      AutoPtr<std::string> res(str);
      Timestamp ts(cv.getTimestampEpoch(), cv.getTimestampNanos());
      ts.toString(*str);
      return res;
    }
    case thrift::GFXDType::CLOB:
      return getFullClobData(cv, columnIndex, "CHAR");
    case thrift::GFXDType::BLOB:
      return getFullBlobData(cv, columnIndex, "CHAR");
    default:
      break;
  }
  throw GET_DATACONVERSION_ERROR(cv, "CHAR", columnIndex);
}

AutoPtr<Decimal> Row::convertDecimal(const thrift::ColumnValue& cv,
    const uint32_t columnIndex, const uint32_t realPrecision) const {
  switch (cv.getType()) {
    case thrift::GFXDType::SMALLINT:
      return AutoPtr<Decimal>(new Decimal(cv.getI16()));
    case thrift::GFXDType::INTEGER:
      return AutoPtr<Decimal>(new Decimal(cv.getI32()));
    case thrift::GFXDType::BIGINT:
      return AutoPtr<Decimal>(new Decimal(cv.getI64()));
    case thrift::GFXDType::TINYINT:
      return AutoPtr<Decimal>(new Decimal(cv.getByte()));
    case thrift::GFXDType::NULLTYPE:
      return AutoPtr<Decimal>(NULL);
    case thrift::GFXDType::BOOLEAN:
      return AutoPtr<Decimal>(new Decimal((uint32_t)(cv.getBool() ? 1 : 0)));
    case thrift::GFXDType::FLOAT:
      return AutoPtr<Decimal>(new Decimal(cv.getFloat(), realPrecision));
    case thrift::GFXDType::DOUBLE:
      return AutoPtr<Decimal>(new Decimal(cv.getDouble(), realPrecision));
    case thrift::GFXDType::VARCHAR:
      return AutoPtr<Decimal>(new Decimal(*cv.getString(), columnIndex));
    default:
      break;
  }
  throw GET_DATACONVERSION_ERROR(cv, "DECIMAL", columnIndex);
}

// TODO: PERF: not efficient but probably does not matter since in nearly
// all cases we would expect underlying type to also be DECIMAL
AutoPtr<thrift::Decimal> Row::convertTDecimal(const thrift::ColumnValue& cv,
    const uint32_t columnIndex, const uint32_t realPrecision) const {
  switch (cv.getType()) {
    case thrift::GFXDType::SMALLINT: {
      Decimal dec(cv.getI16());
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      dec.copyTo(*tdec);
      return tdec;
    }
    case thrift::GFXDType::INTEGER: {
      Decimal dec(cv.getI32());
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      dec.copyTo(*tdec);
      return tdec;
    }
    case thrift::GFXDType::BIGINT: {
      Decimal dec(cv.getI64());
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      dec.copyTo(*tdec);
      return tdec;
    }
    case thrift::GFXDType::TINYINT: {
      Decimal dec(cv.getByte());
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      dec.copyTo(*tdec);
      return tdec;
    }
    case thrift::GFXDType::NULLTYPE:
      return AutoPtr<thrift::Decimal>(NULL);
    case thrift::GFXDType::BOOLEAN: {
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      if (cv.getBool()) {
        Decimal::ONE.copyTo(*tdec);
      }
      else {
        Decimal::ZERO.copyTo(*tdec);
      }
      return tdec;
    }
    case thrift::GFXDType::FLOAT: {
      Decimal dec(cv.getFloat(), realPrecision);
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      dec.copyTo(*tdec);
      return tdec;
    }
    case thrift::GFXDType::DOUBLE: {
      Decimal dec(cv.getDouble(), realPrecision);
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      dec.copyTo(*tdec);
      return tdec;
    }
    case thrift::GFXDType::VARCHAR: {
      Decimal dec(*cv.getString(), columnIndex);
      AutoPtr<thrift::Decimal> tdec(new thrift::Decimal());
      dec.copyTo(*tdec);
      return tdec;
    }
    default:
      break;
  }
  throw GET_DATACONVERSION_ERROR(cv, "DECIMAL", columnIndex);
}

DateTime Row::convertDate(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  switch (cv.getType()) {
    case thrift::GFXDType::TIMESTAMP:
      return DateTime(cv.getTimestampEpoch());
    case thrift::GFXDType::TIME:
      return DateTime(cv.getTime());
    case thrift::GFXDType::NULLTYPE:
      return DateTime(0);
    case thrift::GFXDType::VARCHAR: {
      std::string v = boost::algorithm::trim_copy(*cv.getString());
      return DateTime::parseDate(v, columnIndex);
    }
    default:
      break;
  }
  throw GET_DATACONVERSION_ERROR(cv, "DATE", columnIndex);
}

DateTime Row::convertTime(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  switch (cv.getType()) {
    case thrift::GFXDType::TIMESTAMP:
      return DateTime(cv.getTimestampEpoch());
    case thrift::GFXDType::DATE:
      return DateTime(cv.getDate());
    case thrift::GFXDType::NULLTYPE:
      return DateTime(0);
    case thrift::GFXDType::VARCHAR: {
      std::string v = boost::algorithm::trim_copy(*cv.getString());
      return DateTime::parseTime(v, columnIndex);
    }
    default:
      break;
  }
  throw GET_DATACONVERSION_ERROR(cv, "TIME", columnIndex);
}

Timestamp Row::convertTimestamp(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  switch (cv.getType()) {
    case thrift::GFXDType::NULLTYPE:
      return Timestamp(0);
    case thrift::GFXDType::DATE:
      return Timestamp(cv.getDate());
    case thrift::GFXDType::TIME:
      return Timestamp(cv.getTime());
    case thrift::GFXDType::VARCHAR: {
      std::string v = boost::algorithm::trim_copy(*cv.getString());
      return Timestamp::parseString(v, columnIndex);
    }
    default:
      break;
  }
  throw GET_DATACONVERSION_ERROR(cv, "TIMESTAMP", columnIndex);
}

AutoPtr<std::string> Row::convertBinary(const thrift::ColumnValue& cv,
    const uint32_t columnIndex) const {
  switch (cv.getType()) {
    case thrift::GFXDType::BLOB:
      return getFullBlobData(cv, columnIndex,"BINARY");
    case thrift::GFXDType::CLOB:
      return getFullClobData(cv, columnIndex, "BINARY");
    case thrift::GFXDType::NULLTYPE:
      return AutoPtr<std::string>(NULL);
    case thrift::GFXDType::VARCHAR: {
      return AutoPtr<std::string>(cv.getString(), false);
    default:
      break;
    }
  }
  throw GET_DATACONVERSION_ERROR(cv, "BINARY", columnIndex);
}

AutoPtr<std::string> Row::getFullBlobData(const thrift::ColumnValue& cv,
    const uint32_t columnIndex, const char* forType) const {
  // TODO: add proper BLOB/CLOB support
  thrift::BlobChunk* blob = cv.getBlob();
  if (blob != NULL) {
    if (blob->last) {
      return AutoPtr<std::string>(&blob->chunk, false);
    }
    else {
      throw GET_DATACONVERSION_ERROR(cv, forType, columnIndex);
    }
  }
  else {
    return AutoPtr<std::string>(NULL);
  }
}

AutoPtr<std::string> Row::getFullClobData(const thrift::ColumnValue& cv,
    const uint32_t columnIndex, const char* forType) const {
  // TODO: add proper BLOB/CLOB support
  // TODO: also add support for passing in char* to fill in (e.g. from ODBC)
  // instead of having to create a copy here from multiple chunks and then
  // ODBC/... driver has to make another copy; also explore C++11 std::move
  thrift::ClobChunk* clob = cv.getClob();
  if (clob != NULL) {
    if (clob->last) {
      return AutoPtr<std::string>(&clob->chunk, false);
    }
    else {
      throw GET_DATACONVERSION_ERROR(cv, forType, columnIndex);
    }
  }
  else {
    return AutoPtr<std::string>(NULL);
  }
}
