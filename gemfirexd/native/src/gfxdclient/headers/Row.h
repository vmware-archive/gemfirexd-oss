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
 * Row.h
 *
 *      Author: swale
 */

#ifndef ROW_H_
#define ROW_H_

#include "Types.h"
#include "WrapperBase.h"

#include <sstream>

#define GET_DATACONVERSION_ERROR(cv, target, columnIndex) \
     GET_SQLEXCEPTION2(SQLStateMessage::LANG_DATA_TYPE_GET_MISMATCH_MSG, \
         target, Utils::getSQLTypeName(cv), columnIndex)

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        class Row : public thrift::Row
        {
        private:
          // IMPORTANT NOTE: DO NOT ADD ANY ADDITIONAL FIELDS IN THIS CLASS.
          // If need be then add to thrift::Row since higher layers use
          // placement new to freely up-convert thrift::Row to this type
          inline void checkColumnBounds(const uint32_t columnZeroIndex) const {
            if (columnZeroIndex < m_values.size()) {
              return;
            }
            else if (m_values.size() > 0) {
              throw GET_SQLEXCEPTION2(SQLStateMessage::COLUMN_NOT_FOUND_MSG1,
                  columnZeroIndex + 1, static_cast<int>(m_values.size() + 1));
            }
            else {
              throw GET_SQLEXCEPTION2(SQLStateMessage::NO_CURRENT_ROW_MSG);
            }
          }

          // no copy constructor or assignment
          Row(const Row& other);
          Row& operator=(const Row& other);

          friend class ResultSet;
          friend class Result;

        protected:
          // for placement new skip initialization of m_values
          Row(bool updatable) :
              thrift::Row(updatable) {
          }

          inline const thrift::ColumnValue& getColumnValue(
              uint32_t columnIndex) const {
            columnIndex--;
            checkColumnBounds(columnIndex);

            return m_values[columnIndex];
          }

          inline thrift::ColumnValue* getColumnValue_(
              uint32_t columnIndex) {
            columnIndex--;
            checkColumnBounds(columnIndex);

            return &m_values[columnIndex];
          }

          bool convertBoolean(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          int8_t convertByte(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          uint8_t convertUnsignedByte(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          int16_t convertShort(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          uint16_t convertUnsignedShort(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          int32_t convertInt(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          uint32_t convertUnsignedInt(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          int64_t convertLong(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          uint64_t convertUnsignedLong(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          float convertFloat(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          double convertDouble(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          AutoPtr<std::string> convertString(const thrift::ColumnValue& cv,
              const uint32_t columnIndex, const uint32_t realPrecision) const;

          AutoPtr<Decimal> convertDecimal(const thrift::ColumnValue& cv,
              const uint32_t columnIndex, const uint32_t realPrecision) const;

          AutoPtr<thrift::Decimal> convertTDecimal(
              const thrift::ColumnValue& cv, const uint32_t columnIndex,
              const uint32_t realPrecision) const;

          DateTime convertDate(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          DateTime convertTime(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          Timestamp convertTimestamp(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          AutoPtr<std::string> convertBinary(const thrift::ColumnValue& cv,
              const uint32_t columnIndex) const;

          AutoPtr<std::string> getFullBlobData(const thrift::ColumnValue& cv,
              const uint32_t columnIndex, const char* forType) const;

          AutoPtr<std::string> getFullClobData(const thrift::ColumnValue& cv,
              const uint32_t columnIndex, const char* forType) const;

        public:
          Row() :
              thrift::Row() {
          }

          Row(const size_t initialCapacity) :
              thrift::Row(initialCapacity) {
          }

          // C++11 move constructor and move assignment operator
#if __cplusplus >= 201103L
          Row(Row&& other) :
              thrift::Row(std::move(other)) {
          }

          Row& operator=(Row&& other) {
            thrift::Row::operator =(std::move(other));
            return *this;
          }
#endif

          SQLType::type getType(const uint32_t columnIndex) const {
            return getColumnValue(columnIndex).getType();
          }

          bool getBoolean(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetBool()) {
              return cv.getBool();
            }
            else {
              return convertBoolean(cv, columnIndex);
            }
          }

          int8_t getByte(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetByte()) {
              return cv.getByte();
            }
            else {
              return convertByte(cv, columnIndex);
            }
          }

          uint8_t getUnsignedByte(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetByte()) {
              return (uint8_t)cv.getByte();
            }
            else {
              return convertUnsignedByte(cv, columnIndex);
            }
          }

          int16_t getShort(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetI16()) {
              return cv.getI16();
            }
            else {
              return convertShort(cv, columnIndex);
            }
          }

          uint16_t getUnsignedShort(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetI16()) {
              return (uint16_t)cv.getI16();
            }
            else {
              return convertUnsignedShort(cv, columnIndex);
            }
          }

          int32_t getInt(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetI32()) {
              return cv.getI32();
            }
            else {
              return convertInt(cv, columnIndex);
            }
          }

          uint32_t getUnsignedInt(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetI32()) {
              return (uint32_t)cv.getI32();
            }
            else {
              return convertUnsignedInt(cv, columnIndex);
            }
          }

          int64_t getLong(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetI64()) {
              return cv.getI64();
            }
            else {
              return convertLong(cv, columnIndex);
            }
          }

          uint64_t getUnsignedLong(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetI64()) {
              return (uint64_t)cv.getI64();
            }
            else {
              return convertUnsignedLong(cv, columnIndex);
            }
          }

          float getFloat(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetFloat()) {
              return cv.getFloat();
            }
            else {
              return convertFloat(cv, columnIndex);
            }
          }

          double getDouble(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetDouble()) {
              return cv.getDouble();
            }
            else {
              return convertDouble(cv, columnIndex);
            }
          }

          // TODO: add some kind of shared_ptr to have shared ownership by
          // caller and Row else it will be destroyed if user moves to next row
          // same for BLOB/CLOB/BINARY/DECIMAL types
          AutoPtr<std::string> getString(const uint32_t columnIndex,
              const uint32_t realPrecision = DEFAULT_REAL_PRECISION) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetString()) {
              return AutoPtr<std::string>(cv.getString(), false);
            }
            else {
              return convertString(cv, columnIndex, realPrecision);
            }
          }

          int32_t getString(const uint32_t columnIndex, char* outStr,
              const int32_t outMaxLen, const bool truncate = true,
              const uint32_t realPrecision = DEFAULT_REAL_PRECISION) const;

          AutoPtr<Decimal> getDecimal(const uint32_t columnIndex,
              const uint32_t realPrecision = DEFAULT_REAL_PRECISION) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetDecimal()) {
              return AutoPtr<Decimal>(new Decimal(*cv.getDecimal()));
            }
            else {
              return convertDecimal(cv, columnIndex, realPrecision);
            }
          }

          AutoPtr<thrift::Decimal> getTDecimal(const uint32_t columnIndex,
              const uint32_t realPrecision = DEFAULT_REAL_PRECISION) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetDecimal()) {
              return AutoPtr<thrift::Decimal>(cv.getDecimal(), false);
            }
            else {
              return convertTDecimal(cv, columnIndex, realPrecision);
            }
          }

          DateTime getDate(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetDate()) {
              return DateTime(cv.getDate());
            }
            else {
              return convertDate(cv, columnIndex);
            }
          }

          DateTime getTime(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetTime()) {
              return DateTime(cv.getTime());
            }
            else {
              return convertTime(cv, columnIndex);
            }
          }

          Timestamp getTimestamp(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetTimestamp()) {
              return Timestamp(cv.getTimestampEpoch(), cv.getTimestampNanos());
            }
            else {
              return convertTimestamp(cv, columnIndex);
            }
          }

          AutoPtr<std::string> getBinary(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetBinary()) {
              return AutoPtr<std::string>(cv.getBinary(), false);
            }
            else {
              return convertBinary(cv, columnIndex);
            }
          }

          PDXObject getPDXObject(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetPDX()) {
              return PDXObject(cv.getPDX());
            }
            else {
              throw GET_DATACONVERSION_ERROR(cv, "PDX_OBJECT", columnIndex);
            }
          }

          JSONObject getJSONObject(const uint32_t columnIndex) const {
            const thrift::ColumnValue& cv = getColumnValue(columnIndex);

            if (cv.isSetJSON()) {
              return JSONObject(cv.getJSON());
            }
            else {
              throw GET_DATACONVERSION_ERROR(cv, "JSON_OBJECT", columnIndex);
            }
          }

          int32_t numColumns() const {
            return m_values.size();
          }

          inline bool isNull(const uint32_t columnIndex) const {
            return getColumnValue(columnIndex).isNull();
          }

          virtual ~Row() throw () {
          }
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* ROW_H_ */
