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
 * UpdatableRow.cpp
 *
 *      Author: swale
 */

#include "UpdatableRow.h"

#include <boost/dynamic_bitset.hpp>

#include "Utils.h"

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::client;

inline thrift::ColumnValue* UpdatableRow::getColumnValueForUpdate(
    const uint32_t columnIndex) {
  if (m_updatable) {
    thrift::ColumnValue* cv = getColumnValue_(columnIndex);
    if (m_changedColumns == NULL) {
      m_changedColumns = new DynamicBitSet(m_values.size() + 1);
    }
    m_changedColumns->set(columnIndex, true);
    return cv;
  } else {
    throw GET_SQLEXCEPTION2(
        SQLStateMessage::UPDATABLE_RESULTSET_API_DISALLOWED_MSG,
        "UpdatableRow::setXXX");
  }
}

std::vector<int32_t> UpdatableRow::getChangedColumnsAsVector() {
  DynamicBitSet* bitSet = m_changedColumns;
  std::vector<int32_t> changedColumns;
  if (bitSet != NULL) {
    for (size_t pos = bitSet->find_first(); pos != DynamicBitSet::npos;
        pos = bitSet->find_next(pos)) {
      changedColumns.push_back(pos);
    }
  }
  return changedColumns;
}

void UpdatableRow::setBoolean(const uint32_t columnIndex, const bool v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setBool(v);
}

void UpdatableRow::setByte(const uint32_t columnIndex, const int8_t v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setByte(v);
}

void UpdatableRow::setShort(const uint32_t columnIndex, const int16_t v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setI16(v);
}

void UpdatableRow::setInt(const uint32_t columnIndex, const int32_t v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setI32(v);
}

void UpdatableRow::setLong(const uint32_t columnIndex, const int64_t v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setI64(v);
}

void UpdatableRow::setFloat(const uint32_t columnIndex, const float v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setFloat(v);
}

void UpdatableRow::setDouble(const uint32_t columnIndex, const double v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setDouble(v);
}

void UpdatableRow::setString(const uint32_t columnIndex, const std::string& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setString(v);
}

void UpdatableRow::setDecimal(const uint32_t columnIndex, const Decimal& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setDecimal(thrift::Decimal());
  v.copyTo(*cv->getDecimal());
}

void UpdatableRow::setDate(const uint32_t columnIndex, const DateTime v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setDate(v.m_secsSinceEpoch);
}

void UpdatableRow::setTime(const uint32_t columnIndex, const DateTime v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setTime(v.m_secsSinceEpoch);
}

void UpdatableRow::setTimestamp(const uint32_t columnIndex,
    const Timestamp& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setTimestamp(v.getEpochTime(), v.getNanos());
}

void UpdatableRow::setBinary(const uint32_t columnIndex, const std::string& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setBinary(v);
}

void UpdatableRow::setPDXObject(const uint32_t columnIndex,
    const PDXObject& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setPDX(*v.m_p);
}

void UpdatableRow::setJSONObject(const uint32_t columnIndex,
    const JSONObject& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setJSON(*v.m_p);
}

void UpdatableRow::setNull(const uint32_t columnIndex) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setNull();
}

#if __cplusplus >= 201103L

void UpdatableRow::setString(const uint32_t columnIndex, std::string&& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setString(std::move(v));
}

void UpdatableRow::setBinary(const uint32_t columnIndex, std::string&& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setBinary(std::move(v));
}

void UpdatableRow::setPDXObject(const uint32_t columnIndex, PDXObject&& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setPDX(std::move(*v.m_p));
}

void UpdatableRow::setJSONObject(const uint32_t columnIndex, JSONObject&& v) {
  thrift::ColumnValue* cv = getColumnValueForUpdate(columnIndex);
  cv->setJSON(std::move(*v.m_p));
}

#endif
