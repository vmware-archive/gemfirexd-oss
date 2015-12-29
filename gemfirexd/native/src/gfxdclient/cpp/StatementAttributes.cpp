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
 * StatementAttributes.cpp
 *
 *  Created on: 04-May-2013
 *      Author: swale
 */

#include "StatementAttributes.h"

using namespace com::pivotal::gemfirexd::client;

const StatementAttributes StatementAttributes::EMPTY;

void StatementAttributes::setResultSetType(ResultSetType::type rsType) throw () {
  m_attrs.__set_resultSetType((int8_t)rsType);
}

void StatementAttributes::setResultSetHoldability(
    ResultSetHoldability::type holdability) throw () {
  m_attrs.__set_holdCursorsOverCommit(
      holdability == ResultSetHoldability::HOLD_CURSORS_OVER_COMMIT);
}

void StatementAttributes::setUpdatable(bool updatable) throw () {
  m_attrs.__set_updatable(updatable);
}

void StatementAttributes::setBatchSize(uint32_t batchSize) throw () {
  m_attrs.__set_batchSize(batchSize);
}

void StatementAttributes::setFetchDirectionReverse(bool fetchDirection) throw () {
  m_attrs.__set_fetchReverse(fetchDirection);
}

void StatementAttributes::setLobChunkSize(uint32_t size) throw () {
  m_attrs.__set_lobChunkSize(size);
}

void StatementAttributes::setMaxRows(uint32_t num) throw () {
  m_attrs.__set_maxRows(num);
}

void StatementAttributes::setFieldSize(uint32_t size) throw () {
  m_attrs.__set_maxFieldSize(size);
}

void StatementAttributes::setTimeout(uint32_t timeout) throw () {
  m_attrs.__set_timeout(timeout);
}

void StatementAttributes::setCursorName(const std::string& name) throw () {
  m_attrs.__set_cursorName(name);
}

void StatementAttributes::setPoolable(bool poolable) throw () {
  m_attrs.__set_poolable(poolable);
}

void StatementAttributes::setEscapeProcessing(bool enable) throw () {
  m_attrs.__set_doEscapeProcessing(enable);
}
