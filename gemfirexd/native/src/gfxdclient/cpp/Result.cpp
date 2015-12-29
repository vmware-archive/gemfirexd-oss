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
 * Result.cpp
 *
 *      Author: swale
 */

#include "Result.h"

#include "ResultSet.h"
#include "Row.h"
#include "PreparedStatement.h"
#include "StatementAttributes.h"

#include "impl/ClientService.h"

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::client;

Result::Result(impl::ClientService& service, void* serviceId,
    const thrift::StatementAttrs* attrs, PreparedStatement* pstmt) :
    m_service(service), m_serviceId(serviceId), m_attrs(
        attrs == NULL || attrs == &StatementAttributes::EMPTY.m_attrs ? NULL :
            new thrift::StatementAttrs(*attrs)), m_result() {
  // increment service reference
  impl::ClientServiceHolder::instance().incrementReferenceCount(serviceId);
}

void Result::getResultSetArgs(const thrift::StatementAttrs* attrs,
    int32_t& batchSize, bool& updatable, bool& scrollable) {
  batchSize = thrift::g_gfxd_constants.DEFAULT_RESULTSET_BATCHSIZE;
  updatable = false;
  scrollable = false;
  if (attrs != NULL) {
    if (attrs->__isset.batchSize) {
      batchSize = attrs->batchSize;
    }
    if (attrs->__isset.updatable) {
      updatable = attrs->updatable;
    }
    if (attrs->__isset.resultSetType) {
      scrollable = (attrs->resultSetType
          != thrift::g_gfxd_constants.RESULTSET_TYPE_FORWARD_ONLY);
    }
  }
}

ResultSet* Result::newResultSet(thrift::RowSet& rowSet) {
  int32_t batchSize;
  bool updatable, scrollable;
  const thrift::StatementAttrs* attrs = m_attrs.get();
  getResultSetArgs(attrs, batchSize, updatable, scrollable);
  return new ResultSet(&rowSet, attrs, false, m_service, m_serviceId, batchSize,
      updatable, scrollable, false);
}

AutoPtr<ResultSet> Result::getResultSet() {
  if (m_result.__isset.resultSet) {
    return AutoPtr<ResultSet>(newResultSet(m_result.resultSet));
  }
  else {
    return AutoPtr<ResultSet>(NULL);
  }
}

int32_t Result::getUpdateCount() const throw () {
  return (m_result.__isset.updateCount ? m_result.updateCount : -1);
}

AutoPtr<const Row> Result::getOutputParameters() const {
  if (m_result.__isset.procedureOutParams) {
    return AutoPtr<const Row>(
        new (const_cast<thrift::Row*>(&m_result.procedureOutParams)) Row(false),
        false);
  }
  else {
    return AutoPtr<const Row>(NULL);
  }
}

AutoPtr<ResultSet> Result::getGeneratedKeys() {
  if (m_result.__isset.generatedKeys) {
    return AutoPtr<ResultSet>(newResultSet(m_result.generatedKeys));
  }
  else {
    return AutoPtr<ResultSet>(NULL);
  }
}

AutoPtr<SQLWarning> Result::getWarnings() const {
  if (m_result.__isset.warnings) {
    return AutoPtr<SQLWarning>(new GET_SQLWARNING(m_result.warnings));
  }
  else if (m_result.__isset.resultSet
      && m_result.resultSet.__isset.warnings) {
    return AutoPtr<SQLWarning>(
        new SQLWarning(__FILE__, __LINE__,
            m_result.resultSet.warnings));
  }
  else {
    return AutoPtr<SQLWarning>(NULL);
  }
}

AutoPtr<PreparedStatement> Result::getPreparedStatement() const {
  if (m_result.__isset.preparedResult) {
    // deliberately making a copy of preparedResult to transfer full
    // ownership to that
    const thrift::StatementAttrs* attrs = m_attrs.get();
    return AutoPtr<PreparedStatement>(
        new PreparedStatement(m_service, m_serviceId,
            attrs == NULL ? StatementAttributes::EMPTY.m_attrs : *attrs,
            m_result.preparedResult));
  }
  else {
    return AutoPtr<PreparedStatement>(NULL);
  }
}

Result::~Result() {
  // destructor should *never* throw an exception
  try {
    // decrement service reference
    impl::ClientServiceHolder::instance().decrementReferenceCount(m_serviceId);
  } catch (const SQLException& sqle) {
    Utils::handleExceptionInDestructor("result", sqle);
  } catch (const std::exception& stde) {
    Utils::handleExceptionInDestructor("result", stde);
  } catch (...) {
    Utils::handleExceptionInDestructor("result");
  }
}
