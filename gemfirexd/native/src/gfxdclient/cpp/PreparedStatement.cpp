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
 * PreparedStatement.cpp
 *
 *      Author: swale
 */

#include "PreparedStatement.h"

#include "Parameters.h"
#include "ParametersBatch.h"
#include "ResultSet.h"
#include "Result.h"

#include "impl/ClientService.h"
#include "impl/InternalUtils.h"

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::client;

PreparedStatement::PreparedStatement(impl::ClientService& service,
    void* serviceId, const thrift::StatementAttrs& attrs) :
    m_service(service), m_serviceId(serviceId), m_attrs(attrs),
    m_prepResult(), m_warnings(NULL), m_result(NULL) {
  // register reference for service
  impl::ClientServiceHolder::instance().incrementReferenceCount(serviceId);
}

PreparedStatement::PreparedStatement(impl::ClientService& service,
    void* serviceId, const thrift::StatementAttrs& attrs,
    const thrift::PrepareResult& prepResult) :
    m_service(service), m_serviceId(serviceId), m_attrs(attrs),
    m_prepResult(prepResult), m_warnings(NULL), m_result(NULL) {
  // register reference for service
  impl::ClientServiceHolder::instance().incrementReferenceCount(serviceId);
}

void PreparedStatement::registerOutParameter(const int32_t parameterIndex,
    const SQLType::type type) {
  if (m_outParams.isNull()) {
    m_outParams.reset(new std::map<int32_t, thrift::OutputParameter>());
  }
  thrift::OutputParameter outParam;
  outParam.__set_type(type);
  m_outParams->operator[](parameterIndex) = outParam;
}

void PreparedStatement::registerOutParameter(const int32_t parameterIndex,
    const SQLType::type type, const int32_t scale) {
  if (m_outParams.isNull()) {
    m_outParams.reset(new std::map<int32_t, thrift::OutputParameter>());
  }
  thrift::OutputParameter outParam;
  outParam.__set_type(type);
  outParam.__set_scale(scale);
  m_outParams->operator[](parameterIndex) = outParam;
}

void PreparedStatement::unregisterOutParameter(const int32_t parameterIndex) {
  if (!m_outParams.isNull()) {
    m_outParams->erase(parameterIndex);
  }
}

AutoPtr<Result> PreparedStatement::execute(const Parameters& params) {
  impl::ClientService& service = checkAndGetService();

  AutoPtr<Result> result(
      new Result(service, m_serviceId, &m_attrs.m_attrs, this));

  service.executePrepared(result->m_result, m_prepResult, params,
      m_outParams.isNull() ? EMPTY_OUT_PARAMS : *m_outParams);
  if (result->m_result.__isset.warnings) {
    // set back in PreparedStatement
    AutoPtr<SQLWarning> warnings = result->getWarnings();
    if (warnings.isOwner()) {
      m_warnings.reset(warnings.release());
    }
    else {
      m_warnings.reset(new SQLWarning(*warnings));
    }
  }
  return result;
}

int32_t PreparedStatement::executeUpdate(const Parameters& params) {
  impl::ClientService& service = checkAndGetService();

  thrift::UpdateResult result;
  service.executePreparedUpdate(result, m_prepResult, params);
  if (result.__isset.warnings) {
    // set back in PreparedStatement
    m_warnings.reset(new GET_SQLWARNING(result.warnings));
  }
  if (result.__isset.updateCount) {
    return result.updateCount;
  }
  else {
    return -1;
  }
}

AutoPtr<ResultSet> PreparedStatement::executeQuery(const Parameters& params) {
  impl::ClientService& service = checkAndGetService();

  thrift::PrepareResult& prepResult = m_prepResult;
  int32_t batchSize;
  bool updatable, scrollable;
  Result::getResultSetArgs(&m_attrs.m_attrs, batchSize, updatable, scrollable);

  thrift::RowSet* rsp;
  if (m_result == NULL) {
    rsp = new thrift::RowSet();
    m_result = new ResultSet(rsp, &m_attrs.m_attrs, true, service,
        m_serviceId, batchSize, updatable, scrollable, false /* for reuse */);
    // resize the vectors to some reasonable values
    m_result->m_p->rows.reserve(4);
    m_result->m_p->metadata.reserve(10);
  }
  // TODO: can also enable reuse in case the AutoPtr gets destroyed
  // but for that need to have another flag in AutoPtr which will
  // tell whether this is the last owner of the ptr regardless of
  // whether "managed" is true or not initially, and then call back
  // into ResultSet.cleanupRS
  else if (m_result->m_inUse) {
    // need to create a new resultSet in this case
    rsp = new thrift::RowSet();
    AutoPtr<thrift::RowSet> rs(rsp);
    service.executePreparedQuery(*rsp, prepResult, params);

    AutoPtr<ResultSet> resultSet(
        new ResultSet(rsp, &m_attrs.m_attrs, true, service, m_serviceId,
            batchSize, updatable, scrollable, true));
    rs.release();
    return resultSet;
  }
  else {
    rsp = m_result->m_p;
  }

  service.executePreparedQuery(*rsp, prepResult, params);
  // (re)set the cursorId
  m_result->m_cursorId = rsp->cursorId;
  return AutoPtr<ResultSet>(m_result, false);
}

AutoPtr<std::vector<int32_t> > PreparedStatement::executeBatch(
    const ParametersBatch& paramsBatch) {
  impl::ClientService& service = checkAndGetService();

  thrift::UpdateResult result;
  service.executePreparedBatch(result, m_prepResult, paramsBatch.m_batch);
  if (result.__isset.warnings) {
    // set back in PreparedStatement
    m_warnings.reset(new GET_SQLWARNING(result.warnings));
  }
  if (result.__isset.batchUpdateCounts) {
    return AutoPtr<std::vector<int32_t> >(
        new std::vector<int32_t>(result.batchUpdateCounts));
  }
  else {
    return AutoPtr<std::vector<int32_t> >(NULL);
  }
}

AutoPtr<SQLWarning> PreparedStatement::getWarnings() {
  if (!m_warnings.isNull()) {
    return AutoPtr<SQLWarning>(m_warnings.get(), false);
  }
  else if (m_prepResult.__isset.warnings) {
    return AutoPtr<SQLWarning>(new GET_SQLWARNING(
        m_prepResult.warnings), true);
  }
  else {
    return AutoPtr<SQLWarning>(NULL);
  }
}

ParameterDescriptor PreparedStatement::getParameterDescriptor(
    const uint32_t parameterIndex) {
  // Check that parameterIndex is in range.
  if (parameterIndex >= 1
      && parameterIndex <= m_prepResult.parameterMetaData.size()) {
    return ParameterDescriptor(
        m_prepResult.parameterMetaData[parameterIndex - 1], parameterIndex);
  }
  else {
    throw GET_SQLEXCEPTION2(SQLStateMessage::LANG_INVALID_PARAM_POSITION_MSG,
        parameterIndex, m_prepResult.parameterMetaData.size());
  }
}

namespace _gfxd_impl
{
  struct ClearPSTMT
  {
    ResultSet*& m_result;
    bool& m_isOwner;

    ~ClearPSTMT() {
      m_isOwner = true;
      delete m_result;
      m_result = NULL;
    }
  };
}

void PreparedStatement::cancel() {
  const int32_t stmtId = m_prepResult.statementId;
  if (stmtId != 0) {
    impl::ClientService& service = checkAndGetService();

    service.cancelStatement(stmtId);
  }
}

void PreparedStatement::close() {
  if (m_result != NULL) {
    // force cleanup of cached result set at this point in all cases
    _gfxd_impl::ClearPSTMT clr = { m_result, m_result->m_isOwner };

    if (m_result->m_inUse) {
      m_result->close();
    }
  }
  if (m_serviceId != NULL) {
    if (m_prepResult.statementId != 0) {
      m_service.closeStatement(m_prepResult.statementId);
    }
  }
}

PreparedStatement::~PreparedStatement() throw () {
  // decrement service reference in all cases
  impl::ClearService clr = { m_serviceId, NULL };
  // destructor should *never* throw an exception
  try {
    close();
  } catch (const SQLException& sqle) {
    Utils::handleExceptionInDestructor("prepared statement", sqle);
  } catch (const std::exception& stde) {
    Utils::handleExceptionInDestructor("prepared statement", stde);
  } catch (...) {
    Utils::handleExceptionInDestructor("prepared statement");
  }
}
