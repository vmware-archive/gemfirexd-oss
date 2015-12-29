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
 * PreparedStatement.h
 *
 *      Author: swale
 */

#ifndef PREPAREDSTATEMENT_H_
#define PREPAREDSTATEMENT_H_

#include "Types.h"
#include "ParameterDescriptor.h"
#include "StatementAttributes.h"
#include "ResultSet.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        class PreparedStatement
        {
        private:
          impl::ClientService& m_service;
          void* m_serviceId;
          StatementAttributes m_attrs;
          thrift::PrepareResult m_prepResult;
          AutoPtr<SQLWarning> m_warnings;
          AutoPtr<std::map<int32_t, thrift::OutputParameter> > m_outParams;
          // cached Result/ResultSet for reuse
          ResultSet* m_result;

          friend class Connection;
          friend class Result;

          PreparedStatement(impl::ClientService& service, void* serviceId,
              const thrift::StatementAttrs& attrs);

          PreparedStatement(impl::ClientService& service, void* serviceId,
              const thrift::StatementAttrs& attrs,
              const thrift::PrepareResult& prepResult);

          // no copy constructor or assignment operator
          PreparedStatement(const PreparedStatement&);
          PreparedStatement operator=(const PreparedStatement&);

          inline impl::ClientService& checkAndGetService() const {
            if (m_serviceId != NULL) {
              return m_service;
            }
            else {
              throw GET_SQLEXCEPTION2(SQLStateMessage::ALREADY_CLOSED_MSG);
            }
          }

        public:

          void registerOutParameter(const int32_t parameterIndex,
              const SQLType::type type);

          void registerOutParameter(const int32_t parameterIndex,
              const SQLType::type type, const int32_t scale);

          void unregisterOutParameter(const int32_t parameterIndex);

          AutoPtr<Result> execute(const Parameters& params);

          int32_t executeUpdate(const Parameters& params);

          AutoPtr<ResultSet> executeQuery(const Parameters& params);

          AutoPtr<std::vector<int32_t> > executeBatch(
              const ParametersBatch& paramsBatch);

          inline bool hasWarnings() const throw () {
            return !m_warnings.isNull() || m_prepResult.__isset.warnings;
          }

          AutoPtr<SQLWarning> getWarnings();

          size_t getParameterCount() const throw () {
            return m_prepResult.parameterMetaData.size();
          }

          ParameterDescriptor getParameterDescriptor(
              const uint32_t parameterIndex);

          uint32_t getColumnCount() const throw () {
            return m_prepResult.resultSetMetaData.size();
          }

          inline ColumnDescriptor getColumnDescriptor(
              const uint32_t columnIndex) {
            return ResultSet::getColumnDescriptor(
                m_prepResult.resultSetMetaData, columnIndex);
          }

          void cancel();

          void close();

          ~PreparedStatement() throw ();
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* PREPAREDSTATEMENT_H_ */
