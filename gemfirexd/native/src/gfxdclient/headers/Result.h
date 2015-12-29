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
 * Result.h
 *
 * This class encapsulates the result of execution of a statement or prepared
 * statement.
 *
 *      Author: swale
 */

#ifndef RESULT_H_
#define RESULT_H_

#include "Types.h"
#include "PreparedStatement.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        class Result
        {
        private:
          impl::ClientService& m_service;
          void* const m_serviceId;
          const AutoPtr<const thrift::StatementAttrs> m_attrs;
          thrift::StatementResult m_result;

          Result(impl::ClientService& service, void* serviceId,
              const thrift::StatementAttrs* attrs, PreparedStatement* pstmt =
              NULL);

          Result(const Result&); // no copy constructor
          Result operator=(const Result&); // no assignment operator

          friend class Connection;
          friend class PreparedStatement;

          static void getResultSetArgs(const thrift::StatementAttrs* attrs,
              int32_t& batchSize, bool& updatable, bool& scrollable);

          ResultSet* newResultSet(thrift::RowSet& rowSet);

        public:

          AutoPtr<ResultSet> getResultSet();

          int32_t getUpdateCount() const throw ();

          AutoPtr<const Row> getOutputParameters() const;

          AutoPtr<ResultSet> getGeneratedKeys();

          inline bool hasWarnings() const throw () {
            return m_result.__isset.warnings
                || (m_result.__isset.resultSet
                    && m_result.resultSet.__isset.warnings);
          }

          AutoPtr<SQLWarning> getWarnings() const;

          AutoPtr<PreparedStatement> getPreparedStatement() const;

          ~Result();
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* RESULT_H_ */
