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
 * StatementAttributes.h
 *
 * Specifies various attributes for the statement that can alter the behaviour
 * of a statement execution.
 *
 *      Author: swale
 */

#ifndef STATEMENTATTRIBUTES_H_
#define STATEMENTATTRIBUTES_H_

#include "ClientBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        class StatementAttributes
        {
        public:
          static const StatementAttributes EMPTY;

          StatementAttributes() :
              m_attrs() {
            // TODO: need to implement proper BLOB/CLOB/UDT support
            // get full value in one shot for now
            m_attrs.__set_lobChunkSize(0);
          }

          StatementAttributes(const StatementAttributes& attrs) :
              m_attrs(attrs.m_attrs) {
          }

          ResultSetType::type getResultSetType() const throw () {
            return static_cast<ResultSetType::type>(
                m_attrs.__isset.resultSetType ? m_attrs.resultSetType :
                    thrift::g_gfxd_constants.DEFAULT_RESULTSET_TYPE);
          }

          ResultSetHoldability::type getResultSetHoldability() const throw () {
            return (m_attrs.holdCursorsOverCommit && m_attrs.__isset
                .holdCursorsOverCommit) || thrift::g_gfxd_constants
                .DEFAULT_RESULTSET_HOLD_CURSORS_OVER_COMMIT
                ? ResultSetHoldability::HOLD_CURSORS_OVER_COMMIT
                : ResultSetHoldability::CLOSE_CURSORS_OVER_COMMIT;
          }

          bool isUpdatable() const throw () {
            return m_attrs.__isset.updatable && m_attrs.updatable;
          }

          uint32_t getBatchSize() const throw () {
            return m_attrs.__isset.batchSize ? m_attrs.batchSize
                : thrift::g_gfxd_constants.DEFAULT_RESULTSET_BATCHSIZE;
          }

          bool isFetchDirectionReverse() const throw () {
            return m_attrs.__isset.fetchReverse && m_attrs.fetchReverse;
          }

          uint32_t getLobChunkSize() const throw () {
            return m_attrs.__isset.lobChunkSize ? m_attrs.lobChunkSize
                : thrift::g_gfxd_constants.DEFAULT_LOB_CHUNKSIZE;
          }

          uint32_t getMaxRows() const throw () {
            return m_attrs.__isset.maxRows ? m_attrs.maxRows : 0;
          }

          uint32_t getMaxFieldSize() const throw () {
            return m_attrs.__isset.maxFieldSize ? m_attrs.maxFieldSize : 0;
          }

          uint32_t getTimeout() const throw () {
            return m_attrs.__isset.timeout ? m_attrs.timeout : 0;
          }

          const std::string& getCursorName() const throw () {
            return
                m_attrs.__isset.cursorName ? m_attrs.cursorName : EMPTY_STRING;
          }

          bool isPoolable() const throw () {
            return m_attrs.__isset.poolable && m_attrs.poolable;
          }

          bool getEscapeProcessing() const throw () {
            return m_attrs.__isset.doEscapeProcessing
                && m_attrs.doEscapeProcessing;
          }

          void setResultSetType(ResultSetType::type rsType) throw ();
          void setResultSetHoldability(
              ResultSetHoldability::type holdability) throw ();
          void setUpdatable(bool updatable) throw ();
          void setBatchSize(uint32_t batchSize) throw ();
          void setFetchDirectionReverse(bool fetchDirection) throw ();
          void setLobChunkSize(uint32_t size) throw ();
          void setMaxRows(uint32_t num) throw ();
          void setFieldSize(uint32_t size) throw ();
          void setTimeout(uint32_t timeout) throw ();
          void setCursorName(const std::string& name) throw ();
          void setPoolable(bool poolable) throw ();
          void setEscapeProcessing(bool enable) throw ();

          ~StatementAttributes() throw () {
          }

        private:
          thrift::StatementAttrs m_attrs;

          StatementAttributes(const thrift::StatementAttrs& attrs) :
              m_attrs(attrs) {
          }

          friend class Connection;
          friend class PreparedStatement;
          friend class Result;
          friend class ResultSet;
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* STATEMENTATTRIBUTES_H_ */
