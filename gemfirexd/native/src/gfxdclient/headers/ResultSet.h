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
 * ResultSet.h
 *
 *      Author: swale
 */

#ifndef RESULTSET_H_
#define RESULTSET_H_

#include "Types.h"
#include "ColumnDescriptor.h"
#include "UpdatableRow.h"
#include "WrapperBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        class ResultSet : WrapperBase<thrift::RowSet>
        {
        private:
          impl::ClientService& m_service;
          void* m_serviceId;
          const AutoPtr<const thrift::StatementAttrs> m_attrs;
          int32_t m_cursorId;
          const int32_t m_batchSize;
          const bool m_updatable;
          const bool m_scrollable;
          // TODO: SW: need actual batch offset rather than just number
          uint32_t m_batchOffset;
          std::vector<thrift::ColumnDescriptor>* m_descriptors;
          mutable std::map<std::string, uint32_t>* m_columnPositionMap;
          mutable bool m_inUse;

          ResultSet(thrift::RowSet* rows, const thrift::StatementAttrs* attrs,
              bool copyAttrs, impl::ClientService& service, void* serviceId,
              const int32_t batchSize, bool updatable, bool scrollable,
              bool isOwner);

          // no assignment operator or copy constructor
          ResultSet(const ResultSet&);
          ResultSet operator=(const ResultSet&);

          friend class Connection;
          friend class PreparedStatement;
          friend class Result;

          inline void setOwner(bool owner) {
            m_isOwner = owner;
          }

          inline void checkOpen(const char* operation) const {
            if (m_p != NULL) {
              return;
            } else {
              throw GET_SQLEXCEPTION2(
                  SQLStateMessage::LANG_RESULT_SET_NOT_OPEN_MSG, operation);
            }
          }

          inline void checkScrollable(const char* operation) const {
            if (m_scrollable) {
              return;
            } else {
              throw GET_SQLEXCEPTION2(
                  SQLStateMessage::CURSOR_MUST_BE_SCROLLABLE_MSG, operation);
            }
          }

          inline void copyDescriptors() {
            if (m_descriptors == NULL && m_p->metadata.size() > 0) {
              m_descriptors = new std::vector<thrift::ColumnDescriptor>(
                  m_p->metadata);
            }
          }

          static ColumnDescriptor getColumnDescriptor(
              std::vector<thrift::ColumnDescriptor>& descriptors,
              const uint32_t columnIndex);

          bool moveNextRowSet(int32_t offset);
          bool moveToRowSet(int32_t offset, int32_t batchSize);

          void insertRow(UpdatableRow* row, size_t rowIndex);
          void updateRow(UpdatableRow* row, size_t rowIndex);
          void deleteRow(UpdatableRow* row, size_t rowIndex);

          void cleanupRS();

          template<typename TRow, typename TRowP, bool updatable>
          class Itr
          {
          private:
            ResultSet* m_resultSet;
            thrift::RowSet* m_rows;
            // using STL iterator is bit hard (and slightly expensive) here
            // due to random access nature of ItrScroll that can move it past
            // the current vector where we have to get new set of rows
            TRow* m_currentRow;
            TRow* m_endBatch;
            TRow* m_insertRow;

            void resetPositions() {
              size_t sz = m_rows->rows.size();
              if (sz > 0) {
                m_currentRow = new (&m_rows->rows[0]) TRow(updatable);
                m_endBatch = m_currentRow + sz;
              } else {
                m_currentRow = m_endBatch = NULL;
              }
            }

            void init(uint32_t pos, bool isEnd) {
              if (isEnd) {
                m_currentRow = m_endBatch = NULL;
              }
              // special check for pos == 0
              else if (pos == 0 && m_resultSet->m_batchOffset == 0) {
                resetPositions();
              } else {
                // check if pos lies in current batch
                size_t sz = m_rows->rows.size();
                if (pos < m_resultSet->m_batchOffset
                    || pos >= (m_resultSet->m_batchOffset + sz)) {
                  // move to the required position
                  if (!m_resultSet->moveToRowSet(pos,
                      m_resultSet->m_batchSize)) {
                    // indicate end
                    m_currentRow = m_endBatch = NULL;
                    return;
                  }
                  sz = m_rows->rows.size();
                }
                TRow* start = new (&m_rows->rows[0]) TRow(updatable);
                m_currentRow = start + (pos - m_resultSet->m_batchOffset);
                m_endBatch = start + sz;
                // indicate end if pos is absent in the reply from server
                if (sz == 0 || m_currentRow < start
                    || m_currentRow >= m_endBatch) {
                  m_currentRow = NULL;
                  m_endBatch = NULL;
                }
              }
            }

            template<typename T>
            void setFields(const T& other) {
              m_resultSet = other.m_resultSet;
              m_rows = other.m_rows;
              m_currentRow = other.m_currentRow;
              m_endBatch = other.m_endBatch;
            }

            inline void checkOnRow(const char* operation) const {
              if (m_currentRow != NULL) {
                return;
              } else if (m_rows == NULL) {
                throw GET_SQLEXCEPTION2(
                    SQLStateMessage::LANG_RESULT_SET_NOT_OPEN_MSG, operation);
              } else {
                throw GET_SQLEXCEPTION2(SQLStateMessage::NO_CURRENT_ROW_MSG);
              }
            }

            inline void checkUpdatable(const char* operation) const {
              if (updatable) {
                checkOnRow(operation);
              } else {
                throw GET_SQLEXCEPTION2(
                    SQLStateMessage::UPDATABLE_RESULTSET_API_DISALLOWED_MSG,
                    operation);
              }
            }

          public:
            Itr() :
                m_resultSet(0), m_rows(0), m_currentRow(0), m_endBatch(0),
                m_insertRow(0) {
            }

            Itr(ResultSet* rs, uint32_t pos, bool isEnd) :
                m_resultSet(rs), m_rows(rs->m_p) {
              init(pos, isEnd);
            }

            Itr(const Itr& other) :
                m_resultSet(other.m_resultSet), m_rows(other.m_rows),
                m_currentRow(other.m_currentRow), m_endBatch(other.m_endBatch),
                m_insertRow(other.m_insertRow) {
            }

            ~Itr() throw () {
              clearInsertRow();
            }

            Itr& operator=(const Itr& other) {
              setFields(other);
              return *this;
            }

            void initialize(ResultSet& resultSet) {
              m_resultSet = &resultSet;
              m_rows = resultSet.m_p;
              resetPositions();
            }

            void reset(ResultSet& resultSet, uint32_t pos) {
              m_resultSet = &resultSet;
              m_rows = resultSet.m_p;
              init(pos, false);
            }

            void clear() {
              m_resultSet = NULL;
              m_rows = NULL;
              m_currentRow = NULL;
              m_endBatch = NULL;
              clearInsertRow();
            }

            inline bool isOnRow() const throw () {
              return m_currentRow != NULL;
            }

            TRow& operator*() const {
              return *m_currentRow;
            }

            TRowP operator->() const {
              return m_currentRow;
            }

            TRowP get() const {
              return m_currentRow;
            }

            Itr& operator++() {
              ++m_currentRow;
              // check if we have reached end
              if (m_currentRow >= m_endBatch) {
                if ((m_rows->flags & thrift::g_gfxd_constants
                    .ROWSET_LAST_BATCH) == 0) {
                  // go on to next set of rows
                  size_t sz = m_rows->rows.size();
                  int32_t offset = m_resultSet->m_batchOffset + sz;
                  if (m_resultSet->moveNextRowSet(offset)
                      && (sz = m_rows->rows.size()) > 0) {
                    m_currentRow = new (&m_rows->rows[0]) TRow(updatable);
                    m_endBatch = m_currentRow + sz;
                  }
                  else {
                    // indicate end of iteration
                    m_currentRow = m_endBatch = NULL;
                  }
                }
                else {
                  // indicate end of iteration
                  m_currentRow = m_endBatch = NULL;
                }
              }
              return *this;
            }

            Itr& operator--() {
              // check if we have reached start
              if (m_currentRow <= &m_rows->rows[0]) {
                int32_t offset = m_resultSet->m_batchOffset;
                if (offset > 0) {
                  // go on to previous set of rows
                  size_t sz;
                  int32_t batchSize = m_resultSet->m_batchSize;
                  if (offset >= batchSize) {
                    offset -= batchSize;
                  }
                  else {
                    batchSize = offset;
                    offset = 0;
                  }
                  if (m_resultSet->moveToRowSet(offset, batchSize)
                      && (sz = m_rows->rows.size()) > 0) {
                    m_currentRow = m_endBatch = new (
                        &m_rows->rows[0] + sz) TRow(updatable);
                  }
                  else {
                    // indicate that there is nothing to iterate
                    m_currentRow = m_endBatch = NULL;
                    return *this;
                  }
                }
                else {
                  // indicate that there is nothing to iterate
                  m_currentRow = m_endBatch = NULL;
                  return *this;
                }
              }
              --m_currentRow;
              return *this;
            }

            Itr& operator+=(const uint32_t n) {
              thrift::Row* row = (m_currentRow + n);
              // check if we have reached end
              if (row >= m_endBatch) {
                if ((m_rows->flags
                    & thrift::g_gfxd_constants.ROWSET_LAST_BATCH) == 0) {
                  // jump to an appropriate absolute offset
                  int32_t offset = m_resultSet->m_batchOffset
                      + (row - &m_rows->rows[0]);
                  // move to the required batch offset
                  size_t sz;
                  if (m_resultSet->moveToRowSet(offset,
                      m_resultSet->m_batchSize)
                      && (sz = m_rows->rows.size()) > 0) {
                    m_currentRow = new (&m_rows->rows[0]) TRow(updatable);
                    m_endBatch = m_currentRow + sz;
                  } else {
                    // indicate end of iteration
                    m_currentRow = m_endBatch = NULL;
                    return *this;
                  }
                } else {
                  // indicate end of iteration
                  m_currentRow = m_endBatch = NULL;
                  return *this;
                }
              } else {
                m_currentRow = new (row) TRow(updatable);
              }
              return *this;
            }

            Itr& operator-=(const uint32_t n) {
              // check if we can find the position in current batch
              thrift::Row* start = &m_rows->rows[0];
              TRow* row = (m_currentRow - n);
              if (row < start) {
                int32_t offset = m_resultSet->m_batchOffset;
                if (offset > 0) {
                  // go on to previous set of rows
                  size_t sz;
                  int32_t batchSize = m_resultSet->m_batchSize;
                  int32_t delta = (start - row - 1);
                  bool positionAtEnd = true;
                  if (offset >= (batchSize + delta)) {
                    offset -= (batchSize + delta);
                  }
                  else if (offset > delta) {
                    batchSize = offset - delta;
                    offset = 0;
                  }
                  else {
                    batchSize = offset;
                    offset = 0;
                    positionAtEnd = false;
                  }
                  if (m_resultSet->moveToRowSet(offset, batchSize)
                      && (sz = m_rows->rows.size()) > 0) {
                    if (positionAtEnd) {
                      m_endBatch = (&m_rows->rows[0] + sz);
                      row = m_endBatch - 1;
                    }
                    else {
                      row = &m_rows->rows[0];
                      m_endBatch = (row + sz);
                    }
                  }
                  else {
                    // indicate that there is nothing to iterate
                    m_currentRow = m_endBatch = NULL;
                    return *this;
                  }
                }
                else {
                  // indicate that there is nothing to iterate
                  m_currentRow = m_endBatch = NULL;
                  return *this;
                }
              }
              m_currentRow = row;
              return *this;
            }

            TRow* getInsertRow() {
              checkUpdatable("getInsertRow");

              if (m_insertRow != NULL) {
                return m_insertRow;
              } else {
                return (m_insertRow = new TRow());
              }
            }

            void clearInsertRow() {
              if (m_insertRow != NULL) {
                delete m_insertRow;
                m_insertRow = NULL;
              }
            }

            void insertRow() {
              checkUpdatable("insertRow");

              // TODO: handle SCROLL_SENSITIVE for insertRow/updateRow/deleteRow
              // but first need to add that to server-side

              const size_t position = (m_currentRow - &m_rows->rows[0]);
              m_resultSet->insertRow(m_insertRow, position);
            }

            void updateRow() {
              checkUpdatable("updateRow");

              const size_t position = (m_currentRow - &m_rows->rows[0]);
              m_resultSet->updateRow(m_currentRow, position);
            }

            void deleteRow() {
              checkUpdatable("deleteRow");

              const size_t position = (m_currentRow - &m_rows->rows[0]);
              m_resultSet->deleteRow(m_currentRow, position);
            }
          };

        public:
          typedef Itr<Row, const Row*, false> const_iterator;
          typedef Itr<UpdatableRow, UpdatableRow*, true> iterator;

          const_iterator cbegin(uint32_t pos = 0) const;

          inline const_iterator begin(uint32_t pos = 0) const {
            return cbegin(pos);
          }

          iterator begin(uint32_t pos = 0);

          inline const const_iterator& cend() const {
            return ITR_END_CONST;
          }

          inline const const_iterator& end() const {
            return ITR_END_CONST;
          }

          inline const iterator& end() {
            return ITR_END;
          }

          inline bool isOpen() const throw () {
            return m_p != NULL;
          }

          inline uint32_t getColumnCount() const throw () {
            return (m_descriptors == NULL ? m_p->metadata.size()
                : m_descriptors->size());
          }

          /**
           * Get the column position given its name.
           */
          uint32_t getColumnPosition(const std::string& name) const;

          inline ColumnDescriptor getColumnDescriptor(
              const uint32_t columnIndex) {
            return getColumnDescriptor(
                m_descriptors == NULL ? m_p->metadata : *m_descriptors,
                columnIndex);
          }

          int32_t getRow() const;

          ResultSetType::type getResultSetType() const throw () {
            return static_cast<ResultSetType::type>(m_attrs.get() != NULL
                && m_attrs->__isset.resultSetType ? m_attrs->resultSetType
                    : thrift::g_gfxd_constants.DEFAULT_RESULTSET_TYPE);
          }

          bool isUpdatable() const throw () {
            return m_attrs.get() != NULL && m_attrs->__isset.updatable
                && m_attrs->updatable;
          }

          inline bool hasWarnings() const throw () {
            const thrift::RowSet* rs = m_p;
            return rs != NULL && rs->__isset.warnings;
          }

          AutoPtr<SQLWarning> getWarnings() const;

          AutoPtr<ResultSet> clone() const;

          void close();

          virtual ~ResultSet() throw ();

        private:
          static const const_iterator ITR_END_CONST;
          static const iterator ITR_END;
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* RESULTSET_H_ */
