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
 * ResultSet.cpp
 *
 *      Author: swale
 */

#include "ResultSet.h"

#include "impl/ClientService.h"
#include "impl/InternalUtils.h"
#include "StatementAttributes.h"

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::client;

const ResultSet::const_iterator ResultSet::ITR_END_CONST;
const ResultSet::iterator ResultSet::ITR_END;

ResultSet::ResultSet(thrift::RowSet* rows, const thrift::StatementAttrs* attrs,
    bool copyAttrs, impl::ClientService& service, void* serviceId,
    const int32_t batchSize, bool updatable, bool scrollable, bool isOwner) :
    WrapperBase<thrift::RowSet>(rows, isOwner), m_service(service),
    m_serviceId(serviceId), m_attrs(attrs == NULL || attrs ==
        &StatementAttributes::EMPTY.m_attrs ? NULL
            : (copyAttrs ? new thrift::StatementAttrs(*attrs) : attrs),
        copyAttrs), m_cursorId(rows->cursorId), m_batchSize(batchSize),
        m_updatable(updatable), m_scrollable(scrollable), m_batchOffset(0),
        m_descriptors(NULL), m_columnPositionMap(NULL), m_inUse(true) {
  // register reference for service
  impl::ClientServiceHolder::instance().incrementReferenceCount(serviceId);
}

bool ResultSet::moveNextRowSet(int32_t offset) {
  checkOpen("moveNext");

  // copy descriptors prior to move since descriptors may not be
  // set by server in subsequent calls
  copyDescriptors();

  m_service.scrollCursor(*m_p, m_cursorId, offset, false, false, m_batchSize);
  if (m_p->rows.size() > 0) {
    m_batchOffset = m_p->offset;
    return true;
  }
  else {
    return false;
  }
}

bool ResultSet::moveToRowSet(int32_t offset, int32_t batchSize) {
  checkOpen("moveToRowSet");
  checkScrollable("moveToRowSet");

  // copy descriptors prior to move since descriptors may not be
  // set by server in subsequent calls
  copyDescriptors();

  m_service.scrollCursor(*m_p, m_cursorId, offset, false, false, batchSize);
  if (m_p->rows.size() > 0) {
    m_batchOffset = m_p->offset;
    return true;
  }
  else {
    return false;
  }
}

struct ClearRow {
private:
  UpdatableRow* m_row;

public:
  ClearRow(UpdatableRow* row) :
      m_row(row) {
  }
  ~ClearRow() {
    m_row->clearChangedColumns();
  }
};

void ResultSet::insertRow(UpdatableRow* row, size_t rowIndex) {
  if (row != NULL && row->getChangedColumns() != NULL) {
    ClearRow clearRow(row);
    std::vector<int32_t> changedColumns = row->getChangedColumnsAsVector();
    if (changedColumns.size() > 0) {
      m_service.executeCursorUpdate(m_cursorId,
          thrift::gfxdConstants::CURSOR_INSERT, *row, changedColumns, rowIndex);
      return;
    }
  }
  throw GET_SQLEXCEPTION2(
      SQLStateMessage::CURSOR_NOT_POSITIONED_ON_INSERT_ROW_MSG);
}

void ResultSet::updateRow(UpdatableRow* row, size_t rowIndex) {
  if (row != NULL && row->getChangedColumns() != NULL) {
    ClearRow clearRow(row);
    std::vector<int32_t> changedColumns = row->getChangedColumnsAsVector();
    if (changedColumns.size() > 0) {
      m_service.executeCursorUpdate(m_cursorId,
          thrift::gfxdConstants::CURSOR_UPDATE, *row, changedColumns, rowIndex);
      return;
    }
  }
  throw GET_SQLEXCEPTION2(
      SQLStateMessage::INVALID_CURSOR_UPDATE_AT_CURRENT_POSITION_MSG);
}

void ResultSet::deleteRow(UpdatableRow* row, size_t rowIndex) {
  ClearRow clearRow(row);
  m_service.executeBatchCursorUpdate(m_cursorId,
      Utils::singleVector(thrift::gfxdConstants::CURSOR_DELETE),
      std::vector<thrift::Row>(), std::vector<std::vector<int32_t> >(),
      Utils::singleVector(static_cast<int32_t>(rowIndex)));
}

ResultSet::const_iterator ResultSet::cbegin(uint32_t pos) const {
  checkOpen("cbegin");
  if (pos != 0) {
    checkScrollable("cbegin");
  }

  return const_iterator(const_cast<ResultSet*>(this), pos, false);
}

ResultSet::iterator ResultSet::begin(uint32_t pos) {
  checkOpen("begin");
  if (pos != 0) {
    checkScrollable("begin");
  }

  return iterator(this, pos, false);
}

uint32_t ResultSet::getColumnPosition(const std::string& name) const {
  if (m_columnPositionMap == NULL) {
    // populate the map on first call
    const std::vector<thrift::ColumnDescriptor>* descriptors =
        (m_descriptors != NULL) ? &m_p->metadata : m_descriptors;
    m_columnPositionMap = new std::map<std::string, uint32_t>();
    uint32_t index = 1;
    for (std::vector<thrift::ColumnDescriptor>::const_iterator iter =
        descriptors->begin(); iter != descriptors->end(); ++iter) {
      const thrift::ColumnDescriptor& cd = *iter;
      if (cd.__isset.name) {
        m_columnPositionMap->operator [](cd.name) = index;
        // also push back the fully qualified name
        if (cd.__isset.fullTableName) {
          m_columnPositionMap->operator [](cd.fullTableName + "." + cd.name) =
              index;
        }
      }
      index++;
    }
  }
  std::map<std::string, uint32_t>::const_iterator findColumn =
      m_columnPositionMap->find(name);
  if (findColumn != m_columnPositionMap->end()) {
    return findColumn->second;
  }
  else {
    throw GET_SQLEXCEPTION2(SQLStateMessage::COLUMN_NOT_FOUND_MSG2,
        name.c_str());
  }
}

ColumnDescriptor ResultSet::getColumnDescriptor(
    std::vector<thrift::ColumnDescriptor>& descriptors,
    const uint32_t columnIndex) {
  // Check that columnIndex is in range.
  if (columnIndex < 1 || columnIndex > descriptors.size()) {
    throw GET_SQLEXCEPTION2(SQLStateMessage::COLUMN_NOT_FOUND_MSG1, columnIndex,
        descriptors.size());
  }

  // check if fullTableName, typeAndClassName are missing
  // which may be optimized out for consecutive same values
  thrift::ColumnDescriptor& cd = descriptors[columnIndex - 1];
  if (!cd.__isset.fullTableName) {
    // search for the table
    for (int i = columnIndex - 2; i >= 0; i--) {
      thrift::ColumnDescriptor& cd2 = descriptors[i];
      if (cd2.__isset.fullTableName) {
        cd.__set_fullTableName(cd2.fullTableName);
        break;
      }
    }
  }
  if (cd.type == thrift::GFXDType::JAVA_OBJECT) {
    if (!cd.__isset.udtTypeAndClassName) {
      // search for the UDT typeAndClassName
      for (int i = columnIndex - 2; i >= 0; i--) {
        thrift::ColumnDescriptor& cd2 = descriptors[i];
        if (cd2.__isset.udtTypeAndClassName) {
          cd.__set_udtTypeAndClassName(cd2.udtTypeAndClassName);
          break;
        }
      }
    }
  }
  return ColumnDescriptor(cd, columnIndex);
}

int32_t ResultSet::getRow() const {
  if (m_p != NULL) {
    return m_p->offset;
  }
  else {
    return 0;
  }
}

AutoPtr<SQLWarning> ResultSet::getWarnings() const {
  checkOpen("getWarnings");

  if (m_p->__isset.warnings) {
    return AutoPtr<SQLWarning>(new GET_SQLWARNING(m_p->warnings));
  }
  else {
    return AutoPtr<SQLWarning>(NULL);
  }
}

AutoPtr<ResultSet> ResultSet::clone() const {
  if (m_p != NULL) {
    /* clone the contained object */
    thrift::RowSet* rsp = new thrift::RowSet(*m_p);
    AutoPtr<thrift::RowSet> rs(rsp);

    AutoPtr<ResultSet> resultSet(
        new ResultSet(rsp, m_attrs.get(), true, m_service, m_serviceId,
            m_batchSize, m_updatable, m_scrollable, true /* isOwner */));
    rs.release();
    resultSet->m_batchOffset = m_batchOffset;
    if (m_descriptors != NULL) {
      resultSet->m_descriptors = new std::vector<thrift::ColumnDescriptor>(
          *m_descriptors);
    }
    return resultSet;
  }
  else {
    return AutoPtr<ResultSet>(NULL);
  }
}

void ResultSet::cleanupRS() {
  m_batchOffset = 0;
  if (m_descriptors != NULL) {
    delete m_descriptors;
    m_descriptors = NULL;
  }
  if (m_columnPositionMap != NULL) {
    delete m_columnPositionMap;
    m_columnPositionMap = NULL;
  }
  m_inUse = false;

  WrapperBase<thrift::RowSet>::cleanup();
}

void ResultSet::close() {
  if (m_p != NULL && m_cursorId != thrift::g_gfxd_constants.INVALID_ID) {
    const int32_t cursorId = m_cursorId;
    m_cursorId = thrift::g_gfxd_constants.INVALID_ID;
    // need to make the server call only if this is not the last batch
    // or a scrollable cursor with multiple batches, otherwise server
    // would have already closed the ResultSet
    bool last = (m_p->flags & thrift::g_gfxd_constants.ROWSET_LAST_BATCH) != 0;
    if (!last || (m_scrollable && !(m_batchOffset == 0 && last))) {
      m_service.closeResultSet(cursorId);
    }
  }
  cleanupRS();
}

ResultSet::~ResultSet() throw () {
  // decrement service reference in all cases
  impl::ClearService clr = { m_serviceId, NULL };
  // destructor should *never* throw an exception
  try {
    close();
  } catch (const SQLException& sqle) {
    Utils::handleExceptionInDestructor("result set", sqle);
  } catch (const std::exception& stde) {
    Utils::handleExceptionInDestructor("result set", stde);
  } catch (...) {
    Utils::handleExceptionInDestructor("result set");
  }
}
