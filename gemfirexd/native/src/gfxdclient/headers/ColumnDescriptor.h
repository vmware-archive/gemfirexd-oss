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
 * ColumnDescriptor.h
 *
 *      Author: swale
 */

#ifndef COLUMNDESCRIPTOR_H_
#define COLUMNDESCRIPTOR_H_

#include "ColumnDescriptorBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        class ColumnDescriptor: public ColumnDescriptorBase
        {
        private:
          ColumnDescriptor(thrift::ColumnDescriptor& descriptor,
              const uint32_t columnIndex) :
              ColumnDescriptorBase(descriptor, columnIndex)
          {
          }

          friend class ResultSet;

        public:
          ColumnUpdatable::type getUpdatable() const throw ()
          {
            int16_t flags = m_descriptor.descFlags;
            if ((flags & thrift::g_gfxd_constants.COLUMN_UPDATABLE)) {
              return ColumnUpdatable::UPDATABLE;
            }
            else if ((flags
                & thrift::g_gfxd_constants.COLUMN_DEFINITELY_UPDATABLE)) {
              return ColumnUpdatable::DEFINITELY_UPDATABLE;
            }
            else {
              return ColumnUpdatable::READ_ONLY;
            }
          }

          bool isAutoIncrement() const throw ()
          {
            return (m_descriptor.descFlags
                & thrift::g_gfxd_constants.COLUMN_AUTOINC) != 0;
          }

          bool isCaseSensitive() const throw ()
          {
            switch (m_descriptor.type) {
              case SQLType::CHAR:
              case SQLType::VARCHAR:
              case SQLType::CLOB:
              case SQLType::LONGVARCHAR:
              case SQLType::SQLXML:
                return true;
              default:
                return false;
            }
          }

          bool isSearchable() const throw ()
          {
            // we have no restrictions yet, so this is always true
            return true;
          }

          bool isCurrency() const throw ()
          {
            switch (m_descriptor.type) {
              case SQLType::DECIMAL:
                return true;
              default:
                return false;
            }
          }

          uint32_t getDisplaySize() const throw ()
          {
            uint32_t size;
            switch (m_descriptor.type) {
              case SQLType::TIMESTAMP:
                return 26;
              case SQLType::DATE:
                return 10;
              case SQLType::TIME:
                return 8;
              case SQLType::INTEGER:
                return 11;
              case SQLType::SMALLINT:
                return 6;
              case SQLType::REAL:
              case SQLType::FLOAT:
                return 13;
              case SQLType::DOUBLE:
                return 22;
              case SQLType::TINYINT:
                return 4;
              case SQLType::BIGINT:
                return 20;
              case SQLType::BOOLEAN:
                return 5; // for "false"
              case SQLType::BINARY:
              case SQLType::VARBINARY:
              case SQLType::LONGVARBINARY:
              case SQLType::BLOB:
                size = (2 * getPrecision());
                return (size > 0 ? size : 30);
              default:
                size = getPrecision();
                return (size > 0 ? size : 15);
            }
          }

          std::string getLabel() throw ()
          {
            if (m_descriptor.__isset.name) {
              return m_descriptor.name;
            }
            else {
              char buf[32];
              ::snprintf(buf, sizeof(buf), "Column%d", m_columnIndex);
              return buf;
            }
          }

          ~ColumnDescriptor() throw ()
          {
          }
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* COLUMNDESCRIPTOR_H_ */
