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
 * ColumnDescriptorBase.h
 *
 *      Author: swale
 */

#ifndef COLUMNDESCRIPTORBASE_H_
#define COLUMNDESCRIPTORBASE_H_

#include "Types.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        namespace ColumnNullability
        {
          enum type
          {
            NONULLS = 0,
            NULLABLE = 1,
            UNKNOWN = 2
          };
        }

        namespace ColumnUpdatable
        {
          enum type
          {
            READ_ONLY = 0,
            UPDATABLE = 1,
            DEFINITELY_UPDATABLE = 2
          };
        }

        class ColumnDescriptorBase
        {
        protected:
          thrift::ColumnDescriptor& m_descriptor;
          const uint32_t m_columnIndex;

          ColumnDescriptorBase(thrift::ColumnDescriptor& descriptor,
              const uint32_t columnIndex) :
              m_descriptor(descriptor), m_columnIndex(columnIndex)
          {
          }

        public:
          /**
           * Get the 1-based index of the column/parameter that this
           * descriptor represents.
           */
          uint32_t getIndex() const throw ()
          {
            return m_columnIndex;
          }

          SQLType::type getSQLType() const throw() {
            return m_descriptor.type;
          }

          std::string getName() const
          {
            return m_descriptor.__isset.name ? m_descriptor.name : "";
          }

          std::string getSchema() const
          {
            if (m_descriptor.__isset.fullTableName) {
              const std::string& tableName = m_descriptor.fullTableName;
              size_t dotPos;
              if ((dotPos = tableName.find('.')) != std::string::npos) {
                return tableName.substr(0, dotPos);
              }
            }
            return "";
          }

          std::string getTable() const
          {
            if (m_descriptor.__isset.fullTableName) {
              const std::string& tableName = m_descriptor.fullTableName;
              size_t dotPos;
              if ((dotPos = tableName.find('.')) != std::string::npos) {
                return tableName.substr(dotPos + 1);
              }
              else {
                return tableName;
              }
            }
            return "";
          }

          ColumnNullability::type getNullability() const throw ()
          {
            if ((m_descriptor.descFlags
                & thrift::g_gfxd_constants.COLUMN_NULLABLE)) {
              return ColumnNullability::NULLABLE;
            }
            else if ((m_descriptor.descFlags
                & thrift::g_gfxd_constants.COLUMN_NONULLS)) {
              return ColumnNullability::NONULLS;
            }
            else {
              return ColumnNullability::UNKNOWN;
            }
          }

          bool isSigned() const throw ()
          {
            switch (m_descriptor.type) {
              case SQLType::TINYINT:
              case SQLType::SMALLINT:
              case SQLType::INTEGER:
              case SQLType::BIGINT:
              case SQLType::FLOAT:
              case SQLType::REAL:
              case SQLType::DOUBLE:
              case SQLType::DECIMAL:
                return true;
              default:
                return false;
            }
          }

          int16_t getPrecision() const throw ()
          {
            return m_descriptor.precision;
          }

          int16_t getScale() const throw ()
          {
            if (m_descriptor.__isset.scale) {
              return m_descriptor.scale;
            }
            else {
              switch (m_descriptor.type) {
                case SQLType::BOOLEAN:
                case SQLType::TINYINT:
                case SQLType::SMALLINT:
                case SQLType::INTEGER:
                case SQLType::BIGINT:
                case SQLType::FLOAT:
                case SQLType::REAL:
                case SQLType::DOUBLE:
                case SQLType::DATE:
                case SQLType::TIME:
                  return 0;
                case SQLType::TIMESTAMP:
                  return 6;
                default:
                  return thrift::g_gfxd_constants.COLUMN_SCALE_UNKNOWN;
              }
            }
          }

          /**
           * Returns the database type name for this column, or "UNKNOWN"
           * if the type cannot be determined.
           */
          std::string getTypeName() const throw ()
          {
            if (m_descriptor.__isset.udtTypeAndClassName) {
              const std::string& typeAndClass =
                  m_descriptor.udtTypeAndClassName;
              size_t colonIndex;
              if ((colonIndex = typeAndClass.find(':')) != std::string::npos) {
                return typeAndClass.substr(0, colonIndex);
              }
              else {
                return typeAndClass;
              }
            }
            else {
              switch (m_descriptor.type) {
                case SQLType::TINYINT:
                  return "TINYINT";
                case SQLType::SMALLINT:
                  return "SMALLINT";
                case SQLType::INTEGER:
                  return "INTEGER";
                case SQLType::BIGINT:
                  return "BIGINT";
                case SQLType::FLOAT:
                  return "FLOAT";
                case SQLType::REAL:
                  return "REAL";
                case SQLType::DOUBLE:
                  return "DOUBLE";
                case SQLType::DECIMAL:
                  return "DECIMAL";
                case SQLType::CHAR:
                  return "CHAR";
                case SQLType::VARCHAR:
                  return "VARCHAR";
                case SQLType::LONGVARCHAR:
                  return "LONG VARCHAR";
                case SQLType::DATE:
                  return "DATE";
                case SQLType::TIME:
                  return "TIME";
                case SQLType::TIMESTAMP:
                  return "TIMESTAMP";
                case SQLType::BINARY:
                  return "CHAR FOR BIT DATA";
                case SQLType::VARBINARY:
                  return "VARCHAR FOR BIT DATA";
                case SQLType::LONGVARBINARY:
                  return "LONG VARCHAR FOR BIT DATA";
                case SQLType::JAVA_OBJECT:
                  return "JAVA";
                case SQLType::BLOB:
                  return "BLOB";
                case SQLType::CLOB:
                  return "CLOB";
                case SQLType::BOOLEAN:
                  return "BOOLEAN";
                case SQLType::SQLXML:
                  return "XML";
                case SQLType::PDX_OBJECT:
                  return "PDX";
                case SQLType::JSON_OBJECT:
                  return "JSON";
                default:
                  return "UNKNOWN";
              }
            }
          }

          /**
           * For a Java user-defined type, return the java class name
           * of the type, else returns empty string ("").
           */
          std::string getUDTClassName() const throw ()
          {
            if (m_descriptor.__isset.udtTypeAndClassName) {
              const std::string& typeAndClass =
                  m_descriptor.udtTypeAndClassName;
              size_t colonIndex;
              if ((colonIndex = typeAndClass.find(':')) != std::string::npos) {
                return typeAndClass.substr(colonIndex);
              }
            }
            return "";
          }

          ~ColumnDescriptorBase() throw ()
          {
          }
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* COLUMNDESCRIPTORBASE_H_ */
