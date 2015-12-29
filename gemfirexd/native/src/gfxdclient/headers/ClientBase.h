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
 *  Created on: 2 Jun 2014
 *      Author: swale
 */

#ifndef CLIENTBASE_H_
#define CLIENTBASE_H_

#include "common/Base.h"
#include "common/AutoPtr.h"
#include "common/SystemProperties.h"
#include "messages/SQLStateMessage.h"

#include "thrift/gfxd_constants.h"
#include "thrift/gfxd_types.h"
#include "thrift/gfxd_struct_FieldDescriptor.h"
#include "thrift/gfxd_struct_Decimal.h"
#include "thrift/gfxd_struct_Timestamp.h"
#include "thrift/gfxd_struct_FieldValue.h"
#include "thrift/gfxd_struct_PDXNode.h"
#include "thrift/gfxd_struct_PDXObject.h"
#include "thrift/gfxd_struct_PDXSchemaNode.h"
#include "thrift/gfxd_struct_PDXSchema.h"
#include "thrift/gfxd_struct_JSONField.h"
#include "thrift/gfxd_struct_JSONNode.h"
#include "thrift/gfxd_struct_JSONObject.h"
#include "thrift/gfxd_struct_BlobChunk.h"
#include "thrift/gfxd_struct_ClobChunk.h"
#include "thrift/gfxd_struct_ServiceMetaData.h"
#include "thrift/gfxd_struct_ServiceMetaDataArgs.h"
#include "thrift/gfxd_struct_OpenConnectionArgs.h"
#include "thrift/gfxd_struct_ConnectionProperties.h"
#include "thrift/gfxd_struct_HostAddress.h"
#include "thrift/gfxd_struct_GFXDExceptionData.h"
#include "thrift/gfxd_struct_StatementAttrs.h"
#include "thrift/gfxd_struct_DateTime.h"
#include "thrift/gfxd_struct_ColumnValue.h"
#include "thrift/gfxd_struct_ColumnDescriptor.h"
#include "thrift/gfxd_struct_Row.h"
#include "thrift/gfxd_struct_OutputParameter.h"
#include "thrift/gfxd_struct_RowSet.h"
#include "thrift/gfxd_struct_PrepareResult.h"
#include "thrift/gfxd_struct_UpdateResult.h"
#include "thrift/gfxd_struct_StatementResult.h"

#include <map>
#include <set>

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        static const uint32_t DEFAULT_REAL_PRECISION = 8;

        namespace impl
        {
          class ClientService;
        }

        class ColumnDescriptor;
        class Connection;
        class DatabaseMetaData;
        class DatabaseMetaDataArgs;
        class ParameterDescriptor;
        class Parameters;
        class ParametersBatch;
        class PreparedStatement;
        class Result;
        class ResultSet;
        class Row;
        class SQLException;
        class SQLWarning;
        class StatementAttributes;
        class UpdatableRow;

        struct SQLType : public thrift::GFXDType
        {
        };

        struct DatabaseMetaDataCall : public thrift::ServiceMetaDataCall
        {
        };

        struct DatabaseFeature : public thrift::ServiceFeature
        {
        };

        namespace DriverType
        {
          enum type
          {
            JDBC = 1,
            ODBC = 2
          };
        }

        namespace ResultSetType
        {
          enum type
          {
            TYPE_FORWARD_ONLY = 1,
            TYPE_INSENSITIVE = 2,
            TYPE_SENSITIVE = 3
          };
        }

        namespace ResultSetHoldability
        {
          enum type
          {
            NONE = 0,
            CLOSE_CURSORS_OVER_COMMIT = 1,
            HOLD_CURSORS_OVER_COMMIT = 2
          };
        }

        /**
         * Keeping the values below the same as the Thrift IDL. Unfortunately
         * the C++ genertor does not have these as constants rather as statics.
         */
        namespace IsolationLevel
        {
          enum type
          {
            NONE = 0,
            READ_UNCOMMITTED = 1,
            READ_COMMITTED = 2,
            REPEATABLE_READ = 4,
            SERIALIZABLE = 8,
            NO_CHANGE = 64
          };
        }

        struct TransactionAttribute : public thrift::TransactionAttribute
        {
        };

        class OutputParameter
        {
        private:
          thrift::OutputParameter m_outParam;

        public:
          OutputParameter() :
              m_outParam() {
          }

          OutputParameter(SQLType::type type) :
              m_outParam() {
            m_outParam.__set_type(type);
          }

          const thrift::OutputParameter& getThriftOutputParameter()
              const throw () {
            return m_outParam;
          }

          void setType(SQLType::type type) throw () {
            m_outParam.__set_type(type);
          }

          SQLType::type getType() const throw () {
            return m_outParam.type;
          }

          void setScale(const int32_t scale) throw () {
            m_outParam.__set_scale(scale);
          }

          int32_t getScale() const throw () {
            return m_outParam.scale;
          }

          bool isSetScale() const throw () {
            return m_outParam.__isset.scale;
          }

          void setTypeName(const std::string& typeName) throw () {
            m_outParam.__set_typeName(typeName);
          }

          const std::string& getTypeName() const throw () {
            return m_outParam.typeName;
          }

          bool isSetTypeName() const throw () {
            return m_outParam.__isset.typeName;
          }
        };

        // some statics used to avoid creating objects for empty parameters

        extern const std::string EMPTY_STRING;
        extern const std::set<thrift::TransactionAttribute::type> EMPTY_TX_FLAGS;
        extern const std::map<int32_t, OutputParameter> EMPTY_OUTPUT_PARAMS;
        extern const std::map<int32_t, thrift::OutputParameter> EMPTY_OUT_PARAMS;

        namespace types
        {
          class Blob;
          class Clob;
          class DateTime;
          class Decimal;
          class JSONObject;
          class PDXObject;
          class Timestamp;
        } /* namespace types */

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* CLIENTBASE_H_ */
