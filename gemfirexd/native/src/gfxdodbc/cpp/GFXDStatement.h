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
 * GFXDStatement.h
 *
 * Defines wrapper class for the underlying JDBC Statement classes.
 *
 *      Author: swale
 */

#ifndef GFXDSTATEMENT_H_
#define GFXDSTATEMENT_H_

#include <PreparedStatement.h>
#include <common/ArrayList.h>

#include "GFXDConnection.h"
#include "StringFunctions.h"
#include "SyncLock.h"
#include "GFXDDescriptor.h"

namespace com {
  namespace pivotal {
    namespace gemfirexd {

      namespace Cursor {
        enum type {
          FORWARD_ONLY, SCROLLABLE, UPDATABLE, UPDATABLE_SCROLLABLE
        };
      }

      /**
       * Encapsulates a native Statement, ResultSet, bound parameters,
       * bound output values etc.
       *
       * Convention for interval types not available in native API:
       *
       * SQL_INTERVAL_MONTH: store total number of months as INTEGER with sign
       *
       * SQL_INTERVAL_YEAR: store total number of years as INTEGER with sign
       *
       * SQL_INTERVAL_YEAR_TO_MONTH: store total number of months as INTEGER
       *                             with sign
       *
       * SQL_INTERVAL_DAY: store total number of days as INTEGER with sign
       *
       * SQL_INTERVAL_HOUR: store total number of hours as INTEGER with sign
       *
       * SQL_INTERVAL_MINUTE: store total number of minutes as INTEGER with sign
       *
       * SQL_INTERVAL_SECOND: store total number of secs as INTEGER with sign
       *
       * SQL_INTERVAL_DAY_TO_HOUR: store total number of hours as BIGINT
       *                           with sign
       *
       * SQL_INTERVAL_DAY_TO_MINUTE: store total number of minutes as BIGINT
       *                             with sign
       *
       * SQL_INTERVAL_DAY_TO_SECOND: store total number of secs as BIGINT
       *                             with sign
       *
       * SQL_INTERVAL_HOUR_TO_MINUTE: store total number of minutes as BIGINT
       *                              with sign
       *
       * SQL_INTERVAL_HOUR_TO_SECOND: store total number of secs as BIGINT
       *                              with sign
       *
       * SQL_INTERVAL_MINUTE_TO_SECOND: store total number of secs as BIGINT
       *                                with sign
       */
      class GFXDStatement : public GFXDHandleBase
      {
      private:
        //native::SyncLock m_SyncLock;

        /** the underlying connection */
        GFXDConnection& m_conn;

        /** the underlying native prepared statement */
        AutoPtr<PreparedStatement> m_pstmt;

        /** attributes for this statement */
        StatementAttributes m_stmtAttrs;

        struct Parameter
        {
          uint32_t m_paramNum;
          short m_inputOutputType;
          SQLType::type m_paramType;
          bool m_isDataAtExecParam;
          bool m_isAllocated;

          // original values
          SQLSMALLINT m_o_valueType;
          SQLSMALLINT m_o_paramType;
          SQLULEN m_o_precision;
          SQLSMALLINT m_o_scale;
          SQLPOINTER m_o_value;
          SQLLEN m_o_valueSize;
          SQLLEN* m_o_lenOrIndp;
          bool m_o_useWideStringDefault;

          // fields uninitialized by design with invalid m_inputOutputType
          Parameter() :
              m_inputOutputType(-1), m_isAllocated(false), m_o_value(NULL) {
          }

          inline void set(SQLUSMALLINT paramNum, SQLSMALLINT inputOutputType,
              SQLSMALLINT valueType, SQLSMALLINT paramType, SQLULEN precision,
              SQLSMALLINT scale, SQLPOINTER value, SQLLEN valueSize,
              SQLLEN* lenOrIndp, bool useWideStringDefault) {
            m_paramNum = paramNum;
            m_inputOutputType = inputOutputType;
            m_o_valueType = valueType;
            m_o_paramType = paramType;
            if (precision > 0) {
              m_o_precision = precision;
            } else {
              m_o_precision = DEFAULT_REAL_PRECISION;
            }
            m_o_scale = scale;
            m_o_useWideStringDefault = useWideStringDefault;
            if (IS_DATA_AT_EXEC(lenOrIndp)) {
              m_isDataAtExecParam = true;
              lenOrIndp = NULL;
            } else {
              m_isDataAtExecParam = false;
              if (lenOrIndp != NULL && valueSize < 0) {
                valueSize = *lenOrIndp;
              }
            }
            m_o_lenOrIndp = lenOrIndp;
            m_o_value = m_isDataAtExecParam ? NULL : value;
            m_o_valueSize = m_isDataAtExecParam ? SQL_NTS : valueSize;
            if (m_o_valueType == SQL_C_DEFAULT) {
              m_o_valueType = getCType(m_o_paramType, m_paramNum);
            }
          }
        };

        struct OutputField
        {
          SQLSMALLINT m_targetType;
          SQLPOINTER m_targetValue;
          SQLLEN m_valueSize;
          SQLLEN* m_lenOrIndPtr;

          // fields uninitialized by design with invalid m_targetType
          OutputField() :
              m_targetType(-1) {
          }

          inline void set(SQLSMALLINT targetType, SQLPOINTER targetValue,
              SQLLEN valueSize, SQLLEN *lenOrIndPtr) {
            m_targetType = targetType;
            m_targetValue = targetValue;
            m_valueSize = valueSize;
            m_lenOrIndPtr = lenOrIndPtr;
          }
        };

        /** the parameters bound to this statement */
        ArrayList<Parameter> m_params;

        /** the output fields bound to this statement */
        ArrayList<OutputField> m_outputFields;

        /**
         * The result of a statement execution. This is returned in case normal
         * execute has been invoked on underlying native API and not
         * executeQuery or executeUpdate. It encapsulates ResultSet, if any,
         * procedure output parameters, if any, update count, generated keys etc
         */
        AutoPtr<Result> m_result;

        /**
         * The ResultSet, if any, obtained from the last execution of this
         * statement. If this is NULL then it indicates that the statement
         * execution returned an update count.
         */
        AutoPtr<ResultSet> m_resultSet;

        /**
         * The updatable cursor from the ResultSet, if any, obtained from the
         * last execution of this statement.
         */
        ResultSet::iterator m_cursor;

        /** the type of cursor created for current ResultSet */
        Cursor::type m_cursorType;

        /** True if the cursor has been specified to be sensitive to changes. */
        bool m_resultSetCursorSensitive;

        /**
         * ODBC implicit descriptor handles required for ODBC driver manger
         * */
        GFXDDescriptor* m_apdDesc;
        GFXDDescriptor* m_ipdDesc;
        GFXDDescriptor* m_ardDesc;
        GFXDDescriptor* m_irdDesc;

        /**
         * If the statement has been prepared/executed using the unicode version
         * functions of the ODBC API, then assume wide-character strings in bind
         * and results by default.
         */
        bool m_useWideStringDefault;

        /**
         * if true then use case insensitive arguments to meta-data queries
         * else the values are case sensitive
         */
        bool m_argsAsIdentifiers;

        /** the current bookmark offset */
        SQLLEN m_bookmark;

        /** the maximum number of rows to return from the result set */
        SQLLEN m_maxRows;

        /** the maximum number of rows to return from the result set */
        SQLLEN m_KeySetSize;

        /** row array size for row status pointer)*/
        SQLLEN m_rowArraySize;

        /** value that sets the binding orientation to be used */
        SQLLEN m_bindingOrientation;

        /** value that sets the param binding orientation to be used */
        SQLLEN m_paramBindingOrientation;

        /** param set size for param array binding */
        SQLLEN m_paramSetSize;

        /** Array in which to return status information for each row of parameter values.*/
        SQLUSMALLINT* m_paramStatusArr;

        /** array of SQLUSMALLINT values used to ignore a parameter during execution of an SQL statement.*/
        SQLUSMALLINT* m_paramOperationPtr;

        /**  array of SQLUSMALLINT values used to ignore a row during a bulk operation using SQLSetPos*/
        SQLUSMALLINT* m_rowOperationPtr;

        /** number of sets of parameters processed */
        SQLLEN* m_paramsProcessedPtr;

        /** value that points to an offset added to pointers to change binding of dynamic parameters*/
        SQLLEN* m_paramBindOffsetPtr;

        /** address of the row status array filled by GFXDetch and GFXDetchScroll*/
        SQLUSMALLINT* m_rowStatusPtr;

        /** value that points to a buffer in which to return the number of rows fetched after a call to GFXDetch or GFXDetchScroll*/
        SQLLEN* m_fetchedRowsPtr;

        /** value that points to an offset added to pointers to change binding of column data*/
        SQLUSMALLINT* m_bindOffsetPtr;

        /** current executing parameter */
        int m_currentParameterIndex;

        /** C-style printf GUID format string */
        static const char* s_GUID_FORMAT;

        /** needs to access m_resultSet and some others for GFXDDiagRecField */
        friend class GFXDEnvironment;

        inline GFXDStatement(GFXDConnection* conn) :
            m_conn(*conn), m_params(), m_outputFields() {
          initWithDefaultValues();

          //allocate the implicit descriptor handlers to keep the driver manager happy
          GFXDDescriptor::newDescriptor(SQL_ATTR_APP_PARAM_DESC, m_apdDesc);
          GFXDDescriptor::newDescriptor(SQL_ATTR_IMP_PARAM_DESC, m_ipdDesc);
          GFXDDescriptor::newDescriptor(SQL_ATTR_APP_ROW_DESC, m_ardDesc);
          GFXDDescriptor::newDescriptor(SQL_ATTR_IMP_ROW_DESC, m_irdDesc);
        }

        inline ~GFXDStatement() {
          close();

          // delete the implicit descriptor handles
          GFXDDescriptor::freeDescriptor(m_apdDesc);
          GFXDDescriptor::freeDescriptor(m_ipdDesc);
          GFXDDescriptor::freeDescriptor(m_ardDesc);
          GFXDDescriptor::freeDescriptor(m_irdDesc);
        }

        void initWithDefaultValues() {
          m_cursorType = Cursor::FORWARD_ONLY;
          m_stmtAttrs.setResultSetHoldability(
              m_conn.m_conn.getResultSetHoldability());
          m_resultSetCursorSensitive = false;
          m_useWideStringDefault = false;
          m_bookmark = -1;
          m_bindingOrientation = SQL_BIND_BY_COLUMN;
          m_rowStatusPtr = NULL;
          m_paramBindingOrientation = SQL_PARAM_BIND_BY_COLUMN;
          m_paramSetSize = 1;
          m_currentParameterIndex = 0;
          m_argsAsIdentifiers = false;
          m_KeySetSize = 0;
          m_rowArraySize = 1;
          m_paramStatusArr = NULL;
          m_paramOperationPtr = NULL;
          m_rowOperationPtr = NULL;
          m_paramsProcessedPtr = NULL;
          m_paramBindOffsetPtr = NULL;
          m_fetchedRowsPtr = NULL;
          m_bindOffsetPtr = NULL;
        }

        inline void setResultSet(const AutoPtr<ResultSet>& rs) {
          m_resultSet = rs;
          m_cursor.initialize(*rs);
        }

        /**
         * Checks the unsupported conversion from ctype to sqltype
         * http://msdn.microsoft.com/en-us/library/windows/desktop/ms716298(v=vs.85).aspx
         */
        bool checkSupportedCtypeToSQLTypeConversion(SQLSMALLINT cType,
            SQLSMALLINT sqlType);

        /** bind all the parameters to the underlying prepared statement */
        SQLRETURN bindParameters(Parameters& paramValues,
            std::map<int32_t, OutputParameter>* outParams);

        /** bind a given parameter to the underlying prepared statement */
        SQLRETURN bindParameter(Parameters& paramValues, Parameter& param,
            std::map<int32_t, OutputParameter>* outParams);

        /** fill output values from given Row */
        SQLRETURN fillOutput(const Row& outputRow, const uint32_t outputColumn,
            SQLPOINTER value, const SQLLEN valueSize, SQLSMALLINT ctype,
            const SQLULEN precision, SQLLEN* lenOrIndp);

        /** fill output and input parameters in this prepared statement */
        SQLRETURN fillOutParameters(const Result& result);

        SQLRETURN fillOutputFields();

        SQLRETURN fillOutputFieldsWithArrays(const Row* currentRow);

        SQLRETURN setRowStatus();

        /** Prepare the statement with current parameters. */
        SQLRETURN prepare(const std::string& sqlText, const bool isWideString);

        /*
         * Execute given query string with any parameters already bound.
         */
        SQLRETURN execute(const std::string& sqlText, const bool isWideString);

        /*
         * Executes statement for array of parameters.
         */
        SQLRETURN executeWthArrayOfParams(const std::string& sqlText,
            const bool isWideString);

        template<typename HANDLE_TYPE>
        SQLRETURN handleWarnings(const AutoPtr<HANDLE_TYPE>& handle) {
          if (!handle->hasWarnings()) {
            return SQL_SUCCESS;
          } else {
            AutoPtr<SQLWarning> warnings = m_result->getWarnings();
            if (warnings.isNull()) {
              return SQL_SUCCESS;
            }
            setSQLWarning(*warnings);
            return SQL_SUCCESS_WITH_INFO;
          }
        }

        template<typename CHAR_TYPE>
        SQLRETURN getAttributeT(SQLINTEGER attribute, SQLPOINTER valueBuffer,
            SQLINTEGER bufferLen, SQLINTEGER* valueLen);

        template<typename CHAR_TYPE>
        SQLRETURN setAttributeT(SQLINTEGER attribute, SQLPOINTER valueBuffer,
            SQLINTEGER valueLen);

        template<typename CHAR_TYPE>
        SQLRETURN getResultColumnDescriptorT(SQLUSMALLINT columnNumber,
            CHAR_TYPE* columnName, SQLSMALLINT bufferLength,
            SQLSMALLINT* nameLength, SQLSMALLINT* dataType, SQLULEN* columnSize,
            SQLSMALLINT* decimalDigits, SQLSMALLINT* nullable);

        template<typename CHAR_TYPE>
        SQLRETURN getColumnAttributeT(SQLUSMALLINT columnNumber,
            SQLUSMALLINT fieldId, SQLPOINTER charAttribute,
            SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
            SQLLEN* numericAttribute);

        template<typename CHAR_TYPE>
        SQLRETURN getCursorNameT(CHAR_TYPE* cursorName,
            SQLSMALLINT bufferLength, SQLSMALLINT* nameLength);

        template<typename CHAR_TYPE>
        SQLRETURN setCursorNameT(CHAR_TYPE* cursorName, SQLSMALLINT nameLength);

        template<typename CHAR_TYPE>
        SQLRETURN getTablesT(CHAR_TYPE* catalogName, SQLSMALLINT nameLength1,
            CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
            CHAR_TYPE* tableName, SQLSMALLINT nameLength3,
            CHAR_TYPE* tableTypes, SQLSMALLINT nameLength4);

        template<typename CHAR_TYPE>
        SQLRETURN getTablePrivilegesT(CHAR_TYPE* catalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* schemaName,
            SQLSMALLINT nameLength2, CHAR_TYPE* tableName,
            SQLSMALLINT nameLength3);

        template<typename CHAR_TYPE>
        SQLRETURN getColumnsT(CHAR_TYPE* catalogName, SQLSMALLINT nameLength1,
            CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
            CHAR_TYPE* tableName, SQLSMALLINT nameLength3,
            CHAR_TYPE* columnName, SQLSMALLINT nameLength4);

        template<typename CHAR_TYPE>
        SQLRETURN getSpecialColumnsT(SQLUSMALLINT idType, CHAR_TYPE *catlogName,
            SQLSMALLINT nameLength1, CHAR_TYPE *schemaName,
            SQLSMALLINT nameLength2, CHAR_TYPE *tableName,
            SQLSMALLINT nameLength3, SQLUSMALLINT scope, SQLUSMALLINT nullable);

        template<typename CHAR_TYPE>
        SQLRETURN getColumnPrivilegesT(CHAR_TYPE* catalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* schemaName,
            SQLSMALLINT nameLength2, CHAR_TYPE* tableName,
            SQLSMALLINT nameLength3, CHAR_TYPE* columnName,
            SQLSMALLINT nameLength4);

        template<typename CHAR_TYPE>
        SQLRETURN getIndexInfoT(CHAR_TYPE* catalogName, SQLSMALLINT nameLength1,
            CHAR_TYPE* schemaName, SQLSMALLINT nameLength2,
            CHAR_TYPE* tableName, SQLSMALLINT nameLength3, bool unique,
            bool approximate);

        template<typename CHAR_TYPE>
        SQLRETURN getPrimaryKeysT(CHAR_TYPE* catalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* schemaName,
            SQLSMALLINT nameLength2, CHAR_TYPE* tableName,
            SQLSMALLINT nameLength3);

        template<typename CHAR_TYPE>
        SQLRETURN getImportedKeysT(CHAR_TYPE* catalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* schemaName,
            SQLSMALLINT nameLength2, CHAR_TYPE* tableName,
            SQLSMALLINT nameLength3);

        template<typename CHAR_TYPE>
        SQLRETURN getExportedKeysT(CHAR_TYPE* catalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* schemaName,
            SQLSMALLINT nameLength2, CHAR_TYPE* tableName,
            SQLSMALLINT nameLength3);

        template<typename CHAR_TYPE>
        SQLRETURN getCrossReferenceT(CHAR_TYPE* parentCatalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* parentSchemaName,
            SQLSMALLINT nameLength2, CHAR_TYPE* parentTableName,
            SQLSMALLINT nameLength3, CHAR_TYPE* foreignCatalogName,
            SQLSMALLINT nameLength4, CHAR_TYPE* foreignSchemaName,
            SQLSMALLINT nameLength5, CHAR_TYPE* foreignTableName,
            SQLSMALLINT nameLength6);

        template<typename CHAR_TYPE>
        SQLRETURN getProceduresT(CHAR_TYPE* catalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* schemaPattern,
            SQLSMALLINT nameLength2, CHAR_TYPE* procedureNamePattern,
            SQLSMALLINT nameLength3);

        template<typename CHAR_TYPE>
        SQLRETURN getProcedureColumnsT(CHAR_TYPE* catalogName,
            SQLSMALLINT nameLength1, CHAR_TYPE* schemaPattern,
            SQLSMALLINT nameLength2, CHAR_TYPE* procedureNamePattern,
            SQLSMALLINT nameLength3, CHAR_TYPE* columnNamePattern,
            SQLSMALLINT nameLength4);

        template<typename CHAR_TYPE>
        SQLRETURN getTypeInfoT(SQLSMALLINT dataType);

      public:
        static SQLRETURN newStatement(GFXDConnection* conn,
            GFXDStatement*& stmtRef);

        /**
         * Free the cursor or parameters of the statement, or this statement
         * itself depending on the given option.
         */
        static SQLRETURN freeStatement(GFXDStatement* stmt, SQLUSMALLINT opt);

        static SQLType::type getSQLType(const SQLSMALLINT odbcType,
            const uint32_t paramNum);

        static SQLSMALLINT getCType(const SQLSMALLINT odbcType,
            const uint32_t paramNum);

        static SQLSMALLINT getCTypeFromSQLType(const SQLType::type sqlType);

        static SQLLEN getTypeFromSQLType(const SQLType::type sqlType);

        /**
         * Add a new parameter with given attributes to the parameter list
         * to be bound before execution time.
         */
        SQLRETURN addParameter(SQLUSMALLINT paramNum,
            SQLSMALLINT inputOutputType, SQLSMALLINT valueType,
            SQLSMALLINT paramType, SQLULEN precision, SQLSMALLINT scale,
            SQLPOINTER paramValue, SQLLEN valueSize, SQLLEN* lenOrIndPtr);

        /** Prepare the statement with current parameters. */
        inline SQLRETURN prepare(SQLCHAR *stmtText, SQLINTEGER textLength) {
          std::string stmt;
          StringFunctions::getString(stmtText, textLength, stmt);
          return prepare(stmt, false);
        }

        /** Prepare the statement with current parameters. */
        inline SQLRETURN prepare(SQLWCHAR *stmtText, SQLINTEGER textLength) {
          std::string stmt;
          StringFunctions::getString(stmtText, textLength, stmt);
          return prepare(stmt, true);
        }

        /**
         * Return false if {@link #prepare} has been invoked for this statement
         * else true.
         */
        inline bool isUnprepared() {
          return m_pstmt.isNull();
        }

        /**
         * Return true if {@link #prepare} has been invoked for this statement
         * else false.
         */
        inline bool isPrepared() {
          return !m_pstmt.isNull();
        }

        /*
         * Execute given query string with any parameters already bound.
         */
        inline SQLRETURN execute(SQLCHAR* stmtText, SQLINTEGER textLength) {
          std::string stmt;
          StringFunctions::getString(stmtText, textLength, stmt);
          return execute(stmt, false);
        }

        /*
         * Execute given query string with any parameters already bound.
         */
        inline SQLRETURN execute(SQLWCHAR* stmtText, SQLINTEGER textLength) {
          std::string stmt;
          StringFunctions::getString(stmtText, textLength, stmt);
          return execute(stmt, true);
        }

        /**
         * Execute already prepared query with any parameters already bound.
         */
        SQLRETURN execute();

        /**Execute batched inserts.*/
        SQLRETURN bulkOperations(SQLUSMALLINT operation);

        /**
         * Bind an output column to be fetched by next() calls.
         */
        SQLRETURN bindOutputField(SQLUSMALLINT columnNum,
            SQLSMALLINT targetType, SQLPOINTER targetValue, SQLLEN valueSize,
            SQLLEN *lenOrIndPtr);

        /**
         * Move to the next row of the ResultSet and fetch the values in the
         * bound columns.
         */
        SQLRETURN next();

        /**
         * Fetches the row in the ResultSet with fetchOrientation and fetchOffset.
         */
        SQLRETURN fetchScroll(SQLSMALLINT fetchOrientation,
            SQLINTEGER fetchOffset);

        /**sets the cursor position in a rowset and allows an application
         * to refresh data in the rowset or to update or delete data in the result set.*/
        SQLRETURN setPos(SQLUSMALLINT rowNumber, SQLUSMALLINT operation,
            SQLUSMALLINT lockType);

        /**
         * Retrieve result set data without binding column values.
         */
        SQLRETURN getData(SQLUSMALLINT columnNum, SQLSMALLINT targetType,
            SQLPOINTER targetValue, SQLLEN valueSize, SQLLEN *lenOrIndPtr);

        /**
         * If the previously executed statement was a DML statement, then
         * return the number of rows affected, else return -1.
         * The count pointer is assumed to be non-null.
         */
        SQLRETURN getUpdateCount(SQLLEN* count);

        /**
         * Get a given statement attribute (ODBC SQLGetStmtAttr).
         */
        SQLRETURN getAttribute(SQLINTEGER attribute, SQLPOINTER valueBuffer,
            SQLINTEGER bufferLen, SQLINTEGER* valueLen);

        /**
         * Get a given statement attribute (ODBC SQLGetStmtAttrW).
         */
        SQLRETURN getAttributeW(SQLINTEGER attribute, SQLPOINTER valueBuffer,
            SQLINTEGER bufferLen, SQLINTEGER* valueLen);

        /**
         * Set a given statement attribute (ODBC SQLSetStmtAttr).
         */
        SQLRETURN setAttribute(SQLINTEGER attribute, SQLPOINTER valueBuffer,
            SQLINTEGER valueLen);

        /**
         * Set a given statement attribute (ODBC SQLSetStmtAttrW).
         */
        SQLRETURN setAttributeW(SQLINTEGER attribute, SQLPOINTER valueBuffer,
            SQLINTEGER valueLen);

        /**
         * Return the column description from current result set.
         */
        SQLRETURN getResultColumnDescriptor(SQLUSMALLINT columnNumber,
            SQLCHAR* columnName, SQLSMALLINT bufferLength,
            SQLSMALLINT* nameLength, SQLSMALLINT* dataType, SQLULEN* columnSize,
            SQLSMALLINT* decimalDigits, SQLSMALLINT* nullable);

        /**
         * Return the column description from current result set.
         */
        SQLRETURN getResultColumnDescriptor(SQLUSMALLINT columnNumber,
            SQLWCHAR* columnName, SQLSMALLINT bufferLength,
            SQLSMALLINT* nameLength, SQLSMALLINT* dataType, SQLULEN* columnSize,
            SQLSMALLINT* decimalDigits, SQLSMALLINT* nullable);

        /**
         * Return descriptor information about a column in current result set.
         */
        SQLRETURN getColumnAttribute(SQLUSMALLINT columnNumber,
            SQLUSMALLINT fieldId, SQLPOINTER charAttribute,
            SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
            SQLLEN* numericAttribute);

        /**
         * Return descriptor information about a column in current result set.
         */
        SQLRETURN getColumnAttributeW(SQLUSMALLINT columnNumber,
            SQLUSMALLINT fieldId, SQLPOINTER charAttribute,
            SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
            SQLLEN* numericAttribute);

        /**
         * Returns the description about parameters associated with
         * a prepared SQL statement
         */
        SQLRETURN getParamMetadata(SQLUSMALLINT paramNumber,
            SQLSMALLINT * patamDataTypePtr, SQLULEN * paramSizePtr,
            SQLSMALLINT * decimalDigitsPtr, SQLSMALLINT * nullablePtr);

        /**
         * Returns the number of columns in the current result set.
         */
        SQLRETURN getNumResultColumns(SQLSMALLINT* columnCount);

        /**
         * Returns the number of parameters in the current statement.
         */
        SQLRETURN getNumParameters(SQLSMALLINT* parameterCount);

        /**
         * Return the current cursor name for the open result set.
         */
        SQLRETURN getCursorName(SQLCHAR* cursorName, SQLSMALLINT bufferLength,
            SQLSMALLINT* nameLength);

        /**
         * Return the current cursor name for the open result set.
         */
        SQLRETURN getCursorName(SQLWCHAR* cursorName, SQLSMALLINT bufferLength,
            SQLSMALLINT* nameLength);

        /**
         * Set the current cursor name for the open result set.
         */
        SQLRETURN setCursorName(SQLCHAR* cursorName, SQLSMALLINT nameLength);

        /**
         * Set the current cursor name for the open result set.
         */
        SQLRETURN setCursorName(SQLWCHAR* cursorName, SQLSMALLINT nameLength);

        /**
         * Returns the list of table, catalog, or schema names, and table types
         * as a result set in this statement.
         */
        SQLRETURN getTables(SQLCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLCHAR* schemaName, SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3, SQLCHAR* tableTypes,
            SQLSMALLINT nameLength4);

        /**
         * Returns the list of table, catalog, or schema names, and table types
         * as a result set in this statement.
         */
        SQLRETURN getTables(SQLWCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLWCHAR* schemaName, SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3, SQLWCHAR* tableTypes,
            SQLSMALLINT nameLength4);

        /**
         * Returns a description of the access rights for each table available
         * in a catalog as a result set in this statement.
         */
        SQLRETURN getTablePrivileges(SQLCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLCHAR* schemaName,
            SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Returns a description of the access rights for each table available
         * in a catalog as a result set in this statement.
         */
        SQLRETURN getTablePrivileges(SQLWCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
            SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Get the columns in given tables as a result set in this statement.
         */
        SQLRETURN getColumns(SQLCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLCHAR* schemaName, SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3, SQLCHAR* columnName,
            SQLSMALLINT nameLength4);

        /**
         * Get the columns in given tables as a result set in this statement.
         */
        SQLRETURN getColumns(SQLWCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLWCHAR* schemaName, SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3, SQLWCHAR* columnName,
            SQLSMALLINT nameLength4);

        /**
         * Get the following information for columns in given tables as a result set
         * in this statement.
         * 1. The optimal set of columns that uniquely identifies a row in the table.
         * 2. Columns that are automatically updated when any value in the row is
         *    updated by a transaction.
         */
        SQLRETURN getSpecialColumns(SQLUSMALLINT idType, SQLCHAR *catlogName,
            SQLSMALLINT nameLength1, SQLCHAR *schemaName,
            SQLSMALLINT nameLength2, SQLCHAR *tableName,
            SQLSMALLINT nameLength3, SQLUSMALLINT scope, SQLUSMALLINT nullable);

        /**
         * Get the following information for columns in given tables as a result set
         * in this statement.
         * 1. The optimal set of columns that uniquely identifies a row in the table.
         * 2. Columns that are automatically updated when any value in the row is
         *    updated by a transaction.
         */
        SQLRETURN getSpecialColumns(SQLUSMALLINT identifierType,
            SQLWCHAR* CatalogName, SQLSMALLINT nameLength1,
            SQLWCHAR* schemaName, SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3, SQLUSMALLINT scope, SQLUSMALLINT nullable);

        /**
         * Retrieves a description of the access rights for a table's columns
         * as a result set in this statement.
         */
        SQLRETURN getColumnPrivileges(SQLCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLCHAR* schemaName,
            SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3, SQLCHAR* columnName,
            SQLSMALLINT nameLength4);

        /**
         * Retrieves a description of the access rights for a table's columns
         * as a result set in this statement.
         */
        SQLRETURN getColumnPrivileges(SQLWCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
            SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3, SQLWCHAR* columnName,
            SQLSMALLINT nameLength4);

        /**
         * Get information about the indexes in given tables as a result set
         * in this statement.
         */
        SQLRETURN getIndexInfo(SQLCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLCHAR* schemaName, SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3, bool unique, bool approximate);

        /**
         * Get information about the indexes in given tables as a result set
         * in this statement.
         */
        SQLRETURN getIndexInfo(SQLWCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLWCHAR* schemaName, SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3, bool unique, bool approximate);

        /**
         * Get information about the the given table's primary key columns
         * as a result set in this statement.
         */
        SQLRETURN getPrimaryKeys(SQLCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLCHAR* schemaName, SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Get information about the the given table's primary key columns
         * as a result set in this statement.
         */
        SQLRETURN getPrimaryKeys(SQLWCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLWCHAR* schemaName, SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Get information about the primary/unique key columns that are
         * referenced by the given table's foreign key columns (the
         * primary/unique keys imported by a table) as a result set in
         * this statement.
         */
        SQLRETURN getImportedKeys(SQLCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLCHAR* schemaName, SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Get information about the primary/unique key columns that are
         * referenced by the given table's foreign key columns (the
         * primary/unique keys imported by a table) as a result set in
         * this statement.
         */
        SQLRETURN getImportedKeys(SQLWCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
            SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Get information about the foreign key columns that reference the
         * given table's primary key columns (the foreign keys exported by a
         * table) as a result set in this statement.
         */
        SQLRETURN getExportedKeys(SQLCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLCHAR* schemaName, SQLSMALLINT nameLength2, SQLCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Get information about the foreign key columns that reference the
         * given table's primary key columns (the foreign keys exported by a
         * table) as a result set in this statement.
         */
        SQLRETURN getExportedKeys(SQLWCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLWCHAR* schemaName,
            SQLSMALLINT nameLength2, SQLWCHAR* tableName,
            SQLSMALLINT nameLength3);

        /**
         * Get information about the foreign key columns in the given foreign
         * key table that reference the primary key or the columns representing
         * a unique constraint of the parent table (could be the same or a
         * different table) as a result set in this statement.
         */
        SQLRETURN getCrossReference(SQLCHAR* parentCatalogName,
            SQLSMALLINT nameLength1, SQLCHAR* parentSchemaName,
            SQLSMALLINT nameLength2, SQLCHAR* parentTableName,
            SQLSMALLINT nameLength3, SQLCHAR* foreignCatalogName,
            SQLSMALLINT nameLength4, SQLCHAR* foreignSchemaName,
            SQLSMALLINT nameLength5, SQLCHAR* foreignTableName,
            SQLSMALLINT nameLength6);

        /**
         * Get information about the foreign key columns in the given foreign
         * key table that reference the primary key or the columns representing
         * a unique constraint of the parent table (could be the same or a
         * different table) as a result set in this statement.
         */
        SQLRETURN getCrossReference(SQLWCHAR* parentCatalogName,
            SQLSMALLINT nameLength1, SQLWCHAR* parentSchemaName,
            SQLSMALLINT nameLength2, SQLWCHAR* parentTableName,
            SQLSMALLINT nameLength3, SQLWCHAR* foreignCatalogName,
            SQLSMALLINT nameLength4, SQLWCHAR* foreignSchemaName,
            SQLSMALLINT nameLength5, SQLWCHAR* foreignTableName,
            SQLSMALLINT nameLength6);

        /**
         * Get information about the stored procedures available in the given
         * catalog/schemas as a result set in this statement.
         */
        SQLRETURN getProcedures(SQLCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLCHAR* schemaPattern, SQLSMALLINT nameLength2,
            SQLCHAR* procedureNamePattern, SQLSMALLINT nameLength3);

        /**
         * Get information about the stored procedures available in the given
         * catalog/schemas as a result set in this statement.
         */
        SQLRETURN getProcedures(SQLWCHAR* catalogName, SQLSMALLINT nameLength1,
            SQLWCHAR* schemaPattern, SQLSMALLINT nameLength2,
            SQLWCHAR* procedureNamePattern, SQLSMALLINT nameLength3);

        /**
         * Get information about the given catalog's/schemas' stored procedure
         * parameter and result columns as a result set in this statement.
         */
        SQLRETURN getProcedureColumns(SQLCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLCHAR* schemaPattern,
            SQLSMALLINT nameLength2, SQLCHAR* procedureNamePattern,
            SQLSMALLINT nameLength3, SQLCHAR* columnNamePattern,
            SQLSMALLINT nameLength4);

        /**
         * Get information about the given catalog's/schemas' stored procedure
         * parameter and result columns as a result set in this statement.
         */
        SQLRETURN getProcedureColumns(SQLWCHAR* catalogName,
            SQLSMALLINT nameLength1, SQLWCHAR* schemaPattern,
            SQLSMALLINT nameLength2, SQLWCHAR* procedureNamePattern,
            SQLSMALLINT nameLength3, SQLWCHAR* columnNamePattern,
            SQLSMALLINT nameLength4);

        /**
         * Determines whether more results are available on a statement containing SELECT, UPDATE,
         * INSERT, or DELETE statements and, if so, initializes processing for those results.
         */
        SQLRETURN getMoreResults();

        SQLRETURN getParamData(SQLPOINTER* valuePtr);

        SQLRETURN putData(SQLPOINTER dataPtr, SQLINTEGER strLen);

        /**
         * Get information about the data types supported by the system.
         */
        SQLRETURN getTypeInfo(SQLSMALLINT dataType);

        /**
         * Get information about the data types supported by the system.
         */
        SQLRETURN getTypeInfoW(SQLSMALLINT dataType);

        /**
         * Close any current open ResultSet clearing any remaining results.
         */
        SQLRETURN closeResultSet();

        /**
         * Reset all the parameters to remove all parameters added so far.
         */
        SQLRETURN resetParameters();

        /**
         * Cancel the currently executing statement.
         */
        SQLRETURN cancel();

        /**
         * Close this statement.
         */
        SQLRETURN close();
      };
    }
  }
}

#endif /* GFXDSTATEMENT_H_ */
