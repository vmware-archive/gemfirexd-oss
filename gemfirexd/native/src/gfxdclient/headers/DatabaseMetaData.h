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
 * DatabaseMetaData.h
 *
 *      Author: swale
 */

#ifndef DATABASEMETADATA_H_
#define DATABASEMETADATA_H_

#include "Types.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        struct RowIdLifetime: thrift::RowIdLifetime
        {
        };

        class DatabaseMetaDataArgs
        {
        private:
          thrift::ServiceMetaDataArgs m_args;

          friend class Connection;

        public:
          /** default constructor */
          DatabaseMetaDataArgs();

          /** copy constructor */
          DatabaseMetaDataArgs(const DatabaseMetaDataArgs& other);

          /** assignment operator */
          DatabaseMetaDataArgs& operator=(
              const DatabaseMetaDataArgs& other) throw ();

          const DatabaseMetaDataArgs& setSchema(
              const std::string& schema) throw ()
          {
            m_args.__set_schema(schema);
            return *this;
          }

          const DatabaseMetaDataArgs& setTable(
              const std::string& table) throw ()
          {
            m_args.__set_table(table);
            return *this;
          }

          const DatabaseMetaDataArgs& setTableTypes(
              const std::vector<std::string>& tableTypes) throw ()
          {
            m_args.__set_tableTypes(tableTypes);
            return *this;
          }

          const DatabaseMetaDataArgs& setColumnName(
              const std::string& columnName) throw ()
          {
            m_args.__set_columnName(columnName);
            return *this;
          }

          const DatabaseMetaDataArgs& setForeignSchema(
              const std::string& foreignSchema) throw ()
          {
            m_args.__set_foreignSchema(foreignSchema);
            return *this;
          }

          const DatabaseMetaDataArgs& setForeignTable(
              const std::string& foreignTable) throw ()
          {
            m_args.__set_foreignTable(foreignTable);
            return *this;
          }

          const DatabaseMetaDataArgs& setProcedureName(
              const std::string& procedureName) throw ()
          {
            m_args.__set_procedureName(procedureName);
            return *this;
          }

          const DatabaseMetaDataArgs& setFunctionName(
              const std::string& functionName) throw ()
          {
            m_args.__set_functionName(functionName);
            return *this;
          }

          const DatabaseMetaDataArgs& setAttributeName(
              const std::string& attributeName) throw ()
          {
            m_args.__set_attributeName(attributeName);
            return *this;
          }

          const DatabaseMetaDataArgs& setTypeName(
              const std::string& typeName) throw ()
          {
            m_args.__set_typeName(typeName);
            return *this;
          }

          const DatabaseMetaDataArgs& setType(
              const SQLType::type& type) throw ()
          {
            m_args.__set_typeId(type);
            return *this;
          }

          ~DatabaseMetaDataArgs() throw ();
        };

        class DatabaseMetaData
        {
        private:
          thrift::ServiceMetaData m_metadata;

          DatabaseMetaData();

          friend class Connection;

          bool searchFeature(
              thrift::ServiceFeatureParameterized::type featureName,
              int32_t searchFor) const throw ();

        public:
          /** copy constructor */
          DatabaseMetaData(const DatabaseMetaData& other);

          /** assignment operator */
          DatabaseMetaData& operator=(const DatabaseMetaData& other) throw ();

          bool isFeatureSupported(DatabaseFeature::type feature) const throw ();

          std::string getProductName() const throw ()
          {
            return m_metadata.productName;
          }

          std::string getProductVersion() const throw ()
          {
            return m_metadata.productVersion;
          }

          int32_t getProductMajorVersion() const throw ()
          {
            return m_metadata.productMajorVersion;
          }

          int32_t getProductMinorVersion() const throw ()
          {
            return m_metadata.productMinorVersion;
          }

          int32_t getJdbcMajorVersion() const throw ()
          {
            return m_metadata.jdbcMajorVersion;
          }

          int32_t getJdbcMinorVersion() const throw ()
          {
            return m_metadata.jdbcMinorVersion;
          }

          std::string getIdentifierQuote() const throw ()
          {
            return m_metadata.identifierQuote;
          }

          AutoPtr<const std::vector<std::string> > getSQLKeyWords()
              const throw ()
          {
            return AutoPtr<const std::vector<std::string> >(
                &m_metadata.sqlKeywords, false);
          }

          AutoPtr<const std::vector<std::string> > getNumericFunctions()
              const throw ()
          {
            return AutoPtr<const std::vector<std::string> >(
                &m_metadata.numericFunctions, false);
          }

          AutoPtr<const std::vector<std::string> > getStringFunctions()
              const throw ()
          {
            return AutoPtr<const std::vector<std::string> >(
                &m_metadata.stringFunctions, false);
          }

          AutoPtr<const std::vector<std::string> > getSystemFunctions()
              const throw ()
          {
            return AutoPtr<const std::vector<std::string> >(
                &m_metadata.systemFunctions, false);
          }

          AutoPtr<const std::vector<std::string> > getDateTimeFunctions()
              const throw ()
          {
            return AutoPtr<const std::vector<std::string> >(
                &m_metadata.dateTimeFunctions, false);
          }

          std::string getSearchStringEscape() const throw ()
          {
            return m_metadata.searchStringEscape;
          }

          std::string getExtraNameCharacters() const throw ()
          {
            return m_metadata.__isset.extraNameCharacters
                ? m_metadata.extraNameCharacters : "";
          }

          bool supportsConvert(SQLType::type fromType,
              SQLType::type toType) const throw ();

          std::string getSchemaTerm() const throw ()
          {
            return m_metadata.schemaTerm;
          }

          std::string getProcedureTerm() const throw ()
          {
            return m_metadata.procedureTerm;
          }

          std::string getCatalogTerm() const throw ()
          {
            return m_metadata.catalogTerm;
          }

          std::string getCatalogSeparator() const throw ()
          {
            return m_metadata.catalogSeparator;
          }

          int32_t getMaxBinaryLiteralLength() const throw ()
          {
            return m_metadata.maxBinaryLiteralLength;
          }

          int32_t getMaxCharLiteralLength() const throw ()
          {
            return m_metadata.maxCharLiteralLength;
          }

          int32_t getMaxColumnsInGroupBy() const throw ()
          {
            return m_metadata.maxColumnsInGroupBy;
          }

          int32_t getMaxColumnsInIndex() const throw ()
          {
            return m_metadata.maxColumnsInIndex;
          }

          int32_t getMaxColumnsInOrderBy() const throw ()
          {
            return m_metadata.maxColumnsInOrderBy;
          }

          int32_t getMaxColumnsInSelect() const throw ()
          {
            return m_metadata.maxColumnsInSelect;
          }

          int32_t getMaxColumnsInTable() const throw ()
          {
            return m_metadata.maxColumnsInTable;
          }

          int32_t getMaxConnections() const throw ()
          {
            return m_metadata.maxConnections;
          }

          int32_t getMaxIndexLength() const throw ()
          {
            return m_metadata.maxIndexLength;
          }

          int32_t getMaxRowSize() const throw ()
          {
            return m_metadata.maxRowSize;
          }

          int32_t getMaxOpenStatements() const throw ()
          {
            return m_metadata.maxOpenStatements;
          }

          int32_t getMaxTableNamesInSelect() const throw ()
          {
            return m_metadata.maxTableNamesInSelect;
          }

          int32_t getMaxColumnNameLength() const throw ()
          {
            return m_metadata.maxColumnNameLength;
          }

          int32_t getMaxCursorNameLength() const throw ()
          {
            return m_metadata.maxCursorNameLength;
          }

          int32_t getMaxSchemaNameLength() const throw ()
          {
            return m_metadata.maxSchemaNameLength;
          }

          int32_t getMaxProcedureNameLength() const throw ()
          {
            return m_metadata.maxProcedureNameLength;
          }

          int32_t getMaxCatalogNameLength() const throw ()
          {
            return m_metadata.maxCatalogNameLength;
          }

          int32_t maxTableNameLength() const throw ()
          {
            return m_metadata.maxTableNameLength;
          }

          int32_t maxUserNameLength() const throw ()
          {
            return m_metadata.maxUserNameLength;
          }

          int32_t defaultTransactionIsolation() const throw ()
          {
            return m_metadata.defaultTransactionIsolation;
          }

          int32_t getDefaultResultSetType() const throw ()
          {
            return m_metadata.defaultResultSetType;
          }

          ResultSetHoldability::type getDefaultHoldability() const throw ()
          {
            return m_metadata.defaultResultSetHoldabilityHoldCursorsOverCommit
                ? ResultSetHoldability::HOLD_CURSORS_OVER_COMMIT
                : ResultSetHoldability::CLOSE_CURSORS_OVER_COMMIT;
          }

          bool isSQLStateXOpen() const throw ()
          {
            return m_metadata.sqlStateIsXOpen;
          }

          RowIdLifetime::type getDefaultRowIdLifeTime() const throw ()
          {
            return m_metadata.rowIdLifeTime;
          }

          bool supportsTransactionIsolationLevel(
              IsolationLevel::type isolation) const throw ();

          bool supportsResultSetReadOnly(
              ResultSetType::type rsType) const throw ();

          bool supportsResultSetUpdatable(
              ResultSetType::type rsType) const throw ();

          bool othersUpdatesVisible(ResultSetType::type rsType) const throw ();

          bool othersDeletesVisible(ResultSetType::type rsType) const throw ();

          bool othersInsertsVisible(ResultSetType::type rsType) const throw ();

          bool ownUpdatesVisible(ResultSetType::type rsType) const throw ();

          bool ownDeletesVisible(ResultSetType::type rsType) const throw ();

          bool ownInsertsVisible(ResultSetType::type rsType) const throw ();

          bool updatesDetected(ResultSetType::type rsType) const throw ();

          bool deletesDetected(ResultSetType::type rsType) const throw ();

          bool insertsDetected(ResultSetType::type rsType) const throw ();

          ~DatabaseMetaData() throw ();
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* DATABASEMETADATA_H_ */
