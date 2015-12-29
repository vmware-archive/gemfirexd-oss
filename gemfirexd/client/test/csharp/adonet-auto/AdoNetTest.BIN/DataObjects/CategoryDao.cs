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
 
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using AdoNetTest.BIN.BusinessObjects;
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN.DataObjects
{
    /// <summary>
    /// Specific method implementation for data access operation on Category table
    /// </summary>
    class CategoryDao : DataObject
    {
        public CategoryDao()
            : base()
        {
        }

        public CategoryDao(GFXDClientConnection connection)
            : base(connection)
        {
        }

        public CategoryDao(String schemaName)
            : base(schemaName)
        {
        }

        public CategoryDao(GFXDClientConnection connection, String schemaName)
            : base(connection, schemaName)
        {
        }
        
        public Category Select(long categoryId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.CATEGORY, new long[]{categoryId}));

            DataRow row = null;

            if (Connection != null)
                row = (DataRow)GFXDDbi.Select(Connection, statement, QueryTypes.DATAROW);    
            else
                row = (DataRow)GFXDDbi.Select(statement, QueryTypes.DATAROW);

            if (row == null)
                return null;

            return new Category(row);
        }

        public IList<Category> Select()
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(TableName.CATEGORY));
            DataTable table = null;
            
            if (Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else 
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<Category> categories = new List<Category>();

            foreach (DataRow row in table.Rows)
                categories.Add(new Category(row));

            return categories;
        }

        public long SelectCount()
        {
            return Convert.ToInt64(GFXDDbi.Select(Connection, QualifyTableName(
                DbDefault.GetSelectCountStatement(TableName.CATEGORY)), QueryTypes.SCALAR));
        }

        public Category SelectRandom()
        {
            DataRow row = (DataRow)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.CATEGORY, 1)), QueryTypes.DATAROW);

            return new Category(row);
        }

        public IList<Category> SelectRandom(int numRecords)
        {
            DataTable table = (DataTable)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.CATEGORY, numRecords)), QueryTypes.DATATABLE);

            IList<Category> categories = new List<Category>();

            foreach (DataRow row in table.Rows)
                categories.Add(new Category(row));

            return categories;
        }

        public long Insert(Category category)
        {
            String statement = QualifyTableName(DbDefault.GetInsertStatement(
                TableName.CATEGORY, new long[] { category.CategoryId }));

            statement = BuildQuery(statement, category);

            if (Connection != null)
                return GFXDDbi.Insert(Connection, statement);
            else
                return GFXDDbi.Insert(statement);
        }

        public int Update(Category category)
        {
            String statement = QualifyTableName(DbDefault.GetUpdateStatement(
                TableName.CATEGORY, new long[] { category.CategoryId }));

            if (Connection != null)
                return GFXDDbi.Update(Connection, BuildQuery(statement, category));
            else
                return GFXDDbi.Update(BuildQuery(statement, category));
        }

        public int Delete(long categoryId)
        {
            String statement = QualifyTableName(DbDefault.GetDeleteStatement(
                TableName.CATEGORY, new long[] { categoryId }));

            if(Connection != null)
                return GFXDDbi.Delete(Connection, statement);
            else
                return GFXDDbi.Delete(statement);
        }

        private String BuildQuery(String statement, Category category)
        {
            return String.Format(statement,
                GFXDDbi.Escape(category.Name),
                GFXDDbi.Escape(category.Description));
        }
    }
}
