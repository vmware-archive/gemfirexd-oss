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
    /// Specific method implementation for data access operation on Product table
    /// </summary>
    class ProductDao : DataObject
    {
        public ProductDao()
            : base()
        {
        }

        public ProductDao(GFXDClientConnection connection)
            : base(connection)
        {
        }

        public ProductDao(String schemaName)
            : base(schemaName)
        {
        }

        public ProductDao(GFXDClientConnection connection, String schemaName)
            : base(connection, schemaName)
        {
        }

        public Product Select(long productId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.PRODUCT, new long[]{productId}));

            DataRow row = null;

            if(Connection != null)
                row = (DataRow)GFXDDbi.Select(Connection, statement, QueryTypes.DATAROW);
            else            
                row = (DataRow)GFXDDbi.Select(statement, QueryTypes.DATAROW);

            if (row == null)
                return null;

            return new Product(row);
        }

        public IList<Product> Select()
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(TableName.PRODUCT));
            DataTable table = null;

            if(Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<Product> products = new List<Product>();

            foreach (DataRow row in table.Rows)
                products.Add(new Product(row));

            return products;
        }

        public long SelectCount()
        {
            return Convert.ToInt64(GFXDDbi.Select(Connection, QualifyTableName(
                DbDefault.GetSelectCountStatement(TableName.PRODUCT)), QueryTypes.SCALAR));
        }

        public Product SelectRandom()
        {
            DataRow row = (DataRow)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.PRODUCT, 1)), QueryTypes.DATAROW);

            return new Product(row);
        }

        public IList<Product> SelectRandom(int numRecords)
        {
            DataTable table = (DataTable)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.PRODUCT, numRecords)), QueryTypes.DATATABLE);

            IList<Product> products = new List<Product>();

            foreach (DataRow row in table.Rows)
                products.Add(new Product(row));

            return products;
        }

        public long Insert(Product product)
        {
            String statement = QualifyTableName(DbDefault.GetInsertStatement(
                TableName.PRODUCT, new long[] { product.ProductId }));

            statement = BuildQuery(statement, product);

            if(Connection != null)
                return GFXDDbi.Insert(Connection, statement);
            else
                return GFXDDbi.Insert(statement);
        }

        public int Update(Product product)
        {
            String statement = QualifyTableName(DbDefault.GetUpdateStatement(
                TableName.PRODUCT, new long[] { product.ProductId }));

            if(Connection != null)
                return GFXDDbi.Update(Connection, BuildQuery(statement, product));
            else
                return GFXDDbi.Update(BuildQuery(statement, product));
        }

        public int Delete(long productId)
        {
            String statement = QualifyTableName(DbDefault.GetDeleteStatement(
                TableName.PRODUCT, new long[] { productId }));

            if(Connection != null)
                return GFXDDbi.Delete(Connection, statement);
            else
                return GFXDDbi.Delete(statement);
        }

        private String BuildQuery(String statement, Product product)
        {
            return String.Format(statement,
                GFXDDbi.Escape(product.Name),
                GFXDDbi.Escape(product.Description),
                product.Category.CategoryId,
                product.Supplier.SupplierId,
                product.UnitCost,
                product.RetailPrice,
                product.UnitsInStock,
                product.ReorderQuantity,
                GFXDDbi.Escape(product.LastOrderDate.ToShortDateString()),
                GFXDDbi.Escape(product.NextOrderDate.ToShortDateString()));
        }
    }
}
