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
    /// Specific method implementation for data access operation on Supplier table
    /// </summary>
    class SupplierDao : DataObject
    {
        public SupplierDao()
            : base()
        {
        }

        public SupplierDao(GFXDClientConnection connection)
            : base(connection)
        {
        }

        public SupplierDao(String schemaName)
            : base(schemaName)
        {
        }

        public SupplierDao(GFXDClientConnection connection, String schemaName)
            : base(connection, schemaName)
        {
        }

        public Supplier Select(long supplierId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.SUPPLIER, new long[]{supplierId}));

            DataRow row = null;

            if(Connection != null)
                row = (DataRow)GFXDDbi.Select(Connection, statement, QueryTypes.DATAROW);
            else
                row = (DataRow)GFXDDbi.Select(statement, QueryTypes.DATAROW);

            if(row == null)
                return null;

            return new Supplier(row);
        }

        public IList<Supplier> Select()
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(TableName.SUPPLIER));
            DataTable table = null;

            if(Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<Supplier> suppliers = new List<Supplier>();

            foreach (DataRow row in table.Rows)
                suppliers.Add(new Supplier(row));

            return suppliers;
        }

        public long SelectCount()
        {
            return Convert.ToInt64(GFXDDbi.Select(Connection, QualifyTableName(
                DbDefault.GetSelectCountStatement(TableName.SUPPLIER)), QueryTypes.SCALAR));
        }

        public Supplier SelectRandom()
        {
            DataRow row = (DataRow)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.SUPPLIER, 1)), QueryTypes.DATAROW);

            return new Supplier(row);
        }

        public IList<Supplier> SelectRandom(int numRecords)
        {
            DataTable table = (DataTable)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.SUPPLIER, numRecords)), QueryTypes.DATATABLE);

            IList<Supplier> suppliers = new List<Supplier>();

            foreach (DataRow row in table.Rows)
                suppliers.Add(new Supplier(row));

            return suppliers;
        }

        public long Insert(Supplier supplier)
        {
            String statement = QualifyTableName(DbDefault.GetInsertStatement(
                TableName.SUPPLIER, new long[] { supplier.SupplierId }));

            statement = BuildQuery(statement, supplier);

            if(Connection != null)
                return GFXDDbi.Insert(Connection, statement);
            else
                return GFXDDbi.Insert(statement);
        }

        public int Update(Supplier supplier)
        {
            String statement = QualifyTableName(DbDefault.GetUpdateStatement(
                TableName.SUPPLIER, new long[] { supplier.SupplierId }));

            if(Connection != null)
                return GFXDDbi.Update(Connection, BuildQuery(statement, supplier));
            else
                return GFXDDbi.Update(BuildQuery(statement, supplier));
        }

        public int Delete(long supplierId)
        {
            String statement = QualifyTableName(DbDefault.GetDeleteStatement(
                TableName.SUPPLIER, new long[] { supplierId }));

            if(Connection != null)
                return GFXDDbi.Delete(Connection, statement);
            else
                return GFXDDbi.Delete(statement);
        }

        private String BuildQuery(String statement, Supplier supplier)
        {
            return String.Format(statement,
                GFXDDbi.Escape(supplier.Name),
                supplier.Address.AddressId,
                GFXDDbi.Escape(supplier.Phone),
                GFXDDbi.Escape(supplier.Email));
        }
    }
}
