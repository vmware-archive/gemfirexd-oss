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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using AdoNetTest.BIN.BusinessObjects;
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN.DataObjects
{
    /// <summary>
    /// Specific method implementation for data access operation on Category table
    /// </summary>
    class AddressDao : DataObject
    {
        public AddressDao()
            : base()
        {
        }

        public AddressDao(GFXDClientConnection connection)
            : base(connection)
        {
        }

        public AddressDao(String schemaName)
            : base(schemaName)
        {
        }

        public AddressDao(GFXDClientConnection connection, String schemaName)
            : base(connection, schemaName)
        {
        }

        public Address Select(long addressId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.ADDRESS, new long[]{addressId}));

            DataRow row = null;
            if (Connection != null)
                row = (DataRow)GFXDDbi.Select(Connection, statement, QueryTypes.DATAROW);
            else
                row = (DataRow)GFXDDbi.Select(statement, QueryTypes.DATAROW);
            
            if (row == null)
                return null;

            return new Address(row);
        }

        public IList<Address> Select()
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(TableName.ADDRESS));

            DataTable table = null;

            if (Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<Address> addresses = new List<Address>();

            foreach (DataRow row in table.Rows)
                addresses.Add(new Address(row));

            return addresses;
        }

        public long SelectCount()
        {
            return Convert.ToInt64(GFXDDbi.Select(Connection, QualifyTableName(
                DbDefault.GetSelectCountStatement(TableName.ADDRESS)), QueryTypes.SCALAR));
        }

        public Address SelectRandom()
        {
            DataRow row = (DataRow)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.ADDRESS, 1)), QueryTypes.DATAROW);

            return new Address(row);
        }

        public IList<Address> SelectRandom(int numRecords)
        {
            DataTable table = (DataTable)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.ADDRESS, numRecords)), QueryTypes.DATATABLE);

            IList<Address> addresses = new List<Address>();

            foreach (DataRow row in table.Rows)
                addresses.Add(new Address(row));

            return addresses;
        }

        public long Insert(Address address)
        {
            String statement = QualifyTableName(DbDefault.GetInsertStatement(
                TableName.ADDRESS, new long[] { address.AddressId }));

            statement = BuildQuery(statement, address);

            if (Connection != null)
                return GFXDDbi.Insert(Connection, statement);
            else
                return GFXDDbi.Insert(statement);
        }

        public int Update(Address address)
        {
            String statement = QualifyTableName(DbDefault.GetUpdateStatement(
                TableName.ADDRESS, new long[] { address.AddressId }));

            if (Connection != null)
                return GFXDDbi.Update(Connection, BuildQuery(statement, address));
            else
                return GFXDDbi.Update(BuildQuery(statement, address));
        }

        public int Delete(long addressId)
        {
            String statement = DbDefault.GetDeleteStatement(
                TableName.ADDRESS, new long[] { addressId });

            if (Connection != null)
                return GFXDDbi.Delete(Connection, statement);
            else
                return GFXDDbi.Delete(statement);
        }

        private String BuildQuery(String statement, Address address)
        {
            return String.Format(statement,
                GFXDDbi.Escape(address.Address1),
                GFXDDbi.Escape(address.Address2),
                GFXDDbi.Escape(address.Address3),
                GFXDDbi.Escape(address.City),
                GFXDDbi.Escape(address.State.ToString()),
                GFXDDbi.Escape(address.ZipCode.ToString()),
                GFXDDbi.Escape(address.Province.ToString()),
                (int)address.CountryCode);
        }
    }
}
