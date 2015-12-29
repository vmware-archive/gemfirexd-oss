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
    /// Specific method implementation for data access operation on Customer table
    /// </summary>
    class CustomerDao : DataObject
    {
        public CustomerDao()
            : base()
        {
        }

        public CustomerDao(GFXDClientConnection connection)
            : base(connection)
        {
        }

        public CustomerDao(String schemaName)
            : base(schemaName)
        {
        }

        public CustomerDao(GFXDClientConnection connection, String schemaName)
            : base(connection, schemaName)
        {
        }

        public Customer Select(long customerId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.CUSTOMER, new long[]{customerId}));

            DataRow row = null;

            if (Connection != null)
                row = (DataRow)GFXDDbi.Select(Connection, statement, QueryTypes.DATAROW);
            else 
                row = (DataRow)GFXDDbi.Select(statement, QueryTypes.DATAROW);

            if (row == null)
                return null;

            return new Customer(row);
        }

        public IList<Customer> Select()
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(TableName.CUSTOMER));
            DataTable table = null;

            if (Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<Customer> customers = new List<Customer>();

            foreach (DataRow row in table.Rows)
                customers.Add(new Customer(row));

            return customers;
        }

        public long SelectCount()
        {
            return Convert.ToInt64(GFXDDbi.Select(Connection, QualifyTableName(
                DbDefault.GetSelectCountStatement(TableName.CUSTOMER)), QueryTypes.SCALAR));
        }

        public Customer SelectRandom()
        {
            DataRow row = (DataRow)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.CUSTOMER, 1)), QueryTypes.DATAROW);

            return new Customer(row);
        }

        public IList<Customer> SelectRandom(int numRecords)
        {
            DataTable table = (DataTable)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.CUSTOMER, numRecords)), QueryTypes.DATATABLE);

            IList<Customer> customers = new List<Customer>();

            foreach (DataRow row in table.Rows)
                customers.Add(new Customer(row));

            return customers;
        }

        public long Insert(Customer customer)
        {
            String statement = QualifyTableName(DbDefault.GetInsertStatement(
                TableName.CUSTOMER, new long[] { customer.CustomerId }));

            statement = BuildQuery(statement, customer);

            if(Connection != null)
                return GFXDDbi.Insert(Connection, statement);
            else 
                return GFXDDbi.Insert(statement);
        }

        public int Update(Customer customer)
        {
            String statement = QualifyTableName(DbDefault.GetUpdateStatement(
                TableName.CUSTOMER, new long[] { customer.CustomerId }));

            if(Connection != null)
                return GFXDDbi.Update(Connection,BuildQuery(statement, customer));
            else
                return GFXDDbi.Update(BuildQuery(statement, customer));
        }

        public int Delete(long customerId)
        {
            String statement = QualifyTableName(DbDefault.GetDeleteStatement(
                TableName.CUSTOMER, new long[] { customerId }));
            
            if (Connection != null)
                return GFXDDbi.Delete(Connection, statement);
            else 
                return GFXDDbi.Delete(statement);
        }

        private String BuildQuery(String statement, Customer customer)
        {
            return String.Format(statement,
                GFXDDbi.Escape(customer.FirstName),
                GFXDDbi.Escape(customer.LastName),
                customer.Address.AddressId,
                GFXDDbi.Escape(customer.Phone),
                GFXDDbi.Escape(customer.Email),
                GFXDDbi.Escape(customer.LastOrderDate.ToShortDateString()));
        }
    }
}
