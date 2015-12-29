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
    /// Specific method implementation for data access operation on Order table
    /// </summary>
    class OrderDao : DataObject
    {
        public OrderDao()
            : base()
        {
        }

        public OrderDao(GFXDClientConnection connection)
            : base(connection)
        {
        }

        public OrderDao(String schemaName)
            : base(schemaName)
        {
        }

        public OrderDao(GFXDClientConnection connection, String schemaName)
            : base(connection, schemaName)
        {
        }

        public Order Select(long orderId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.ORDERS, new long[]{orderId}));

            DataRow row = null;

            if (Connection != null)
                row = (DataRow)GFXDDbi.Select(Connection, statement, QueryTypes.DATAROW);
            else
                row = (DataRow)GFXDDbi.Select(statement, QueryTypes.DATAROW);

            if (row == null)
                return null;

            return new Order(row);
        }

        public IList<Order> SelectByCustomer(long customerId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.ORDERS, String.Format("customer_id = {0}", customerId)));

            DataTable table = null;

            if (Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<Order> orders = new List<Order>();

            foreach (DataRow row in table.Rows)
                orders.Add(new Order(row));

            return orders;
        }

        public IList<Order> Select()
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(TableName.ORDERS));
            DataTable table = null;

            if(Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else 
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<Order> orders = new List<Order>();

            foreach (DataRow row in table.Rows)
                orders.Add(new Order(row));

            return orders;
        }

        public long SelectCount()
        {
            return Convert.ToInt64(GFXDDbi.Select(Connection, QualifyTableName(
                DbDefault.GetSelectCountStatement(TableName.ORDERS)), QueryTypes.SCALAR));
        }

        public Order SelectRandom()
        {
            DataRow row = (DataRow)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.ORDERS, 1)), QueryTypes.DATAROW);

            return new Order(row);
        }

        public IList<Order> SelectRandom(int numRecords)
        {
            DataTable table = (DataTable)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.ORDERS, numRecords)), QueryTypes.DATATABLE);

            IList<Order> orders = new List<Order>();

            foreach (DataRow row in table.Rows)
                orders.Add(new Order(row));

            return orders;
        }

        public long Insert(Order order)
        {
            String statement = QualifyTableName(DbDefault.GetInsertStatement(
                TableName.ORDERS, new long[] { order.OrderId }));

            statement = BuildQuery(statement, order);

            if(Connection != null)
                return GFXDDbi.Insert(Connection, statement);
            else
                return GFXDDbi.Insert(statement);
        }

        public int Update(Order order)
        {
            String statement = QualifyTableName(DbDefault.GetUpdateStatement(
                TableName.ORDERS, new long[] { order.OrderId }));

            File.AppendAllText(@"C:\ORDERS.txt", statement);

            if(Connection != null)
                return GFXDDbi.Update(Connection, BuildQuery(statement, order));
            else
                return GFXDDbi.Update(BuildQuery(statement, order));
        }

        public int Delete(long orderId)
        {
            String statement = QualifyTableName(DbDefault.GetDeleteStatement(
                TableName.ORDERS, new long[] { orderId }));

            if(Connection != null)
                return GFXDDbi.Delete(Connection, statement);
            else
                return GFXDDbi.Delete(statement);
        }

        private String BuildQuery(String statement, Order order)
        {
            return String.Format(statement,
                GFXDDbi.Escape(order.OrderDate.ToShortDateString()),
                GFXDDbi.Escape(order.ShipDate.ToShortDateString()),
                order.Customer.CustomerId,
                order.SubTotal);
        }
    }
}
