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
    /// Specific method implementation for data access operation on OrderDetail table
    /// </summary>
    class OrderDetailDao : DataObject
    {
        public OrderDetailDao()
            : base()
        {
        }

        public OrderDetailDao(GFXDClientConnection connection)
            : base(connection)
        {
        }

        public OrderDetailDao(String schemaName)
            : base(schemaName)
        {
        }

        public OrderDetailDao(GFXDClientConnection connection, String schemaName)
            : base(connection, schemaName)
        {
        }

        public OrderDetail Select(long orderId, long productId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.ORDERDETAIL, new long[] { orderId, productId }));

            DataRow row = null;

            if(Connection != null)
                row = (DataRow)GFXDDbi.Select(Connection, statement, QueryTypes.DATAROW);
            else
                row = (DataRow)GFXDDbi.Select(statement, QueryTypes.DATAROW);

            if (row == null)
                return null;

            return new OrderDetail(row);
        }

        public IList<OrderDetail> Select(long orderId)
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(
                TableName.ORDERDETAIL, new long[] { orderId}));

            DataTable table = null;

            if (Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<OrderDetail> orderdetails = new List<OrderDetail>();

            foreach (DataRow row in table.Rows)
                orderdetails.Add(new OrderDetail(row));

            return orderdetails;
        }

        public IList<OrderDetail> Select()
        {
            String statement = QualifyTableName(DbDefault.GetSelectStatement(TableName.ORDERDETAIL));
            DataTable table = null;

            if(Connection != null)
                table = (DataTable)GFXDDbi.Select(Connection, statement, QueryTypes.DATATABLE);
            else
                table = (DataTable)GFXDDbi.Select(statement, QueryTypes.DATATABLE);

            IList<OrderDetail> orderdetails = new List<OrderDetail>();

            foreach (DataRow row in table.Rows)
                orderdetails.Add(new OrderDetail(row));

            return orderdetails;
        }

        public long SelectCount()
        {
            return Convert.ToInt64(GFXDDbi.Select(Connection, QualifyTableName(
                DbDefault.GetSelectCountStatement(TableName.ORDERDETAIL)), QueryTypes.SCALAR));
        }

        public OrderDetail SelectRandom()
        {
            DataRow row = (DataRow)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.ORDERDETAIL, 1)), QueryTypes.DATAROW);

            return new OrderDetail(row);
        }

        public IList<OrderDetail> SelectRandom(int numRecords)
        {
            DataTable table = (DataTable)GFXDDbi.Select(QualifyTableName(
                DbDefault.GetSelectRandomStatement(TableName.ORDERDETAIL, numRecords)), QueryTypes.DATATABLE);

            IList<OrderDetail> orderdetails = new List<OrderDetail>();

            foreach (DataRow row in table.Rows)
                orderdetails.Add(new OrderDetail(row));

            return orderdetails;
        }

        public long Insert(OrderDetail ordDetail)
        {
            String statement = QualifyTableName(DbDefault.GetInsertStatement(
                TableName.ORDERDETAIL, new long[] { ordDetail.OrderId, ordDetail.ProductId }));

            statement = BuildQuery(statement, ordDetail);

            if(Connection != null)
                return GFXDDbi.Insert(Connection, statement);
            else
                return GFXDDbi.Insert(statement);
        }

        public int Update(OrderDetail ordDetail)
        {
            String statement = QualifyTableName(DbDefault.GetUpdateStatement(
                TableName.ORDERDETAIL, new long[] { ordDetail.OrderId, ordDetail.ProductId }));
                        
            if(Connection != null)
                return GFXDDbi.Update(Connection, BuildQuery(statement, ordDetail));
            else 
                return GFXDDbi.Update(BuildQuery(statement, ordDetail));
        }

        public int Delete(long orderId, long productId)
        {
            String statement = QualifyTableName(DbDefault.GetDeleteStatement(
                TableName.ORDERDETAIL, new long[] { orderId, productId }));

            if (Connection != null)
                return GFXDDbi.Delete(Connection, statement);
            else 
                return GFXDDbi.Delete(statement);
        }

        private String BuildQuery(String statement, OrderDetail ordDetail)
        {
            return String.Format(statement,
                ordDetail.Quantity,
                ordDetail.UnitPrice,
                ordDetail.Discount);
        }
    }
}
