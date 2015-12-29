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
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN;

namespace AdoNetTest.BIN.DataObjects
{
    /// <summary>
    /// Provides generalized client interface with GemFireXD
    /// </summary>
    public class GFXDDbi
    {
        private static readonly string connString = Configuration.GFXDConfigManager.GetGFXDServerConnectionString();

        public static int Create(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(connString))
            {
                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql;

                    connection.Open();

                    return command.ExecuteNonQuery();
                }
            }
        }

        public static int Create(GFXDClientConnection connection, string sql)
        {
            GFXDCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = sql;

            if (connection.IsClosed)
                connection.Open();

            return command.ExecuteNonQuery();
        }

        public static int Drop(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(connString))
            {
                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql;

                    connection.Open();

                    return command.ExecuteNonQuery();
                }
            }
        }

        public static int Drop(GFXDClientConnection connection, string sql)
        {
            GFXDCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = sql;

            if (connection.IsClosed)
                connection.Open();

            return command.ExecuteNonQuery();
        }

        public static long Insert(string sql)
        {
            return Insert(sql, false);
        }

        public static long Insert(GFXDClientConnection connection, string sql)
        {
            return Insert(connection, sql, false);
        }

        public static long Insert(string sql, bool getId)
        {
            int result = 0;

            using (GFXDClientConnection connection = new GFXDClientConnection(connString))
            {
                ++GFXDTestRunner.ConnCount;

                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql;

                    connection.Open();

                    result = command.ExecuteNonQuery();

                    if (getId)
                    {
                        command.CommandText = "SELECT @@IDENTITY";
                        result = int.Parse(command.ExecuteScalar().ToString());
                    }
                }
            }
            return result;
        }

        public static long Insert(GFXDClientConnection connection, string sql, bool getId)
        {
            int result = 0;

            GFXDCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = sql;

            if (connection.IsClosed)
                connection.Open();

            result = command.ExecuteNonQuery();

            if (getId)
            {
                command.CommandText = "SELECT @@IDENTITY";
                result = int.Parse(command.ExecuteScalar().ToString());
            }

            return result;
        }

        public static int Update(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(connString))
            {
                ++GFXDTestRunner.ConnCount;

                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql;

                    connection.Open();

                    return command.ExecuteNonQuery();
                }
            }
        }

        public static int Update(GFXDClientConnection connection, string sql)
        {
            GFXDCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = sql;

            if (connection.IsClosed)
                connection.Open();

            return command.ExecuteNonQuery();
        }

        public static int Delete(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(connString))
            {
                ++GFXDTestRunner.ConnCount;

                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql;

                    connection.Open();

                    return command.ExecuteNonQuery();
                }
            }
        }

        public static int Delete(GFXDClientConnection connection, string sql)
        {
            GFXDCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = sql;

            if (connection.IsClosed)
                connection.Open();

            return command.ExecuteNonQuery();
        }

        public static object Select(string sql, QueryTypes type)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(connString))
            {
                ++GFXDTestRunner.ConnCount;

                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql;

                    if(connection.IsClosed)
                        connection.Open();

                    if (type == QueryTypes.SCALAR)
                        return command.ExecuteScalar();

                    using (GFXDDataAdapter adapter = command.CreateDataAdapter())
                    {
                        switch (type)
                        {
                            case QueryTypes.DATAROW:      return GetDataRow(connection, adapter);
                            case QueryTypes.DATATABLE:    return GetDataTable(connection, adapter);
                            case QueryTypes.DATASET:      return GetDataSet(connection, adapter);
                            default:    return null;
                        }
                    }                   
                }
            }
        }

        public static object Select(GFXDClientConnection connection, string sql, QueryTypes type)
        {
            GFXDCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = sql;

            if (connection.IsClosed)
                connection.Open();

            if (type == QueryTypes.SCALAR)
                return command.ExecuteScalar();

            using (GFXDDataAdapter adapter = command.CreateDataAdapter())
            {
                switch (type)
                {
                    case QueryTypes.DATAROW: return GetDataRow(connection, adapter);
                    case QueryTypes.DATATABLE: return GetDataTable(connection, adapter);
                    case QueryTypes.DATASET: return GetDataSet(connection, adapter);
                    default: return null;
                }
            }
        }

        public static DataTable SelectRandom(TableName tableName, int numRows)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(connString))
            {
                ++GFXDTestRunner.ConnCount;

                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = String.Format(
                        "SELECT * FROM {0} ORDER BY RANDOM() FETCH FIRST {1} ROWS ONLY",
                        tableName.ToString(), numRows);

                    using (GFXDDataAdapter adapter = command.CreateDataAdapter())
                    {
                        return GetDataTable(connection, adapter);
                    }
                }
            }
        }

        public static DataTable SelectRandom(GFXDClientConnection connection, TableName tableName, int numRows)
        {
            GFXDCommand command = connection.CreateCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = String.Format(
                "SELECT * FROM {0} ORDER BY RANDOM() FETCH FIRST {1} ROWS ONLY",
                tableName.ToString(), numRows);

            using (GFXDDataAdapter adapter = command.CreateDataAdapter())
            {
                return GetDataTable(connection, adapter);
            }
        }

        private static DataRow GetDataRow(GFXDClientConnection connection, GFXDDataAdapter adapter)
        {
            DataTable dt = GetDataTable(connection, adapter);
            DataRow dr = null;

            if (dt.Rows.Count > 0)
                dr = dt.Rows[0];

            return dr;
        }

        private static DataTable GetDataTable(GFXDClientConnection connection, GFXDDataAdapter adapter)
        {
            DataTable dt = new DataTable();
            adapter.Fill(dt);
            return dt;
        }

        private static DataSet GetDataSet(GFXDClientConnection connection, GFXDDataAdapter adapter)
        {
            DataSet ds = new DataSet();
            adapter.Fill(ds);
            return ds;
        }

        public static String Escape(String str)
        {
            if (String.IsNullOrEmpty(str))
                return "NULL";
            else
                return ("'" + str.Trim().Replace("'", "''") + "'");
        }

        public static String Escape(String str, int length)
        {
            if (String.IsNullOrEmpty(str))
                return "NULL";
            else
            {
                str = str.Trim();

                if(str.Length > length)
                    str = str.Substring(0, length - 1);

                return ("'" + str.Trim().Replace("'", "''") + "'");
            }
        }
    }
}
