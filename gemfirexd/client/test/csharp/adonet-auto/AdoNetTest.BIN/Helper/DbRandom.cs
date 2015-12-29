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
using System.IO;
using System.Data;
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Provides a quick and easy way to create, populate, and test database logic and
    /// interfaces 
    /// </summary>
    class DbRandom
    {
        private static int batchSize = 10;
        private static readonly long startPKeyId = 100000000;

        private static DbTable randomTable
            = new DbTable(DbHelper.GetRandomString(10),
                new List<String>{"col_id"},
                new List<String>{""},
                new List<DbField>{
                    new DbField("col_id", GFXDType.Long, 10),
                    new DbField("col_bigint", GFXDType.Long, 10),
                    new DbField("col_binary", GFXDType.Binary, 5),
                    new DbField("col_blob", GFXDType.Blob, 24),
                    new DbField("col_char", GFXDType.Char, 1),
                    new DbField("col_clob", GFXDType.Clob, 24),
                    new DbField("col_date", GFXDType.Date),
                    new DbField("col_decimal", GFXDType.Decimal, 7),
                    new DbField("col_double", GFXDType.Double, 5),
                    new DbField("col_float", GFXDType.Double, 5),
                    new DbField("col_integer", GFXDType.Integer, 5),
                    new DbField("col_varbinary", GFXDType.VarBinary, 10),
                    new DbField("col_longvarbinary", GFXDType.LongVarBinary, 24),
                    new DbField("col_longvarchar", GFXDType.LongVarChar, 24),
                    new DbField("col_read", GFXDType.Real, 5),
                    new DbField("col_smallint", GFXDType.Short, 3),
                    new DbField("col_time", GFXDType.Time),
                    new DbField("col_timestamp", GFXDType.TimeStamp),
                    new DbField("col_varchar", GFXDType.VarChar, 24)                    
                },
                new List<String>{""},
                1000000000);


        public static String BuildRandomTable(int numRows)
        {
            return BuildRandomTable("APP", numRows);
        }

        public static String BuildRandomTable(String schemaName, int numRows)
        {            
            return BuildRandomTable(schemaName, DbHelper.GetRandomString(10), numRows);
        }

        public static String BuildRandomTable(String schemaName, String tableName, int numRows)
        {
            if (!DbHelper.SchemaExists(schemaName) && !String.IsNullOrEmpty(schemaName))
                CreateSchema(schemaName);
            else
                DbHelper.Log(String.Format("Schema {0} already exists", schemaName));

            CreateTable(schemaName, tableName);
            InsertTable(schemaName, tableName, numRows);

            return tableName;
        }

        private static void CreateSchema(String schemaName)
        {
            if(!DbHelper.SchemaExists(schemaName))
            {
                String serverGroup = 
                    Configuration.GFXDConfigManager.GetServerGroupSetting("defaultGroup");
                      
                String statement = String.Format("CREATE SCHEMA {0}", schemaName, serverGroup);
                
                DbHelper.ExecuteNonQueryStatement(statement);
            }
        }

        public static void CreateTable(String schemaName, String tableName)
        {
            String tableFullName = String.Format("{0}.{1}", schemaName, tableName);

            if (!DbHelper.TableExists(tableFullName))
            {
                StringBuilder sql = new StringBuilder();

                sql.AppendFormat("CREATE TABLE {0} (", tableFullName);

                foreach (DbField field in randomTable.Columns)
                    sql.AppendFormat("{0} {1} NOT NULL, ",
                        field.FieldName,
                        DbTypeMap.GetGFXDType(field.FieldType, field.Length));

                sql.AppendFormat(" CONSTRAINT pk_{0} PRIMARY KEY ({1})) ",
                    tableName, randomTable.PKColumns[0]);

                sql.Append("PARTITION BY PRIMARY KEY ");
                //sql.Append("REPLICATE ");

                DbHelper.ExecuteNonQueryStatement(sql.ToString());
            }
            else
                throw new Exception(
                    String.Format("Table {0} already exists",
                    tableFullName));
        }

        public static void InsertTable(String schemaName, String tableName, int numRows)
        {
            GFXDClientConnection conn = DbHelper.OpenNewConnection();
            GFXDCommand cmd = conn.CreateCommand();
            String tableFullName = String.Format("{0}.{1}", schemaName, tableName);
            StringBuilder sql = new StringBuilder();
            long startId = startPKeyId;

            sql.AppendFormat("INSERT INTO {0} (", tableFullName);
            foreach (DbField field in randomTable.Columns)
                sql.AppendFormat("{0}, ", field.FieldName);

            sql = sql.Remove(sql.Length - 2, 1).Append(") ");
            sql.Append("VALUES (");

            foreach (DbField field in randomTable.Columns)
                sql.Append("?, ");

            sql = sql.Remove(sql.Length - 2, 1).Append(") ");
            cmd.CommandText = sql.ToString();
            cmd.Prepare();

            DbHelper.Log(cmd.CommandText);

            int batchCount = 0;
            for (int i = 0; i < numRows; i++)
            {
                foreach (DbField field in randomTable.Columns)
                {
                    cmd.Parameters.Add(GetRandomFieldData(field));
                }
                cmd.Parameters[0] = startId++;

                cmd.AddBatch();

                if ((++batchCount) == batchSize)
                {
                    cmd.ExecuteBatch();
                    batchCount = 0;
                }
            }
            cmd.ExecuteBatch();
        }

        public static int GetRandomTableColumnCount()
        {
            return randomTable.Columns.Count;
        }

        public static void DropTable(String tableName)
        {
            DropTable("APP", tableName);
        }

        public static void DropTable(String schemaName, String tableName)
        {
            String tableFullName = String.Format("{0}.{1}", schemaName, tableName);

            if (DbHelper.TableExists(tableFullName))
            {
                StringBuilder sql = new StringBuilder();
                sql.AppendFormat("DROP TABLE {0}", tableFullName);

                DbHelper.ExecuteNonQueryStatement(sql.ToString());
            }
        }

        public static IList<Object> GetRandomRowData()
        {
            IList<Object> rowData = new List<Object>();

            foreach (DbField field in randomTable.Columns)
                rowData.Add(GetRandomFieldData(field));

            return rowData;
        }

        public static object GetRandomFieldData(DbField field)
        {
           switch (field.FieldType)
            {
                case GFXDType.Binary:
                case GFXDType.Blob:
                case GFXDType.VarBinary:
                case GFXDType.LongVarBinary:
                    return DbHelper.ConvertToBytes(
                        DbHelper.GetRandomString(field.Length));
                case GFXDType.Clob:
                    return DbHelper.GetRandomString(field.Length);
                case GFXDType.Char:
                case GFXDType.LongVarChar:
                case GFXDType.VarChar:
                    return DbHelper.GetRandomString(field.Length);
                case GFXDType.Date:
                    return DateTime.Today;
                case GFXDType.Time:
                    //return DateTime.Now.ToUniversalTime();
                    //return DateTime.Now.ToLocalTime();
                    return DateTime.Now.ToShortTimeString();
                    //return DateTime.Now.ToLongTimeString();
                case GFXDType.TimeStamp:
                    return DateTime.Now;
                case GFXDType.Decimal:
                    return DbHelper.GetRandomNumber(field.Length) + 0.99;
                case GFXDType.Double:
                    return DbHelper.GetRandomNumber(field.Length) + 0.25;
                case GFXDType.Float:
                    return DbHelper.GetRandomNumber(field.Length) + 0.12;
                case GFXDType.Short:
                case GFXDType.Integer:
                case GFXDType.Long:
                    return DbHelper.GetRandomNumber(field.Length);
                case GFXDType.Numeric:
                    return DbHelper.GetRandomNumber(field.Length) / 3;
                case GFXDType.Real:
                    return DbHelper.GetRandomNumber(field.Length) / 3;
                case GFXDType.Boolean:
                    return true;
                case GFXDType.Null:
                    return null;
                case GFXDType.JavaObject:
                case GFXDType.Other:
                default:
                    return new object();
            }

            return null;
        }

        public static bool Compare(object data, DataRow row, int index)
        {
            switch(row.Table.Columns[index].ColumnName)
            {
                case "COL_BINARY":
                case "COL_BLOB":
                case "COL_VARBINARY":
                case "COL_LONGVARBINARY":
                    return (DbHelper.ConvertToString((byte[])data) ==
                        DbHelper.ConvertToString((byte[])row[index]));
                case "COL_DATE":
                    return (DateTime.Parse(data.ToString()).ToShortDateString() ==
                        DateTime.Parse(row[index].ToString()).ToShortDateString());
                case "COL_TIME":
                    return (DateTime.Parse(data.ToString()).AddHours(1).ToLongTimeString() ==
                        DateTime.Parse(row[index].ToString()).ToLongTimeString());
                case "COL_TIMESTAMP":
                    return (data.ToString() == row[index].ToString());
                default:
                    return (data.ToString() == row[index].ToString());
            }
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////

        public static String GetCreateTableStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, DbDefault.GetCreateTableBasicStatement(tableName));
        }

        public static String GetCreateTablePKPartitionStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, DbDefault.GetCreateTablePKPartitionStatement(tableName));
        }

        public static String GetCreateTablePartitionByPKeyReplicateStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, DbDefault.GetCreateTableReplicateStatement(tableName));
        }

        public static String GetCreateTableFKPartitionStatement(String schemaName, Relation relation)
        {
            return QualifyTableName(schemaName, DbDefault.GetCreateTableFKPartitionStatement(relation));
        }

        public static String GetCreateTablePKPartitionColocateStatement(String schemaName, Relation relation)
        {
            return QualifyTableName(schemaName, DbDefault.GetCreateTablePKPartitionColocateStatement(relation));
        }

        public static String GetCreateTableFKPartitionColocateStatement(String schemaName, Relation relation)
        {
            return QualifyTableName(schemaName, DbDefault.GetCreateTableFKPartitionColocateStatement(relation));
        }

        public static String GetSelectStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(tableName));
        }

        public static String GetSelectStatement(String schemaName, TableName tableName, String condition)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(tableName, condition));
        }

        public static String GetSelectStatement(String schemaName, TableName tableName, long[] pKey)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(tableName, pKey));
        }

        public static String GetInnerJoinSelectStatement(String schemaName, Relation relation, long[] pKey)
        {
            return QualifyTableName(schemaName, DbDefault.GetInnerJoinSelectStatement(relation, pKey));
        }

        public static String GetLeftOuterJoinSelectStatement(String schemaName, Relation relation, String condition)
        {
            return QualifyTableName(schemaName, DbDefault.GetLeftOuterJoinSelectStatement(relation, condition));
        }

        public static String GetInsertStatement(String schemaName, TableName tableName, long[] pKey)
        {
            return QualifyTableName(schemaName, DbDefault.GetInsertStatement(tableName, pKey));
        }

        public static String GetUpdateStatement(String schemaName, TableName tableName, long[] pKey)
        {
            return QualifyTableName(schemaName, DbDefault.GetUpdateStatement(tableName, pKey));
        }

        public static String GetDeleteStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, DbDefault.GetDeleteStatement(tableName));
        }

        public static String GetDeleteStatement(String schemaName, TableName tableName, String condition)
        {
            return QualifyTableName(schemaName, DbDefault.GetDeleteStatement(tableName, condition));
        }

        public static String GetDeleteStatement(String schemaName, TableName tableName, long[] primaryKey)
        {
            return QualifyTableName(schemaName, DbDefault.GetDeleteStatement(tableName, primaryKey));
        }

        public static String GetDropTableStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, DbDefault.GetDropTableStatement(tableName));
        }

        public static String GetTableName(String schemaName, TableName tbEnum)
        {
            return QualifyTableName(schemaName, DbDefault.GetTableName(tbEnum));
        }

        public static String GetAddressQuery(String schemaName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(TableName.ADDRESS));
        }

        public static String GetSupplierQuery(String schemaName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(TableName.SUPPLIER));
        }

        public static String GetCategoryQuery(String schemaName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(TableName.CATEGORY));
        }

        public static String GetProductQuery(String schemaName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(TableName.PRODUCT));
        }

        public static String GetCustomerQuery(String schemaName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(TableName.CUSTOMER));
        }

        public static String GetOrderQuery(String schemaName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(TableName.ORDERS));
        }

        public static String GetOrderDetailQuery(String schemaName)
        {
            return QualifyTableName(schemaName, DbDefault.GetSelectStatement(TableName.ORDERDETAIL));
        }

        private static String QualifyTableName(String schemaName, String sql)
        {
            foreach (TableName tname in Enum.GetValues(typeof(TableName)))
            {
                if (sql.Contains(tname.ToString()))
                {
                    sql = sql.Replace(tname.ToString(),
                        String.Format("{0}.{1}", schemaName, tname.ToString()));
                }
            }

            return sql;
        }
    }
}
