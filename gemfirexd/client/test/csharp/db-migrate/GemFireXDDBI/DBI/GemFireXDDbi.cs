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
using System.Threading;
using GemFireXDDBI.DBObjects;
using Pivotal.Data.GemFireXD;

namespace GemFireXDDBI.DBI
{
    /// <summary>
    /// Specific implementation of GemFireXD database interface
    /// </summary>
    class GemFireXDDbi
    {
        public static string DbConnectionString { get; set; }

        public static int TestConnection()
        {
            GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString);
            connection.Open();

            if(connection.State == ConnectionState.Open)
            {
                connection.Close();
                return 1;
            }

            return 0;
        }

        public static bool SchemaExists(String schemaName)
        {
            bool exists = false;

            if (Convert.ToInt32(ExecuteScalarStatement(OpenNewConnection(), String.Format(
                    "SELECT COUNT(*) FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '{0}'",
                        schemaName.ToUpper()))) > 0)
                exists = true;
            
            return exists;
        }

        public static bool TableExists(String tableName)
        {
            bool exists = false;

            if (Convert.ToInt32(ExecuteScalarStatement(OpenNewConnection(), String.Format(
                    "SELECT COUNT(*) FROM SYS.SYSTABLES WHERE TABLENAME = '{0}'",
                        tableName.ToUpper()))) > 0)
                exists = true;

            return exists;
        }

        public static bool IndexExists(String indexName)
        {
            bool exists = false;

            if (Convert.ToInt32(ExecuteScalarStatement(OpenNewConnection(), String.Format(
                    @"SELECT COUNT(*) FROM SYS.SYSCONGLOMERATES A 
                    INNER JOIN SYS.SYSKEYS B ON A.CONGLOMERATEID = B.CONGLOMERATEID 
                    INNER JOIN SYS.SYSCONSTRAINTS C ON B.CONSTRAINTID = C.CONSTRAINTID
                    WHERE A.ISINDEX = 1 AND C.CONSTRAINTNAME = '{0}'",
                        indexName.ToUpper()))) > 0)
                exists = true;

            return exists;
        }

        public static DataTable GetTableNames()
        {
            string sql = @"SELECT B.SCHEMANAME, A.TABLENAME FROM SYS.SYSTABLES A
                           INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID 
                           WHERE A.TABLETYPE = 'T' ORDER BY B.SCHEMANAME ASC";

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetIndexes()
        {
            string sql = @"SELECT D.SCHEMANAME, E.TABLENAME, C.CONSTRAINTNAME, F.COLUMNNAME, 
                        A.ISINDEX FROM SYS.SYSCONGLOMERATES A 
                        INNER JOIN SYS.SYSKEYS B ON A.CONGLOMERATEID = B.CONGLOMERATEID 
                        INNER JOIN SYS.SYSCONSTRAINTS C ON B.CONSTRAINTID = C.CONSTRAINTID
                        INNER JOIN SYS.SYSSCHEMAS D ON A.SCHEMAID = D.SCHEMAID 
                        INNER JOIN SYS.SYSTABLES E ON E.TABLEID = A.TABLEID 
                        INNER JOIN SYS.SYSCOLUMNS F ON F.REFERENCEID = A.TABLEID
                        WHERE A.ISINDEX = 1";

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetIndex(string indexName)
        {
            string[] temp = indexName.Split(new char[] { '.' });
            indexName = temp[temp.Length - 1];

            string sql = String.Format(@"SELECT D.SCHEMANAME, E.TABLENAME, C.CONSTRAINTNAME, 
                        F.COLUMNNAME, A.ISINDEX FROM SYS.SYSCONGLOMERATES A 
                        INNER JOIN SYS.SYSKEYS B ON A.CONGLOMERATEID = B.CONGLOMERATEID 
                        INNER JOIN SYS.SYSCONSTRAINTS C ON B.CONSTRAINTID = C.CONSTRAINTID
                        INNER JOIN SYS.SYSSCHEMAS D ON A.SCHEMAID = D.SCHEMAID 
                        INNER JOIN SYS.SYSTABLES E ON E.TABLEID = A.TABLEID 
                        INNER JOIN SYS.SYSCOLUMNS F ON F.REFERENCEID = A.TABLEID
                        WHERE C.CONSTRAINTNAME = '{0}'", indexName.ToUpper());

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetStoredProcedures()
        {
            string sql = @"SELECT B.SCHEMANAME, A.ALIAS, ALIASTYPE FROM SYS.SYSALIASES A
                        INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID
                        WHERE A.ALIASTYPE = 'P' AND SYSTEMALIAS = 1";

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetStoredProcedure(string procedureName)
        {
            string[] temp = procedureName.Split(new char[] { '.' });
            procedureName = temp[temp.Length - 1];

            string sql = String.Format(
                    @"SELECT B.SCHEMANAME, A.ALIAS, ALIASTYPE FROM SYS.SYSALIASES A
                    INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID
                    WHERE A.ALIASTYPE = 'P' AND SYSTEMALIAS = 1 AND A.ALIAS = '{0}'", 
                    procedureName.ToUpper());

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetFunctions()
        {
            string sql = @"SELECT B.SCHEMANAME, A.ALIAS, ALIASTYPE FROM SYS.SYSALIASES A
                        INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID
                        WHERE A.ALIASTYPE = 'F' AND SYSTEMALIAS = 1";

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetFunction(string functionName)
        {
            string[] temp = functionName.Split(new char[] { '.' });
            functionName = temp[temp.Length - 1];

            string sql = String.Format(
                    @"SELECT B.SCHEMANAME, A.ALIAS, ALIASTYPE FROM SYS.SYSALIASES A
                    INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID
                    WHERE A.ALIASTYPE = 'F' AND SYSTEMALIAS = 1 AND A.ALIAS = '{0}'",
                    functionName.ToUpper());

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetTriggers()
        {
            string sql = @"SELECT B.SCHEMANAME, C.TABLENAME, A.TRIGGERNAME, A.TYPE, 
                        A.EVENT, A.STATE, A.TRIGGERDEFINITION, A.FIRINGTIME FROM SYS.SYSTRIGGERS A 
                        INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID
                        INNER JOIN SYS.SYSTABLES C ON A.TABLEID = C.TABLEID ";

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetTrigger(string triggerName)
        {
            string[] temp = triggerName.Split(new char[] { '.' });
            triggerName = temp[temp.Length - 1];

            string sql = String.Format(
                        @"SELECT B.SCHEMANAME, C.TABLENAME, A.TRIGGERNAME, A.TYPE, 
                        A.EVENT, A.STATE, A.TRIGGERDEFINITION, A.FIRINGTIME FROM SYS.SYSTRIGGERS A 
                        INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID
                        INNER JOIN SYS.SYSTABLES C ON A.TABLEID = C.TABLEID WHERE
                        TRIGGERNAME = '{0}'", triggerName.ToUpper());

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetViews()
        {
            string sql = @"SELECT S.SCHEMANAME, T.TABLENAME, V.VIEWDEFINITION FROM SYS.SYSVIEWS V 
                           INNER JOIN SYS.SYSTABLES T ON V.TABLEID = T.TABLEID 
                           INNER JOIN SYS.SYSSCHEMAS S ON T.SCHEMAID = S.SCHEMAID
                           WHERE T.TABLETYPE = 'V'";

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetView(string viewName)
        {
            string[] temp = viewName.Split(new char[] { '.' });
            viewName = temp[temp.Length - 1];

            string sql = String.Format(
                            @"SELECT S.SCHEMANAME, T.TABLENAME, V.VIEWDEFINITION FROM SYS.SYSVIEWS V 
                           INNER JOIN SYS.SYSTABLES T ON V.TABLEID = T.TABLEID 
                           INNER JOIN SYS.SYSSCHEMAS S ON T.SCHEMAID = S.SCHEMAID
                           WHERE T.TABLETYPE = 'V' AND T.TABLENAME = '{0}'", viewName);

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static DataTable GetViewData(string viewName)
        {
            string sql = String.Format(@"SELECT * FROM {0}", viewName.ToUpper());

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static int Create(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString))
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

        public static int Drop(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString))
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

        public static long Insert(string sql)
        {
            return Insert(sql, false);
        }

        public static long Insert(string sql, bool getId)
        {
            int result = 0;

            using (GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString))
            {
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

        public static int BatchInsert(GFXDClientConnection connection, string sql, DataTable sqlTable, DbTable esqlTable)
        {
            GFXDCommand command = null;
            int result = 0;
            int batchSize = 10;

            try
            {
                using (command = connection.CreateCommand())
                {
                    IDictionary<String, object> refColumns = new Dictionary<String, object>();

                    if (connection.IsClosed)
                        connection.Open();

                    command.CommandType = CommandType.Text;
                    command.CommandText = sql;
                    command.Prepare();

                    Util.Helper.Log(command.CommandText);

                    int batchCount = 0;
                    for (int i = 0; i < sqlTable.Rows.Count; i++)
                    {
                        DataRow row = sqlTable.Rows[i];

                        for (int j = 0; j < esqlTable.Columns.Count; j++)
                        {
                            GFXDParameter param = command.CreateParameter();
                            param.Type = esqlTable.Columns[j].FieldType;

                            // Set self-referenced column with null
                            if (esqlTable.Columns[j].FieldName == esqlTable.SelfRefColumn)
                            {
                                refColumns.Add(row[0].ToString(), row[esqlTable.SelfRefColumn].ToString());
                                param.Value = DBNull.Value;
                            }
                            else
                                param.Value = FormatFieldData(esqlTable.Columns[j].FieldType, row[j]);

                            command.Parameters.Add(param);
                        }

                        command.AddBatch();

                        if ((++batchCount) == batchSize)
                        {
                            command.ExecuteBatch();
                            batchCount = 0;
                        }
                    }

                    command.ExecuteBatch();

                    // Now update self-referenced column with actual value
                    if (!String.IsNullOrEmpty(esqlTable.SelfRefColumn))
                    {
                        command.CommandText = String.Format("UPDATE {0} SET {1} = ? WHERE {2} = ?",
                            esqlTable.TableFullName, esqlTable.SelfRefColumn, sqlTable.Columns[0].ColumnName);
                        command.Prepare();

                        batchCount = 0;
                        foreach (String key in refColumns.Keys)
                        {
                            if (String.IsNullOrEmpty((String)refColumns[key])
                                || ((String)refColumns[key]).ToUpper() == "NULL")
                                continue;

                            command.Parameters.Add(refColumns[key]);
                            command.Parameters.Add(key);
                            command.AddBatch();

                            if ((++batchCount) == batchSize)
                            {
                                command.ExecuteBatch();
                                batchCount = 0;
                            }
                        }

                        command.ExecuteBatch();
                    }
                }
            }
            catch (Exception e)
            {
                Util.Helper.Log(e);
            }

            return result;
        }

        public static int Update(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString))
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

        public static int Delete(string sql)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString))
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

        public static DataTable GetTableData(string tableName)
        {
            string sql = String.Format("SELECT * FROM {0} ORDER BY {1} ASC", 
                tableName, GetPKColumnName(tableName));

            Util.Helper.Log(sql);

            return (DataTable)Select(sql, Util.QueryType.DATATABLE);
        }

        public static String GetPKColumnName(string tableName)
        {
            string[] temp = tableName.Split(new char[] { '.' });
            tableName = temp[temp.Length - 1];

            string sql = String.Format(@"SELECT C.COLUMNNAME FROM SYS.SYSCOLUMNS C 
                                        INNER JOIN SYS.SYSTABLES T ON C.REFERENCEID = T.TABLEID 
                                        WHERE T.TABLENAME = '{0}' AND C.COLUMNNUMBER = 1", 
                                        tableName.ToUpper());

            return (String)ExecuteScalarStatement(OpenConnection(DbConnectionString), sql);
        }

        public static object Select(string sql, Util.QueryType type)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString))
            {
                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql;

                    if (connection.IsClosed)
                        connection.Open();

                    if (type == Util.QueryType.SCALAR)
                        return command.ExecuteScalar();

                    using (GFXDDataAdapter adapter = command.CreateDataAdapter())
                    {
                        switch (type)
                        {
                            case Util.QueryType.DATAROW: return GetDataRow(connection, adapter);
                            case Util.QueryType.DATATABLE: return GetDataTable(connection, adapter);
                            case Util.QueryType.DATASET: return GetDataSet(connection, adapter);
                            default: return null;
                        }
                    }
                }
            }
        }

        public static DataTable SelectRandom(String tableName, int numRows)
        {
            using (GFXDClientConnection connection = new GFXDClientConnection(DbConnectionString))
            {
                using (GFXDCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = String.Format(
                        "SELECT * FROM {0} ORDER BY RANDOM() FETCH FIRST {1} ROWS ONLY",
                        tableName, numRows);

                    using (GFXDDataAdapter adapter = command.CreateDataAdapter())
                    {
                        return GetDataTable(connection, adapter);
                    }
                }
            }
        }

        public static object ExecuteScalarStatement(GFXDClientConnection conn, String statement)
        {
            GFXDCommand cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = statement;

            return cmd.ExecuteScalar();
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

                if (str.Length > length)
                    str = str.Substring(0, length - 1);

                return ("'" + str.Trim().Replace("'", "''") + "'");
            }
        }

        public static GFXDClientConnection OpenNewConnection()
        {
            return OpenConnection(DbConnectionString);
        }

        public static GFXDClientConnection OpenConnection(String connStr)
        {
            GFXDClientConnection conn = new GFXDClientConnection(connStr);
            conn.Open();
            
            return conn;
        }

        private static object FormatFieldData(GFXDType type, object data)
        {
            if (data.GetType() == typeof(DBNull))
                return DBNull.Value;

            switch (type)
            {
                case GFXDType.Binary:
                case GFXDType.Blob:
                case GFXDType.VarBinary:
                case GFXDType.LongVarBinary:
                    return ConvertToBytes(data.ToString());
                case GFXDType.Clob:
                case GFXDType.Char:
                case GFXDType.LongVarChar:
                case GFXDType.VarChar:
                    return data.ToString();
                case GFXDType.Date:
                    return DateTime.Parse(data.ToString()).ToShortDateString();
                case GFXDType.Time:
                    return DateTime.Parse(data.ToString()).ToShortTimeString();
                case GFXDType.TimeStamp:
                    return DateTime.Parse(data.ToString());
                case GFXDType.Decimal:
                    return Convert.ToDecimal(data);
                case GFXDType.Double:
                case GFXDType.Float:
                    return Convert.ToDouble(data);
                case GFXDType.Short:
                    return Convert.ToInt16(data);
                case GFXDType.Integer:
                    return Convert.ToInt32(data);
                case GFXDType.Long:
                    return Convert.ToInt64(data);
                case GFXDType.Numeric:
                    return Convert.ToInt64(data);
                case GFXDType.Real:
                    return Convert.ToDecimal(data);
                case GFXDType.Boolean:
                    return Convert.ToBoolean(data);
                case GFXDType.Null:
                    return Convert.DBNull;
                case GFXDType.JavaObject:
                case GFXDType.Other:
                default:
                    return new object();
            }
        }

        public static byte[] ConvertToBytes(string txtString)
        {
            System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
            return encoding.GetBytes(txtString);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static string ConvertToString(byte[] bytes)
        {
            System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
            return encoding.GetString(bytes);
        }

        public static IList<String> ReservedWords = new List<String>{
            "ADD",
            "ALL",
            "ALLOCATE",
            "ALTER",
            "AND",
            "ANY",
            "ARE",
            "AS",
            "ASC",
            "ASSERTION",
            "AT",
            "AUTHORIZATION",
            "AVG",
            "BEGIN", 
            "BETWEEN", 
            "BIGINT",
            "BIT",
            "BOOLEAN",
            "BOTH",
            "BY",
            "CALL", 
            "CASCADE", 
            "CASCADED",
            "CASE",
            "CAST",
            "CHAR",
            "CHARACTER",
            "CHECK",
            "CLOSE",
            "COALESCE",
            "COLLATE",
            "COLLATION", 
            "COLUMN",
            "COMMIT",
            "CONNECT",
            "CONNECTION", 
            "CONSTRAINT", 
            "CONSTRAINTS", 
            "CONTINUE", 
            "CONVERT", 
            "CORRESPONDING", 
            "CREATE", 
            "CURRENT", 
            "CURRENT_DATE", 
            "CURRENT_TIME", 
            "CURRENT_TIMESTAMP", 
            "CURRENT_USER", 
            "CURSOR", 
            "DEALLOCATE", 
            "DEC", 
            "DECIMAL", 
            "DECLARE", 
            "DEFAULT", 
            "DEFERRABLE", 
            "DEFERRED", 
            "DELETE", 
            "DESC", 
            "DESCRIBE", 
            "DIAGNOSTICS", 
            "DISCONNECT", 
            "DISTINCT", 
            "DOUBLE", 
            "DROP", 
            "ELSE", 
            "END", 
            "END-EXEC", 
            "ESCAPE", 
            "EXCEPT", 
            "EXCEPTION", 
            "EXEC", 
            "EXECUTE", 
            "EXISTS", 
            "EXPLAIN", 
            "EXTERNAL", 
            "FALSE", 
            "FETCH", 
            "FIRST", 
            "FLOAT", 
            "FOR", 
            "FOREIGN", 
            "FOUND", 
            "FROM", 
            "FULL", 
            "FUNCTION", 
            "GET", 
            "GETCURRENTCONNECTION", 
            "GLOBAL", 
            "GO", 
            "GOTO", 
            "GRANT", 
            "GROUP", 
            "HAVING", 
            "HOUR", 
            "IDENTITY", 
            "IMMEDIATE", 
            "IN", 
            "INDICATOR", 
            "INITIALLY", 
            "INNER", 
            "INOUT", 
            "INPUT", 
            "INSENSITIVE", 
            "INSERT", 
            "INT", 
            "INTEGER", 
            "INTERSECT", 
            "INTO", 
            "IS", 
            "ISOLATION", 
            "JOIN", 
            "KEY", 
            "LAST", 
            "LEFT", 
            "LIKE", 
            "LOWER", 
            "LTRIM", 
            "MATCH", 
            "MAX", 
            "MIN", 
            "MINUTE", 
            "NATIONAL", 
            "NATURAL", 
            "NCHAR", 
            "NVARCHAR", 
            "NEXT", 
            "NO", 
            "NOT", 
            "NULL", 
            "NULLIF", 
            "NUMERIC", 
            "OF", 
            "ON", 
            "ONLY", 
            "OPEN", 
            "OPTION", 
            "OR", 
            "ORDER", 
            "OUTER", 
            "OUTPUT", 
            "OVERLAPS", 
            "PAD", 
            "PARTIAL", 
            "PREPARE", 
            "PRESERVE", 
            "PRIMARY", 
            "PRIOR", 
            "PRIVILEGES", 
            "PROCEDURE", 
            "PUBLIC", 
            "READ", 
            "REAL", 
            "REFERENCES", 
            "RELATIVE", 
            "RESTRICT", 
            "REVOKE", 
            "RIGHT", 
            "ROLLBACK", 
            "ROWS", 
            "RTRIM", 
            "SCHEMA", 
            "SCROLL", 
            "SECOND", 
            "SELECT", 
            "SESSION_USER", 
            "SET", 
            "SMALLINT", 
            "SOME", 
            "SPACE", 
            "SQL", 
            "SQLCODE", 
            "SQLERROR", 
            "SQLSTATE", 
            "SUBSTR", 
            "SUBSTRING", 
            "SUM", 
            "SYSTEM_USER", 
            "TABLE", 
            "TEMPORARY", 
            "TIMEZONE_HOUR", 
            "TIMEZONE_MINUTE", 
            "TO", 
            "TRANSACTION", 
            "TRANSLATE", 
            "TRANSLATION", 
            "TRUE", 
            "UNION", 
            "UNIQUE", 
            "UNKNOWN", 
            "UPDATE", 
            "UPPER", 
            "USER", 
            "USING", 
            "VALUES", 
            "VARCHAR", 
            "VARYING", 
            "VIEW", 
            "WHENEVER", 
            "WHERE", 
            "WITH", 
            "WORK", 
            "WRITE", 
            "XML", 
            "XMLEXISTS", 
            "XMLPARSE", 
            "XMLQUERY", 
            "XMLSERIALIZE", 
            "YEAR" 
        };

        public static bool IsReservedWord(string word)
        {
            return ReservedWords.Contains(word.ToUpper());
        }
    }
}
