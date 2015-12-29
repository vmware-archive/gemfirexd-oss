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
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Diagnostics;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.Configuration;

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Provides utility methods for creating and interacting with Default database
    /// </summary>
    class DbHelper
    {        
        private static Logger.Logger logger;
        private static object locker;

        public static void Initialize()
        {
            if (logger != null)
                logger.Close();

            //logger = new Logger.Logger(GFXDConfigManager.GetClientSetting("logDir"), 
            //    String.Format("{0}.log", typeof(DbHelper).FullName));
            logger = new Logger.Logger(Environment.GetEnvironmentVariable("GFXDADOOUTDIR"),
                  String.Format("{0}.log", typeof(DbHelper).FullName));
            locker = new object();

            try
            {
                GFXDServerMgr.Initialize();
                DbDefault.CreateDB(String.Empty, DbCreateType.Replicate);
            }
            catch (Exception e)
            {
                Log(GetExceptionDetails(e, new StringBuilder()).ToString());
                throw new Exception(
                    "Encountered exception in Helper.Initialize(). Check log for detail.");
            }
        }

        public static void Cleanup()
        {
            try
            {
                DbDefault.DropDB(String.Empty);
            }
            catch (Exception e)
            {
                Log(GetExceptionDetails(e, new StringBuilder()).ToString());
                throw new Exception(
                    "Encountered exception in Helper.Cleanup(). Check log for detail.");
            }
            finally
            {
                GFXDServerMgr.Cleanup();
            }
        }

        public static GFXDClientConnection OpenNewConnection()
        {
            return OpenConnection(GFXDConfigManager.GetGFXDLocatorConnectionString());
            //return OpenConnection(GFXDConfigManager.GetGFXDServerConnectionString());
        }

        public static GFXDClientConnection OpenConnection(String connStr)
        {
            GFXDClientConnection conn = new GFXDClientConnection(connStr);

            try
            {
                conn.Open();
                ++GFXDTestRunner.ConnCount;               
                
            }
            catch (Exception e)
            {
                throw new Exception(e.Message, e.InnerException);
            }

            return conn;
        }

        public static long[] GetAllRowIds(GFXDClientConnection conn, 
            string tableName, string identityName)
        {
            IList<long> listIds = new List<long>();

            try
            {
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = String.Format("SELECT {0} FROM {1} ORDER BY {2} ASC",
                    identityName, tableName, identityName);

                GFXDDataReader rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    listIds.Add(rdr.GetInt64(0));
                }
            }
            catch (Exception e)
            {                
                throw new Exception(e.Message, e.InnerException);
            }

            return listIds.ToArray<long>();
        }

        public static void DeleteFromTable(String tableName)
        {
            DeleteFromTable(
                OpenNewConnection(),
                tableName);
        }

        public static long DeleteFromTable(GFXDClientConnection conn, 
            String tableName)
        {
            long rowsDeleted = ExecuteNonQueryStatement(
                conn, String.Format("DELETE FROM {0}", tableName));

            return rowsDeleted;
        }

        public static void CreateSchema(String schemaName)
        {
            if (!SchemaExists(schemaName))
            {
                String statement = String.Format("CREATE SCHEMA {0}", schemaName);
                ExecuteNonQueryStatement(statement);
            }
        }

        public static bool SchemaExists(String schemaName)
        {
            return SchemaExists(OpenNewConnection(), schemaName);
        }

        public static bool SchemaExists(GFXDClientConnection conn, String schemaName)
        {
            bool exists = false;

            if (Convert.ToInt32(ExecuteScalarStatement(conn, String.Format(
                    "SELECT COUNT(*) FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '{0}'",
                        schemaName.ToUpper()))) == 1)
                exists = true;

            return exists;
        }

        public static bool TableExists(String tableName)
        {
            return TableExists(OpenNewConnection(), tableName);
        }

        public static bool TableExists(GFXDClientConnection conn, String tableName)
        {
            bool exists = false;

            if (Convert.ToInt32(ExecuteScalarStatement(conn, String.Format(
                    "SELECT COUNT(*) FROM SYS.SYSTABLES WHERE TABLENAME = '{0}'",
                        tableName.ToUpper()))) == 1)
                exists = true;

            return exists;
        }

        public static int GetTableRowCount(String tableName)
        {
            return GetTableRowCount(OpenNewConnection(), tableName);
        }

        public static int GetTableRowCount(
            GFXDClientConnection conn, String tableName)
        {
            int rowCount = Convert.ToInt32(ExecuteScalarStatement(
                conn, String.Format("SELECT COUNT(*) FROM {0}", tableName)));

            return rowCount;
        }

        public static int GetTableColumnCount(String tableName)
        {
            return GetTableColumnCount(OpenNewConnection(), tableName);
        }

        public static int GetTableColumnCount(
            GFXDClientConnection conn, String tableName)
        {
            int colCount = Convert.ToInt32(ExecuteScalarStatement(
                conn, String.Format(
                    "SELECT COUNT(*) FROM SYS.SYSCOLUMNS WHERE REFERENCEID = "
                    + "(SELECT TABLEID FROM SYS.SYSTABLES WHERE TABLENAME = '{0}')",
                        tableName)));

            return colCount;
        }

        public static DataTable GetDataTable(String statement)
        {
            return GetDataTable(OpenNewConnection(), statement);
        }

        public static DataTable GetDataTable(GFXDClientConnection conn, String statement)
        {
            DataTable table = new DataTable();
            GFXDCommand cmd = new GFXDCommand(statement, conn);
            GFXDDataAdapter adpt = cmd.CreateDataAdapter();

            adpt.Fill(table);

            return table;
        }

        public static object ExecuteScalarStatement(String statement)
        {
            return ExecuteScalarStatement(OpenNewConnection(), statement);
        }

        public static object ExecuteScalarStatement(GFXDClientConnection conn, String statement)
        {
            GFXDCommand cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = statement;

            return cmd.ExecuteScalar();
        }

        public static int ExecuteNonQueryStatement(String statement)
        {
            Log(statement);

            int result = 0;

            try
            {
                result = ExecuteNonQueryStatement(OpenNewConnection(), statement);
            }
            catch (Exception e)
            {
                Log(GetExceptionDetails(e, new StringBuilder()).ToString());
                throw new Exception(
                    "Encountered exception in Helper.ExecuteNonQueryStatement(). Check log for detail.");
            }
            return result;
        }

        public static int ExecuteNonQueryStatement(GFXDClientConnection conn, String statement)
        {
            GFXDCommand cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = statement;

            return cmd.ExecuteNonQuery();
        }

        public static void DropTable(String tableName)
        {
            DropTable(OpenNewConnection(), tableName);
        }

        public static void DropTable(GFXDClientConnection conn, String tableName)
        {
            try
            {
                ExecuteNonQueryStatement(conn, String.Format("DROP TABLE {0}", 
                    tableName));
            }
            catch(Exception e)
            {
                throw new Exception(e.Message, e.InnerException);
            }
        }

        public static long GetLastRowId(string tableName, string identityName)
        {
            return GetLastRowId(OpenNewConnection(), tableName, identityName);
        }

        public static long GetLastRowId(GFXDClientConnection conn, string tableName, 
            string identityName)
        {
            long id = 0;

            try
            {
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = String.Format(
                    "SELECT {0} FROM {1} ORDER BY {2} DESC FETCH FIRST 1 ROWS ONLY",
                        identityName, tableName, identityName);

                GFXDDataReader rdr = cmd.ExecuteReader();
                while (rdr.Read())
                    id = long.Parse(rdr.GetString(0));
            }
            catch (Exception e)
            {
                throw new Exception(e.Message, e.InnerException);
            }

            return id;
        }

        public static long GetRandomRowId(string tableName, string identityName)
        {
            return GetRandomRowId(
                OpenConnection(GFXDConfigManager.GetGFXDServerConnectionString()),
                tableName, identityName);
        }

        public static long GetRandomRowId(GFXDClientConnection conn, string tableName, 
            string identityName)
        {
            try
            {
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = String.Format(
                    "SELECT {0} FROM {1} ORDER BY RANDOM() FETCH FIRST 1 ROWS ONLY",
                        identityName, tableName);

                return Convert.ToInt64(cmd.ExecuteScalar());

            }
            catch (Exception e)
            {
                throw new Exception(e.Message, e.InnerException);
            }
        }



        public static DataRow GetRandomRow(String tableName)
        {            
            String statement = String.Format(
                        "SELECT * FROM {0} ORDER BY RANDOM() FETCH FIRST 1 ROWS ONLY",
                        tableName);

            return GetDataTable(statement).Rows[0];
        }

        public static String ParseDataRow(DataRow row)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.Table.Columns.Count; i++)
            {
                if (row[i] is byte[])
                    sb.Append(DbHelper.ConvertToString((byte[])row[i]));
                else
                    sb.Append(row[i].ToString() + ", ");
            }

            return sb.ToString();
        }

        public static object GetDataField(string tableName, string fieldName, 
            string identityName, long identityValue)
        {
            return GetDataField(OpenNewConnection(),
                tableName, fieldName, identityName, identityValue);
            
        }

        public static object GetDataField(GFXDClientConnection conn, string tableName, 
            string fieldName, string identityName, long identityValue)
        {
            if (conn == null) 
                return null;
            if (conn.IsClosed) 
                conn.Open();

            object field = new object();

            try
            {
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = String.Format("SELECT {0} FROM {1} WHERE {2}={3}",
                    fieldName, tableName, identityName, identityValue);
                
                field = cmd.ExecuteScalar();
                cmd.Close();
            }
            catch(Exception e)
            {
                throw new Exception(e.Message, e.InnerException);
            }

            return field;
        }

        public static StringBuilder GetExceptionDetails(Exception e, StringBuilder sb)
        {
            sb.AppendFormat(". Message: {0}", e.Message);
            sb.AppendFormat(". Source:  {0}", e.Source);
            sb.AppendFormat(". TargetSite: {0}", e.TargetSite);
            sb.AppendFormat(". StackTrace: {0}", e.StackTrace);

            if (e is GFXDException)
            {
                GFXDException gfxde = (GFXDException)e;
                if (gfxde.NextException != null)
                {
                    sb.Append(". GFXDException->NextException: ");
                    GetExceptionDetails(gfxde.NextException, sb);
                }
            }
            else if (e is DbException)
            {
                DbException dbe = (DbException)e;
                if (dbe.InnerException != null)
                {
                    sb.Append(". DbException->InnerException: ");
                    GetExceptionDetails(e.InnerException, sb);
                }
            }
            else if (e.InnerException != null)
            {
                sb.Append(". Exception->InnerException: ");
                GetExceptionDetails(e.InnerException, sb);
            }

            return sb;
        }


        private static Random rand = new Random((int)System.DateTime.Now.Ticks);

        public static String GetRandomString(int length)
        {
            StringBuilder randText = new StringBuilder(length);
            object obj = new object();

            lock (obj)
            {
                int minVal = 65;
                int maxVal = 90;
                int randRange = maxVal - minVal;
                double randVal;

                for (int i = 0; i < length; i++)
                {
                    randVal = rand.NextDouble();
                    randText.Append((char)(minVal + randVal * randRange));
                }
            }
            return randText.ToString();            
        }

        public static char GetRandomChar()
        {
            object obj = new object();
            char ch;
            lock (obj)
            {
                int minVal = 65;
                int maxVal = 90;
                int randRange = maxVal - minVal;
                double randVal;

                randVal = rand.NextDouble();
                ch = (char)(minVal + randVal * randRange);
            }
            return ch;
        }

        public static int GetRandomNumber(int length)
        {
            int min, max;

            switch (length)
            {
                case 1: min = 0; max = 9;
                    break;
                case 2: min = 10; max = 90;
                    break;
                case 3: min = 100; max = 900;
                    break;
                case 4: min = 1000; max = 9000;
                    break;
                case 5: min = 10000; max = 90000;
                    break;
                case 6: min = 100000; max = 900000;
                    break;
                case 7: min = 1000000; max = 9000000;
                    break;
                case 8: min = 10000000; max = 90000000;
                    break;
                case 9: min = 100000000; max = 900000000;
                    break;
                case 10: min = 1000000000; max = 2000000000;
                    break;
                default:
                    return 0;
            }

            return rand.Next(min, max);
        }

        public static long GetRandomNumber(long min, long max)
        {
            return rand.Next((int)min, (int)max);
        }

        public static byte[] ConvertToBytes(string txtString)
        {
            System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
            return encoding.GetBytes(txtString);
        }

        public static string ConvertToString(byte[] bytes)
        {
            System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
            return encoding.GetString(bytes);
        }

        public static void Log(String msg)
        {            
            lock (locker)
            {
                logger.Write(msg);
            }
        }
    }
}
