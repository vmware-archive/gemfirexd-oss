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
using System.Configuration;
using System.Threading;
using System.IO;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.Configuration;
using AdoNetTest.Logger;

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Base class for all test cases. A test class must derive from this class to
    /// be included in the test run.
    /// </summary>
    class GFXDTest
    {
        private object locker = new object();

        private ManualResetEvent resetEvent;
        private Logger.Logger logger;

        protected TestResult tstResult;
        public TestResult Result 
        { 
            get 
            { 
                return tstResult; 
            } 
        }

        private String connectionString;
        protected String ConnectionString
        {
            get
            {
                if (connectionString == null || connectionString == String.Empty)
                    connectionString = GFXDConfigManager.GetGFXDLocatorConnectionString();

                return connectionString;
            }
            set
            {
                connectionString = value;
            }
        }

        private GFXDClientConnection connection;
        public GFXDClientConnection Connection
        {
            get
            {
                if (connection == null)
                {
                    connection = new GFXDClientConnection(ConnectionString);
                    connection.Open();
                    ++GFXDTestRunner.ConnCount;
                }
                return connection;
            }
            set
            {
                connection = value;
            }
        }

        private GFXDCommand command;
        protected GFXDCommand Command
        {
            get
            {
                if (command == null)
                {
                    command = Connection.CreateCommand();
                }
                return command;
            }
            set
            {
                command = value;
            }
        }

        private GFXDCommandBuilder commandBuilder;
        protected GFXDCommandBuilder CommandBuilder
        {
            get
            {
                if(commandBuilder == null)
                {
                    commandBuilder = new GFXDCommandBuilder(DataAdapter);
                }
                return commandBuilder;
            }
            set
            {
                commandBuilder = value;
            }
        }

        private GFXDDataAdapter dataAdapter;
        protected GFXDDataAdapter DataAdapter
        {
            get
            {
                if (dataAdapter == null)
                {
                    dataAdapter = Command.CreateDataAdapter();
                }
                return dataAdapter;
            }
            set
            {
                dataAdapter = value;
            }
        }

        private GFXDDataReader dataReader;
        protected GFXDDataReader DataReader
        {
            get
            {
                if (dataReader == null)
                {
                    dataReader = Command.ExecuteReader();
                }
                return dataReader;
            }
            set
            {
                dataReader = value;
            }
        }

        public GFXDTest() //: this(new ManualResetEvent(false))
        {
            if (logger != null)
                logger.Close();

            //logger = new Logger.Logger(GFXDConfigManager.GetClientSetting("logDir"), 
            //    this.GetType().FullName + ".log");
            logger = new Logger.Logger(Environment.GetEnvironmentVariable("GFXDADOOUTDIR"),
                  this.GetType().FullName + ".log");

            Start();         
        }

        public GFXDTest(ManualResetEvent resetEvent)
            : this()
        {   
            this.resetEvent = resetEvent;
        }

        public virtual void Run(Object context)
        {
            Complete(context);
        }

        protected void Log(String msg)
        {
            lock (locker)
            {
                logger.Write(msg);
            }
        }

        protected void Log(Exception e)
        {
            Log(DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString());
        }

        protected void Start()
        {
            Log(String.Format("Test {0} started", this.GetType().Name));
            tstResult = TestResult.PASSED;
        }

        protected void Pass()
        {
            tstResult = TestResult.PASSED;
        }

        protected void Fail()
        {
            tstResult = TestResult.FAILED;
        }

        protected void Fail(String reason)
        {
            Fail();
            Log(reason);
        }

        protected void Fail(Exception e)
        {
            Fail("Encountered exception: ");
            Log(e);
        }

        protected void Complete(Object context)
        {            
            TestEventArgs e = new TestEventArgs(this.GetType().FullName, 
                TestState.Finished.ToString(), tstResult.ToString(), "Finished");

            GFXDTestRunner.OnTestEvent(e);

            lock (context)
            {
                GFXDTestRunner.TestCount -= 1;

                if (GFXDTestRunner.TestCount == 0)
                {
                    if(resetEvent != null)
                        resetEvent.Set();
                }
            }

            Log(String.Format("Test {0} completed. Result: {1}", 
                this.GetType().FullName, tstResult));

            try
            {
                if ((Connection != null) && (!Connection.IsClosed)
                    && (Connection.State != ConnectionState.Executing))
                    Connection.Close();
            }
            catch (Exception ex)
            {                
                Log(ex);
            }
        }

        protected void ParseDataRow(DataRow row)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.Table.Columns.Count; i++)
            {
                if (row[i] is byte[])
                    sb.Append(DbHelper.ConvertToString((byte[])row[i]));
                else
                    sb.Append(row[i].ToString() + ", ");
            }
            Log(String.Format("{0}", sb.ToString()));
        }

        protected void ParseDataTable(DataTable table)
        {
            Log(String.Format("Parsing DataTable {0}...", 
                table.TableName));
            foreach (DataRow row in table.Rows)
            {
                ParseDataRow(row);
            }
        }

        protected void ParseDataSet(DataSet dataset)
        {
            Log(String.Format("Parsing DataSet {0}...", 
                dataset.DataSetName));
            foreach (DataTable table in dataset.Tables)
            {
                ParseDataTable(table);
            }
        }        

        protected DataRow Select(TableName tableName, long identity)
        {
            String condition = String.Format("{0} = {1}",
                DbDefault.GetTableStructure(tableName).PKColumns[0], identity);

            return Select(tableName, condition).Rows[0];
        }

        protected DataTable Select(TableName tableName)
        {
            return Select(tableName, string.Empty);
        }

        protected DataTable Select(TableName tableName, String condition)
        {            
            DataTable table = new DataTable();

            if (condition == null)
                condition = String.Empty;
            if (condition != String.Empty)
                condition = "WHERE " + condition;

            Command.CommandText = String.Format(
                "SELECT * FROM {0} {1}", tableName.ToString(), condition);

            Log(String.Format("Select command: {0}", 
                Command.CommandText));

            DataAdapter.Fill(table);

            return table;
        }

        protected int[] Insert(TableName tableName)
        {
            return Insert(tableName, 1);
        }

        protected int[] Insert(TableName tableName, int numRows)
        {
            IList<DbField> fields = DbDefault.GetTableStructure(tableName).Columns;
            int[] result = new int[numRows];

            PrepareInsert(tableName);
            for (int i = 0; i < numRows; i++)
            {
                foreach (DbField field in fields)
                {
                    Command.Parameters.Add(
                        DbRandom.GetRandomFieldData(field));

                }

                Command.AddBatch();
            }

            return Command.ExecuteBatch();
        }

        private void PrepareInsert(TableName tableName)
        {
            IList<DbField> fields = DbDefault.GetTableStructure(tableName).Columns;
            StringBuilder sql = new StringBuilder();

            sql.Append("INSERT INTO " + tableName.ToString() + "(");

            for (int j = 0; j < fields.Count; j++)
            {
                sql.Append(fields[j].FieldName + ", ");
            }

            sql = sql.Remove(sql.Length - 2, 1).Append(")");

            sql.Append(" VALUES(");

            for (int k = 0; k < fields.Count; k++)
            {
                sql.Append("?, ");
            }

            sql = sql.Remove(sql.Length - 2, 1).Append(")");

            Command.CommandType = CommandType.Text;
            Command.CommandText = sql.ToString();
            Command.Prepare();
        }

        protected int Update(TableName tableName, int identity)
        {
            return 0;
        }

        protected int Update(TableName tableName, String condition)
        {
            Command.CommandType = CommandType.Text;
            Command.CommandText = String.Format("");
            return 0;
        }

        protected int Delete(String tableName, int identity)
        {
            return 0;
        }

        protected int Delete(TableName tableName, String condition)
        {
            return 0;
        }

        protected int ExecuteNonQuery(String statement)
        {
            Command.CommandType = CommandType.Text;
            Command.CommandText = statement;

            return Command.ExecuteNonQuery();
        }

        protected void ParseDataReader(GFXDDataReader reader)
        {
            while (reader.Read())
            {
                StringBuilder row = new StringBuilder();
                for (int i = 0; i < reader.FieldCount; i++)
                    row.AppendFormat("{0}, ", reader.GetString(i));

                Log(row.ToString());
            }
        }
    }
}
