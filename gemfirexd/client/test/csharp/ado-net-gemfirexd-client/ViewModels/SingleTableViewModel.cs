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
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Configuration;
using System.Data;
//using System.Data.SqlClient;
using Pivotal.Data.GemFireXD;
using System.Linq;
using System.Text;
using System.Threading;
using System.Windows.Threading;
using ado_net_gemfirexd_client.Properties;
using ado_net_sqlsvr_client.Properties;


//SELECT TABLE_NAME
//FROM AdventureWorks.INFORMATION_SCHEMA.TABLES
//WHERE TABLE_SCHEMA = 'Person';


namespace ado_net_gemfirexd_client.ViewModels
{
    public class SingleTableViewModel : INotifyPropertyChanged // : BaseDetailsViewModel<string>
    {
        public SingleTableViewModel()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;            
        }

        public void LoadTableRowListData(string table)
        {
            using (GFXDClientConnection conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "SELECT * FROM " + table;
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                GFXDDataAdapter da = new GFXDDataAdapter(cmd);
                _dtTableDataRows.Clear();
                da.Fill(_dtTableDataRows);

            }
        }

        // only show grid data after button pressed... 
        private DataTable _dtTableDataRows = new DataTable();
        public DataTable GridData
        {
            get
            {
                return _dtTableDataRows;
            }
        }

        //public void LoadTableListData(string schema)
        public void LoadTableListData()
        {
            using (GFXDClientConnection conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "SELECT DISTINCT B.SCHEMANAME, A.TABLENAME FROM SYS.SYSTABLES A INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID WHERE A.TABLETYPE = 'T' ORDER BY B.SCHEMANAME ASC";
                //"SELECT ROW_NUMBER() OVER(ORDER BY TABLE_NAME) AS rownum, TABLE_NAME as name FROM AdventureWorks.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' and TABLE_NAME not like 'v%'";
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                GFXDDataAdapter da = new GFXDDataAdapter(cmd);
                _dtTableList.Clear();
                da.Fill(_dtTableList);

                LoadTableList();
            }
        }

        private void LoadTableList()
        {
            _tablelist.Clear();
            int cnt = _dtTableList.Rows.Count;
            foreach (DataRow row in _dtTableList.Rows)
                _tablelist.Add(row[0].ToString() + "." + row[1].ToString());

            //OnPropertyChanged("TableList");
        }

        // only show grid data after button pressed... 
        private DataTable _dtTableList = new DataTable();
        private ObservableCollection<string> _tablelist = new ObservableCollection<string>();
        public ObservableCollection<string> TableList
        {
            get
            {
                return _tablelist;
            }
        }


        public void LoadSchemaListData()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;

            using (GFXDClientConnection conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "SELECT DISTINCT B.SCHEMANAME, A.TABLENAME FROM SYS.SYSTABLES A INNER JOIN SYS.SYSSCHEMAS B ON A.SCHEMAID = B.SCHEMAID WHERE A.TABLETYPE = 'T' ORDER BY B.SCHEMANAME ASC";
                //"select ROW_NUMBER() OVER(ORDER BY schema_id) AS rownum, 'AW.' + name as name FROM AdventureWorks.sys.schemas where schema_id between 5 and 9";
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                GFXDDataAdapter da = new GFXDDataAdapter(cmd);
                //_dtSchemaList.Clear();
                da.Fill(_dtSchemaList);
            }
        }


        // only show grid data after button pressed... 
        private DataTable _dtSchemaList = new DataTable();
        public DataTable DataTbl
        {
            get
            {
                //string naem;
                //foreach (DataRow row in _dtSchemaList.Rows)
                //    naem = row[1].ToString();
                return _dtSchemaList;
            }
        }

        public void InsertBulkRows(string tablename)
        {
            // get data from _dtTableDataRows DataTable and get an idea of each column datatype
            // then try to add bulk rows into this table using the tablename
            //DataTable insertDT = new DataTable(schemaname + "." + tablename);

            //foreach (DataColumn dc in _dtTableDataRows.Columns)
            //{
            //    DataColumn newDC = new DataColumn(dc.ColumnName);

            //}
        }

        public void DeleteBulkRows(string tablename)
        {

        }

        private string _connectionString = string.Empty;
        public string ConnectionString
        {
            get { return _connectionString; }
            set
            {
                _connectionString = value;
               // OnPropertyChanged("ConnectionString");
            }
        }

        protected virtual void FromModelToView()
        {
            //throw new NotImplementedException();
        }

        protected virtual void FromViewToModel()
        {
            //throw new NotImplementedException();
        }

        #region INotifyPropertyChanged Members

        public event PropertyChangedEventHandler PropertyChanged;

        #endregion
    }
}
