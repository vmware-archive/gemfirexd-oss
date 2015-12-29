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
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Windows.Threading;
using ado_net_sqlsvr_client.Properties;


//SELECT TABLE_NAME
//FROM AdventureWorks.INFORMATION_SCHEMA.TABLES
//WHERE TABLE_SCHEMA = 'Person';


namespace ado_net_sqlsvr_client.ViewModels
{
    public class SelectTablesViewModel //: INotifyPropertyChanged // : BaseListViewModel<string>
    {
        public SelectTablesViewModel()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;
        }

        public void LoadData(string schema)
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "SELECT ROW_NUMBER() OVER(ORDER BY TABLE_NAME) AS rownum, TABLE_NAME as name FROM AdventureWorks.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' and TABLE_NAME not like 'v%'";
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                SqlDataAdapter da = new SqlDataAdapter(cmd);
                if (_dt != null)
                {
                    _dt = null;
                    _dt = new DataTable();
                }
                da.Fill(_dt);

                LoadTableList();
            }
        }

        private void LoadTableList()
        {
            if (_tablelist != null)
                _tablelist.Clear();
            int cnt = _dt.Rows.Count;
            foreach (DataRow row in _dt.Rows)
                _tablelist.Add(row[1].ToString());

            //OnPropertyChanged("TableList");
        }

        // only show grid data after button pressed... 
        private DataTable _dt = new DataTable();
        private ObservableCollection<string> _tablelist = new ObservableCollection<string>();
        public ObservableCollection<string> TableList
        {
            get
            {
                return _tablelist;
            }
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

        //#region INotifyPropertyChanged Members

        //public event PropertyChangedEventHandler PropertyChanged;

        //public void OnPropertyChanged(string propertyName)
        //{
        //    PropertyChangedEventHandler handler = PropertyChanged;
        //    if (handler != null)
        //    {
        //        PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
        //    }
        //}
        //#endregion
    }
}
