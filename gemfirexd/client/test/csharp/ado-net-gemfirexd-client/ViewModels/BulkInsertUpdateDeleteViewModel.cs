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
using ado_net_gemfirexd_client.Properties;
using Pivotal.Data.GemFireXD;
using System.Data;
using ado_net_sqlsvr_client.Properties;

namespace ado_net_gemfirexd_client.ViewModels
{
    public class BulkInsertUpdateDeleteViewModel
    {
        public BulkInsertUpdateDeleteViewModel()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;            
        }

        public void LoadTableRowListData(string table)
        {
            using (var conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string[] words = table.Split('.');
                string query =
                   "SELECT * FROM " + table + " order by " + words[1] + "ID";
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                var da = new GFXDDataAdapter(cmd);
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
    }
}
