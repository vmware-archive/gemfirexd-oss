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
    public class SelectSchemasViewModel // later try derive from BaseListViewModel class
    {
        public SelectSchemasViewModel() {}

        public void LoadData()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;

            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "select ROW_NUMBER() OVER(ORDER BY schema_id) AS rownum, 'AW.' + name as name FROM AdventureWorks.sys.schemas where schema_id between 5 and 9";
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(_dt);
            }
        }


        // only show grid data after button pressed... 
        private DataTable _dt = new DataTable();
        public DataTable DataTbl
        {
            get
            {
                //string naem;
                //foreach (DataRow row in _dt.Rows)
                //    naem = row[1].ToString();
                return _dt;
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
