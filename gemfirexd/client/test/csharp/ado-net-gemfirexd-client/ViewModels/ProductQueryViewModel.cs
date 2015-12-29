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
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using ado_net_gemfirexd_client.Properties;
using Pivotal.Data.GemFireXD;
using ado_net_sqlsvr_client.Properties;

namespace ado_net_gemfirexd_client.ViewModels
{
    public class ProductQueryViewModel
    {
        public ProductQueryViewModel()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;            
        }

        public void LoadProductCategoryData()
        {
            using (var conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "select ProductCategoryID, Name from production.productcategory order by Name";
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                var da = new GFXDDataAdapter(cmd);
                _dt.Clear();
                da.Fill(_dt);

                LoadCategoryList();
            }
        }

        public void LoadProductSubCategoryData()
        {
            using (var conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "select ProductSubCategoryID, Name from production.productsubcategory where productcategoryid = " + CategoryIdSelected.ToString() + " order by Name";
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                var da = new GFXDDataAdapter(cmd);
                _dt.Clear();
                da.Fill(_dt);

                LoadSubCategoryList();
            }
        }

        public void LoadColorData()
        {
            using (var conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query =
                   "select Distinct Color from production.product p" +
                   " join production.productsubcategory ps on ps.ProductSubCategoryID = p.ProductSubCategoryID" +
                   " where ps.name = '" + SubCategorySelected + "' order by Color";
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                var da = new GFXDDataAdapter(cmd);
                _dt.Clear();
                // need to add an empty row for deselecting currently selected item.
                _dt.Rows.Add();
                _dt.AcceptChanges();
                //
                da.Fill(_dt);

                LoadColorList();
            }
        }

        public void BuildQueryAndExecute()
        {
            string query = string.Empty;

            try
            {
                using (var conn = new GFXDClientConnection(ConnectionString))
                {
                    conn.Open();

                    // load no data 1=0, but get the columns... 
                    query =
                        "select * from production.product p join production.productsubcategory ps on" +
                        " ps.ProductSubCategoryID = p.ProductSubCategoryID where ps.name = '" +
                        SubCategorySelected + "'";

                    if (!string.IsNullOrEmpty(ColorSelected))
                    {
                        query += " AND p.Color = '" + ColorSelected + "'";
                    }

                    if (!string.IsNullOrEmpty(SellStartDateSelected))
                    {
                        query += " AND p.SellStartDate >= CAST ('" + SellStartDateSelected + "' as TIMESTAMP)";
                    }

                    if ((!string.IsNullOrEmpty(ListPriceGTSelected)) || (!string.IsNullOrEmpty(ListPriceLTSelected)))
                    {
                        if ((!string.IsNullOrEmpty(ListPriceGTSelected)) && (!string.IsNullOrEmpty(ListPriceLTSelected)))
                        {
                            query += " AND p.ListPrice BETWEEN " + ListPriceGTSelected + " AND " + ListPriceLTSelected;
                        }
                        else if (!string.IsNullOrEmpty(ListPriceGTSelected))
                        {
                            query += " AND p.ListPrice > " + ListPriceGTSelected;
                        }
                        else if (!string.IsNullOrEmpty(ListPriceLTSelected))
                        {
                            query += " AND p.ListPrice < " + ListPriceLTSelected;
                        }
                    }

                    query += " order by p.ProductId";

                    GFXDCommand cmd = conn.CreateCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = query;

                    var da = new GFXDDataAdapter(cmd);
                    _dtTableDataRows.Clear();
                    da.Fill(_dtTableDataRows);
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (!MainWindow.PeristedQList.Contains(query))
                {
                    MainWindow.PeristedQList.Add(query);
                }
            }
        }

        private void LoadCategoryList()
        {
            _catetory.Clear();
            int cnt = _dt.Rows.Count;
            foreach (DataRow row in _dt.Rows)
                _catetory.Add((int)row["ProductCategoryID"], row["Name"].ToString());

            //OnPropertyChanged("TableList");
        }

        private void LoadSubCategoryList()
        {
            _subcatetory.Clear();
            int cnt = _dt.Rows.Count;
            foreach (DataRow row in _dt.Rows)
                _subcatetory.Add(row["Name"].ToString());

            //OnPropertyChanged("TableList");
        }

        private void LoadColorList()
        {
            _color.Clear();
            int cnt = _dt.Rows.Count;
            foreach (DataRow row in _dt.Rows)
                _color.Add(row["Color"].ToString());

            //OnPropertyChanged("TableList");
        }

        private DataTable _dt = new DataTable();
        private Dictionary<int, string> _catetory = new Dictionary<int, string>();
        public Dictionary<int, string> CategoryList
        {
            get { return _catetory; }
        }

        private ObservableCollection<string> _subcatetory = new ObservableCollection<string>();
        public ObservableCollection<string> SubCategoryList
        {
            get { return _subcatetory; }
        }

        private ObservableCollection<string> _color = new ObservableCollection<string>();
        public ObservableCollection<string> Color
        {
            get
            {
                return _color;
            }
        }

        private DataTable _dtTableDataRows = new DataTable();
        public DataTable GridData
        {
            get
            {
                return _dtTableDataRows;
            }
        }

        public int CategoryIdSelected { get; set; }
        public string SubCategorySelected { get; set; }
        public string SellStartDateSelected { get; set; }
        public string ColorSelected { get; set; }
        public string ListPriceGTSelected { get; set; }
        public string ListPriceLTSelected { get; set; }
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
