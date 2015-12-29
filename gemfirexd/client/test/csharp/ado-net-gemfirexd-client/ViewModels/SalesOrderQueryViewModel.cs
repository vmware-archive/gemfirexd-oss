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
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using ado_net_gemfirexd_client.Properties;
using Pivotal.Data.GemFireXD;
using ado_net_sqlsvr_client.Properties;
using ado_net_sqlsvr_client.Utilities;

namespace ado_net_gemfirexd_client.ViewModels
{
    public class SalesOrderQueryViewModel
    {
        public SalesOrderQueryViewModel()
        {
            // load the connection string from the configuration files 
            _connectionString = Settings.Default.AdventureWorksConnectionString;            
        }

        public void LoadSalesTerritoryData(int key, string value)
        {
            using (var conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query;
                if (String.IsNullOrEmpty(value))
                {
                    query = "select TerritoryID, Name from Sales.SalesTerritory order by Name";
                }
                else
                {
                    query = "select TerritoryID, Name from Sales.SalesTerritory where TerritoryID = " +
                            key.ToString() + " order by Name";                  
                }

                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                var da = new GFXDDataAdapter(cmd);
                _dt.Clear();
                da.Fill(_dt);

                LoadSalesTerritoryList();
            }
        }

        public void LoadCustomerData(int key, string value)
        {
            using (var conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query;
                if (String.IsNullOrEmpty(value))
                {
                    query = "select cus.customerid, c.firstname, c.middlename, c.lastname from person.contact c " +
                        "join sales.individual i on i.contactid = c.contactid join sales.customer cus on " +
                        "cus.customerid = i.customerid order by cus.customerid";
                }
                else
                {
                    query = "select cus.customerid, c.firstname, c.middlename, c.lastname from person.contact c " +
                        "join sales.individual i on i.contactid = c.contactid join sales.customer cus on " +
                        "cus.customerid = i.customerid where cus.territoryid = " +
                            key.ToString() + " order by cus.customerid";
                }

                   
                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                var da = new GFXDDataAdapter(cmd);
                _dt.Clear();
                // need to add an empty row for deselecting currently selected item.
                //_dt.Rows.Add();
                //_dt.AcceptChanges();

                da.Fill(_dt);

                LoadCustomerList();
            }
        }

        public void LoadSalesPersonData(int key, string value)
        {
            using (var conn = new GFXDClientConnection(ConnectionString))
            {
                conn.Open();

                // load no data 1=0, but get the columns... 
                string query;
                if (String.IsNullOrEmpty(value))
                {
                    query = "select sp.salespersonid, c.firstname, c.middlename, c.lastname from person.contact c " +
                        "join humanresources.employee e on e.contactid = c.contactid join sales.salesperson sp on " +
                        "sp.salespersonid = e.employeeid order by sp.salespersonid";
                }
                else
                {
                    query = "select sp.salespersonid, c.firstname, c.middlename, c.lastname from person.contact c " +
                        "join humanresources.employee e on e.contactid = c.contactid join sales.salesperson sp on " +
                        "sp.salespersonid = e.employeeid where sp.territoryid = " +
                            key.ToString() + " order by sp.salespersonid";
                }

                GFXDCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                var da = new GFXDDataAdapter(cmd);
                _dt.Clear();
                // need to add an empty row for deselecting currently selected item.
                //_dt.Rows.Add();
                //_dt.AcceptChanges();
                
                da.Fill(_dt);

                LoadSalesPersonList();
            }
        }

        public void BuildQueryAndExecute()
        {
            //string query = string.Empty;

            //try
            //{
            //    using (var conn = new GFXDClientConnection(ConnectionString))
            //    {
            //        conn.Open();

            //        // load no data 1=0, but get the columns... 
            //        query =
            //            "select * from production.product p join production.productsubcategory ps on" +
            //            " ps.ProductSubCategoryID = p.ProductSubCategoryID where ps.name = '" +
            //            SubCategorySelected + "'";

            //        if (!string.IsNullOrEmpty(ColorSelected))
            //        {
            //            query += " AND p.Color = '" + ColorSelected + "'";
            //        }

            //        if (!string.IsNullOrEmpty(SellStartDateSelected))
            //        {
            //            query += " AND p.SellStartDate >= CAST ('" + SellStartDateSelected + "' as TIMESTAMP)";
            //        }

            //        if ((!string.IsNullOrEmpty(ListPriceGTSelected)) || (!string.IsNullOrEmpty(ListPriceLTSelected)))
            //        {
            //            if ((!string.IsNullOrEmpty(ListPriceGTSelected)) && (!string.IsNullOrEmpty(ListPriceLTSelected)))
            //            {
            //                query += " AND p.ListPrice BETWEEN " + ListPriceGTSelected + " AND " + ListPriceLTSelected;
            //            }
            //            else if (!string.IsNullOrEmpty(ListPriceGTSelected))
            //            {
            //                query += " AND p.ListPrice > " + ListPriceGTSelected;
            //            }
            //            else if (!string.IsNullOrEmpty(ListPriceLTSelected))
            //            {
            //                query += " AND p.ListPrice < " + ListPriceLTSelected;
            //            }
            //        }

            //        query += " order by p.ProductId";

            //        GFXDCommand cmd = conn.CreateCommand();
            //        cmd.CommandType = CommandType.Text;
            //        cmd.CommandText = query;

            //        var da = new GFXDDataAdapter(cmd);
            //        _dtTableDataRows.Clear();
            //        da.Fill(_dtTableDataRows);
            //    }
            //}
            //catch (Exception e)
            //{
            //    throw e;
            //}
            //finally
            //{
                //if (!MainWindow.PeristedQList.Contains(query))
                //{
                //    MainWindow.PeristedQList.Add(query);
                //}
            //}
        }

        private void LoadSalesTerritoryList()
        {
            _salesterritorylist.Clear();
            int cnt = _dt.Rows.Count;
            foreach (DataRow row in _dt.Rows)
                _salesterritorylist.Add((int) row["TerritoryID"], row["Name"].ToString());

            //OnPropertyChanged("TableList");
        }

        private void LoadCustomerList()
        {
            var list = new SerializableDictionary<int, string>();
            int cnt = _dt.Rows.Count;
            foreach (DataRow row in _dt.Rows)
            {
                string name = row["firstname"].ToString() + " " + row["middlename"].ToString() + " " + row["lastname"].ToString();
                list.Add((int)row["CustomerID"], name);
            }

            CustomerList = list;
        }

        private void LoadSalesPersonList()
        {
            var list = new SerializableDictionary<int, string>();
            int cnt = _dt.Rows.Count;
            foreach (DataRow row in _dt.Rows)
            {
                string name = row["firstname"].ToString() + " " + row["middlename"].ToString() + " " + row["lastname"].ToString();
                list.Add((int)row["SalesPersonID"], name);
            }

            SalesPersonList = list;
        }

        private DataTable _dt = new DataTable();

        private SerializableDictionary<int, string> _salesterritorylist = new SerializableDictionary<int, string>();
        public SerializableDictionary<int, string> SalesTerritoryList
        {
            get { return _salesterritorylist; }
            set
            {
                _salesterritorylist.Clear();
                foreach (KeyValuePair<int, string> keyValuePair in value)
                {
                    _salesterritorylist.Add(keyValuePair.Key, keyValuePair.Value);
                }

                //OnPropertyChanged(new PropertyChangedEventArgs("SalesTerritoryList"));
            }
        }

        private SerializableDictionary<int, string> _salespersonlist = new SerializableDictionary<int, string>();
        public SerializableDictionary<int, string> SalesPersonList
        {
            get { return _salespersonlist; }
            set
            {
                _salespersonlist.Clear();
                foreach (KeyValuePair<int, string> keyValuePair in value)
                {
                    _salespersonlist.Add(keyValuePair.Key, keyValuePair.Value);
                }

                //OnPropertyChanged(new PropertyChangedEventArgs("SalesPersonList"));
            }
        }

        private SerializableDictionary<int, string> _customerlist = new SerializableDictionary<int, string>();
        public SerializableDictionary<int, string> CustomerList
        {
            get { return _customerlist; }
            set
            {
                _customerlist.Clear();
                foreach (KeyValuePair<int, string> keyValuePair in value)
                {
                    _customerlist.Add(keyValuePair.Key, keyValuePair.Value);
                }

                //OnPropertyChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Add, _customerlist.Values));
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
