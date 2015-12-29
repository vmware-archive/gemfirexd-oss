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
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Interop;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using ado_net_gemfirexd_client.ViewModels;
using ado_net_sqlsvr_client.Utilities;

namespace ado_net_sqlsvr_client.Views
{
    /// <summary>
    /// Interaction logic for SalesOrderQueryView.xaml
    /// </summary>
    public partial class SalesOrderQueryView : Window
    {
        private const int GWL_STYLE = -16;
        private const int WS_SYSMENU = 0x80000;
        [DllImport("user32.dll", SetLastError = true)]
        private static extern int GetWindowLong(IntPtr hWnd, int nIndex);
        [DllImport("user32.dll")]
        private static extern int SetWindowLong(IntPtr hWnd, int nIndex, int dwNewLong); 


        private SalesOrderQueryViewModel _soqvm = new SalesOrderQueryViewModel();

        public SalesOrderQueryView()
        {
            InitializeComponent();
            _soqvm.LoadSalesTerritoryData(int.MinValue, string.Empty);
            _salesTerritory = _soqvm.SalesTerritoryList;
            salesterritory.DataContext = _salesTerritory;
        }

        private SerializableDictionary<int, string> _customer = new SerializableDictionary<int, string>();
        public SerializableDictionary<int, string> Customer
        {
            get { return _customer; }
        }

        private SerializableDictionary<int, string> _salesTerritory;
        public SerializableDictionary<int, string> SalesTerritory
        {
            get { return _salesTerritory; }
        }

        private SerializableDictionary<int, string> _salesPerson = new SerializableDictionary<int, string>();
        public SerializableDictionary<int, string> SalesPerson
        {
            get { return _salesPerson; }
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            var hwnd = new WindowInteropHelper(this).Handle;
            SetWindowLong(hwnd, GWL_STYLE, GetWindowLong(hwnd, GWL_STYLE) & ~WS_SYSMENU);
        }

        private void SubmitClick(object sender, RoutedEventArgs e)
        {
            if (null != salesterritory.SelectedItem)
            {
                var st = (KeyValuePair<int, string>) salesterritory.SelectedItem;
                if (st.Value != null)
                {
                    if (null != customer.SelectedItem)
                    {
                        var cus = (KeyValuePair<int, string>) customer.SelectedItem;
                        if (cus.Value != null)
                        {
                            if (null != salesperson.SelectedItem)
                            {
                                var sp = (KeyValuePair<int, string>) salesperson.SelectedItem;
                                if (sp.Value != null)
                                {

                                }
                            }
                        }
                    }
                }
            }
            else
            {
                MessageBox.Show("Please Select a SalesTerritory");
            }



            //string sct = subcategory.SelectedItem.ToString();
            //if (!string.IsNullOrEmpty(sct))
            //{
            //    _soqvm.SubCategorySelected = sct;

            //    if (colorselector.HasItems)
            //    {
            //        if (colorselector.SelectedItem != null)
            //            _soqvm.ColorSelected = colorselector.SelectedItem.ToString();
            //    }

            //    if (!string.IsNullOrEmpty(CurrentlySelectedDate.SelectedDate.ToString()))
            //    {
            //        DateTime dt = DateTime.Parse(CurrentlySelectedDate.SelectedDate.ToString());
            //        string st = dt.ToString("yyyy-MM-dd HH:mm:ss");
            //        _soqvm.SellStartDateSelected = st;
            //    }

            //    if (!string.IsNullOrEmpty(listpricegt.Text))
            //    {
            //        _soqvm.ListPriceGTSelected = listpricegt.Text;
            //    }

            //    if (!string.IsNullOrEmpty(listpricelt.Text))
            //    {
            //        _soqvm.ListPriceLTSelected = listpricelt.Text;
            //    }

            //    _soqvm.BuildQueryAndExecute();
            //    DataRow.DataContext = _soqvm.GridData.AsDataView();
            //}
            //else
            //{
            //    MessageBox.Show("Please Select a SubProductCategory");
            //}
        }

        private void CloseClick(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private void SalesTerritorySelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (null != salesterritory.SelectedItem)
            {
                var st = (KeyValuePair<int, string>) salesterritory.SelectedItem;
                if (st.Value != null)
                {
                    salesperson.SelectedItem = -1;
                    salesperson.Text = "";
                    customer.SelectedItem = -1;
                    customer.Text = "";

                    _soqvm.LoadSalesPersonData(st.Key, st.Value);
                    salesperson.ItemsSource = _soqvm.SalesPersonList;
                    salesperson.DataContext = _soqvm.SalesPersonList;

                    _soqvm.LoadCustomerData(st.Key, st.Value);
                    customer.ItemsSource = _soqvm.CustomerList;
                    customer.DataContext = _soqvm.CustomerList;
                }
            }
        }

        private void ClearClick(object sender, RoutedEventArgs e)
        {
            salesterritory.SelectedItem = -1;
            salesterritory.Text = "";
            salesperson.SelectedItem = -1;
            salesperson.Text = "";
            salesperson.ItemsSource = null;
            customer.SelectedItem = -1;
            customer.Text = "";
            customer.ItemsSource = null;
        }

        //private void CustomerSelectionChanged(object sender, SelectionChangedEventArgs e)
        //{
        //    var cus = (KeyValuePair<int, string>)customer.SelectedItem;
        //    if (cus.Value != null)
        //    {
        //        // check if salesTerritory or salesPerson has been selected
        //        if (null == salesterritory.SelectedItem)
        //        {
        //            if (null == salesperson.SelectedItem)
        //            {
        //                _soqvm.LoadSalesTerritoryData(cus.Key, cus.Value);
        //                _salesTerritory = _soqvm.SalesTerritoryList;
        //                salesterritory.DataContext = _salesTerritory;
        //                _soqvm.LoadSalesPersonData(cus.Key, cus.Value);
        //                _salesPerson = _soqvm.SalesPersonList;
        //                salesperson.DataContext = _salesPerson;
        //            }
        //        }
        //    }
        //}

        //private void SalesPersonSelectionChanged(object sender, SelectionChangedEventArgs e)
        //{
        //    var sp = (KeyValuePair<int, string>)salesperson.SelectedItem;
        //    if (sp.Value != null)
        //    {
        //        // check if salesTerritory or customer has been selected
        //        if (null == salesterritory.SelectedItem)
        //        {
        //            if (null == customer.SelectedItem)
        //            {
        //                _soqvm.LoadSalesTerritoryData(sp.Key, sp.Value);
        //                _salesTerritory = _soqvm.SalesTerritoryList;
        //                salesterritory.DataContext = _salesTerritory;
        //                _soqvm.LoadCustomerData(sp.Key, sp.Value);
        //                _customer = _soqvm.CustomerList;
        //                customer.DataContext = _customer;
        //            }
        //        }
        //    }
        //}
    }
}
