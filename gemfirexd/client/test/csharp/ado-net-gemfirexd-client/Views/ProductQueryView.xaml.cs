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
using System.Data;
using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Interop;
using ado_net_gemfirexd_client.ViewModels;

namespace ado_net_gemfirexd_client.Views
{
    /// <summary>
    /// Interaction logic for ProductQueryView.xaml
    /// </summary>
    public partial class ProductQueryView : Window
    {
        private const int GWL_STYLE = -16;
        private const int WS_SYSMENU = 0x80000;
        [DllImport("user32.dll", SetLastError = true)]
        private static extern int GetWindowLong(IntPtr hWnd, int nIndex);
        [DllImport("user32.dll")]
        private static extern int SetWindowLong(IntPtr hWnd, int nIndex, int dwNewLong); 



        private ProductQueryViewModel _pqvm = new ProductQueryViewModel();

        private Dictionary<int, string> _catetory;
        private ObservableCollection<string> _subcatetory;
        private ObservableCollection<string> _color;


        public ProductQueryView()
        {
            InitializeComponent();
            Submit.IsEnabled = false;
            _pqvm.LoadProductCategoryData();
            _catetory = _pqvm.CategoryList;
            category.DataContext = _catetory;
        }

        public Dictionary<int, string> Category
        {
            get { return _catetory; }
        }

        public ObservableCollection<string> SubCategory
        {
            get { return _subcatetory; }
        }

        public ObservableCollection<string> Color
        {
            get { return _color; }
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            var hwnd = new WindowInteropHelper(this).Handle;
            SetWindowLong(hwnd, GWL_STYLE, GetWindowLong(hwnd, GWL_STYLE) & ~WS_SYSMENU);
        }

        private void CategorySelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            var ct = (KeyValuePair<int, string>)category.SelectedItem;
            if (ct.Value != null)
            {
                _pqvm.CategoryIdSelected = ct.Key;
                _pqvm.LoadProductSubCategoryData();
                _subcatetory = _pqvm.SubCategoryList;
                subcategory.DataContext = _subcatetory;
                Submit.IsEnabled = true;
            }
        }

        private void SubcategorySelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (subcategory.HasItems)
            {
                string sct = subcategory.SelectedItem.ToString();
                if (!string.IsNullOrEmpty(sct))
                {
                    _pqvm.SubCategorySelected = sct;
                    _pqvm.LoadColorData();
                    _color = _pqvm.Color;
                    colorselector.DataContext = _color;
                }
            }
        }

        private void SubmitClick(object sender, RoutedEventArgs e)
        {
            string sct = subcategory.SelectedItem.ToString();
            if (!string.IsNullOrEmpty(sct))
            {
                _pqvm.SubCategorySelected = sct;

                if (colorselector.HasItems)
                {
                    if (colorselector.SelectedItem != null)
                        _pqvm.ColorSelected = colorselector.SelectedItem.ToString();
                }

                if (!string.IsNullOrEmpty(CurrentlySelectedDate.SelectedDate.ToString()))
                {
                    DateTime dt = DateTime.Parse(CurrentlySelectedDate.SelectedDate.ToString());
                    string st = dt.ToString("yyyy-MM-dd HH:mm:ss");
                    _pqvm.SellStartDateSelected = st;
                }

                if (!string.IsNullOrEmpty(listpricegt.Text))
                {
                    _pqvm.ListPriceGTSelected = listpricegt.Text;
                }

                if (!string.IsNullOrEmpty(listpricelt.Text))
                {
                    _pqvm.ListPriceLTSelected = listpricelt.Text;
                }

                _pqvm.BuildQueryAndExecute();
                DataRow.DataContext = _pqvm.GridData.AsDataView();
            }
            else
            {
                MessageBox.Show("Please Select a SubProductCategory");
            }
        }

        private void CloseClick(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }
}
