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
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using System.Windows.Threading;
using System.Data;
using ado_net_gemfirexd_client.ViewModels;

namespace ado_net_gemfirexd_client.Views
{
    /// <summary>
    /// Interaction logic for SingleTableSelecView.xaml
    /// </summary>
    public partial class SingleTableSelecView
    {
        private SingleTableViewModel _stvm = new SingleTableViewModel();

        private DataTable _dataTable;
        private ObservableCollection<string> _dataTable2;
        private ObservableCollection<DataRow> _dataTable3;

        private string _selectedSchema;
        private string _selectedTable;

        public SingleTableSelecView()
        {
            InitializeComponent();
            _stvm.LoadTableListData();
            this.tables.DataContext = _stvm.TableList;
            _dataTable2 = _stvm.TableList;
            this.tables.ItemsSource = _dataTable2;
            //this.databases.DataContext = _stvm.DataTbl;
            //_dataTable = _stvm.DataTbl;
        }

        public DataTable SchemaData
        {
            get { return _dataTable; }
        }

        public ObservableCollection<string> TableData
        {
            get { return _dataTable2; }
        }

        private void Database_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            //DataRowView dr = databases.SelectedItem as DataRowView;
            //if (dr != null)
            //{
            //    _selectedSchema = dr.Row[1].ToString();
            //    _selectedSchema = _selectedSchema.Remove(0, 3);
            //    _stvm.LoadTableListData(_selectedSchema);

            //    this.tables.DataContext = _stvm.TableList;
            //    _dataTable2 = _stvm.TableList;
            //    this.tables.ItemsSource = _dataTable2;
            //}
        }

        private void Table_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (tables.HasItems)
            {
                _selectedTable = (tables.SelectedItem == null) ? tables.Items.CurrentItem.ToString() :
                                   tables.SelectedItem.ToString();
                //else
                //    _selectedTable = tables.Items.CurrentItem.ToString();

                if (!string.IsNullOrEmpty(_selectedTable))
                {
                    _stvm.LoadTableRowListData(_selectedTable);
                    this.DataRow.DataContext = _stvm.GridData.AsDataView();
                }
            }
        }

        private void InsertBulkRows_Click(object sender, RoutedEventArgs e)
        {
            string selectedTbl;
            if (tables.SelectedItem != null)
            {
                selectedTbl = tables.SelectedItem.ToString();

                // insert rows to this table and existing data should be here
                _stvm.InsertBulkRows(selectedTbl);
                // do a get on this table after the bulk insert
                _stvm.LoadTableRowListData(selectedTbl);
            }
            else
            {
                MessageBox.Show("Please select a table from the TableList");
            }
        }

        private void DeleteBulkRows_Click(object sender, RoutedEventArgs e)
        {
            string selectedTbl;
            if (tables.SelectedItem != null)
            {
                //selectedSchema = databases.SelectedItem.ToString();
                selectedTbl = tables.SelectedItem.ToString();

                // delete rows from this table and existing data should be here
                _stvm.DeleteBulkRows(selectedTbl);
                // do a get on this table after the bulk delete
                _stvm.LoadTableRowListData(selectedTbl);
            }
            else
            {
                MessageBox.Show("Please select a table from the TableList");
            }
        }
    }
}
