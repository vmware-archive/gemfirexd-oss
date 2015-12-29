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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interop;
using ado_net_gemfirexd_client.Models;
using ado_net_gemfirexd_client.ViewModels;

namespace ado_net_gemfirexd_client.Views
{
    /// <summary>
    /// Interaction logic for BulkInsertUpdateDeleteDataView.xaml
    /// </summary>
    public partial class BulkInsertUpdateDeleteDataView : Window
    {
        private const int GWL_STYLE = -16;
        private const int WS_SYSMENU = 0x80000;
        [DllImport("user32.dll", SetLastError = true)]
        private static extern int GetWindowLong(IntPtr hWnd, int nIndex);
        [DllImport("user32.dll")]
        private static extern int SetWindowLong(IntPtr hWnd, int nIndex, int dwNewLong); 



        private ObservableCollection<string> _tableList = new ObservableCollection<string>();
        BulkInsertUpdateDeleteViewModel _stvm = new BulkInsertUpdateDeleteViewModel();

        private string _selectedTable;

        public BulkInsertUpdateDeleteDataView()
        {
            InitializeComponent();
            ExtendedInitialization();
            tables.DataContext = _tableList;
            tables.ItemsSource = _tableList;
        }

        private void ExtendedInitialization()
        {
            _tableList.Add("Sales.SalesReason");
            _tableList.Add("dbo.TestData1");
            _tableList.Add("dbo.TestData2");
            _tableList.Add("dbo.TestData3");
            _tableList.Add("dbo.TestData4");
            _tableList.Add("dbo.TestData5");
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            var hwnd = new WindowInteropHelper(this).Handle;
            SetWindowLong(hwnd, GWL_STYLE, GetWindowLong(hwnd, GWL_STYLE) & ~WS_SYSMENU);
        }

        private void Table_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (tables.HasItems)
            {
                _selectedTable = (tables.SelectedItem == null) ? tables.Items.CurrentItem.ToString() :
                                   tables.SelectedItem.ToString();

                if (!string.IsNullOrEmpty(_selectedTable))
                {
                    _stvm.LoadTableRowListData(_selectedTable);

                    DataRow.DataContext = _stvm.GridData.AsDataView();
                }
            }
        }

        private void tables_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (tables.HasItems)
            {
                _selectedTable = (tables.SelectedItem == null) ? tables.Items.CurrentItem.ToString() :
                                   tables.SelectedItem.ToString();

                if (!string.IsNullOrEmpty(_selectedTable))
                {
                    _stvm.LoadTableRowListData(_selectedTable);
                    DataRow.DataContext = _stvm.GridData.AsDataView();
                }
            }
        }

        private void InsertBulkRows_Click(object sender, RoutedEventArgs e)
        {
            if (tables.HasItems)
            {
                //_selectedTable = (tables.SelectedItem == null)
                //                     ? tables.Items.CurrentItem.ToString()
                //                     : tables.SelectedItem.ToString();

                IList list = tables.SelectedItems;
                var nList = list.OfType<string>().ToList();

                var selectedQ = new List<string>(nList.Count);
                selectedQ.AddRange(nList.Select(s => s));

                if (selectedQ.Count > 0)
                {
                    ExcuteBulkTasks("I", selectedQ);
                }
                else
                {
                    MessageBox.Show("Please select a item from the table list first");
                }
            }
        }

        private void UpdateBulkRows_Click(object sender, RoutedEventArgs e)
        {
            if (tables.HasItems)
            {
                //_selectedTable = (tables.SelectedItem == null)
                //                     ? tables.Items.CurrentItem.ToString()
                //                     : tables.SelectedItem.ToString();

                IList list = tables.SelectedItems;
                var nList = list.OfType<string>().ToList();

                var selectedQ = new List<string>(nList.Count);
                selectedQ.AddRange(nList.Select(s => s));

                if (selectedQ.Count > 0)
                {
                    ExcuteBulkTasks("U", selectedQ);
                }
                else
                {
                    MessageBox.Show("Please select a item from the table list first");
                }
            }
        }

        private void DeleteBulkRows_Click(object sender, RoutedEventArgs e)
        {
            if (tables.HasItems)
            {
                IList list = tables.SelectedItems;
                var nList = list.OfType<string>().ToList();

                var selectedQ = new List<string>(nList.Count);
                selectedQ.AddRange(nList.Select(s => s));

                if (selectedQ.Count > 0)
                {
                    ExcuteBulkTasks("D", selectedQ);
                }
                else
                {
                    MessageBox.Show("Please select a item from the table list first");
                }
            }
        }

        private void IUDTransaction_Click(object sender, RoutedEventArgs e)
        {
            if (tables.HasItems)
            {
                IList list = tables.SelectedItems;
                var nList = list.OfType<string>().ToList();

                var selectedQ = new List<string>(nList.Count);
                selectedQ.AddRange(nList.Select(s => s));

                if (selectedQ.Count > 0)
                {
                    ExcuteTransaction(selectedQ);
                }
                else
                {
                    MessageBox.Show("Please select a item from the table list first");
                }
            }
        }

        private void ExcuteTransaction(List<string> selectedQ)
        {
            try
            {
                var da = new DataAccess();
                da.ExcuteTransaction(selectedQ);
            }
            catch (Exception)
            {
                {}
                throw;
            }
 
        }

        private void CloseClick(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private void ExcuteBulkTasks(string operation, List<string> selectedQ)
        {
            int tc = selectedQ.Count;

            var domains = new AppDomain[tc];
            var threads = new Thread[tc];

            for (int i = 0; i < tc; i++)
            {
                domains[i] = AppDomain.CreateDomain("myDomain_" + i.ToString());
                domains[i].SetData("MyMessage", selectedQ[i]);
                domains[i].SetData("Operation", operation);
                threads[i] = new Thread(ThreadProc);
            }

            for (int i = 0; i < tc; i++) threads[i].Start(domains[i]);

            for (int i = 0; i < tc; i++) threads[i].Join();

            for (int i = 0; i < tc; i++) AppDomain.Unload(domains[i]);
        }

        // This is thread specific method
        [LoaderOptimization(LoaderOptimization.MultiDomainHost)]
        private static void ThreadProc(object state)
        {
            var domain = (AppDomain)state;
            domain.DoCallBack(Login);
        }

        private static void Login()
        {
            try
            {
                //Thread.Sleep(3000);
                var message = (string)AppDomain.CurrentDomain.GetData("MyMessage");
                var op = (string)AppDomain.CurrentDomain.GetData("Operation");

                var da = new DataAccess();

                switch (op)
                {
                    case "I":
                        da.InsertBulkData(message);
                        break;
                    case "D":
                        da.DeleteBulkData(message);
                        break;
                    case "U":
                        da.UpdateBulkData(message);
                        break;
                    case "T":
                        break;
                }


                string[] args = {
                                AppDomain.CurrentDomain.FriendlyName, AppDomain.CurrentDomain.Id.ToString(),
                                Thread.CurrentThread.ManagedThreadId.ToString()
                            };

                string content = "This is my AppDomainFriendlyName : " + args[0] + ", my ID is : " + args[1] +
                                 " and my ThreadID is : " + args[2] + " and TimeStamp : " + DateTime.Now.ToString();

                // Create a new file.
                //const string fileName = "executeQueryResults.txt";
                //using (var w = new StreamWriter(fileName, true))
                //{
                //    w.WriteLine(content);
                //}
            }
            catch (Exception ex)
            {
                string err = ex.Message;
                // Handle exception here;
            }
        }
    }
}
