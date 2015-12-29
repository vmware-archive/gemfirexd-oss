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
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Windows;
using System.Windows.Interop;
using System.Xml.Serialization;
using ado_net_gemfirexd_client.Models;
using ado_net_gemfirexd_client.Views;
using ado_net_sqlsvr_client.Utilities;
using ado_net_sqlsvr_client.Views;

namespace ado_net_gemfirexd_client
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private const int GWL_STYLE = -16;
        private const int WS_SYSMENU = 0x80000;
        [DllImport("user32.dll", SetLastError = true)]
        private static extern int GetWindowLong(IntPtr hWnd, int nIndex);
        [DllImport("user32.dll")]
        private static extern int SetWindowLong(IntPtr hWnd, int nIndex, int dwNewLong); 


        public string xmlDic;
        public SerializableDictionary<int, string> persistedQueries = new SerializableDictionary<int, string>();
        private readonly string _fileName = "persistQ.xml";
        internal static ObservableCollection<string> PeristedQList = new ObservableCollection<string>();
        internal int ThreadCnt;

        private static int _queryCountSet = 0;

        public MainWindow()
        {
            InitializeComponent();

            // read persisted queries from file
            if (File.Exists(_fileName))
            {
                var serializer = new XmlSerializer(persistedQueries.GetType());
                var reader = new StreamReader(_fileName);

                persistedQueries = (SerializableDictionary<int, string>) serializer.Deserialize(reader);
                reader.Close();

                foreach (var s in persistedQueries)
                {
                    PeristedQList.Add(s.Value);
                }
            }

            // display the query list
            QueryDataRow.DataContext = PeristedQList;
            QueryDataRow.ItemsSource = PeristedQList;
        }


        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            var hwnd = new WindowInteropHelper(this).Handle;
            SetWindowLong(hwnd, GWL_STYLE, GetWindowLong(hwnd, GWL_STYLE) & ~WS_SYSMENU); 
        }

        private void btnDismissApp_Click(object sender, RoutedEventArgs e)
        {
            persistedQueries.Clear();
            int i = 0;
            foreach (var s in PeristedQList)
            {
                persistedQueries.Add(i++,s);
            }

            var serializer = new XmlSerializer(persistedQueries.GetType());

            var writer = new StreamWriter(_fileName);
            serializer.Serialize(writer, persistedQueries);
            writer.Close();

            Application.Current.Shutdown();
        }

        private void btnInsertUpdateDeleteClick(object sender, RoutedEventArgs e)
        {
            var iud = new BulkInsertUpdateDeleteDataView();
            iud.Show();
        }

        private void btnProduct_Queries_Click(object sender, RoutedEventArgs e)
        {
            var prod = new ProductQueryView();
            prod.Show();
        }

        private void btnSalesOrder_Queries_Click(object sender, RoutedEventArgs e)
        {
            var sales = new SalesOrderQueryView();
            sales.Show();
            //var da = new DataAccess();
            //da.ExecuteNonQuery();
            //da.ExecuteQuery("Insert into dbo.TestData5(TestData5ID, name) Values (1,'Syed')");
        }

        private void btnExecuteQuery_Click(object sender, RoutedEventArgs e)
        {
            _queryCountSet = 0;

            ThreadCnt = Convert.ToInt32(AppDomainCount.Text);

            // get the list of queries, spawn off new appdomain for each query
            IList list = QueryDataRow.SelectedItems;
            var nList = list.OfType<string>().ToList();

            var selectedQ = new List<string>(nList.Count);
            selectedQ.AddRange(nList.Select(s => s));

            if (selectedQ.Count == 0)
            {
                MessageBox.Show("Please select a query item from the list first");
            }

            int appDomainCount;
            if (selectedQ.Count % ThreadCnt == 0)
            {
                appDomainCount = selectedQ.Count/ThreadCnt;
            }
            else
            {
                appDomainCount = selectedQ.Count;
            }


            var domains = new AppDomain[appDomainCount];
            var threads = new Thread[selectedQ.Count];

            if (ThreadCnt > 1 && appDomainCount != selectedQ.Count)
            {
                for (int i = 0; i < appDomainCount; i++)
                {
                    domains[i] = AppDomain.CreateDomain("myDomain_" + i.ToString());
                    
                    for (int j = 0; j < ThreadCnt; j++)
                    {
                        domains[i].SetData("MyMessage", selectedQ[_queryCountSet++]);
                        threads[j] = new Thread(ThreadProc);

                        threads[j].Start(domains[i]);
                        threads[j].Join();
                    }
                }
            }
            else
            {
                for (int i = 0; i < selectedQ.Count; i++)
                {
                    domains[i] = AppDomain.CreateDomain("myDomain_" + i.ToString());
                    domains[i].SetData("MyMessage", selectedQ[i]);
                    threads[i] = new Thread(ThreadProc);
                }

                for (int i = 0; i < selectedQ.Count; i++) threads[i].Start(domains[i]);
                for (int i = 0; i < selectedQ.Count; i++) threads[i].Join();
            }


            for (int i = 0; i < appDomainCount; i++) AppDomain.Unload(domains[i]);
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
            var message = (string)AppDomain.CurrentDomain.GetData("MyMessage");

            var da = new DataAccess();
            var ds = da.ExecuteQuery(message);

            string[] args = {
                                AppDomain.CurrentDomain.FriendlyName, AppDomain.CurrentDomain.Id.ToString(),
                                Thread.CurrentThread.ManagedThreadId.ToString(), ds.Tables[0].Rows.Count.ToString()
                            };

            string content = "This is my AppDomainFriendlyName : " + args[0] + ", my ID is : " + args[1] +
                             " and my ThreadID is : " + args[2] + " and Return DataRow Count : " + args[3] +
                             ", TimeStamp : " + DateTime.Now.ToString();

            // Create a new file.
            const string fileName = "executeQueryResults.txt";
            using (var w = new StreamWriter(fileName, true))
            {
                w.WriteLine(content);
            }
        }

        private void btnDDLQuery_Click(object sender, RoutedEventArgs e)
        {

        }

        private void btnCustomer_Queries_Click(object sender, RoutedEventArgs e)
        {

        }
    }
}
