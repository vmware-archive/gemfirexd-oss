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
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Input;
using System.Xml;

namespace ado_net_gemfirexd_client.Utilities
{
    public static class Commands
    {
        public static readonly RoutedUICommand NewSalesOrder;
        public static readonly RoutedUICommand DeleteSalesOrder;
        public static readonly RoutedUICommand UpdateSalesOrder;

        public static readonly RoutedUICommand NewCustomer;
        public static readonly RoutedUICommand DeleteCustomer;
        public static readonly RoutedUICommand UpdateCustomer;

        public static readonly RoutedUICommand NewProduct;
        public static readonly RoutedUICommand DeleteProduct;
        public static readonly RoutedUICommand UpdateProduct;

        static Commands()
        {
            NewSalesOrder = new RoutedUICommand("Create a new Sales Order", "NewSalesOrder", typeof(Commands));
            DeleteSalesOrder = new RoutedUICommand("Delete current Sales Order", "DeleteSalesOrder", typeof(Commands));
            UpdateSalesOrder = new RoutedUICommand("Update current Sales Order", "UpdateSalesOrder", typeof(Commands));

            NewCustomer = new RoutedUICommand("Create a new customer", "NewCustomer", typeof(Commands));
            DeleteCustomer = new RoutedUICommand("Delete current customer", "DeleteCustomer", typeof(Commands));
            UpdateCustomer = new RoutedUICommand("Update current customer", "UpdateCustomer", typeof(Commands));

            NewProduct = new RoutedUICommand("Create a new product", "NewProduct", typeof(Commands));
            DeleteProduct = new RoutedUICommand("Delete current product", "DeleteProduct", typeof(Commands));
            UpdateProduct = new RoutedUICommand("Update current product", "UpdateProduct", typeof(Commands));
        }


        public static string ToString(DataSet dsData)
        {
            var objStream = new MemoryStream();
            dsData.WriteXml(objStream);

            var objXmlWriter = new XmlTextWriter(objStream, Encoding.UTF8);
            objStream = (MemoryStream)objXmlWriter.BaseStream;

            var objEncoding = new UTF8Encoding();

            return objEncoding.GetString(objStream.ToArray());
        }
    }
}
