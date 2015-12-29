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
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.BusinessObjects;

namespace AdoNetTest.BIN.Negative
{
    /// <summary>
    /// Attempts to close the GFXD connection in one thread while it is being actively used
    /// in another thread. Expects an exception when the attempt is made
    /// </summary>
    class ThreadClosesConnectionUsedByAnotherThread : GFXDTest
    {
        public ThreadClosesConnectionUsedByAnotherThread(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }            

        public override void Run(object context)
        {
            try
            {
                Thread t1 = new Thread(new ThreadStart(ReadData));
                Thread t2 = new Thread(new ThreadStart(CloseConnection));

                t1.Start();
                Thread.Sleep(100);
                t2.Start();

                t1.Join();
                t2.Join();
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                base.Run(context);
            }
        }

        private void ReadData()
        {
            try
            {
                DbController dbc = new DbController(Connection);
                IList<Product> products = dbc.GetProducts();
                foreach(Product product in products)
                    Log(product.ToString());

                IList<Order> orders = dbc.GetOrders();                
                foreach(Order order in orders)
                    Log(order.ToString());

            }
            catch (Exception e)
            {
                Fail(e);
            }
        }

        private void CloseConnection()
        {
            try
            {
                Connection.Close();

                Fail("Expected exception when closing connection");
            }
            catch (GFXDException e)
            {
                Log(e);
            }
        }
    }
}
