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
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.BusinessObjects;


namespace AdoNetTest.BIN.Concurrency
{
    /// <summary>
    /// Performs concurent queries on the same table from different threads using the same
    /// connection. Expect no exception and all data records are returned.
    /// </summary>
    class ConcurrentThreadsQueryAllRowsSameConnection : GFXDTest
    {
        public ConcurrentThreadsQueryAllRowsSameConnection(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            int numThreads = 10;
            DbController dbc = new DbController(Connection);
            
            try
            {
                long rowCount = dbc.GetProductCount();
                Thread[] ts = new Thread[numThreads];

                for (int i = 0; i < numThreads; i++)
                {
                    ts[i] = new Thread(new ParameterizedThreadStart(SelectRows));
                    ts[i].Start(rowCount);
                }
                for (int i = 0; i < numThreads; i++)
                {
                    ts[i].Join();
                }
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

        private void SelectRows(object param)
        {
            long rowCount = Convert.ToInt64(param);

            try
            {
                DbController dbc = new DbController(Connection);
                IList<Product> products = dbc.GetProducts();

                if(products.Count != rowCount)
                    Fail(String.Format("Query returned incorrect number or rows. "
                        + "Expected [{0}]; Actual [{1}]",
                        rowCount, products.Count));
            }
            catch (Exception e)
            {
                Fail(e);
            }
        }
    }
}
