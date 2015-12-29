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

namespace AdoNetTest.BIN.Transaction
{
    /// <summary>
    /// Performs concurrent chaos update transactions on the same data record. Expects no conflict
    /// exception from either transactions while both are active.
    /// </summary>
    class ConcurrentTransactionsUpdateSameRowChaos : GFXDTest
    {
        public ConcurrentTransactionsUpdateSameRowChaos(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            int numTrans = 2;
            DbController dbc = new DbController(Connection);

            try
            {
                Customer customer = dbc.GetRandomCustomer();
                Log(String.Format("Original customer data. [{0}]{1}",
                    Thread.CurrentThread.ManagedThreadId, customer.ToString()));

                Thread[] ts = new Thread[numTrans];

                for (int i = 0; i < numTrans; i++)
                {
                    ts[i] = new Thread(new ParameterizedThreadStart(UpdateRow));

                    Log(String.Format("Start customer update thread. TID: [{0}]", ts[i].ManagedThreadId));
                    ts[i].Start(customer.CustomerId);
                    Thread.Sleep(10);
                }
                for (int i = 0; i < numTrans; i++)
                    ts[i].Join();
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

        private void UpdateRow(object param)
        {
            GFXDClientConnection conn = null;
            long customerId = Convert.ToInt64(param);

            try
            {
                conn = new GFXDClientConnection(ConnectionString);
                conn.Open();
                conn.AutoCommit = false;

                DbController dbc = new DbController(conn);

                Log(String.Format("Begin customer update transaction. TID: [{0}]",
                    Thread.CurrentThread.ManagedThreadId));

                conn.BeginGFXDTransaction(System.Data.IsolationLevel.Chaos);

                Customer newCustData = (Customer)ObjectFactory.Create(ObjectType.Customer);
                newCustData.CustomerId = customerId;

                try
                {
                    Log(String.Format("Try retrieving customer record. TID: [{0}]",
                        Thread.CurrentThread.ManagedThreadId));
                    Customer currCustData = dbc.GetCustomer(customerId);
                    if (currCustData != null)
                        Log(String.Format("Successfully retrieved customer record. TID: [{0}]",
                            Thread.CurrentThread.ManagedThreadId));
                    else
                        Log(String.Format("Failed to customer record. TID: [{0}]",
                            Thread.CurrentThread.ManagedThreadId));
                }
                catch (Exception e)
                {
                    Log(String.Format("Failed to retrieve customer record. TID: [{0}]. {1}",
                        Thread.CurrentThread.ManagedThreadId,
                        DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString()));
                }
                try
                {
                    Log(String.Format("Try updating customer record. TID: [{0}]",
                        Thread.CurrentThread.ManagedThreadId));
                    if (dbc.UpdateCustomer(newCustData) >= 1)
                        Log(String.Format("Successfully updated customer record. TID: [{0}]",
                            Thread.CurrentThread.ManagedThreadId));
                    else
                        Log(String.Format("Failed to update customer record. TID: [{0}]",
                            Thread.CurrentThread.ManagedThreadId));
                }
                catch (GFXDException e)
                {                    
                    Fail(String.Format("Update conflict exception occurred. TID: [{0}]. {1}",
                        Thread.CurrentThread.ManagedThreadId,
                        DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString()));
                }
                try
                {
                    Log(String.Format("Try committing customer update transaction. TID: [{0}]",
                        Thread.CurrentThread.ManagedThreadId));
                    conn.Commit();
                    Log(String.Format("Successfully committed customer update transaction. TID: [{0}]",
                        Thread.CurrentThread.ManagedThreadId));
                }
                catch (Exception e)
                {
                    conn.Rollback();
                    Log(String.Format("Failed to commit customer update transaction. TID: [{0}]. {1}",
                        Thread.CurrentThread.ManagedThreadId,
                        DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString()));
                }

                Customer updatedCustData = dbc.GetCustomer(newCustData.CustomerId);
            }
            catch (Exception e)
            {
                conn.Rollback();
                Log(String.Format("Rolled back customer update transaction. TID: [{0}]",
                        Thread.CurrentThread.ManagedThreadId));
                Fail(e);
            }
        }
    }
}
