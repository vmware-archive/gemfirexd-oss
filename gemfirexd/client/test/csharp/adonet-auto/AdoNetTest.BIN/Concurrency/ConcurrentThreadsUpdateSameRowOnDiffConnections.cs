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
    class ConcurrentThreadsUpdateSameRowOnDiffConnections : GFXDTest
    {
        public ConcurrentThreadsUpdateSameRowOnDiffConnections(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            int numThreads = 2;
            DbController dbc = new DbController(Connection);

            try
            {
                Product product = dbc.GetRandomProduct();
                Thread[] ts = new Thread[numThreads];

                for (int i = 0; i < numThreads; i++)
                {
                    ts[i] = new Thread(new ParameterizedThreadStart(UpdateRow));
                    ts[i].Start(product.ProductId);                    
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

        private void UpdateRow(object param)
        {
            GFXDClientConnection conn = null;
            Product product = (Product)ObjectFactory.Create(ObjectType.Product);
            product.ProductId = Convert.ToInt64(param);

            try
            {
                conn = new GFXDClientConnection(ConnectionString);
                conn.Open();
                conn.BeginGFXDTransaction();

                DbController dbc = new DbController(conn);
                dbc.UpdateProduct(product);

                Product updatedProd = dbc.GetProduct(product.ProductId);

                conn.Commit();

                if (!product.Validate(updatedProd))
                    Fail("Product update validation failed.");
            }
            catch(GFXDException se)
            {
                if(!se.State.Equals("X0Z02"))
                    Fail(se);
                
                conn.Rollback();
            }
            catch (Exception e)
            {                
                Fail(e);
                conn.Rollback();
            }
        }
    }
}
