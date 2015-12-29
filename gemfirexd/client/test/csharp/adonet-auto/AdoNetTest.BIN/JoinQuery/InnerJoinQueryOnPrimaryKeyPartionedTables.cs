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

namespace AdoNetTest.BIN.JoinQuery
{
    /// <summary>
    /// Performs inner join query on related tables that are partitioned on their
    /// primary key
    /// </summary>
    class InnerJoinQueryOnPrimaryKeyPartionedTables : GFXDTest
    {
        public InnerJoinQueryOnPrimaryKeyPartionedTables(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            String schemaName = DbHelper.GetRandomString(5);

            try
            {
                DbDefault.CreateDB(schemaName, DbCreateType.PKPartition);
                Command.CommandText = DbDefault.GetInnerJoinSelectStatement(
                    schemaName, Relation.ORDER_CUSTOMER, null);
                DataTable dt = new DataTable();
                Log(Command.CommandText);
                DataAdapter.Fill(dt);

                if (dt.Rows.Count <= 0)
                    Fail("Failed to perform join query");
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    DbDefault.DropDB(schemaName);
                }
                catch (Exception e)
                {
                    Log(e);
                }

                base.Run(context);
            }
        }
    }
}
