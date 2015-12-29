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

namespace AdoNetTest.BIN.DDLOperations
{
    /// <summary>
    /// Create a table with the same name and definition within 2 different
    /// database schemas
    /// </summary>
    class CreateTableWithSameNameOnDiffSchema : GFXDTest
    {
        public CreateTableWithSameNameOnDiffSchema(ManualResetEvent resetEvent)
            : base(resetEvent)
        {   
        }

        public override void Run(object context)
        {
            String schemaName1 = DbHelper.GetRandomString(10);
            String schemaName2 = DbHelper.GetRandomString(10);
            String tableName = DbHelper.GetRandomString(8);

            try
            {
                DbRandom.BuildRandomTable(schemaName1, tableName, 10);
                DbRandom.BuildRandomTable(schemaName2, tableName, 15);

                Command.CommandText = String.Format("SELECT * FROM {0}.{1}", schemaName1, tableName);
                DataTable table = new DataTable();
                DataAdapter.Fill(table);
                ParseDataTable(table);

                Command.CommandText = String.Format("SELECT * FROM {0}.{1}", schemaName2, tableName);
                table.Clear();
                DataAdapter.Fill(table);
                ParseDataTable(table);
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    DbHelper.DropTable(String.Format("{0}.{1}", schemaName1, tableName));
                    DbHelper.DropTable(String.Format("{0}.{1}", schemaName2, tableName));
                }
                catch (Exception e)
                {
                    Fail(e);
                }

                base.Run(context);
            }
        }
    }
}
