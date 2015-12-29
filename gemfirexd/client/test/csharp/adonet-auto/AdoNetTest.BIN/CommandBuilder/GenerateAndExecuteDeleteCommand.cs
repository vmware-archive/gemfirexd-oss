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
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN.CommandBuilder
{
    /// <summary>
    /// Verifies GFXDCommandBuilder correctly compiles the delete command
    /// based on the GFXDCommand object's select command
    /// </summary>
    class GenerateAndExecuteDeleteCommand : GFXDTest
    {
        public GenerateAndExecuteDeleteCommand(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
            
        }

        public override void Run(object context)
        {
            String tableName = null;

            try
            {
                tableName = DbRandom.BuildRandomTable(10);

                Command.CommandText = String.Format(
                    "SELECT * FROM {0} ORDER BY COL_ID ASC ", tableName);                

                if (CommandBuilder.GetDeleteCommand() == null)
                    Fail("GetDeleteCommand returns null");
                else if (String.IsNullOrEmpty(CommandBuilder.GetDeleteCommand().CommandText))
                    Fail("CommandText is null or empty");
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    DbHelper.DropTable(tableName);
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
