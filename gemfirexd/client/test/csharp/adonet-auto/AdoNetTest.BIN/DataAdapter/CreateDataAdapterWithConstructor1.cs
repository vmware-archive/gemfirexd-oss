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


namespace AdoNetTest.BIN.DataAdapter
{
    /// <summary>
    /// Verifies creating the GFXDDataAdapter using class's default construtor
    /// </summary>
    class CreateDataAdapterWithConstructor1 : GFXDTest
    {
        public CreateDataAdapterWithConstructor1(ManualResetEvent resetEvent)
            : base(resetEvent)
        {            
        }

        public override void Run(object context)
        {
            GFXDDataAdapter adapter = null;
            Command.CommandText = DbDefault.GetAddressQuery();

            try
            {
                adapter = new GFXDDataAdapter();
                adapter.SelectCommand = Command;
                
                if (adapter.SelectCommand != Command)
                {
                    Fail("SelectCommand does not reference Command object.");
                }
                if (adapter.SelectCommand.CommandText != Command.CommandText)
                {
                    Fail("SelectCommand.CommandText does not reference Command.CommandText");
                }
                if (adapter.SelectCommand.Connection != Connection)
                {
                    Fail("SelectCommand.Connection does not reference Connection object");
                }
                if (!adapter.AcceptChangesDuringFill)
                {
                    Fail(String.Format("AcceptChangesDuringFill: "
                        + "Expected [{0}]; Actual [{1}]",
                        true, adapter.AcceptChangesDuringFill));
                }
                if (!adapter.AcceptChangesDuringUpdate)
                {
                    Fail(String.Format("AcceptChangesDuringUpdate: "
                        + "Expected [{0}]; Actual [{1}]",
                        true, adapter.AcceptChangesDuringUpdate));
                }
                if (adapter.ContinueUpdateOnError)
                {
                    Fail(String.Format("ContinueUpdateOnError: "
                        + "Expected [{0}]; Actual [{1}]",
                        false, adapter.ContinueUpdateOnError));
                }
                if (adapter.FillLoadOption != LoadOption.OverwriteChanges)
                {
                    Fail(String.Format("FillLoadOption: "
                        + "Expected [{0}]; Actual[{1}]",
                        LoadOption.OverwriteChanges, adapter.FillLoadOption));
                }
                if (adapter.MissingMappingAction != MissingMappingAction.Passthrough)
                {
                    Fail(String.Format("MissingMappingAction: "
                        + "Expected [{0}]; Actual [{1}]",
                        MissingMappingAction.Passthrough, adapter.MissingMappingAction));
                }
                if (adapter.MissingSchemaAction != MissingSchemaAction.Add)
                {
                    Fail(String.Format("MissingSchemaAction: "
                        + "Expected [{0}]; Actual [{1}]",
                        MissingSchemaAction.Add, adapter.MissingSchemaAction));
                }
                if (adapter.ReturnProviderSpecificTypes)
                {
                    Fail(String.Format("ReturnProviderSpecificTypes: "
                        + "Expected [{0}]; Actual [{1}]",
                        false, adapter.ReturnProviderSpecificTypes));
                }
                if (adapter.UpdateBatchSize != 1)
                {
                    Fail(String.Format("UpdateBatchSize: "
                        + "Expected [{0}]; Actual [{1}]",
                        1, adapter.UpdateBatchSize));
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
    }
}
