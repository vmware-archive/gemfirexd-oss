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

namespace AdoNetTest.BIN.Command
{
    /// <summary>
    /// Verifies default GFXDCommand object settings
    /// </summary>
    class CommandDefaultPropertySettings : GFXDTest
    {
        public CommandDefaultPropertySettings(ManualResetEvent resetEvent)
            : base(resetEvent)
        {            
        }

        public override void Run(object context)
        {
            GFXDCommand command;
            String cmdString = "SELECT * FROM " + DbDefault.GetAddressQuery();

            try
            {
                command = new GFXDCommand(cmdString, Connection);

                if (command.CommandText != cmdString)
                {
                    Fail(String.Format(
                        "CommandText property is not initialized with specified "
                        + " command string. Expected {0}; Actual {1}",
                        cmdString, command.CommandText));
                }
                if (command.Connection != Connection)
                {
                    Fail(String.Format(
                        "Connection property is not initialized with the specified "
                        + "GFXDConnection object"));
                }
                if (command.CommandTimeout != -1)
                {
                    Fail(String.Format(
                        "CommandTimeout default setting is incorrect. "
                        + "Expected [{0}]; Actual [{1}]",
                        -1, command.CommandTimeout));
                }
                if (command.CommandType != System.Data.CommandType.Text)
                {
                    Fail(String.Format(
                        "CommandType default setting is incorrect. "
                        + "Expected [{0}]; Actual [{1}]",
                        System.Data.CommandType.Text, command.CommandType));
                }
                if (command.FetchSize != 0)
                {
                    Fail(String.Format(
                        "FetchSize default setting is incorrect. "
                        + "Expected [{0}]; Actual [{1}]",
                        0, command.FetchSize));
                }
                // CHANGED if (command.ReaderHoldOverCommit != false)
                //{
                //    Fail(String.Format(
                //        "ReaderHoldOverCommit default setting is incorrect. "
                //        + "Expected [{0}]; Actual [{1}]",
                //        false, command.ReaderHoldOverCommit));
                //}
                if (command.ReaderType != GFXDCommand.DataReaderType.ForwardOnly)
                {
                    Fail(String.Format(
                        "ReaderType default setting is incorrect. "
                        + "Expected [{0}]; Actual [{1}]",
                        GFXDCommand.DataReaderType.ForwardOnly, command.ReaderType));
                }
                // CHANGED if (command.ReaderUpdatable != false)
                //{
                //    Fail(String.Format(
                //        "ReaderUpdatable default setting is incorrect. "
                //        + "Expected [{0}]; Actual [{1}]",
                //        false, command.ReaderUpdatable));
                //}
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
