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

namespace AdoNetTest.BIN.Command
{
    /// <summary>
    /// Verifies GFXDParameters with specified name and type can be directly
    /// added to GFXDCommand's Parameters collection
    /// </summary>
    class AddNamedParamsToParamsCollection : GFXDTest
    {
        public AddNamedParamsToParamsCollection(ManualResetEvent resetEvent)
            : base(resetEvent)
        {
        }

        public override void Run(object context)
        {
            IList<GFXDType> gfxdTypes = Enum.GetValues(
                typeof(GFXDType)).Cast<GFXDType>().ToList(); 

            try
            {
                for (int i = 0; i < gfxdTypes.Count; i++)
                {
                    Command.Parameters.Add(new GFXDParameter(
                        String.Format("param{0}", 
                        gfxdTypes[i].GetType().Name), gfxdTypes[i]));
                }

                if (Command.Parameters.Count != gfxdTypes.Count)
                {
                    Fail(String.Format(
                        "Failed to add parameters to Parameters collection. "
                        + "Expected [{0}]; Actual [{1}]",
                        gfxdTypes.Count, Command.Parameters.Count));
                }
                for (int i = 0; i < gfxdTypes.Count; i++)
                {
                    if (Command.Parameters[i].GetType() != gfxdTypes[i].GetType())
                    {
                        Fail(String.Format(
                            "Parameter type is incorrectly added. "
                            + "Expected [{0}]; Actual [{1}]",
                            gfxdTypes[i].GetType().Name, Command.Parameters[i].GetType().Name));
                    }
                }         
            }
            catch (Exception e)
            {
                Fail(e);
            }
            finally
            {
                try
                {
                    Command.Close();                    
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
