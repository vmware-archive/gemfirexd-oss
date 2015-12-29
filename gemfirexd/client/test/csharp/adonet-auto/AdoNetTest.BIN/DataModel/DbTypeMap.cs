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
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN
{
    class DbTypeMap
    {
        private static readonly IDictionary<GFXDType, String> typeMaps
            = new Dictionary<GFXDType, String>
            {
                { GFXDType.Null, "NULL"},
                { GFXDType.Boolean, "BOOLEAN"},
                { GFXDType.Char, "CHAR"},
                { GFXDType.Numeric, "DECIMAL"},
                { GFXDType.Integer, "INT"},
                { GFXDType.Short, "SMALLINT"},
                { GFXDType.Float, "DOUBLE PRECISION"},
                { GFXDType.Real, "REAL"},
                { GFXDType.Double, "DOUBLE PRECISION"},
                { GFXDType.Decimal, "DECIMAL"},
                { GFXDType.VarChar, "VARCHAR"},
                { GFXDType.Date, "DATE"},
                { GFXDType.Time, "TIME"},
                { GFXDType.TimeStamp, "TIMESTAMP"},
                { GFXDType.Blob, "BLOB"},
                { GFXDType.Clob, "CLOB"},
                { GFXDType.Long, "BIGINT"},
                { GFXDType.LongVarBinary, "LONG VARCHAR FOR BIT DATA"},
                { GFXDType.VarBinary, "VARCHAR FOR BIT DATA"},
                { GFXDType.LongVarChar, "LONG VARCHAR"},
                { GFXDType.Binary, "CHAR FOR BIT DATA" }
            };

        public static String GetGFXDType(GFXDType type, int length)
        {
            String sqlType = typeMaps[type];
            
            if(length > 0)
            {
                if (sqlType.StartsWith("VARCHAR"))
                    sqlType = sqlType.Insert("VARCHAR".Length, String.Format("({0})", length));
                else if (sqlType.StartsWith("CHAR"))
                    sqlType = sqlType.Insert("CHAR".Length, String.Format("({0})", length));
                else if (sqlType.StartsWith("DECIMAL"))
                    sqlType = sqlType.Insert("DECIMAL".Length, String.Format("({0},2)", length + 2));
            }

            return sqlType;
        }
    }
}
