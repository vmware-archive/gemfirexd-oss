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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Pivotal.Data.GemFireXD;

namespace GemFireXDDBI.DBObjects
{
    class DbTypeMap
    {
        private static readonly IDictionary<SqlDbType, String> SqlToJdbcMap
            = new Dictionary<SqlDbType, String>
            {
                { SqlDbType.BigInt, "BIGINT" },
                { SqlDbType.Binary, "CHAR FOR BIT DATA" },
                { SqlDbType.Bit, "SMALLINT" },
                { SqlDbType.Char, "CHAR" },
                { SqlDbType.Date, "DATE" },
                { SqlDbType.DateTime, "TIMESTAMP" },
                { SqlDbType.DateTime2, "TIMESTAMP" },                           
                //{ SqlDbType.DateTimeOffset, "?" },                      
                { SqlDbType.Decimal, "DECIMAL" },
                { SqlDbType.Float, "DOUBLE PRECISION" },
                { SqlDbType.Image, "BLOB" },
                { SqlDbType.Int, "INT" },
                { SqlDbType.Money, "DECIMAL" },
                { SqlDbType.NChar, "VARCHAR" },
                { SqlDbType.NText, "VARCHAR" },                           
                { SqlDbType.NVarChar, "VARCHAR" },
                { SqlDbType.Real, "REAL" },
                { SqlDbType.SmallDateTime, "TIMESTAMP" },                 
                { SqlDbType.SmallInt, "SMALLINT"},
                { SqlDbType.SmallMoney, "DECIMAL" },
                //{ SqlDbType.Structured, "?" },                          
                { SqlDbType.Text, "VARCHAR" },                            
                { SqlDbType.Time, "TIME" },
                { SqlDbType.Timestamp, "TIMESTAMP" },
                { SqlDbType.TinyInt, "SMALLINT" },
                //{ SqlDbType.Udt, "?" },                                 
                { SqlDbType.UniqueIdentifier, "VARCHAR" }, // FOR BIT DATA" },
                { SqlDbType.VarBinary, "VARCHAR FOR BIT DATA" },
                { SqlDbType.VarChar, "VARCHAR" },
                { SqlDbType.Variant, "VARCHAR" },                         
                { SqlDbType.Xml, "XML" }                
            };
        private static readonly IDictionary<SqlDbType, GFXDType> SqlToGFXDMap
            = new Dictionary<SqlDbType, GFXDType>
            {
                { SqlDbType.BigInt, GFXDType.Long },
                { SqlDbType.Binary, GFXDType.Binary },
                { SqlDbType.Bit, GFXDType.Boolean },
                { SqlDbType.Char, GFXDType.Char },
                { SqlDbType.Date, GFXDType.Date },
                { SqlDbType.DateTime, GFXDType.TimeStamp },
                { SqlDbType.DateTime2, GFXDType.TimeStamp },                           
                { SqlDbType.DateTimeOffset, GFXDType.Other },                      
                { SqlDbType.Decimal, GFXDType.Decimal },
                { SqlDbType.Float, GFXDType.Float },
                { SqlDbType.Image, GFXDType.Blob },
                { SqlDbType.Int, GFXDType.Integer },
                { SqlDbType.Money, GFXDType.Decimal },
                { SqlDbType.NChar, GFXDType.VarChar },
                { SqlDbType.NText, GFXDType.VarChar },                           
                { SqlDbType.NVarChar, GFXDType.VarChar },
                { SqlDbType.Real, GFXDType.Real },
                { SqlDbType.SmallDateTime, GFXDType.TimeStamp },                 
                { SqlDbType.SmallInt, GFXDType.Short},
                { SqlDbType.SmallMoney, GFXDType.Decimal },
                { SqlDbType.Structured, GFXDType.Other },                          
                { SqlDbType.Text, GFXDType.VarChar },                            
                { SqlDbType.Time, GFXDType.Time },
                { SqlDbType.Timestamp, GFXDType.TimeStamp },
                { SqlDbType.TinyInt, GFXDType.Short },
                { SqlDbType.Udt, GFXDType.Other },                                 
                { SqlDbType.UniqueIdentifier, GFXDType.VarChar },
                { SqlDbType.VarBinary, GFXDType.VarBinary },
                { SqlDbType.VarChar, GFXDType.VarChar },
                { SqlDbType.Variant, GFXDType.VarChar },                         
                { SqlDbType.Xml, GFXDType.LongVarChar }                
            };

        private static readonly IDictionary<GFXDType, String> GFXDToJdbcMap
            = new Dictionary<GFXDType, String>
            {
                { GFXDType.Null, "NULL"},
                { GFXDType.Boolean, "SMALLINT"},
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

        public static String GetJdbcTypeFromGFXDType(GFXDType esqlType, int length)
        {
            String jdbcType = GFXDToJdbcMap[esqlType];
            String subType = jdbcType.Split(new char[] { ' ' })[0].ToUpper();

            if (length > 0)
            {
                if (subType == "DECIMAL")
                    jdbcType = jdbcType.Insert(subType.Length, String.Format("({0},2)", length + 2));
                else if (subType == "VARCHAR" || subType == "CHAR")
                    jdbcType = jdbcType.Insert(subType.Length, String.Format("({0})", length));
            }
            else if (length <= 0)
            {
                if (subType == "DECIMAL")
                    jdbcType = jdbcType.Insert(subType.Length, String.Format("({0},2)", 9));
                else if (subType == "VARCHAR")
                    jdbcType = jdbcType.Insert(0, String.Format("LONG ", length));
                else if (subType == "CHAR")
                    jdbcType = jdbcType.Insert(subType.Length, String.Format("({0}) ", 32));
            }

            return jdbcType;
        }

        public static GFXDType GetGFXDTypeFromSqlType(SqlDbType sqlType)
        {
            return SqlToGFXDMap[sqlType];
        }

        public static GFXDType GetGFXDTypeFromSqlType(string typeName)
        {
            if (typeName.ToLower() == "numeric")
                typeName = "Decimal";

            return SqlToGFXDMap[(SqlDbType)Enum.Parse(typeof(SqlDbType), typeName, true)];
        }

        public static String GetJdbcTypeFromSqlType(string typeName, int length)
        {
            if (typeName.ToLower() == "numeric")
                typeName = "Decimal";

            String jdbcType = SqlToJdbcMap[(SqlDbType)Enum.Parse(typeof(SqlDbType), typeName, true)];
            String subType = jdbcType.Split(new char[] { ' ' })[0].ToUpper();

            if (length > 0)
            {
                if (subType == "DECIMAL")
                    jdbcType = jdbcType.Insert(subType.Length, String.Format("({0},2)", length > 0 ? (length + 2) : 9));
                else if (subType == "VARCHAR" || subType == "CHAR")
                    jdbcType = jdbcType.Insert(subType.Length, String.Format("({0})", length));
            }
            else if (length <= 0)
                if (subType == "VARCHAR")
                    jdbcType = jdbcType.Insert(0, String.Format("LONG ", length));

            return jdbcType;
        }

        public static String GetJdbcTypeFromSqlType(string typeName)
        {
            if (typeName.ToLower() == "numeric")
                typeName = "Decimal";

            return SqlToJdbcMap[(SqlDbType)Enum.Parse(typeof(SqlDbType), typeName, true)];
        }
    }
}
