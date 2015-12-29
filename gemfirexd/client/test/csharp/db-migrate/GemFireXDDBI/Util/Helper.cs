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
using Pivotal.Data.GemFireXD;

namespace GemFireXDDBI.Util
{
    /// <summary>
    /// Some helper methods
    /// </summary>
    class Helper
    {
        public static readonly string LogDir = Environment.CurrentDirectory;
        private static Logger logger = new Logger(LogDir, typeof(GemFireXDDBI).FullName + ".log");
        
        public static StringBuilder GetExceptionDetails(Exception e, StringBuilder sb)
        {
            sb.AppendFormat(". Message: {0}", e.Message);
            sb.AppendFormat(". Source:  {0}", e.Source);
            sb.AppendFormat(". TargetSite: {0}", e.TargetSite);
            sb.AppendFormat(". StackTrace: {0}", e.StackTrace);

            if (e is GFXDException)
            {
                GFXDException esqle = (GFXDException)e;
                if (esqle.NextException != null)
                {
                    sb.Append(". GFXDException->NextException: ");
                    GetExceptionDetails(esqle.NextException, sb);
                }
            }
            else if (e is DbException)
            {
                DbException dbe = (DbException)e;
                if (dbe.InnerException != null)
                {
                    sb.Append(". DbException->InnerException: ");
                    GetExceptionDetails(e.InnerException, sb);
                }
            }
            else if (e.InnerException != null)
            {
                sb.Append(". Exception->InnerException: ");
                GetExceptionDetails(e.InnerException, sb);
            }

            return sb;
        }

        public static void ParseDataRow(DataRow row)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.Table.Columns.Count; i++)
            {
                sb.Append(row[i].ToString() + ", ");
            }
            Log(String.Format("{0}", sb.ToString()));
        }

        public static void ParseDataTable(DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                ParseDataRow(row);
            }
        }

        public static void ParseDataSet(DataSet dataset)
        {
            foreach (DataTable table in dataset.Tables)
            {
                ParseDataTable(table);
            }
        }

        public static void Log(Exception e)
        {
            Log(GetExceptionDetails(e, new StringBuilder()).ToString());
        }

        public static void Log(String msg)
        {
            object obj = new object();
            lock (obj)
            {
                logger.Write(msg);
            }
        }

        public static byte[] ConvertToBytes(string txtString)
        {
            System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
            return encoding.GetBytes(txtString);
        }

        public static byte[] ConvertToBytes(object obj)
        {
            return (byte[])obj;
        }

        public static string ConvertToString(byte[] bytes)
        {
            System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
            return encoding.GetString(bytes);
        }
    }
}
