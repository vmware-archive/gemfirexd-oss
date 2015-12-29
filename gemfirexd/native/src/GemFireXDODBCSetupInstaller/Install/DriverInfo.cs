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

namespace install
{

  static class DriverInfo
  {
    public static readonly string Name = "GemFireXD Network Client 1.0";
    public static readonly string UID = string.Empty;
    public static readonly string PWD = string.Empty;
    public static readonly string SQLLevel = "1";  //Number that indicates the SQL-92 grammar that the driver supports
    public static readonly string Description = "GemFireXD Network Client Driver for Windows";
    public static readonly string APILevel = "2"; //ODBC interface conformance level that the driver supports
    public static readonly string Server = "127.0.0.1";
    public static readonly string Port = "1527";
    public static readonly string DriverODBCVersion = "03.51";
    public static readonly string CPTimeout = "0";


    public static string GetDefaultDriverInfo()
    {
      return string.Format("UID={0}|PWD={1}|SQLLevel={2}|Description={3}|APILevel={4}|SERVER={5}|PORT={6}|DriverODBCVer={7}|CPTimeout={8}",
        UID, PWD, SQLLevel, Description, APILevel, Server, Port, DriverODBCVersion, CPTimeout);
    }
  }
}
