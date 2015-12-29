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
using Microsoft.Deployment.WindowsInstaller;
using System.Runtime.InteropServices;

namespace CustomActionOdbcDriver
{
    static class DriverInfo
    {
        public static readonly string DRIVERDLL = "gemfirexdodbc.dll";
        public static readonly string SETUPDLL = "gemfirexdodbcSetup.dll";
        public static readonly string Name = "Pivotal GemFire XD ODBC Driver";
        public static readonly string UID = string.Empty;
        public static readonly string PWD = string.Empty;
        public static readonly string SQLLevel = "1";  //Number that indicates the SQL-92 grammar that the driver supports
        public static readonly string Description = "Pivotal GemFire XD ODBC Driver for Windows";
        public static readonly string APILevel = "2"; //ODBC interface conformance level that the driver supports
        public static readonly string Server = "127.0.0.1";
        public static readonly string Port = "1527";
        public static readonly string DriverODBCVersion = "03.51";
        public static readonly string CPTimeout = "0";
        public static readonly string ConnectFunctions = "YYN";
        public static UInt16 ODBC_INSTALL_COMPLETE = 2;
    }

    public class CustomActions
    {
        [DllImport("odbccp32.dll", CharSet = CharSet.Ansi, EntryPoint = "SQLRemoveDriver", CallingConvention = CallingConvention.Winapi)]
        static extern Boolean SQLRemoveDriver(
                                 string lpszDriver,
                                 bool fRemoveDSN,
                                 ref long pcbMsgOut
          );

        [DllImport("odbccp32.dll", CharSet = CharSet.Ansi, EntryPoint = "SQLInstallDriverEx", CallingConvention = CallingConvention.Winapi)]
        static extern Boolean SQLInstallDriverEx(
                                 string lpszDriver,
                                 string lpszPathIn,
                                 string lpszPathOut,
                                 UInt16 cbMsgMax,
                                 ref long pcbMsgOut,
                                 UInt16 fRequest,
                                 ref long usageCount
          );

        [CustomAction]
        public static ActionResult Install(Session session)
        {
            session.Log("Begin CustomAction Install");
            string path = session.CustomActionData["location"];

            string name = DriverInfo.Name + "\0";
            string DriverLL = "Driver=" + DriverInfo.DRIVERDLL + "\0";
            string SetupDLL = "Setup=" + DriverInfo.SETUPDLL + "\0";
            string Description = "Description=" + DriverInfo.Description + "\0";
            string SQLLevel = "SQLLevel=" + DriverInfo.SQLLevel + "\0";
            string DriverODBCVer = "DriverODBCVer=" + DriverInfo.DriverODBCVersion + "\0";
            string ConnectFunctions = "ConnectFunctions=" + DriverInfo.ConnectFunctions + "\0";
            string APILevel = "APILevel=" + DriverInfo.APILevel + "\0";
            string CPTimeout = "CPTimeout=" + DriverInfo.CPTimeout + "\0";
            string Server = "Server=" + DriverInfo.Server + "\0";
            string Port = "Port=" + DriverInfo.Port + "\0";
            string UID = "UID=" + DriverInfo.UID + "\0";
            string PWD = "PWD=" + DriverInfo.PWD + "\0";

            string output = name + DriverLL + SetupDLL + Description + SQLLevel + DriverODBCVer + ConnectFunctions + APILevel + CPTimeout + Server + Port + UID + PWD;
            //Console.WriteLine(output);

            long retVal = 0;
            long retVal1 = 0;
            bool result = false;

            result = SQLInstallDriverEx(output, path, "", 255, ref retVal1, DriverInfo.ODBC_INSTALL_COMPLETE, ref retVal);
            session.Log("End CustomAction Install");
            return ActionResult.Success;
        }

        [CustomAction]
        public static ActionResult UnInstall(Session session)
        {
            session.Log("Begin CustomAction UnInstall");
            long retVal = 0;
            bool removeDSN = false;
            bool result = false;
            result = SQLRemoveDriver(DriverInfo.Name, removeDSN, ref retVal);
            session.Log("End CustomAction UnInstall");
            return ActionResult.Success;
        }
    }
}
