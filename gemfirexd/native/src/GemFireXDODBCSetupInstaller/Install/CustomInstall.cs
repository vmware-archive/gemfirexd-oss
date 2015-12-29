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
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Configuration;
using System.Windows.Forms;
using install;

namespace Install
{
  [RunInstaller(true)]
  public class CustomInstall : Installer
  {
    private const int ODBC_INSTALL_DRIVER = 1;
    private const int ODBC_REMOVE_DRIVER = 2;
    private const int ODBC_CONFIG_DRIVER = 3;

    [DllImport("gemfirexdodbcSetup.dll", CharSet = CharSet.Ansi, EntryPoint = "ConfigDriver", CallingConvention = CallingConvention.Winapi)]
    static extern Boolean ConfigDriver(IntPtr hwndParent,
                             UInt16 fRequest,
                             string lpszDriver,
                             string lpszArgs,
                             string lpszMsg,
                             UInt16 cbMsgMax,
                             ref long pcbMsgOut
      );


    public override void Install(System.Collections.IDictionary stateSaver)
    {
      base.Install(stateSaver);
      string path = this.Context.Parameters["targetdir"];

      long retVal = 0;
      bool result = false;
      IntPtr hwnd = new IntPtr(1);

      // install the driver using "gemfirexdodbc" as driver name
      result = ConfigDriver(hwnd, ODBC_INSTALL_DRIVER, DriverInfo.Name, path, "", 0, ref retVal);
      if (!result)
      {
        //MessageBox.Show("Driver installation failed");
        Rollback(stateSaver);
      }

      // These are default values associated with this driver
      result = ConfigDriver(hwnd, ODBC_CONFIG_DRIVER, DriverInfo.Name, DriverInfo.GetDefaultDriverInfo(), "", 0, ref retVal);
      if (!result)
      {
        //MessageBox.Show("Driver configuration failed");
        Rollback(stateSaver);
      }
    }

    public override void Uninstall(System.Collections.IDictionary stateSaver)
    {
      base.Uninstall(stateSaver);
      string path = this.Context.Parameters["targetdir"];
      // Do something with path.

      long retVal = 0;
      bool result = false;
      IntPtr hwnd = new IntPtr(1);

      result = ConfigDriver(hwnd, ODBC_REMOVE_DRIVER, DriverInfo.Name, path, "", 0, ref retVal);

      //if (result)
      //  MessageBox.Show("Result value is > 0");
    }

  }
}
