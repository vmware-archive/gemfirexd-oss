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
using System.IO;

namespace GemFireXDDBI.Util
{
    /// <summary>
    /// Message loging
    /// </summary>
    public class Logger
    {
        private FileStream logFile;
        private StreamWriter logWriter;
        private bool isClosed;

        public Logger(string logDir, string fileName)
            : this(logDir, String.Empty, fileName)
        {
        }

        public Logger(string logDir, string testName, string fileName)
        {
            logFile = new FileStream(CreateLogDir(logDir + "\\Logs\\"
                + testName) + "\\" + fileName, FileMode.Append, FileAccess.Write);
            logWriter = new StreamWriter(logFile);
            logWriter.AutoFlush = true;
            isClosed = false;
        }

        private string CreateLogDir(string logDir)
        {
            DirectoryInfo dirInfo = new DirectoryInfo(logDir);

            if (!dirInfo.Exists)
                dirInfo = Directory.CreateDirectory(logDir);

            return dirInfo.FullName;
        }

        public void Write(string msg)
        {
            if (!isClosed && logWriter != null)
            {
                logWriter.WriteLine(String.Format("{0} {1} [{2,5}] {3}",
                    DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString(),
                    System.Threading.Thread.CurrentThread.ManagedThreadId, msg));

                logWriter.Flush();
            }
        }

        public void Close()
        {
            isClosed = true;
            logWriter.Flush();
            logWriter.Close();
            logFile.Close();
        }
    }
}
