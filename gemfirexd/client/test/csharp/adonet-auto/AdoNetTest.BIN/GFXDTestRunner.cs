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
using System.Reflection;
using System.IO;
using System.Threading;
using AdoNetTest.BIN.Configuration;
using AdoNetTest.Logger;
using Pivotal.Data.GemFireXD;


namespace AdoNetTest.BIN
{
    /// <summary>
    /// Automates the test initialization, execution, and cleanup
    /// </summary>
    public class GFXDTestRunner
    {        
        /// <summary>
        /// Test event & delegate
        /// </summary>
        public delegate void TestEventHandler(TestEventArgs e);   
        public static event TestEventHandler TestEvent;

        /// <summary>
        /// Static member properties
        /// </summary>
        private static Logger.Logger logger;
        private object tLocker = new object();
        private static object lLocker = new object();
        public static int TestCount = 0;
        public static int ConnCount = 0;

        /// <summary>
        /// Instance members
        /// </summary>
        private ManualResetEvent resetEvent;
        private bool runContinuously;
        private bool interrupted;
        private string testname;

        public GFXDTestRunner()
        {
            this.resetEvent = new ManualResetEvent(false);
            this.runContinuously = false;
            this.interrupted = false;

            if (logger != null)
                logger.Close();

            //logger = new Logger.Logger(GFXDConfigManager.GetClientSetting("logDir"), 
            //    typeof(GFXDTestRunner).FullName + ".log");   

            logger = new Logger.Logger(Environment.GetEnvironmentVariable("GFXDADOOUTDIR"),
                  typeof(GFXDTestRunner).FullName + ".log");   
        }

        public GFXDTestRunner(bool runContinuously)
            : this()
        {
            this.runContinuously = runContinuously;
        }

        public GFXDTestRunner(string testname)
          : this()
        {
          this.testname = testname;
        }

        public void Interrupt()
        {
            interrupted = true;
        }

        public void SetConfigFile(String configXML)
        {
            GFXDConfigManager.SetConfigFile(configXML);
        }

        public static List<string> GetTestList()
        {
          List<string> testlist = new List<string>();
          try
          {            
            Assembly assembly = Assembly.GetExecutingAssembly();

            foreach (Type type in assembly.GetTypes())
            {
              if (type.IsSubclassOf(typeof(GFXDTest)))
              {
                testlist.Add(type.FullName);
              }
            }
          }
          catch (Exception e)
          {
            Log(DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString());
            OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty,
                "Encountered exception in GFXDTestRunner.GetTestList(). Check log for detail"));
          }
          return testlist;
        }

        public void RunOne()
        {
          try
          {
            Initialize();
            Assembly assembly = Assembly.GetExecutingAssembly();

            GFXDTest test = null;

            foreach (Type type in assembly.GetTypes())
            {
              if (type.IsSubclassOf(typeof(GFXDTest)) && type.FullName.Equals(testname))
              {
                test = (GFXDTest)Activator.CreateInstance(type, resetEvent);
              }
            }

            OnTestEvent(new TestEventArgs(test.GetType().FullName + " Queued"));
            ThreadPool.QueueUserWorkItem(test.Run, tLocker);

            lock (tLocker)
            {
              TestCount += 1;
            }

            Thread.Sleep(1000);
          }
          catch (Exception e)
          {
            Log(DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString());
            OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty,
                "Encountered exception in GFXDTestRunner.RunOne(). Check log for detail"));
          }
          finally
          {
            try
            {
              Cleanup();
            }
            catch (Exception e)
            {
              Log(DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString());
              OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty,
              "Encountered exception in GFXDTestRunner.Cleanup(). Check log for detail"));
            }
          }

          OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty, "Done"));
        }

        /// <summary>
        /// Entry point to automated test run
        /// </summary>
        public void Run()
        {
            try
            {
                Initialize();
                Assembly assembly = Assembly.GetExecutingAssembly();

                do
                {
                    foreach (Type type in assembly.GetTypes())
                    {
                        if (interrupted)
                        {
                            runContinuously = false;
                            break;
                        }
                        if (type.IsSubclassOf(typeof(GFXDTest)))
                        {
                            GFXDTest test = (GFXDTest)Activator.CreateInstance(type, resetEvent);
                            OnTestEvent(new TestEventArgs(test.GetType().FullName + " Queued"));
                            ThreadPool.QueueUserWorkItem(test.Run, tLocker);

                            lock (tLocker)
                            {
                                TestCount += 1;
                            }

                            Thread.Sleep(1000);
                        }
                    }

                    while (TestCount > 0)
                    {
                        resetEvent.WaitOne();
                    }

                    Thread.Sleep(1000);
                } while (runContinuously);                
            }
            catch (Exception e)
            {
                Log(DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString());
                OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty,
                    "Encounter exception in EsqlTestRunner.Run(). Check log for detail"));
            }
            finally
            {
                try
                {
                    Cleanup();
                }
                catch (Exception e)
                {
                    Log(DbHelper.GetExceptionDetails(e, new StringBuilder()).ToString());
                    OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty,
                    "Encounter exception in EsqlTestRunner.Cleanup(). Check log for detail"));
                }
            }

            OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty, "Done"));
        }       

        public static void OnTestEvent(TestEventArgs e)
        {
            if(TestEvent != null)
                TestEvent(e);

            Log(String.Format("{0}", e.ToString()));
        }

        private void Initialize()
        {
            OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty,
                    "Initializing... please wait"));

            DbHelper.Initialize();
        }

        private void Cleanup()
        {
            OnTestEvent(new TestEventArgs(string.Empty, string.Empty, string.Empty,
                    "Cleaning up... please wait"));

            DbHelper.Cleanup();                           
        }

        private static void Log(String msg)
        {
            lock(lLocker)
            {
                logger.Write(msg);
            }
        }
    }
}
