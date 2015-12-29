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
using System.Reflection;

using NUnit.Framework;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// Simple runner for junit test debugging etc. This just invokes the methods
  /// that have been hard-coded below without any regard to attributes etc. Use
  /// only for debugging tests in Visual Studio, for example.
  /// </summary>
  class Program
  {
    static void Main(string[] args)
    {
      Console.WriteLine("Current directory: " + Environment.CurrentDirectory);

      object testObject;
      int testNumber = 0;
      List<MethodInfo> fixtureSetup = new List<MethodInfo>();
      List<MethodInfo> fixtureTearDown = new List<MethodInfo>();
      List<MethodInfo> setup = new List<MethodInfo>();
      List<MethodInfo> tearDown = new List<MethodInfo>();
      Assembly assembly = Assembly.GetAssembly(typeof(BasicTests));
      List<MethodInfo> testMethods = new List<MethodInfo>();
      foreach (Type testType in assembly.GetTypes()) {
        object[] attrs = testType.GetCustomAttributes(
          typeof(TestFixtureAttribute), false);
        if (attrs != null && attrs.Length > 0) {
          testMethods.Clear();
          fixtureSetup.Clear();
          fixtureTearDown.Clear();
          setup.Clear();
          tearDown.Clear();
          foreach (MethodInfo method in testType.GetMethods()) {
            attrs = method.GetCustomAttributes(
              typeof(TestFixtureSetUpAttribute), true);
            if (attrs != null && attrs.Length > 0) {
              fixtureSetup.Add(method);
            }
            attrs = method.GetCustomAttributes(
              typeof(TestFixtureTearDownAttribute), true);
            if (attrs != null && attrs.Length > 0) {
              fixtureTearDown.Add(method);
            }
            attrs = method.GetCustomAttributes(
              typeof(SetUpAttribute), true);
            if (attrs != null && attrs.Length > 0) {
              setup.Add(method);
            }
            attrs = method.GetCustomAttributes(
              typeof(TearDownAttribute), true);
            if (attrs != null && attrs.Length > 0) {
              tearDown.Add(method);
            }
            attrs = method.GetCustomAttributes(typeof(TestAttribute), false);
            if (attrs != null && attrs.Length > 0) {
              testMethods.Add(method);
            }
          }
          if (testMethods.Count > 0) {
            testObject = Activator.CreateInstance(testType);
            foreach (MethodInfo method in fixtureSetup) {
              try {
                Console.WriteLine("TestFixtureSetup: " + method.DeclaringType +
                  '.' + method.Name + " for class: " + method.ReflectedType);
                method.Invoke(testObject, null);
              } catch (Exception ex) {
                Console.WriteLine("TestFixtureSetup: unexpected exception: " +
                  ex);
                throw;
              }
            }
            try {
              foreach (MethodInfo testMethod in testMethods) {
                foreach (MethodInfo method in setup) {
                  try {
                    Console.WriteLine("TestSetup: " + method.DeclaringType +
                      '.' + method.Name + " for class: " + method.ReflectedType);
                    method.Invoke(testObject, null);
                  } catch (Exception ex) {
                    Console.WriteLine("TestSetup: unexpected exception: " +
                      ex);
                    throw;
                  }
                }
                try {
                  Console.WriteLine("==========  Running test [ " + testType +
                                    '.' + testMethod.Name + "(" +
                                    (++testNumber) + ")] ============");
                  testMethod.Invoke(testObject, null);
                } catch (Exception ex) {
                  Console.WriteLine("Test: unexpected exception: " + ex);
                  throw;
                } finally {
                  foreach (MethodInfo method in tearDown) {
                    try {
                      Console.WriteLine("TearDown: " + method.DeclaringType +
                        '.' + method.Name + " for class: " +
                        method.ReflectedType);
                      method.Invoke(testObject, null);
                    } catch (Exception ex) {
                      Console.WriteLine("TearDown: unexpected exception: " +
                        ex);
                    }
                  }
                }
              }
            } finally {
              foreach (MethodInfo method in fixtureTearDown) {
                try {
                  Console.WriteLine("TestFixtureTearDown: " +
                    method.DeclaringType + '.' + method.Name + " for class: " +
                    method.ReflectedType);
                  method.Invoke(testObject, null);
                } catch (Exception ex) {
                  Console.WriteLine("TestFixtureTearDown: " +
                    "unexpected exception: " + ex);
                }
              }
            }
          }
        }
      }
      Console.WriteLine();
      Console.WriteLine("----------- Successfully ran " + testNumber +
                        " tests.");
    }

    /*
    static void Main(string[] args)
    {
      int result;
      using (DbConnection conn = new GFXDClientConnection(
               "server=localhost:1527")) {

        conn.Open();
        DbCommand cmd = conn.CreateCommand();

        // employee table and data
        cmd.CommandText = "create table employee (" +
          " id int PRIMARY KEY," +
          " first_name varchar (100) NOT NULL," +
          " middle_name varchar (100)," +
          " last_name varchar (100)," +
          " email varchar(200))";
        result = cmd.ExecuteNonQuery();

        cmd.CommandText = "insert into employee values " +
          "(1, 'Vijay', 'S', 'Mehra', 'vmehra@gmail.com')," +
          "(2, 'Shirish', NULL, 'Kumar', 'shirish@gmail.com')," +
          "(3, 'Venkatesh', 'T', 'Ramachandran', 'venkat@rediff.com')";
        result = cmd.ExecuteNonQuery();

        Console.WriteLine("Inserted " + result + " records in employee table");
        Console.WriteLine();

        // query data back displaying the results
        cmd.CommandText = "select * from employee";
        using (DbDataReader reader = cmd.ExecuteReader()) {
          while (reader.Read()) {
            Console.WriteLine(string.Format("Employee details -- Name:" +
              " {0} {1} {2}, ID: {3}, Email: {4}", reader.GetString(1),
              reader.GetString(2), reader.GetString(3), reader.GetInt32(0),
              reader.GetString(4)));
          }
        }

        conn.Close();

        Console.WriteLine();
        Console.WriteLine("Done.");
      }
    }
    */

  }
}
