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
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;
using System.Diagnostics;


namespace ado_net_connection_pooling_tests
{
  class Program
  {
    static void Main(string[] args)
    {
      string[] query = {"INSERT INTO SYED.IBM(IBMINFO) VALUES ('OUT OF THE WHOLE WORLD')",
                        "INSERT INTO SYED.USERS(NAME) VALUES ('OUT OF THE WHOLE WORLD')",
                        "INSERT INTO SYED.VENDORS(NAME) VALUES ('OUT OF THE WHOLE WORLD')",
                        "INSERT INTO SYED.SPONSORS(NAME) VALUES ('OUT OF THE WHOLE WORLD')"}
      ;

      int appDomainCount = 4;
      int threadCount = 10;



      try
      {
        var domains = new AppDomain[appDomainCount];
        var threads = new Thread[threadCount];

        DateTime startTime = DateTime.Now;

        for (int i = 0; i < appDomainCount; i++)
        {
          domains[i] = AppDomain.CreateDomain("myDomain_" + i.ToString());

          for (int j = 0; j < threadCount; j++)
          {
            domains[i].SetData("MyMessage", query[i]);
            threads[j] = new Thread(ThreadProc);
            threads[j].Start(domains[i]);
          }
        }

        for (int i = 0; i < threadCount; i++) threads[i].Join();


        DateTime endTime = DateTime.Now;
        TimeSpan diff = endTime.Subtract(startTime);
        Console.WriteLine("Total time to execute with Connection Pooling = {0}", diff);

        //connection = new GFXDClientConnection(connectionStr);
        //connection.IsPooling = true;
        //connection.Open();
        //GFXDCommand command = connection.CreateCommand();
        //command.CommandType = CommandType.Text;



        //for (int i = 0; i < 100000; i++)
        //{
        //  sql = string.Format("INSERT INTO APP.GROCERIES(PRODUCT) VALUES ('BANANA_DRINK_{0}')", i);
        //  command.CommandText = sql;
        //  command.ExecuteNonQuery();
        //}


        //connection.ConnectionAttributes = "bootPassword=x983xkADkdf;upgrade=true";
        //connection.Open();
        //GFXDDataReader reader = command.ExecuteReader();
        //StringBuilder row = new StringBuilder();
        //while (reader.Read())
        //{
        //  for (int i = 0; i < reader.FieldCount; i++)
        //    row.AppendFormat("{0}, ", reader.GetString(i));
        //  Console.WriteLine(row.ToString());
        //}

        Console.ReadLine();
      }
      catch (Exception e)
      {
        Console.WriteLine(e.Data);
      }
      //finally
      //{
      //  connection.Close();
      //}
    }

    [LoaderOptimization(LoaderOptimization.MultiDomainHost)]
    private static void ThreadProc(object state)
    {
      var domain = (AppDomain)state;
      domain.DoCallBack(Login);
    }

    private static void Login()
    {
      var message = (string)AppDomain.CurrentDomain.GetData("MyMessage");

      var da = new DataAccess();
      var ds = da.ExecuteQuery(message);
    }
  }

  public class DataAccess
  {

    string connectionStr = string.Format(@"server={0}:{1}", "localhost", 1551);


    public DataAccess()
    {
      // load the connection string from the configuration files 
    }

    public DataSet ExecuteQuery(string query)
    {

      // comment this line if only non-connection pooling test desired
      connectionStr += ";userID=syed;password=syed;MinimumPoolSize=1;MaximumPoolSize=5;ConnectionLifetime=60;Pooling=true";


      var dt = new DataSet();

      try
      {
        var conn = new GFXDClientConnection(connectionStr);
        for (int i = 0; i < 1000; i++)
        {
          conn.Open();
          GFXDCommand cmd = conn.CreateCommand();
          //cmd.CommandType = CommandType.Text;

          cmd.CommandText = query;



          //for (int i = 0; i < 1; i++)
          //{
          cmd.ExecuteNonQuery();
          //}


          //var da = new GFXDDataAdapter(cmd);
          //da.Fill(dt);

          conn.Close();
        }
      }
      catch (GFXDException ex)
      {
        string err = ex.Message;
      }
      catch (Exception ex)
      {
        string err = ex.Message;
      }

      return dt;
    }
  }

}
