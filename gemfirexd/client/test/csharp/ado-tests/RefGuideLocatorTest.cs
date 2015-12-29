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
using System.Data.Common;
using System.IO;
using System.Threading;

using NUnit.Framework;

namespace Pivotal.Data.GemFireXD.Tests
{
  /// <summary>
  /// Examples given in the reference guide.
  /// </summary>
  [TestFixture]
  public class RefGuideLocatorTest : TestBase
  {
    private int m_locPort = -1;

    #region Setup/TearDown methods

    private StartPeerDelegate m_peerStart = new StartPeerDelegate(
        StartGFXDPeer);

    [TestFixtureSetUp]
    public override void FixtureSetup()
    {
      InternalSetup(GetType());
      SetDefaultDriverType();
      // not starting any servers/locators here rather it will be per test
    }

    [TestFixtureTearDown]
    public override void FixtureTearDown()
    {
      // not stopping any servers/locators here rather it will be per test
      com.pivotal.gemfirexd.@internal.shared.common.sanity.SanityManager
        .SET_DEBUG_STREAM(null);
      if (s_logFileWriter != null) {
        s_logFileWriter.close();
        s_logFileWriter = null;
      }
    }

    [SetUp]
    public override void SetUp()
    {
      // nothing since we do not create/drop any DB object in this test
    }

    [TearDown]
    public override void TearDown()
    {
      // bring down the servers and locator
      /*
      if (m_locPort <= 0 || s_clientPort <= 0) {
        throw new NotSupportedException("No locator is running");
      }
      */
      string serverDir2 = s_testOutDir + "/gfxdserver2";
      StopGFXDPeer(m_defaultDriverType, "server", serverDir2);
      string serverDir1 = s_testOutDir + "/gfxdserver1";
      StopGFXDPeer(m_defaultDriverType, "server", serverDir1);

      string locDir = s_testOutDir + "/gfxdlocator";
      StopGFXDPeer(m_defaultDriverType, "locator", locDir);
      s_clientPort = -1;
    }

    protected void SetupCommon(string authArgs)
    {
      m_locPort = GetAvailableTCPPort();
      // starting a locator and two servers
      initClientPort();
      // first locator
      string locDir = s_testOutDir + "/gfxdlocator";
      if (Directory.Exists(locDir)) {
        Directory.Delete(locDir, true);
      }
      Directory.CreateDirectory(locDir);
      StartGFXDPeer(m_defaultDriverType, "locator", locDir, string.Empty,
                    s_clientPort, " -peer-discovery-address=localhost" +
                    " -peer-discovery-port=" + m_locPort + authArgs);

      // then two servers
      IAsyncResult[] starts = new IAsyncResult[2];
      int clientPort = GetAvailableTCPPort();
      string serverDir = s_testOutDir + "/gfxdserver1";
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      starts[0] = m_peerStart.BeginInvoke(m_defaultDriverType, "server",
                                          serverDir, " -locators=localhost[" +
                                          m_locPort + ']', clientPort, authArgs,
                                          null, null);

      clientPort = GetAvailableTCPPort();
      serverDir = s_testOutDir + "/gfxdserver2";
      if (Directory.Exists(serverDir)) {
        Directory.Delete(serverDir, true);
      }
      Directory.CreateDirectory(serverDir);
      starts[1] = m_peerStart.BeginInvoke(m_defaultDriverType, "server",
                                          serverDir, " -locators=localhost[" +
                                          m_locPort + ']', clientPort, authArgs,
                                          null, null);

      // wait for servers to start
      foreach (IAsyncResult start in starts) {
        m_peerStart.EndInvoke(start);
      }
    }

    #endregion

    [Test]
    public void ConnectLocatorIncludingWithProperty()
    {
      // start the locator and servers without any authentication
      SetupCommon("");

      // Open a new connection to the network server running on localhost
      string host = "localhost";
      int port = s_clientPort;
      string connStr = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // check a query
        Assert.AreEqual(3, new GFXDCommand("select count(id) from sys.members",
                                           conn).ExecuteScalar());
        conn.Close();
      }

      // Open a new connection to the locator with load-balancing disabled
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        Dictionary<string, string> props = new Dictionary<string, string>();
        props.Add("load-balance", "false");
        conn.Open(props);

        // check a query
        Assert.AreEqual(3, new GFXDCommand("select count(id) from sys.members",
                                           conn).ExecuteScalar());
        conn.Close();
      }
    }

    [Test]
    public void ConnectLocatorWithAuth()
    {
      // start the locator and servers with authentication
      SetupCommon(" -auth-provider=BUILTIN -gemfirexd.user.gem1=gem1" +
                  " -user=gem1 -password=gem1");

      // first create a new user
      string host = "localhost";
      int port = s_clientPort;
      string user = "gem1";
      string passwd = "gem1";
      string connStr = string.Format("server={0}:{1};user={2};password={3}",
                                     host, port, user, passwd);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // create user
        new GFXDCommand("call sys.create_user('gemfirexd.user.gem2', 'gem2')",
                        conn).ExecuteNonQuery();
        conn.Close();
      }
      // Open a new connection to the locator having network server on localhost
      // with username and password in the connection string.
      user = "gem2";
      passwd = "gem2";
      connStr = string.Format("server={0}:{1};user={2};password={3}",
                              host, port, user, passwd);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr)) {
        conn.Open();

        // check a query
        Assert.AreEqual(3, new GFXDCommand("select count(id) from sys.members",
                                           conn).ExecuteScalar());
        conn.Close();
      }

      // Open a new connection to the locator having network server on localhost
      // with username and password passed as properties.
      string connStr2 = string.Format("server={0}:{1}", host, port);
      using (GFXDClientConnection conn = new GFXDClientConnection(connStr2)) {
        Dictionary<string, string> props = new Dictionary<string, string>();
        props.Add("user", user);
        props.Add("password", passwd);
        conn.Open(props);

        // check a query
        Assert.AreEqual(3, new GFXDCommand("select count(id) from sys.members",
                                           conn).ExecuteScalar());
        conn.Close();

      }
    }
  }
}
