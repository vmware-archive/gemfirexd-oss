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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.admin.internal.InetAddressUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
public class AvailablePortJUnitTest extends TestCase {
  
  public void testIsPortAvailable() throws IOException {
    ServerSocket socket = new ServerSocket();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    socket.bind(new InetSocketAddress(InetAddressUtil.LOOPBACK,  port));
    try {
     Assert.assertFalse(AvailablePort.isPortAvailable(port,
          AvailablePort.SOCKET,
          InetAddress.getByName(InetAddressUtil.LOOPBACK_ADDRESS)));
      //Get local host will return the hostname for the server, so this should succeed, since we're bound to the loopback address only.
      Assert.assertTrue(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET, InetAddress.getLocalHost()));
      //This should test all interfaces.
      Assert.assertFalse(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET));
    } finally {
      socket.close();
    }
  }
  
  public void testWildcardAddressBound() throws IOException {
    String osName = System.getProperty("os.name");
    if(osName != null && osName.startsWith("Windows")) {
      //AvailablePort is useless on windows because it uses
      //setReuseAddr. Do nothing on windows. See bug #39368
      return;
    }
    ServerSocket socket = new ServerSocket();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    socket.bind(new InetSocketAddress((InetAddress)null, port));
    try {
      Assert.assertFalse(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET));
    } finally {
      socket.close();
    }
  }
}
