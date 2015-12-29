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
package com.gemstone.gemfire.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.org.jgroups.util.GemFireTracer;

/**
 * Sent to a locator to request it to find (and possibly start)
 * a jmx manager for us. It returns a JmxManagerLocatorResponse.
 * 
 * @author darrel
 * @since 7.0
 *
 */
public class JmxManagerLocatorRequest implements DataSerializableFixedID {

  public JmxManagerLocatorRequest() {
    super();
  }
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }

  public void toData(DataOutput out) throws IOException {
  }

  public int getDSFID() {
    return DataSerializableFixedID.JMX_MANAGER_LOCATOR_REQUEST;
  }

  @Override
  public String toString() {
    return "JmxManagerLocatorRequest";
  }
  
  private static final JmxManagerLocatorRequest SINGLETON = new JmxManagerLocatorRequest();
  
  /**
   * Send a request to the specified locator asking it to find (and start if
   * needed) a jmx manager. A jmx manager will only be started
   * 
   * @param locatorHost
   *          the name of the host the locator is on
   * @param locatorPort
   *          the port the locator is listening on
   * @param msTimeout
   *          how long in milliseconds to wait for a response from the locator
   * @param sslConfigProps
   *          Map carrying SSL configuration that can be used by SocketCreator
   * @return a response object that describes the jmx manager.
   * @throws IOException
   *           if we can not connect to the locator, timeout waiting for a
   *           response, or have trouble communicating with it.
   */
  public static JmxManagerLocatorResponse send(String locatorHost, int locatorPort, int msTimeout, final Map<String, String> sslConfigProps) throws IOException {
    final Properties distConfProps = new Properties();

    InetAddress addr = InetAddress.getByName(locatorHost);
    try {
      // Changes for 46623
      // initialize the SocketCreator with props which may contain SSL config
      // empty distConfProps will reset SocketCreator
      if (sslConfigProps != null) {
        distConfProps.putAll(sslConfigProps);
      }

      GemFireTracer.getLog(JmxManagerLocator.class).setLogWriter(
          new LocalLogWriter(LogWriterImpl.NONE_LEVEL));
      GemFireTracer.getLog(JmxManagerLocator.class).setSecurityLogWriter(
          new LocalLogWriter(LogWriterImpl.NONE_LEVEL));
      SocketCreator.getDefaultInstance(distConfProps);

      Object responseFromServer = TcpClient.requestToServer(addr, locatorPort, SINGLETON, msTimeout);
      return (JmxManagerLocatorResponse)responseFromServer;
    } catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException(unexpected);
    } catch (ClassCastException unexpected) {
      // FIXME - Abhishek: object read is type "int" instead of
      // JmxManagerLocatorResponse when the Locator is using SSL & the request 
      // didn't use SSL -> this causes ClassCastException. Not sure how to make 
      // locator meaningful message
      throw new IllegalStateException(unexpected);
    } finally {
      distConfProps.clear();
    }
  }

  public static JmxManagerLocatorResponse send(String locatorHost, int locatorPort, int msTimeout) throws IOException {
    return send(locatorHost, locatorPort, msTimeout, null);
  }
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
