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

package com.pivotal.gemfirexd.thrift.server;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import org.apache.thrift.transport.TTransportException;

import com.pivotal.gemfirexd.thrift.common.GfxdTSSLSocketFactory;
import com.pivotal.gemfirexd.thrift.common.SocketParameters;

/**
 * A Factory for providing and setting up server SSL wrapped GfxdTServerSocket.
 * <p>
 * Modified from <code>TSSLTransportFactory</code> to add GFXD specific config.
 */
public abstract class GfxdTSSLServerSocketFactory {

  private GfxdTSSLServerSocketFactory() {
    // no instance
  }

  /**
   * Get a configured SSL wrapped TServerSocket bound to the specified port and
   * interface.
   * <p>
   * If SocketParameters have SSL properties set, then they are used to set the
   * values for the algorithms, keystore, truststore and other settings.
   * <p>
   * Else if SocketParameters don't have SSL settings, then the default settings
   * are used. Default settings are retrieved from server System properties.
   * 
   * Example system properties: -Djavax.net.ssl.trustStore=<truststore location>
   * -Djavax.net.ssl.trustStorePassword=password
   * -Djavax.net.ssl.keyStore=<keystore location>
   * -Djavax.net.ssl.keyStorePassword=password
   * 
   * 
   * @return An SSL wrapped {@link GfxdTSSLServerSocket}
   */
  public static GfxdTSSLServerSocket getServerSocket(
      InetSocketAddress bindAddress, SocketParameters params)
      throws TTransportException {
    if (params.hasSSLParams()) {
      if (!params.isSSLKeyStoreSet() && !params.isSSLTrustStoreSet()) {
        throw new TTransportException(
            "Either one of the KeyStore or TrustStore must be set "
                + "for SocketParameters having SSL parameters");
      }

      SSLContext ctx = GfxdTSSLSocketFactory.createSSLContext(params);
      return createServer(ctx.getServerSocketFactory(), bindAddress, params);
    }
    else {
      SSLServerSocketFactory factory = (SSLServerSocketFactory)SSLServerSocketFactory
          .getDefault();
      return createServer(factory, bindAddress, params);
    }
  }

  private static GfxdTSSLServerSocket createServer(
      SSLServerSocketFactory factory, InetSocketAddress bindAddress,
      SocketParameters params) throws TTransportException {
    try {
      SSLServerSocket serverSocket = (SSLServerSocket)factory
          .createServerSocket(bindAddress.getPort(), 100,
              bindAddress.getAddress());
      if (params != null) {
        if (params.getSSLEnabledProtocols() != null) {
          serverSocket.setEnabledProtocols(params.getSSLEnabledProtocols());
        }
        if (params.getSSLCipherSuites() != null) {
          serverSocket.setEnabledCipherSuites(params.getSSLCipherSuites());
        }
        serverSocket.setNeedClientAuth(params.getSSLClientAuth());
      }
      return new GfxdTSSLServerSocket(serverSocket, bindAddress, params);
    } catch (Exception e) {
      throw new TTransportException(TTransportException.NOT_OPEN,
          "Could not bind to host:port " + bindAddress.toString(), e);
    }
  }
}
