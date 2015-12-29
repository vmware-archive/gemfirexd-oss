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

package hydra;

import com.gemstone.gemfire.internal.AvailablePort;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides support to hydra client for selecting ports.
 */
public class PortHelper {

  public static final String ADDRESS_ALREADY_IN_USE = "Address already in use";
  public static final String UNRECOGNIZED_SOCKET = "Unrecognized Windows Sockets error: 0: JVM_Bind";
  public static final long MAX_RETRY_MS = 120000; // two minutes
  public static final long THROTTLE_MS = 1000; // 1 second

  private static Map<String,List<Integer>> ReservedPorts = new HashMap();

  /**
   * Selects a random port not already in use on the local host.
   */
  public static int getRandomPort() {
    String host = HostHelper.getLocalHost();
    InetAddress addr = HostHelper.getIPAddress();
    int port = -1;
    boolean reserved = false;
    while (!reserved) {
      port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET, addr);
      if (RemoteTestModule.Master == null) {
        reserved = reservePort(host, port);
      } else {
        try {
          reserved = RemoteTestModule.Master.reservePort(host, port);
        } catch (RemoteException e) {
          String s = "Unable to contact master to reserve port";
          throw new HydraRuntimeException(s, e);
        }
      }
    }
    return port;
  }

  public static synchronized boolean reservePort(String host, int port) {
    List<Integer> ports = ReservedPorts.get(host);
    if (ports == null) {
      ports = new ArrayList<Integer>();
      ReservedPorts.put(host, ports);
    }
    if (ports.contains(port)) {
      return false;
    } else {
      ports.add(port);
      return true;
    }
  }

  /**
   * Answers whether the caller should retry the operation that threw
   * the given IOException. This is true if the exception satisfies
   * {@link #addressAlreadyInUse(Throwable)} and the retry limit of
   * {@link #MAX_RETRY_MS} ms has not been exceeded.
   * <p>
   * The method first logs network statistics to help diagnose the exception,
   * regardless of its cause. It also throttles for {@link #THROTTLE_MS} ms
   * before returning <code>true</code>.
   */
  public static boolean retrySocketBind(IOException e, long startTimeMs) {
    ProcessMgr.logNetworkStatistics();
    if (System.currentTimeMillis() - startTimeMs < MAX_RETRY_MS
        && addressAlreadyInUse(e)) {
      try {
        Thread.sleep(THROTTLE_MS);
        Log.getLogWriter().warning("Allowing retry after IOException");
        return true;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  /**
   * Returns true if a cause of the given (non-null) exception is a
   * BindException with message containing {@link #ADDRESS_ALREADY_IN_USE}.
   * or a SocketException with message containing {@link #UNRECOGNIZED_SOCKET}.
   */
  public static boolean addressAlreadyInUse(Throwable t) {
    if (t == null) {
      throw new IllegalArgumentException("Exception cannot be null");
    }
    Throwable root = t;
    while (root != null) {
      if (root instanceof BindException &&
          root.getMessage() != null &&
          root.getMessage().contains(ADDRESS_ALREADY_IN_USE)) {
        Log.getLogWriter().warning("Got BindException: " + ADDRESS_ALREADY_IN_USE);
        return true;
      }
      if (root instanceof SocketException &&
          root.getMessage() != null &&
          root.getMessage().contains(UNRECOGNIZED_SOCKET)) {
        Log.getLogWriter().warning("Got SocketException: " + UNRECOGNIZED_SOCKET);
        return true;
      }
      root = root.getCause();
    }
    return false;
  }
}
