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

import java.io.File;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Supports configuring network traffic between hosts.  All methods involve
 * synchronous remote calls and do not return until the network configuration
 * is complete.  Methods are synchronized in the hydra master so that all
 * network configuration requests are processed sequentially.
 * <p>
 * It is illegal to stop network traffic to or from the master host.  Tests
 * using this API must therefore use at least three hosts, including the
 * master host.
 * <p>
 * Hosts for which network traffic will be configured must be reserved for
 * exclusive use.  It is the responsibility of the user to ensure that all
 * connections have been restored when the reservation is complete.  The
 * script <code>netclean.sh</code> generated in the test result directory
 * can be used for this.  It is invoked automatically by batterytest after
 * the test completes, regardless of the test outcome.
 */
public class NetworkHelper {

  protected static final int DROP    = 0;
  protected static final int RESTORE = 1;
  protected static final int CLEAR   = 2;
  protected static final int SHOW    = 3;

  private static final int ONEWAY = 0;
  private static final int TWOWAY = 1;

  private static Map Drops;

  /**
   * Simulates a one-way dropped connection by stopping all network packets
   * from the source host to the target host.  Packets are stopped at the
   * source NIC.
   *
   * @throws HydraRuntimeException if the required network control script is
   * not available on the source host or there is an RMI exception.
   *
   * @throws IllegalArgumentException if an argument is null, or the master
   * host, or not in use by this test, or if the source and target are equal.
   *
   * @throws IllegalStateException if any part of the network is already in
   * the desired state.
   */
  public static synchronized void dropConnectionOneWay(String source,
                                                       String target) {
    checkArgumentsOnClient(source, target);
    Log.getLogWriter().info("Dropping network connection from " + source
      + " to " + target);
    configureConnection(source, target, DROP, ONEWAY);
    Log.getLogWriter().info("Dropped network connection from " + source
      + " to " + target);
  }

  /**
   * Simulates a two-way dropped connection by stopping all network packets
   * between the source host and the target host.  Outgoing packets between
   * the two hosts are stopped on the local NICs.
   * <p>
   * This is equivalent to invoking two mirror-image one-way calls, except
   * that the two-way call will drop both directions under synchronization.
   * It is possible for one direction to succeed and the other fail, resulting
   * in an exception.
   *
   * @throws HydraRuntimeException if the required network control script is
   * not available on the source host or there is an RMI exception.
   *
   * @throws IllegalArgumentException if an argument is null, or the master
   * host, or not in use by this test, or if the source and target are equal.
   *
   * @throws IllegalStateException if any part of the network is already in
   * the desired state.
   */
  public static synchronized void dropConnectionTwoWay(String source,
                                                       String target) {
    checkArgumentsOnClient(source, target);
    Log.getLogWriter().info("Dropping network connection from " + source
       + " to " + target + " and from " + target + " to " + source);
    configureConnection(source, target, DROP, TWOWAY);
    Log.getLogWriter().info("Dropped network connection from " + source
       + " to " + target + " and from " + target + " to " + source);
  }

  /**
   * Restores a previously dropped one-way connection by allowing all network
   * packets from the source host to the target host.
   *
   * @throws HydraRuntimeException if the required network control script is
   * not available on the source host or there is an RMI exception.
   *
   * @throws IllegalArgumentException if an argument is null, or the master
   * host, or not in use by this test, or if the source and target are equal.
   *
   * @throws IllegalStateException if any part of the network is already in
   * the desired state.
   */
  public static synchronized void restoreConnectionOneWay(String source,
                                                          String target) {
    checkArgumentsOnClient(source, target);
    Log.getLogWriter().info("Restoring network connection from " + source
      + " to " + target);
    configureConnection(source, target, RESTORE, ONEWAY);
    Log.getLogWriter().info("Restored network connection from " + source
      + " to " + target);
  }

  /**
   * Restores a previously dropped two-way connection by allowing all network
   * packets between the source host and the target host.
   * <p>
   * This is equivalent to invoking two mirror-image one-way calls, except
   * that the two-way call will restore both directions under synchronization.
   * It is possible for one direction to succeed and the other fail, resulting
   * in an exception.
   *
   * @throws HydraRuntimeException if the required network control script is
   * not available on the source host or there is an RMI exception.
   *
   * @throws IllegalArgumentException if an argument is null, or the master
   * host, or not in use by this test, or if the source and target are equal.
   *
   * @throws IllegalStateException if any part of the network is already in
   * the desired state.
   */
  public static synchronized void restoreConnectionTwoWay(String source,
                                                          String target) {
    checkArgumentsOnClient(source, target);
    Log.getLogWriter().info("Restoring network connection from " + source
       + " to " + target + " and from " + target + " to " + source);
    configureConnection(source, target, RESTORE, TWOWAY);
    Log.getLogWriter().info("Restored network connection from " + source
       + " to " + target + " and from " + target + " to " + source);
  }

  /**
   * Returns the current state of network connections, as viewed by hydra,
   * mapping each source to its list of dropped targets.
   *
   * @throws HydraRuntimeException if there is an RMI exception.
   */
  public static synchronized Map getConnectionState() {
    try {
      return RemoteTestModule.Master.getNetworkConnectionState();
    } catch (RemoteException e) {
      throw new HydraRuntimeException(e.getMessage(), e);
    } catch (RuntimeException e) {
      throw e;
    }
  }

  /**
   * Prints the current state of network connections, as viewed by hydra.
   */
  public static void printConnectionState() {
    Map drops = getConnectionState();
    StringBuffer buf = new StringBuffer();
    buf.append("Currently dropped network connections: ");
    if (drops.size() == 0) {
      buf.append("none");
    } else {
      for (Iterator i = drops.keySet().iterator(); i.hasNext();) {
        String source = (String)i.next();
        List targets = (List)drops.get(source);
        buf.append("\n" + source + " to " + targets);
      }
    }
    Log.getLogWriter().info(buf.toString());
  }

  /**
   * Shows the current state of network connections, as viewed by iptables,
   * for each host used in the test.
   *
   * @throws HydraRuntimeException if there is an RMI exception.
   */
  public static void showConnectionState() {
    try {
      RemoteTestModule.Master.showNetworkConnectionState();
    } catch (RemoteException e) {
      throw new HydraRuntimeException(e.getMessage(), e);
    } catch (RuntimeException e) {
      throw e;
    }
  }

  /**
   * Asks master to execute the request.
   */
  private static void configureConnection(String source, String target,
                                           int op, int way) {
    try {
      RemoteTestModule.Master.configureNetworkConnection(source, target,
                                                          op, way);
    } catch (RemoteException e) {
      throw new HydraRuntimeException(e.getMessage(), e);
    } catch (RuntimeException e) {
      throw e;
    }
  }

  /**
   * Runs on master, using ProcessMgr to execute the command on the source.
   */
  protected static synchronized void configure(String source, String target,
                                               int op, int way) {
    checkArgumentsOnMaster(source, target);

    checkState(source, target, op);
    if (way == TWOWAY) {
      checkState(target, source, op);
    }

    configure(source, target, op);
    if (way == TWOWAY) {
      configure(target, source, op);
    }
  }

  /** Runs on master. */
  protected static synchronized Map getState() {
    if (Drops == null) { // first access
      Drops = new HashMap();
      generateCleanupScript();
    }
    return Drops;
  }

  /** Runs on master. */
  protected static synchronized void showState() {
    StringBuffer buf = new StringBuffer();
    Vector hosts = TestConfig.getInstance().getPhysicalHostsIncludingHadoop();
    if (hosts == null) {
      buf.append("No hosts for which to show network connection state");
    } else {
      buf.append("Showing network connection state for ").append(hosts);
      for (Iterator i = hosts.iterator(); i.hasNext();) {
        String host = (String)i.next();
        String cmd = getNetcontrolCommand(host, null, SHOW);
        String result = ProcessMgr.fgexec(host, cmd, 300);
        buf.append("\n").append("HOST: ").append(host)
           .append("\n").append(result);
      }
    }
    Log.getLogWriter().info(buf.toString());
  }

  private static void configure(String source, String target, int op) {
    String msg = "network connection from " + source + " to " + target
               + ": " + opToString(op);
    Log.getLogWriter().info("Configuring " + msg);
    String cmd = getNetcontrolCommand(source, target, op);
    String result = ProcessMgr.fgexec(source, cmd, 300);
    if (result.indexOf("ERROR") == -1) {
      updateState(source, target, op);
      Log.getLogWriter().info(result);
      Log.getLogWriter().info("Configured " + msg);
    } else {
      Log.getLogWriter().severe(result);
      String s = "Error configuring " + msg;
      throw new HydraRuntimeException(s);
    }
  }

  /** Runs on master. */
  private static String getNetcontrolCommand(String source, String target,
                                             int op) {
    if (HostHelper.isLocalHost(source)) {
      return Platform.getInstance().getNetcontrolCommand(target, op);
    } else {
      try {
        return HostAgentMgr.getHostAgent(source).getNetcontrolCommand(target, op);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + source;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /** Runs on master. */
  private static void checkState(String source, String target, int op) {
    List targets = (List)getState().get(source);
    if (targets == null) {
      targets = new ArrayList();
      getState().put(source, targets);
    }
    if (op == DROP) {
      if (targets.contains(target)) {
        String s = "Network connection from " + source + " to " + target
                 + " is already dropped due to a previous \"drop\" request";
        throw new IllegalStateException(s);
      }
    } else if (op == RESTORE) {
      if (!targets.contains(target)) {
        String s = "Network connection from " + source + " to " + target
                 + " is not currently dropped so it cannot be restored";
        throw new IllegalStateException(s);
      }
    } else {
      String s = "Should not happen";
      throw new HydraInternalException(s);
    }
  }

  private static void updateState(String source, String target, int op) {
    List targets = (List)getState().get(source);
    if (op == DROP) {
      targets.add(target);
    } else if (op == RESTORE) {
      targets.remove(target);
      if (targets.size() == 0) {
        getState().remove(source);
      }
    } else {
      String s = "Should not happen";
      throw new HydraInternalException(s);
    }
  }

  /** Runs on master. */
  private static void generateCleanupScript() {
    Log.getLogWriter().info("Generating network connection cleanup script");
    Vector hosts = TestConfig.getInstance().getPhysicalHostsIncludingHadoop();
    if (hosts == null) {
      return;
    }

    StringBuffer buf = new StringBuffer();

    // add header
    buf.append(Platform.getInstance().getScriptHeader())
       .append(Platform.getInstance().getCommentPrefix())
       .append(" execute this script to restore all network connections")
       .append(" involved in this test\n\n");

    // add commands
    for (Iterator i = hosts.iterator(); i.hasNext();) {
      String host = (String)i.next();
      String cmd = getNetcontrolCommand(host, null, CLEAR);
      HostDescription hd = TestConfig.getInstance()
                                     .getAnyPhysicalHostDescription(host);
      buf.append(Nuker.getInstance().wrapCommand(cmd, hd) + "\n");
    }

    // script
    String script = System.getProperty("user.dir") + File.separator
                  + "netclean" + Platform.getInstance().getFileExtension();

    // write file
    FileUtil.appendToFile(script, buf.toString());
    Platform.getInstance().setExecutePermission(script);
    Log.getLogWriter().info("Generated network connection cleanup script: "
                           + script);
  }

  private static void checkArgumentsOnClient(String source, String target) {
    if (source == null) {
      throw new IllegalArgumentException("Source host cannot be null");
    }
    if (target == null) {
      throw new IllegalArgumentException("Target host cannot be null");
    }
  }

  private static void checkArgumentsOnMaster(String source, String target) {
    // make sure source and target are in use by this test
    Vector hosts = TestConfig.getInstance().getPhysicalHostsIncludingHadoop();
    if (!hosts.contains(source)) {
      String s = "Source host not in use by this test: " + source;
      Log.getLogWriter().warning(s);
      throw new IllegalArgumentException(s);
    }
    if (!hosts.contains(target)) {
      String s = "Target host not in use by this test: " + target;
      Log.getLogWriter().warning(s);
      throw new IllegalArgumentException(s);
    }

    // make sure source and target are not the master hsot
    String localhost = HostHelper.getLocalHost();
    if (source.equals(localhost)) {
      String s = "Source host is the master host: " + source;
      Log.getLogWriter().warning(s);
      throw new IllegalArgumentException(s);
    }
    if (target.equals(localhost)) {
      String s = "Target host is the master host: " + target;
      Log.getLogWriter().warning(s);
      throw new IllegalArgumentException(s);
    }

    // make sure source and target are not equal
    if (source.equals(target)) {
      String s = "Source host equals target host: " + source;
      Log.getLogWriter().warning(s);
      throw new IllegalArgumentException(s);
    }
  }

  private static String opToString(int op) {
    switch (op) {
      case DROP:
        return "DROP";
      case RESTORE:
        return "RESTORE";
      case CLEAR:
        return "CLEAR";
      case SHOW:
        return "SHOW";
      default:
        String s = "Unknown operation: " + op;
        throw new HydraInternalException(s);
    }
  }
}
