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

package com.pivotal.gemfirexd.thrift.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import com.gemstone.gemfire.internal.shared.SystemProperties;
import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TObjectProcedure;
import com.pivotal.gemfirexd.internal.client.net.NetConnection;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import com.pivotal.gemfirexd.thrift.GFXDException;
import com.pivotal.gemfirexd.thrift.HostAddress;
import com.pivotal.gemfirexd.thrift.LocatorService;
import com.pivotal.gemfirexd.thrift.ServerType;
import com.pivotal.gemfirexd.thrift.common.GfxdTSSLSocket;
import com.pivotal.gemfirexd.thrift.common.GfxdTSocket;
import com.pivotal.gemfirexd.thrift.common.SocketParameters;
import com.pivotal.gemfirexd.thrift.common.ThriftExceptionUtil;
import com.pivotal.gemfirexd.thrift.common.ThriftUtils;

/**
 * Holds locator, server information to use for failover. Also provides
 * convenience methods to actually search for an appropriate host for failover.
 * <p>
 * One distributed system is supposed to have one ControlConnection.
 * 
 * @author swale
 * @since gfxd 1.1
 */
final class ControlConnection {
  private final SocketParameters socketParams;
  private final Set<ServerType> gfxdServerTypeSet;
  // TODO: SW: the initial set of locators created from initial connection
  // into this DS need not be static; we should refresh/adjust if later
  // connections specify different combination of locators
  private final List<HostAddress> locators;
  private HostAddress controlHost;
  private LocatorService.Client controlLocator;
  private final ArrayList<HostAddress> controlHosts;
  private final THashSet controlHostSet;

  public static final class SearchRandomServer implements TObjectProcedure {

    private final ServerType searchServerType;
    private final Set<HostAddress> failedServers;
    private HostAddress foundServer;
    private int searchIndex;

    public static final Random rand = new Random();

    SearchRandomServer(ServerType searchServerType, int setSize,
        Set<HostAddress> failedServers) {
      this.searchServerType = searchServerType;
      this.failedServers = failedServers != null ? failedServers : Collections
          .<HostAddress> emptySet();
      this.searchIndex = rand.nextInt(setSize);
    }

    public HostAddress getRandomServer() {
      return this.foundServer;
    }

    @Override
    public boolean execute(Object o) {
      if (this.searchIndex > 0) {
        this.searchIndex--;
      }
      else if (this.foundServer != null) {
        return false;
      }

      HostAddress hostAddr = (HostAddress)o;
      if (hostAddr.getServerType() == this.searchServerType
          && !this.failedServers.contains(hostAddr)) {
        this.foundServer = hostAddr;
      }
      return true;
    }
  }

  /**
   * Since one DS is supposed to have one ControlConnection, so we expect the
   * total size of this static global list to be small.
   */
  private static final ArrayList<ControlConnection> allControlConnections =
      new ArrayList<ControlConnection>(2);

  ControlConnection(ClientService service) {
    this.socketParams = service.socketParams;
    this.gfxdServerTypeSet = Collections.singleton(getServerType());
    this.locators = service.connHosts;
    this.controlHosts = new ArrayList<HostAddress>(service.connHosts);
    this.controlHostSet = new THashSet(service.connHosts);
  }

  final ServerType getServerType() {
    return this.socketParams.getServerType();
  }

  static ControlConnection getOrCreateControlConnection(HostAddress hostAddr,
      ClientService service) throws GFXDException {
    // loop through all ControlConnections since size of this global list is
    // expected to be in single digit (total number of distributed systems)
    synchronized (allControlConnections) {
      int index = allControlConnections.size();
      while (--index >= 0) {
        ControlConnection controlService = allControlConnections.get(index);
        synchronized (controlService) {
          if (controlService.controlHostSet.contains(hostAddr)) {
            // we expect the serverType to match
            if (controlService.getServerType() == service.getServerType()) {
              return controlService;
            }
            else {
              throw ThriftExceptionUtil.newGFXDException(
                  SQLState.DRDA_CONNECTION_TERMINATED, null,
                  hostAddr != null ? hostAddr.toString() : null,
                  "found server " + hostAddr
                      + " as registered but having different type "
                      + controlService.getServerType() + " than connection "
                      + service.getServerType());
            }
          }
        }
      }
      // if we reached here, then need to create a new ControlConnection
      ControlConnection controlService = new ControlConnection(service);
      controlService.getPreferredServer(null, null, true);
      allControlConnections.add(controlService);
      return controlService;
    }
  }

  private HostAddress getLocatorPreferredServer(
      Set<HostAddress> failedServers, Set<String> serverGroups)
      throws GFXDException, TException {
    if (SanityManager.TraceClientHA) {
      SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
          "getPreferredServer() trying using host " + this.controlHost);
    }
    if (SanityManager.TraceClientStatement) {
      final long ns = System.nanoTime();
      SanityManager.DEBUG_PRINT_COMPACT("getPreferredServer_S", null, 0, ns,
          true, null);
    }

    HostAddress preferredServer = controlLocator.getPreferredServer(
        this.gfxdServerTypeSet, serverGroups, failedServers);

    if (SanityManager.TraceClientStatement) {
      final long ns = System.nanoTime();
      SanityManager.DEBUG_PRINT_COMPACT("getPreferredServer_E", null, 0, ns,
          false, null);
    }
    return preferredServer;
  }

  synchronized HostAddress getPreferredServer(Set<HostAddress> failedServers,
      Set<String> serverGroups, boolean forFailover) throws GFXDException {

    if (controlLocator == null) {
      failedServers = failoverToAvailableHost(failedServers, null);
      forFailover = true;
    }

    while (true) {
      try {
        HostAddress preferredServer;
        if (forFailover) {
          if (SanityManager.TraceClientHA) {
            SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
                "getAllServersWithPreferredServer() trying using host "
                    + this.controlHost);
          }
          if (SanityManager.TraceClientStatement) {
            final long ns = System.nanoTime();
            SanityManager.DEBUG_PRINT_COMPACT(
                "getAllServersWithPreferredServer_S", null, 0, ns, true, null);
          }

          // refresh the full host list
          List<HostAddress> prefServerAndAllHosts = controlLocator
              .getAllServersWithPreferredServer(this.gfxdServerTypeSet,
                  serverGroups, failedServers);
          // refresh the new server list
          List<HostAddress> allHosts = prefServerAndAllHosts.subList(1,
              prefServerAndAllHosts.size());
          refreshAllHosts(allHosts);
          preferredServer = prefServerAndAllHosts.get(0);

          if (SanityManager.TraceClientHA) {
            SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
                "Returning all hosts " + allHosts
                    + " using control connection to " + this.controlHost);
          }
          if (SanityManager.TraceClientStatement) {
            final long ns = System.nanoTime();
            SanityManager.DEBUG_PRINT_COMPACT(
                "getAllServersWithPreferredServer_E", null, 0, ns, false,
                null);
          }
        }
        else {
          preferredServer = getLocatorPreferredServer(failedServers,
              serverGroups);
        }

        if (SanityManager.TraceClientHA) {
          SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
              "Got preferred server " + preferredServer
                  + " using control connection to " + this.controlHost
                  + (preferredServer.getPort() <= 0 ? "" : " (trying random "
                      + "server since no preferred server received)"));
        }
        if (preferredServer.getPort() <= 0) {
          // for this case we don't have a locator so choose some server
          // randomly as the "preferredServer"
          SearchRandomServer search = new SearchRandomServer(getServerType(),
              this.controlHostSet.size(), failedServers);
          this.controlHostSet.forEach(search);
          if ((preferredServer = search.getRandomServer()) == null) {
            // try with no failedServers list before failing
            preferredServer = getLocatorPreferredServer(null, serverGroups);
            if (preferredServer.getPort() <= 0) {
              throw failoverExhausted(failedServers, null);
            }
          }
        }
        if (SanityManager.TraceClientHA) {
          SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
              "Returning preferred server " + preferredServer
                  + " with current control connection to " + this.controlHost);
        }
        return preferredServer;
      } catch (TException te) {
        if (SanityManager.TraceClientHA) {
          SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
              "getPreferredServer() received exception from control host "
                  + this.controlHost, te);
        }
        if (te instanceof GFXDException) {
          GFXDException gfxde = (GFXDException)te;
          NetConnection.FailoverStatus status;
          if ((status = NetConnection.getFailoverStatus(gfxde
              .getExceptionData().getSqlState(), gfxde.getExceptionData()
              .getSeverity(), gfxde)).isNone()) {
            throw gfxde;
          }
          else if (status == NetConnection.FailoverStatus.RETRY) {
            forFailover = true;
            continue;
          }
        }
        // search for a new host for locator query
        if (failedServers == null) {
          @SuppressWarnings("unchecked")
          Set<HostAddress> servers = new THashSet(2);
          failedServers = servers;
        }
        failedServers.add(controlHost);
        controlLocator.getOutputProtocol().getTransport().close();
        failedServers = failoverToAvailableHost(failedServers, te);
      } catch (Throwable t) {
        throw unexpectedError(t, controlHost);
      }
      forFailover = true;
    }
  }

  private synchronized Set<HostAddress> failoverToAvailableHost(
      Set<HostAddress> failedServers, Exception failure) throws GFXDException {

    final SystemProperties sysProps = SystemProperties.getClientInstance();
    NEXT_SERVER: for (HostAddress controlAddr : this.controlHosts) {
      /* (try all including those previously reported as failed)
      if (failedServers != null && failedServers.contains(controlAddr)) {
        continue;
      }
      */
      this.controlHost = null;
      this.controlLocator = null;

      TTransport inTransport = null;
      TTransport outTransport = null;
      TProtocol inProtocol = null;
      TProtocol outProtocol = null;
      while (true) {
        try {
          if (outTransport != null) {
            outTransport.close();
          }

          if (SanityManager.TraceClientHA) {
            SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
                "Trying control connection to host " + controlAddr);
          }
          if (SanityManager.TraceClientStatement
              | SanityManager.TraceClientConn) {
            final long ns = System.nanoTime();
            SanityManager.DEBUG_PRINT_COMPACT(
                "failoverToAvailableHost_S", null, 0, ns, true,
                SanityManager.TraceClientConn ? new Throwable() : null);
          }

          if (getServerType().isThriftSSL()) {
            inTransport = outTransport = new GfxdTSSLSocket(controlAddr,
                this.socketParams, sysProps);
          }
          else {
            inTransport = outTransport = new GfxdTSocket(controlAddr, true,
                ThriftUtils.isThriftSelectorServer(), this.socketParams,
                sysProps);
          }
          if (getServerType().isThriftBinaryProtocol()) {
            inProtocol = new TBinaryProtocol(inTransport);
            outProtocol = new TBinaryProtocol(outTransport);
          }
          else {
            inProtocol = new TCompactProtocol(inTransport);
            outProtocol = new TCompactProtocol(outTransport);
          }
          break;
        } catch (TException te) {
          if (SanityManager.TraceClientHA) {
            SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
                "Received exception in control connection to host "
                    + controlAddr, te);
          }
          failure = te;
          // search for a new host for locator query
          if (failedServers == null) {
            @SuppressWarnings("unchecked")
            Set<HostAddress> servers = new THashSet(2);
            failedServers = servers;
          }
          failedServers.add(controlAddr);
          if (outTransport != null) {
            outTransport.close();
          }
          continue NEXT_SERVER;
        } catch (Throwable t) {
          throw unexpectedError(t, controlAddr);
        }
      }
      this.controlHost = controlAddr;
      this.controlLocator = new LocatorService.Client(inProtocol, outProtocol);

      if (SanityManager.TraceClientHA) {
        SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_HA,
            "Established control connection to host " + controlAddr);
      }
      if (SanityManager.TraceClientStatement
          | SanityManager.TraceClientConn) {
        final long ns = System.nanoTime();
        SanityManager.DEBUG_PRINT_COMPACT(
            "failoverToAvailableHost_E", null, 0, ns, false,
            SanityManager.TraceClientConn ? new Throwable() : null);
      }
      return failedServers;
    }
    throw failoverExhausted(failedServers, failure);
  }

  private void refreshAllHosts(List<HostAddress> allHosts) {
    // refresh the new server list

    // we remove all from the set and re-populate since we would like
    // to prefer the ones coming as "allServers" with "isServer" flag
    // correctly set rather than the ones in "secondary-locators"
    this.controlHostSet.clear();
    this.controlHosts.subList(this.locators.size(), this.controlHosts.size())
        .clear();

    this.controlHostSet.addAll(allHosts);
    this.controlHostSet.addAll(this.controlHosts);

    this.controlHosts.addAll(allHosts);
  }

  private GFXDException unexpectedError(Throwable t, HostAddress host) {
    this.controlHost = null;
    if (this.controlLocator != null) {
      this.controlLocator.getOutputProtocol().getTransport().close();
      this.controlLocator = null;
    }
    return ThriftExceptionUtil.newGFXDException(SQLState.JAVA_EXCEPTION, t,
        host != null ? host.toString() : null, t.getClass(), t.getMessage());
  }

  private GFXDException failoverExhausted(Set<HostAddress> failedServers,
      Throwable cause) {
    return ThriftExceptionUtil.newGFXDException(SQLState.DATA_CONTAINER_CLOSED,
        cause, failedServers != null ? failedServers.toString() : null,
        this.locators.get(0), " {failed after trying all available servers: "
            + controlHostSet + (this.locators.size() > 1
                ? ", secondary-locators=" + this.locators.subList(
                    1, this.locators.size()) : "")
            + (cause instanceof TException ? " with: "
                + ThriftExceptionUtil.getExceptionString(cause) + '}' : "}"));
  }
}
