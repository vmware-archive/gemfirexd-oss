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

package com.pivotal.gemfirexd;

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricAgentImpl;
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricLocatorImpl;
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServerImpl;
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl;

/**
 * A factory class for returning singleton instance of either a
 * <code>{@link FabricServer}</code> or <code>{@link FabricLocator}</code>. A
 * node can be booted as either a {@link FabricServer} or a
 * {@link FabricLocator} but not both. To start a fabric server with an embedded
 * locator use the "start-locator" property. See
 * <code>{@link FabricServer}</code> and <code>{@link FabricLocator}</code> for
 * more information on managing servers and locators.
 * 
 * @author Soubhik C
 */
public abstract class FabricServiceManager {

  private static final Object instanceLock = new Object();

  private FabricServiceManager() {
  }

  /**
   * Get the singleton instance of {@link FabricServer}.
   */
  public final static FabricServer getFabricServerInstance() {
    FabricService instance = FabricServiceImpl.getInstance();
    if (instance != null) {
      return checkServerInstance(instance);
    }
    synchronized (instanceLock) {
      if ((instance = FabricServiceImpl.getInstance()) == null) {
        final FabricServer server = new FabricServerImpl();
        FabricServiceImpl.setInstance(server);
        return server;
      }
      return checkServerInstance(instance);
    }
  }

  /**
   * Get the singleton instance of {@link FabricLocator}.
   */
  public final static FabricLocator getFabricLocatorInstance() {
    FabricService instance = FabricServiceImpl.getInstance();
    if (instance != null) {
      return checkLocatorInstance(instance);
    }
    synchronized (instanceLock) {
      if ((instance = FabricServiceImpl.getInstance()) == null) {
        final FabricLocator locator = new FabricLocatorImpl();
        FabricServiceImpl.setInstance(locator);
        return locator;
      }
      return checkLocatorInstance(instance);
    }
  }

  /**
   * Get the singleton instance of {@link FabricAgent}.
   */
  public final static FabricAgent getFabricAgentInstance() {
    FabricService instance = FabricServiceImpl.getInstance();
    if (instance != null) {
      return checkAgentInstance(instance);
    }
    synchronized (instanceLock) {
      if ((instance = FabricServiceImpl.getInstance()) == null) {
        final FabricAgent agent = new FabricAgentImpl();
        FabricServiceImpl.setInstance(agent);
        return agent;
      }
      return checkAgentInstance(instance);
    }
  }
  
  /**
   * Get the current instance of either {@link FabricServer} or
   * {@link FabricLocator}. This can be null if neither of
   * {@link #getFabricServerInstance()} or {@link #getFabricLocatorInstance()}
   * have been invoked, or the instance has been stopped.
   */
  public final static FabricService currentFabricServiceInstance() {
    return FabricServiceImpl.getInstance();
  }

  private static FabricServer checkServerInstance(final FabricService instance) {
    if (instance instanceof FabricServer) {
      return (FabricServer)instance;
    }
    throw new IllegalStateException(
        "Found an existing instance of running locator (" + instance
            + "). Use getFabricLocatorInstance() to retrieve it.");
  }

  private static FabricLocator checkLocatorInstance(final FabricService instance) {
    if (instance instanceof FabricLocator) {
      return (FabricLocator)instance;
    }
    throw new IllegalStateException(
        "Found an existing instance of running server (" + instance
            + "). Use getFabricServerInstance() to retrieve it.");
  }
  
  private static FabricAgent checkAgentInstance(final FabricService instance) {
    if (instance instanceof FabricAgent) {
      return (FabricAgent)instance;
    }
    throw new IllegalStateException(
        "Found an existing instance of running locator or server (" + instance
            + "). Use getFabricServerInstance() to retrieve it.");
  }
}
