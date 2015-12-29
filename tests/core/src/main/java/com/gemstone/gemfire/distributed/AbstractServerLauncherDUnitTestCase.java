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
package com.gemstone.gemfire.distributed;

import java.io.File;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.ServerState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.DistributionLocator;
import com.gemstone.gemfire.internal.cache.AbstractBridgeServer;

import dunit.SerializableRunnable;

@SuppressWarnings("serial")
public abstract class AbstractServerLauncherDUnitTestCase extends AbstractLauncherDUnitTestCase {
  
  protected transient volatile int serverPort;
  protected transient volatile ServerLauncher launcher;
  protected transient volatile File cacheXmlFile;

  public AbstractServerLauncherDUnitTestCase(String name) {
    super(name);
  }
  
  @Override
  public final void subSetUp() throws Exception {
    System.setProperty("gemfire." + DistributionConfig.MCAST_PORT_NAME, Integer.toString(0));
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    overrideDefaultPort(port);
    invokeInEveryVM(new SerializableRunnable("overrideDefaultPort") {
      @Override
      public void run() {
        overrideDefaultPort(port);
      }
    });
    this.serverPort = port;
    subSetUp1();
  }

  @Override
  public final void subTearDown() throws Exception {    
    System.clearProperty("gemfire." + DistributionConfig.MCAST_PORT_NAME);
    System.clearProperty(DistributionConfig.CACHE_XML_FILE_NAME);
    cleanupServerLauncher();
    delete(this.cacheXmlFile); this.cacheXmlFile = null;
    this.serverPort = 0;
    restoreDefaultPort();
    invokeInEveryVM(new SerializableRunnable("restoreDefaultPort") {
      @Override
      public void run() {
        restoreDefaultPort();
      }
    });
    subTearDown1();
  }
  
  /**
   * To be overridden in subclass.
   */
  protected abstract void subSetUp1() throws Exception;
  
  /**
   * To be overridden in subclass.
   */
  protected abstract void subTearDown1() throws Exception;
  
  protected void cleanupServerLauncher() {
    try {
      if (this.launcher != null) {
        this.launcher.stop();
        this.launcher = null;
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
  
  protected void waitForServerToStart(final ServerLauncher launcher, long timeout, long interval, boolean throwOnTimeout) {
    waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        try {
          final ServerState serverState = launcher.status();
          assertNotNull(serverState);
          return Status.ONLINE.equals(serverState.getStatus());
        }
        catch (RuntimeException e) {
          return false;
        }
      }

      @Override
      public String description() {
        return "waiting for local Server to start: " + launcher.status();
      }
    }, timeout, interval, throwOnTimeout);
  }

  protected void waitForServerToStart(final ServerLauncher launcher, boolean throwOnTimeout) {
    waitForServerToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForServerToStart(final ServerLauncher launcher, long timeout, boolean throwOnTimeout) {
    waitForServerToStart(launcher, timeout, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForServerToStart(final ServerLauncher launcher) {
    waitForServerToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
  }
  
  protected static void overrideDefaultPort(final int port) {
    System.setProperty(AbstractBridgeServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(port));
  }
  
  protected static void restoreDefaultPort() {
    System.clearProperty(AbstractBridgeServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY);
  }
}
