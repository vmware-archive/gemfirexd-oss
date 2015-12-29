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

import java.io.IOException;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.DistributionLocator;

import dunit.SerializableRunnable;

  @SuppressWarnings("serial")
public abstract class AbstractLocatorLauncherDUnitTestCase extends AbstractLauncherDUnitTestCase {

  protected transient volatile int locatorPort;
  protected transient volatile LocatorLauncher launcher;
  
  public AbstractLocatorLauncherDUnitTestCase(String name) {
    super(name);
  }
  
  @Override
  public final void subSetUp() throws Exception {
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    overrideDefaultPort(port);
    invokeInEveryVM(new SerializableRunnable("overrideDefaultPort") {
      @Override
      public void run() {
        overrideDefaultPort(port);
      }
    });
    this.locatorPort = port;
    subSetUp1();
  }
  
  @Override
  public final void subTearDown() throws Exception {    
    cleanupLocatorLauncher();
    this.locatorPort = 0;
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
  
  protected void cleanupLocatorLauncher() {
    try {
      if (this.launcher != null) {
        this.launcher.stop();
        this.launcher = null;
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher, long timeout, long interval, boolean throwOnTimeout) {
    waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        try {
          final LocatorState LocatorState = launcher.status();
          return (LocatorState != null && Status.ONLINE.equals(LocatorState.getStatus()));
        }
        catch (RuntimeException e) {
          return false;
        }
      }
  
      @Override
      public String description() {
        return "waiting for local  to start: " + launcher.status();
      }
    }, timeout, interval, throwOnTimeout);
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher, long timeout, boolean throwOnTimeout) {
    waitForLocatorToStart(launcher, timeout, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher, boolean throwOnTimeout) {
    waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher) {
    waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
  }
  
  protected static void waitForLocatorToStart(int port, long timeout, long interval, boolean throwOnTimeout) throws IOException {
    final LocatorLauncher locatorLauncher = new Builder().setPort(port).build();

    waitForCriterion(new WaitCriterion() {
      public boolean done() {
        try {
          final LocatorState locatorState = locatorLauncher.status();
          return (locatorState != null && Status.ONLINE.equals(locatorState.getStatus()));
        }
        catch (RuntimeException e) {
          return false;
        }
      }
      public String description() {
        return "Waiting for Locator in other process to start.";
      }
    }, timeout, interval, throwOnTimeout);
  }
  
  protected static void overrideDefaultPort(final int port) {
    System.setProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(port));
  }
  
  protected static void restoreDefaultPort() {
    System.clearProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY);
  }
}
