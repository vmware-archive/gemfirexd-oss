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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.process.PidUnavailableException;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.internal.process.ProcessStreamReader.InputListener;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

@SuppressWarnings("serial")
public abstract class AbstractLauncherDUnitTestCase extends CacheTestCase {

  protected static final int TIMEOUT_MILLISECONDS = 300 * 1000; // 5 minutes
  protected static final int INTERVAL_MILLISECONDS = 100; // 100 milliseconds
  
  protected transient volatile ServerSocket socket;
  
  protected transient volatile File pidFile;
  protected transient volatile File stopRequestFile;
  protected transient volatile File statusRequestFile;
  protected transient volatile File statusFile;
  
  public AbstractLauncherDUnitTestCase(String name) {
    super(name);
  }

  @Override
  public final void setUp() throws Exception {
    super.setUp();
    disconnectFromDS();
    System.setProperty("gemfire." + DistributionConfig.MCAST_PORT_NAME, Integer.toString(0));
    subSetUp();
  }

  @Override
  public final void tearDown2() throws Exception {    
    System.clearProperty("gemfire." + DistributionConfig.MCAST_PORT_NAME);
    System.clearProperty(DistributionConfig.CACHE_XML_FILE_NAME);
    if (this.socket != null) {
      this.socket.close();
      this.socket = null;
    }
    delete(this.pidFile); this.pidFile = null;
    delete(this.stopRequestFile); this.stopRequestFile = null;
    delete(this.statusRequestFile); this.statusRequestFile = null;
    delete(this.statusFile); this.statusFile = null;
    subTearDown();
  }
  
  /**
   * To be overridden in subclass.
   */
  protected abstract void subSetUp() throws Exception;
  
  /**
   * To be overridden in subclass.
   */
  protected abstract void subTearDown() throws Exception;
  
  protected void delete(final File file) {
    waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        if (file == null) {
          return true;
        }
        try {
          FileUtil.delete(file);
        } catch (IOException e) {
        }
        return !file.exists();
      }

      @Override
      public String description() {
        return "deleting " + file;
      }
    }, 10*1000, 1000, true);
  }
  
  protected boolean isPidAlive(int pid) {
    for (VirtualMachineDescriptor vm : VirtualMachine.list()) {
      if (vm.id().equals(String.valueOf(pid))) {
        return true; // found the vm
      }
    }

    return false;
  }

  protected void waitForPidToStop(final int pid, boolean throwOnTimeout) {
    waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return !isPidAlive(pid);
      }

      @Override
      public String description() {
        return "waiting for pid " + pid + " to die";
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForPidToStop(final int pid) {
    waitForPidToStop(pid, true);
  }
  
  protected void waitForFileToDelete(final File file, boolean throwOnTimeout) {
    if (file == null) {
      return;
    }
    waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return !file.exists();
      }

      @Override
      public String description() {
        return "waiting for file " + file + " to delete";
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForFileToDelete(final File file) {
    waitForFileToDelete(file, true);
  }
  
  protected static int getPid() throws PidUnavailableException {
    return ProcessUtils.identifyPid();
  }

  protected InputListener createLoggingListener(final String name, final String header) {
    return new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        getLogWriter().info(new StringBuilder("[").append(header).append("]").append(line).toString());
      }
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected InputListener createCollectionListener(final String name, final String header, final List<String> lines) {
    return new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        lines.add(line);
      }
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected InputListener createExpectedListener(final String name, final String header, final String expected, final AtomicBoolean atomic) {
    return new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        if (line.contains(expected)) {
          atomic.set(true);
        }
      }
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected void writeGemfireProperties(final Properties gemfireProperties, final File gemfirePropertiesFile) throws IOException {
    if (!gemfirePropertiesFile.exists()) {
      gemfireProperties.store(new FileWriter(gemfirePropertiesFile), "Configuration settings for the GemFire Server");
    }
  }

  protected int readPid(final File pidFile) throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(pidFile));
      return Integer.parseInt(StringUtils.trim(reader.readLine()));
    }
    finally {
      IOUtils.close(reader);
    }
  }

  protected void writePid(final File pidFile, final int pid) throws IOException {
    FileWriter writer = new FileWriter(pidFile);
    writer.write(String.valueOf(pid));
    writer.write("\n");
    writer.flush();
    writer.close();
  }

  protected void waitForFileToExist(final File file, boolean throwOnTimeout) {
    waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return file.exists();
      }
      @Override
      public String description() {
        return "waiting for file to exist: " + file;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForFileToExist(final File file) {
    waitForFileToExist(file, true);
  }
}
