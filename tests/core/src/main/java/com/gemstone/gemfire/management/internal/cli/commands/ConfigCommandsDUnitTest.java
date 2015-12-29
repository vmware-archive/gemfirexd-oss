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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandProcessor;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * 
 * Dunit class for testing GemFire config commands : export config
 * 
 * @author David Hoots Sourabh Bansod
 * @since 7.0
 * 
 */
public class ConfigCommandsDUnitTest extends CliCommandTestBase {
  private static final long serialVersionUID = 1L;
  
  private final static String LOG__LEVEL = "info";
  
  private final static int LOG__FILE__SIZE__LIMIT = 50;
  private final static int ARCHIVE__DISK__SPACE__LIMIT = 32;
  private final static int ARCHIVE__FILE__SIZE__LIMIT = 49;
  private final static int STATISTIC__SAMPLE__RATE = 2000;
  private final static String STATISTIC__ARCHIVE__FILE = "stat.gfs";
  private final static boolean STATISTIC__SAMPLING__ENABLED = true;
  private final static int LOG__DISK__SPACE__LIMIT = 10;
  
  File managerConfigFile = new File("Manager-cache.xml");
  File managerPropsFile = new File("Manager-gf.properties");
  File vm1ConfigFile = new File("VM1-cache.xml");
  File vm1PropsFile = new File("VM1-gf.properties");
  File vm2ConfigFile = new File("VM2-cache.xml");
  File vm2PropsFile = new File("VM2-gf.properties");
  File shellConfigFile = new File("Shell-cache.xml");
  File shellPropsFile = new File("Shell-gf.properties");
  File subDir = new File("ConfigCommandsDUnitTestSubDir");
  File subManagerConfigFile = new File(subDir, managerConfigFile.getName());

  
  public ConfigCommandsDUnitTest(String name) {
    super(name);
  }

  public void tearDown2() throws Exception {
    deleteTestFiles();
    super.tearDown2();
  }

  public void testDescribeConfig() throws ClassNotFoundException, IOException {
    createDefaultSetup(null);
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "Member1";
    final String controllerName = "Member2";

    /***
     * Create properties for the controller VM
     */
    final Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    localProps.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    localProps.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME, "true");
    localProps.setProperty(DistributionConfig.NAME_NAME, controllerName);
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "G1");
    getSystem(localProps);
    Cache cache = getCache();
    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    CacheServer cs = getCache().addCacheServer();
    cs.setPort(ports[0]);
    cs.setMaxThreads(10);
    cs.setMaxConnections(9);
    cs.start();

    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    List<String> jvmArgs = runtimeBean.getInputArguments();

    getLogWriter().info("#SB Actual JVM Args : ");

    for (String jvmArg : jvmArgs) {
      getLogWriter().info("#SB JVM " + jvmArg);
    }

    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    DistributionConfig config = system.getConfig();
    config.setArchiveFileSizeLimit(1000);

    String command = CliStrings.DESCRIBE_CONFIG + " --member=" + controllerName;
    CommandProcessor cmdProcessor = new CommandProcessor();
    cmdProcessor.createCommandStatement(command, Collections.EMPTY_MAP).process();

    CommandResult cmdResult = executeCommand(command);

    String resultStr = commandResultToString(cmdResult);
    getLogWriter().info("#SB Hiding the defaults\n" + resultStr);

    assertEquals(true, cmdResult.getStatus().equals(Status.OK));
    assertEquals(true, resultStr.contains("G1"));
    assertEquals(true, resultStr.contains(controllerName));
    assertEquals(true, resultStr.contains("archive-file-size-limit"));
    assertEquals(true, !resultStr.contains("copy-on-read"));

    cmdResult = executeCommand(command + " --" + CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS + "=false");
    resultStr = commandResultToString(cmdResult);
    getLogWriter().info("#SB No hiding of defaults\n" + resultStr);

    assertEquals(true, cmdResult.getStatus().equals(Status.OK));
    assertEquals(true, resultStr.contains("is-server"));
    assertEquals(true, resultStr.contains(controllerName));
    assertEquals(true, resultStr.contains("copy-on-read"));

    cs.stop();
  }

  @SuppressWarnings("serial")
  public void testExportConfig() throws IOException {
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, "Manager");
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
    createDefaultSetup(localProps);

    // Create a cache in another VM (VM1)
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, "VM1");
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Create a cache in a 3rd VM (VM2)
    Host.getHost(0).getVM(2).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, "VM2");
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Create a cache in the local VM
    localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, "Shell");
    getSystem(localProps);
    Cache cache = getCache();

    // Test export config for all members
    deleteTestFiles();
    CommandResult cmdResult = executeCommand("export config");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertTrue(this.managerConfigFile.exists());
    assertTrue(this.managerPropsFile.exists());
    assertTrue(this.vm1ConfigFile.exists());
    assertTrue(this.vm1PropsFile.exists());
    assertTrue(this.vm2ConfigFile.exists());
    assertTrue(this.vm2PropsFile.exists());
    assertTrue(this.shellConfigFile.exists());
    assertTrue(this.shellPropsFile.exists());

    // Test exporting member
    deleteTestFiles();
    cmdResult = executeCommand("export config --member=Manager");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertTrue(this.managerConfigFile.exists());
    assertFalse(this.vm1ConfigFile.exists());
    assertFalse(this.vm2ConfigFile.exists());
    assertFalse(this.shellConfigFile.exists());

    // Test exporting group
    deleteTestFiles();
    cmdResult = executeCommand("export config --group=Group2");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertFalse(this.managerConfigFile.exists());
    assertTrue(this.vm1ConfigFile.exists());
    assertTrue(this.vm2ConfigFile.exists());
    assertFalse(this.shellConfigFile.exists());

    // Test export to directory
    deleteTestFiles();
    cmdResult = executeCommand("export config --dir=" + subDir.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertFalse(this.managerConfigFile.exists());
    assertTrue(this.subManagerConfigFile.exists());

    // Test the contents of the file
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    CacheXmlGenerator.generate(cache, printWriter, false, false, false);
    String configToMatch = stringWriter.toString();

    deleteTestFiles();
    cmdResult = executeCommand("export config --member=Shell");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    char[] fileContents = new char[configToMatch.length()];
    try {
      FileReader reader = new FileReader(shellConfigFile);
      reader.read(fileContents);
    } catch (Exception ex) {
      fail("Unable to read file contents for comparison", ex);
    }

    assertEquals(configToMatch, new String(fileContents));
  }

  public void testAlterRuntimeConfig() throws ClassNotFoundException, IOException {
    final String controller = "controller";
    createDefaultSetup(null);
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, controller);
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "error");
    getSystem(localProps);
    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__MEMBER, controller);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, LOG__LEVEL);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, Integer.toString(LOG__FILE__SIZE__LIMIT));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, Integer.toString(ARCHIVE__DISK__SPACE__LIMIT));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, Integer.toString(ARCHIVE__FILE__SIZE__LIMIT));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, Integer.toString(STATISTIC__SAMPLE__RATE));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, STATISTIC__ARCHIVE__FILE);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, Boolean.toString(STATISTIC__SAMPLING__ENABLED));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, Integer.toString(LOG__DISK__SPACE__LIMIT));
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultString = commandResultToString(cmdResult);
    getLogWriter().info("Result\n");
    getLogWriter().info(resultString);
    assertEquals(true, cmdResult.getStatus().equals(Status.OK));
    assertEquals(LogWriterImpl.INFO_LEVEL, config.getLogLevel());
    assertEquals(LOG__FILE__SIZE__LIMIT, config.getLogFileSizeLimit());
    assertEquals(ARCHIVE__DISK__SPACE__LIMIT, config.getArchiveDiskSpaceLimit());
    assertEquals(ARCHIVE__FILE__SIZE__LIMIT, config.getArchiveFileSizeLimit());
    assertEquals(STATISTIC__SAMPLE__RATE, config.getStatisticSampleRate());
    assertEquals(STATISTIC__ARCHIVE__FILE, config.getStatisticArchiveFile().getName());
    assertEquals(STATISTIC__SAMPLING__ENABLED, config.getStatisticSamplingEnabled());
    assertEquals(LOG__DISK__SPACE__LIMIT, config.getLogDiskSpaceLimit());
    
    
    CommandProcessor commandProcessor = new CommandProcessor();
    Result result = commandProcessor.createCommandStatement("alter runtime", Collections.EMPTY_MAP).process();
  }

  public void testAlterRuntimeConfigRandom() {
    final String member1 = "VM1";
    final String controller = "controller";
    createDefaultSetup(null);
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, controller);
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "error");
    getSystem(localProps);
    final GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
    final DistributionConfig config = cache.getSystem().getConfig();
    
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, member1);
        getSystem(localProps);
        Cache cache = getCache();
      }
    });
    
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultAsString = commandResultToString(cmdResult);
    getLogWriter().info(resultAsString);
    assertEquals(true, cmdResult.getStatus().equals(Status.ERROR));
    assertTrue(resultAsString.contains(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE));
    
    csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "2000000000");
    cmdResult = executeCommand(csb.getCommandString());
    resultAsString = commandResultToString(cmdResult);
    getLogWriter().info("#SB Result\n");
    getLogWriter().info(resultAsString);
    assertEquals(true, cmdResult.getStatus().equals(Status.ERROR));
    
  }
  public void testAlterRuntimeConfigOnAllMembers() {
    final String member1 = "VM1";
    final String controller = "controller";
    createDefaultSetup(null);
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, controller);
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "error");
    getSystem(localProps);
    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();

    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, member1);
        getSystem(localProps);
        Cache cache = getCache();
      }
    });
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, LOG__LEVEL);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, Integer.toString(LOG__FILE__SIZE__LIMIT));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, Integer.toString(ARCHIVE__DISK__SPACE__LIMIT));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, Integer.toString(ARCHIVE__FILE__SIZE__LIMIT));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, Integer.toString(STATISTIC__SAMPLE__RATE));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, STATISTIC__ARCHIVE__FILE);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, Boolean.toString(STATISTIC__SAMPLING__ENABLED));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, Integer.toString(LOG__DISK__SPACE__LIMIT));
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultString = commandResultToString(cmdResult);
    getLogWriter().info(resultString);
    
    assertEquals(true, cmdResult.getStatus().equals(Status.OK));
    
    assertEquals(LogWriterImpl.INFO_LEVEL, config.getLogLevel());
    assertEquals(LOG__FILE__SIZE__LIMIT, config.getLogFileSizeLimit());
    assertEquals(ARCHIVE__DISK__SPACE__LIMIT, config.getArchiveDiskSpaceLimit());
    assertEquals(ARCHIVE__FILE__SIZE__LIMIT, config.getArchiveFileSizeLimit());
    assertEquals(STATISTIC__SAMPLE__RATE, config.getStatisticSampleRate());
    assertEquals(STATISTIC__ARCHIVE__FILE, config.getStatisticArchiveFile().getName());
    assertEquals(STATISTIC__SAMPLING__ENABLED, config.getStatisticSamplingEnabled());
    assertEquals(LOG__DISK__SPACE__LIMIT, config.getLogDiskSpaceLimit());
    
    // Validate the changes in the vm1
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        GemFireCacheImpl cacheVM1 = (GemFireCacheImpl) getCache();
        final DistributionConfig configVM1 = cacheVM1.getSystem().getConfig();
        assertEquals(LogWriterImpl.INFO_LEVEL, configVM1.getLogLevel());
        assertEquals(LOG__FILE__SIZE__LIMIT, configVM1.getLogFileSizeLimit());
        assertEquals(ARCHIVE__FILE__SIZE__LIMIT, configVM1.getArchiveFileSizeLimit());
        assertEquals(ARCHIVE__DISK__SPACE__LIMIT, configVM1.getArchiveDiskSpaceLimit());
        assertEquals(STATISTIC__SAMPLE__RATE, configVM1.getStatisticSampleRate());
        assertEquals(STATISTIC__ARCHIVE__FILE, configVM1.getStatisticArchiveFile().getName());
        assertEquals(STATISTIC__SAMPLING__ENABLED, configVM1.getStatisticSamplingEnabled());
        assertEquals(LOG__DISK__SPACE__LIMIT, configVM1.getLogDiskSpaceLimit());
        
      }
    });

  }

  private final void deleteTestFiles() throws IOException {
    this.managerConfigFile.delete();
    this.managerPropsFile.delete();
    this.vm1ConfigFile.delete();
    this.vm1PropsFile.delete();
    this.vm2ConfigFile.delete();
    this.vm2PropsFile.delete();
    this.shellConfigFile.delete();
    this.shellPropsFile.delete();

    FileUtils.deleteDirectory(this.subDir);
  }
}
