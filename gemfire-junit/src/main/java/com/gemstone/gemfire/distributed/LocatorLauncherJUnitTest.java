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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.Command;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.NativeCalls;
import com.gemstone.gemfire.internal.util.IOUtils;
import joptsimple.OptionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The LocatorLauncherJUnitTest class is a test suite of test cases for testing the contract and functionality of
 * launching a GemFire Locator.
 *
 * @author John Blum
 * @author Kirk Lund
 * @see com.gemstone.gemfire.distributed.CommonLauncherTestSuite
 * @see com.gemstone.gemfire.distributed.LocatorLauncher
 * @see com.gemstone.gemfire.distributed.LocatorLauncher.Builder
 * @see com.gemstone.gemfire.distributed.LocatorLauncher.Command
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
public class LocatorLauncherJUnitTest extends CommonLauncherTestSuite {

  private static final String GEMFIRE_PROPERTIES_FILE_NAME = "gemfire.properties";
  private static final String TEMPORARY_FILE_NAME = "beforeLocatorLauncherJUnitTest_" + GEMFIRE_PROPERTIES_FILE_NAME;
  
  @BeforeClass
  public static void setUp() {
    if (!NativeCalls.getInstance().getOSType().isWindows()) {
      File file = new File(GEMFIRE_PROPERTIES_FILE_NAME);
      if (file.exists()) {
        File dest = new File(TEMPORARY_FILE_NAME);
        assertTrue(file.renameTo(dest));
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    if (!NativeCalls.getInstance().getOSType().isWindows()) {
      File file = new File(TEMPORARY_FILE_NAME);
      if (file.exists()) {
        File dest = new File(GEMFIRE_PROPERTIES_FILE_NAME);
        assertTrue(file.renameTo(dest));
      }
    }
  }

  @Test
  public void testBuilderParseArguments() throws Exception {
    final String expectedWorkingDirectory = System.getProperty("user.dir");
    final Builder builder = new Builder();

    builder.parseArguments("start", "memberOne", "--bind-address", InetAddress.getLocalHost().getHostAddress(),
      "--dir", expectedWorkingDirectory, "--hostname-for-clients", "Tucows", "--pid", "1234", "--port", "11235",
        "--redirect-output", "--force", "--debug");

    assertEquals(Command.START, builder.getCommand());
    assertEquals(InetAddress.getLocalHost(), builder.getBindAddress());
    assertEquals(expectedWorkingDirectory, builder.getWorkingDirectory());
    assertEquals("Tucows", builder.getHostnameForClients());
    assertEquals(1234, builder.getPid().intValue());
    assertEquals(11235, builder.getPort().intValue());
    assertTrue(builder.getRedirectOutput());
    assertTrue(builder.getForce());
    assertTrue(builder.getDebug());
  }

  @Test
  public void testBuilderParseArgumentsWithCommandInArguments() {
    final String expectedWorkingDirectory = System.getProperty("user.dir");
    final Builder builder = new Builder();

    builder.parseArguments("start", "--dir=" + expectedWorkingDirectory, "--port", "12345", "memberOne");

    assertEquals(Command.START, builder.getCommand());
    assertFalse(Boolean.TRUE.equals(builder.getDebug()));
    assertFalse(Boolean.TRUE.equals(builder.getForce()));
    assertFalse(Boolean.TRUE.equals(builder.getHelp()));
    assertNull(builder.getBindAddress());
    assertNull(builder.getHostnameForClients());
    assertEquals("12345", builder.getMemberName());
    assertNull(builder.getPid());
    assertEquals(expectedWorkingDirectory, builder.getWorkingDirectory());
    assertEquals(12345, builder.getPort().intValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderParseArgumentsWithNonNumericPort() {
    try {
      new Builder().parseArguments("start", "locator1", "--port", "oneTwoThree");
    }
    catch (IllegalArgumentException expected) {
      assertTrue(expected.getCause() instanceof OptionException);
      assertTrue(expected.getMessage(), expected.getMessage().contains(
        LocalizedStrings.Launcher_Builder_PARSE_COMMAND_LINE_ARGUMENT_ERROR_MESSAGE.toLocalizedString(
          "Locator", expected.getCause().getMessage())));
      throw expected;
    }
  }

  @Test
  public void testForceDefaultsToFalse() {
    assertFalse(new Builder().getForce());
  }

  @Test
  public void testForceSetToTrue() {
    final Builder builder = new Builder();

    builder.parseArguments("start", "--force");

    assertTrue(Boolean.TRUE.equals(builder.getForce()));
  }

  @Test
  public void testSetAndGetCommand() {
    final Builder builder = new Builder();

    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());
    assertSame(builder, builder.setCommand(Command.START));
    assertEquals(Command.START, builder.getCommand());
    assertSame(builder, builder.setCommand(Command.STATUS));
    assertEquals(Command.STATUS, builder.getCommand());
    assertSame(builder, builder.setCommand(Command.STOP));
    assertEquals(Command.STOP, builder.getCommand());
    assertSame(builder, builder.setCommand(null));
    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());
  }

  @Test
  public void testSetAndGetBindAddress() throws UnknownHostException {
    final Builder builder = new Builder();

    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress(null));
    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress(""));
    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress("  "));
    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress(InetAddress.getLocalHost().getCanonicalHostName()));
    assertEquals(InetAddress.getLocalHost(), builder.getBindAddress());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetBindAddressToUnknownHost() {
    try {
      new Builder().setBindAddress("badhostname.badcompany.bad");
    }
    catch (IllegalArgumentException expected) {
      final String expectedMessage = LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toLocalizedString("Locator");
      assertEquals(expectedMessage, expected.getMessage());
      assertTrue(expected.getCause() instanceof UnknownHostException);
      throw expected;
    }
  }

  @Test
  public void testSetAndGetHostnameForClients() {
    final Builder builder = new Builder();

    assertNull(builder.getHostnameForClients());
    assertSame(builder, builder.setHostnameForClients("Pegasus"));
    assertEquals("Pegasus", builder.getHostnameForClients());
    assertSame(builder, builder.setHostnameForClients(null));
    assertNull(builder.getHostnameForClients());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetHostnameForClientsWithBlankString() {
    try {
      new Builder().setHostnameForClients(" ");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.LocatorLauncher_Builder_INVALID_HOSTNAME_FOR_CLIENTS_ERROR_MESSAGE
        .toLocalizedString(), expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetHostnameForClientsWithEmptyString() {
    try {
      new Builder().setHostnameForClients("");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.LocatorLauncher_Builder_INVALID_HOSTNAME_FOR_CLIENTS_ERROR_MESSAGE
        .toLocalizedString(), expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetMemberName() {
    final Builder builder = new Builder();

    assertNull(builder.getMemberName());
    assertSame(builder, builder.setMemberName("locatorOne"));
    assertEquals("locatorOne", builder.getMemberName());
    assertSame(builder, builder.setMemberName(null));
    assertNull(builder.getMemberName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMemberNameWithBlankString() {
    try {
      new Builder().setMemberName("  ");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMemberNameWithEmptyString() {
    try {
      new Builder().setMemberName("");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetPid() {
    final Builder builder = new Builder();

    assertNull(builder.getPid());
    assertSame(builder, builder.setPid(0));
    assertEquals(0, builder.getPid().intValue());
    assertSame(builder, builder.setPid(1));
    assertEquals(1, builder.getPid().intValue());
    assertSame(builder, builder.setPid(1024));
    assertEquals(1024, builder.getPid().intValue());
    assertSame(builder, builder.setPid(12345));
    assertEquals(12345, builder.getPid().intValue());
    assertSame(builder, builder.setPid(null));
    assertNull(builder.getPid());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPidToInvalidValue() {
    try {
      new Builder().setPid(-1);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_PID_ERROR_MESSAGE.toLocalizedString(), expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetPort() {
    final Builder builder = new Builder();

    assertEquals(LocatorLauncher.DEFAULT_LOCATOR_PORT, builder.getPort());
    assertSame(builder, builder.setPort(65535));
    assertEquals(65535, builder.getPort().intValue());
    assertSame(builder, builder.setPort(1024));
    assertEquals(1024, builder.getPort().intValue());
    assertSame(builder, builder.setPort(80));
    assertEquals(80, builder.getPort().intValue());
    assertSame(builder, builder.setPort(1));
    assertEquals(1, builder.getPort().intValue());
    assertSame(builder, builder.setPort(0));
    assertEquals(0, builder.getPort().intValue());
    assertSame(builder, builder.setPort(null));
    assertEquals(LocatorLauncher.DEFAULT_LOCATOR_PORT, builder.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPortToOverflow() {
    try {
      new Builder().setPort(65536);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_INVALID_PORT_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPortToUnderflow() {
    try {
      new Builder().setPort(-1);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_INVALID_PORT_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetWorkingDirectory() {
    final Builder builder = new Builder();

    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(null));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(""));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory("  "));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(System.getProperty("user.dir")));
    assertEquals(System.getProperty("user.dir"), builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(System.getProperty("java.io.tmpdir")));
    assertEquals(IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(System.getProperty("java.io.tmpdir"))),
      builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(null));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToFile() throws IOException {
    final File tmpFile = File.createTempFile("tmp", "file");

    assertNotNull(tmpFile);
    assertTrue(tmpFile.isFile());

    tmpFile.deleteOnExit();

    try {
      new Builder().setWorkingDirectory(tmpFile.getCanonicalPath());
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Locator"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals(tmpFile.getCanonicalPath(), expected.getCause().getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToNonExistingDirectory() {
    try {
      new Builder().setWorkingDirectory("/path/to/non_existing/directory");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Locator"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals("/path/to/non_existing/directory", expected.getCause().getMessage());
      throw expected;
    }
  }

  @Test
  public void testBuild() {
    final Builder builder = new Builder();

    final LocatorLauncher launcher = builder.setCommand(Command.START)
      .setDebug(true)
      .setHostnameForClients("beanstock.vmware.com")
      .setMemberName("Beanstock")
      .setPort(8192)
      .setWorkingDirectory(AbstractLauncher.DEFAULT_WORKING_DIRECTORY)
      .build();

    assertNotNull(launcher);
    assertEquals(builder.getCommand(), launcher.getCommand());
    assertTrue(launcher.isDebugging());
    assertEquals(builder.getHostnameForClients(), launcher.getHostnameForClients());
    assertEquals(builder.getMemberName(), launcher.getMemberName());
    assertEquals(builder.getPort(), launcher.getPort());
    assertEquals(builder.getWorkingDirectory(), launcher.getWorkingDirectory());
    assertFalse(launcher.isHelping());
    assertFalse(launcher.isRunning());
  }

  @Test
  public void testBuildWithMemberNameSetInApiPropertiesOnStart() {
    LocatorLauncher launcher = new Builder()
      .setCommand(LocatorLauncher.Command.START)
      .setMemberName(null)
      .set(DistributionConfig.NAME_NAME, "locatorABC")
      .build();

    assertNotNull(launcher);
    assertEquals(LocatorLauncher.Command.START, launcher.getCommand());
    assertNull(launcher.getMemberName());
    assertEquals("locatorABC", launcher.getProperties().getProperty(DistributionConfig.NAME_NAME));
  }

  @Test
  public void testBuildWithMemberNameSetInGemfirePropertiesOnStart() {
    // TODO fix this test on Windows; File renameTo and delete in finally fail on Windows
    if (NativeCalls.getInstance().getOSType().isWindows()) {
      return;
    }

    final Properties gemfireProperties = new Properties();

    gemfireProperties.setProperty(DistributionConfig.NAME_NAME, "locator123");

    final File gemfirePropertiesFile = writeGemFirePropertiesToFile(gemfireProperties, "gemfire.properties",
      String.format("Test gemfire.properties file for %1$s.%2$s.", getClass().getSimpleName(),
        "testBuildWithMemberNameSetInGemfirePropertiesOnStart"));

    assertNotNull(gemfirePropertiesFile);
    assertTrue(gemfirePropertiesFile.isFile());

    try {
      LocatorLauncher launcher = new Builder().setCommand(Command.START).setMemberName(null).build();

      assertNotNull(launcher);
      assertEquals(Command.START, launcher.getCommand());
      assertNull(launcher.getMemberName());
    }
    finally {
      assertTrue(gemfirePropertiesFile.delete());
      assertFalse(gemfirePropertiesFile.isFile());
    }
  }

  @Test
  public void testBuildWithMemberNameSetInSystemPropertiesOnStart() {
    try {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME, "locatorXYZ");

      LocatorLauncher launcher = new Builder()
        .setCommand(LocatorLauncher.Command.START)
        .setMemberName(null)
        .build();

      assertNotNull(launcher);
      assertEquals(LocatorLauncher.Command.START, launcher.getCommand());
      assertNull(launcher.getMemberName());
    }
    finally {
      System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildWithNoMemberNameOnStart() {
    try {
      new Builder().setCommand(Command.START).build();
    }
    catch (IllegalStateException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildWithMismatchingCurrentAndWorkingDirectoryOnStart() {
    try {
      new Builder().setCommand(Command.START)
        .setMemberName("memberOne")
        .setWorkingDirectory(System.getProperty("java.io.tmpdir"))
        .build();
    }
    catch (IllegalStateException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE
        .toLocalizedString("Locator"), expected.getMessage());
      throw expected;
    }
  }

}
