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

package com.pivotal.gemfirexd.tools.internal;

import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.gemstone.gemfire.internal.Assert;
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.tools.internal.ToolsBase.ProcessCommand;

/**
 * Command-line tools to install/replace/remove JARs in a running GemFireXD
 * system. Unlike the system procedure SQLJ.INSTALL_JAR(), these methods do not
 * require the jar file to be accessible on the server rather ship the actual
 * jar bytes from client to the server.
 * 
 * @author swale
 * @since 7.0
 */
public final class JarTools extends ToolsBase {

  // top-level command names keys and values
  static final String INSTALL_JAR;
  static final String REMOVE_JAR;
  static final String REPLACE_JAR;

  // command name usage and description message keys
  private static final String INSTALL_JAR_DESC_KEY;
  private static final String REMOVE_JAR_DESC_KEY;
  private static final String REPLACE_JAR_DESC_KEY;

  private static final Map<String, String> validCommands;

  // command options
  private static final String JAR_NAME;
  private static final String FILE_URL;

  private String jarName;
  private String fileURL;

  static {
    INSTALL_JAR = LocalizedResource.getMessage("JARTOOLS_INSTALL_JAR");
    REMOVE_JAR = LocalizedResource.getMessage("JARTOOLS_REMOVE_JAR");
    REPLACE_JAR = LocalizedResource.getMessage("JARTOOLS_REPLACE_JAR");

    INSTALL_JAR_DESC_KEY = "JARTOOLS_INSTALL_JAR_DESC";
    REMOVE_JAR_DESC_KEY = "JARTOOLS_REMOVE_JAR_DESC";
    REPLACE_JAR_DESC_KEY = "JARTOOLS_REPLACE_JAR_DESC";

    JAR_NAME = LocalizedResource.getMessage("JARTOOLS_JAR_NAME");
    FILE_URL = LocalizedResource.getMessage("TOOLS_FILE");

    validCommands = new LinkedHashMap<String, String>();
    validCommands.put(INSTALL_JAR, INSTALL_JAR_DESC_KEY);
    validCommands.put(REPLACE_JAR, REPLACE_JAR_DESC_KEY);
    validCommands.put(REMOVE_JAR, REMOVE_JAR_DESC_KEY);
  }

  public static void main(String[] args) {
    final JarTools instance = new JarTools();
    instance.invoke(args);
  }

  public static Map<String, String> getValidCommands() {
    return validCommands;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Map<String, String> getCommandToDescriptionKeyMap() {
    return validCommands;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected ProcessCommand getCommandProcessor(final String cmd) {
    if (INSTALL_JAR.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addFileOption(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            IOException, SQLException {
          final Connection conn = getConnection(cmdLine, cmd, cmdDescKey, true);

          final CallableStatement cstmt = conn
              .prepareCall("call SQLJ.INSTALL_JAR_BYTES(?, ?)");
          final InputStream is = SharedUtils.openURL(fileURL);
          cstmt.setBinaryStream(1, is);
          cstmt.setString(2, jarName);
          cstmt.executeUpdate();
          System.out.println(LocalizedResource.getMessage(
              "JARTOOLS_INSTALL_SUCCESS", fileURL, jarName));
        }
      };
    }
    else if (REMOVE_JAR.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(Options opts) {
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          final Connection conn = getConnection(cmdLine, cmd, cmdDescKey, true);

          final CallableStatement cstmt = conn
              .prepareCall("call SQLJ.REMOVE_JAR(?, 0)");
          cstmt.setString(1, jarName);
          cstmt.executeUpdate();
          System.out.println(LocalizedResource.getMessage(
              "JARTOOLS_REMOVE_SUCCESS", fileURL, jarName));
        }
      };
    }
    else if (REPLACE_JAR.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addFileOption(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            IOException, SQLException {
          final Connection conn = getConnection(cmdLine, cmd, cmdDescKey, true);

          final CallableStatement cstmt = conn
              .prepareCall("call SQLJ.REPLACE_JAR_BYTES(?, ?)");
          final InputStream is = SharedUtils.openURL(fileURL);
          cstmt.setBinaryStream(1, is);
          cstmt.setString(2, jarName);
          cstmt.executeUpdate();
          System.out.println(LocalizedResource.getMessage(
              "JARTOOLS_REPLACE_SUCCESS", fileURL, jarName));
        }
      };
    }
    else {
      return null;
    }
  }

  @Override
  protected void addCommonOptions(final Options opts) {
    GfxdOption opt;

    super.addCommonOptions(opts);
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "JARTOOLS_JAR_NAME_ARG")).hasArg().isRequired(true)
        .withValueSeparator('=').withDescription(LocalizedResource.getMessage(
            "JARTOOLS_JAR_NAME_MESSAGE")).create(JAR_NAME);
    opts.addOption(opt);
  }

  protected void addFileOption(final Options opts) {
    GfxdOption opt;

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_FILE_URL_ARG")).hasArg().isRequired(true)
        .withValueSeparator('=').withDescription(LocalizedResource.getMessage(
            "JARTOOLS_JAR_FILE_MESSAGE")).create(FILE_URL);
    opts.addOption(opt);
  }

  protected Connection getConnection(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey, final boolean hasFileName)
      throws ParseException, SQLException {
    final ConnectionOptions connOpts = new ConnectionOptions();
    final Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (JAR_NAME.equals(opt.getOpt())) {
        this.jarName = opt.getValue();
      }
      else if (hasFileName && FILE_URL.equals(opt.getOpt())) {
        this.fileURL = opt.getValue();
      }
      else if (!handleCommonOption(opt, cmd, cmdDescKey)) {
        if (!handleConnectionOption(opt, connOpts)) {
          Assert.fail(opt.toString());
        }
      }
    }

    return getConnection(connOpts, cmd, cmdDescKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getUsageString(String cmd, String cmdDescKey) {
    return LocalizedResource.getMessage("TOOLS_COMMON_DESC");
  }
}
