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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.GemFireTerminateError;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource;
import com.pivotal.gemfirexd.tools.internal.ToolsBase.ProcessCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.platform.gemfirexd.GemFireXDPeerPlatform;
import org.apache.ddlutils.platform.gemfirexd.GemFireXDPlatform;
import org.apache.ddlutils.task.*;
import org.apache.tools.ant.types.FileSet;

/**
 * Command-line launcher class for some of the tools shipped with DdlUtils
 * (avoiding the need to have and use ant for most cases).
 * 
 * @author swale
 */
public final class GfxdDdlUtils extends ToolsBase {

  // top-level command names keys and values
  static final String WRITE_SCHEMA_TO_XML;
  static final String WRITE_SCHEMA_TO_SQL;
  static final String WRITE_DATA_TO_XML;
  static final String WRITE_DATA_DTD_TO_FILE;
  static final String WRITE_SCHEMA_TO_DB;
  static final String WRITE_DATA_TO_DB;
  static final String REPLAY_FAILED_DMLS;

  // command name usage and description message keys
  private static final String WRITE_SCHEMA_TO_XML_DESC_KEY;
  private static final String WRITE_SCHEMA_TO_SQL_DESC_KEY;
  private static final String WRITE_DATA_TO_XML_DESC_KEY;
  private static final String WRITE_DATA_DTD_TO_FILE_DESC_KEY;
  private static final String WRITE_SCHEMA_TO_DB_DESC_KEY;
  private static final String WRITE_DATA_TO_DB_DESC_KEY;
  private static final String REPLAY_FAILED_DMLS_KEY;

  private static final Map<String, String> validCommands;

  // common task options
  private static final String DATABASE_TYPE;
  private static final String CATALOG_PATTERN;
  private static final String SCHEMA_PATTERN;
  private static final String DELIMITED_IDENTIFIERS;
  private static final String ISOLATION_LEVEL;
  private static final String ISOLATION_LEVEL_RU;
  private static final String ISOLATION_LEVEL_RC;
  private static final String ISOLATION_LEVEL_RR;
  private static final String ISOLATION_LEVEL_SER;

  // common DatabaseToDdlTask options
  private static final String INCLUDE_TABLES;
  private static final String INCLUDE_TABLE_FILTER;
  private static final String EXCLUDE_TABLES;
  private static final String EXCLUDE_TABLE_FILTER;

  // common top-level options
  private static final String VERBOSITY;

  // common command arguments
  private static String URL;
  private static String DRIVER_CLASS;

  // command specific arguments
  private String FILE_NAME;
  private String FILE_NAMES;
  private String SCHEMA_FILE_NAMES;
  private String BATCH_SIZE;
  private String DO_DROPS;
  private String ENSURE_FK_ORDER;
  private String ALTER_IDENTITY_COLUMNS;
  private String XML_SCHEMA_FILES;
  private String TO_DATABASE_TYPE;
  private String EXPORT_GENERIC;
  private String EXPORT_ALL;
  private String ERROR_FILE;

  /**
   * Encapsulates values of common data source options.
   */
  private static final class DataSourceOptions extends ConnectionOptions {
    String url;
    String driverClass;
  }

  /**
   * Encapsulates values of common task options.
   */
  private static final class TaskOptions {
    String dbType;
    String catalogPattern;
    String schemaPattern;
    boolean useDelimitedIdentifiers;
    int isolationLevel = -1;
    String includeTables;
    String includeTableFilter;
    String excludeTables;
    String excludeTableFilter;
  }

  static final int DEFAULT_BATCH_SIZE;

  static {
    WRITE_SCHEMA_TO_XML = LocalizedResource
        .getMessage("DDLUTILS_WRITE_SCHEMA_TO_XML");
    WRITE_SCHEMA_TO_SQL = LocalizedResource
        .getMessage("DDLUTILS_WRITE_SCHEMA_TO_SQL");
    WRITE_DATA_TO_XML = LocalizedResource
        .getMessage("DDLUTILS_WRITE_DATA_TO_XML");
    WRITE_DATA_DTD_TO_FILE = LocalizedResource
        .getMessage("DDLUTILS_WRITE_DATA_DTD_TO_FILE");
    WRITE_SCHEMA_TO_DB = LocalizedResource
        .getMessage("DDLUTILS_WRITE_SCHEMA_TO_DB");
    WRITE_DATA_TO_DB = LocalizedResource
        .getMessage("DDLUTILS_WRITE_DATA_TO_DB");
    REPLAY_FAILED_DMLS = LocalizedResource.getMessage("DDLUTILS_REPLAY_FAILED_DMLS");

    WRITE_SCHEMA_TO_XML_DESC_KEY = "DDLUTILS_WRITE_SCHEMA_TO_XML_DESC";
    WRITE_SCHEMA_TO_SQL_DESC_KEY = "DDLUTILS_WRITE_SCHEMA_TO_SQL_DESC";
    WRITE_DATA_TO_XML_DESC_KEY = "DDLUTILS_WRITE_DATA_TO_XML_DESC";
    WRITE_DATA_DTD_TO_FILE_DESC_KEY = "DDLUTILS_WRITE_DATA_DTD_TO_FILE_DESC";
    WRITE_SCHEMA_TO_DB_DESC_KEY = "DDLUTILS_WRITE_SCHEMA_TO_DB_DESC";
    WRITE_DATA_TO_DB_DESC_KEY = "DDLUTILS_WRITE_DATA_TO_DB_DESC";
    REPLAY_FAILED_DMLS_KEY = "DDLUTILS_REPLAY_FAILED_DMLS_DESC";

    DATABASE_TYPE = LocalizedResource.getMessage("DDLUTILS_DATABASE_TYPE");
    CATALOG_PATTERN = LocalizedResource.getMessage("DDLUTILS_CATALOG_PATTERN");
    SCHEMA_PATTERN = LocalizedResource.getMessage("DDLUTILS_SCHEMA_PATTERN");
    DELIMITED_IDENTIFIERS = LocalizedResource
        .getMessage("DDLUTILS_DELIMITED_IDENTIFIERS");

    ISOLATION_LEVEL = LocalizedResource.getMessage("DDLUTILS_ISOLATION_LEVEL");
    ISOLATION_LEVEL_RU = LocalizedResource
        .getMessage("DDLUTILS_ISOLATION_LEVEL_RU");
    ISOLATION_LEVEL_RC = LocalizedResource
        .getMessage("DDLUTILS_ISOLATION_LEVEL_RC");
    ISOLATION_LEVEL_RR = LocalizedResource
        .getMessage("DDLUTILS_ISOLATION_LEVEL_RR");
    ISOLATION_LEVEL_SER = LocalizedResource
        .getMessage("DDLUTILS_ISOLATION_LEVEL_SER");

    INCLUDE_TABLES = LocalizedResource.getMessage("DDLUTILS_INCLUDE_TABLES");
    INCLUDE_TABLE_FILTER = LocalizedResource
        .getMessage("DDLUTILS_INCLUDE_TABLE_FILTER");
    EXCLUDE_TABLES = LocalizedResource.getMessage("DDLUTILS_EXCLUDE_TABLES");
    EXCLUDE_TABLE_FILTER = LocalizedResource
        .getMessage("DDLUTILS_EXCLUDE_TABLE_FILTER");

    VERBOSITY = LocalizedResource.getMessage("DDLUTILS_VERBOSITY");

    DEFAULT_BATCH_SIZE = 1000;

    validCommands = new LinkedHashMap<String, String>();
    validCommands.put(WRITE_SCHEMA_TO_XML, WRITE_SCHEMA_TO_XML_DESC_KEY);
    validCommands.put(WRITE_SCHEMA_TO_SQL, WRITE_SCHEMA_TO_SQL_DESC_KEY);
    validCommands.put(WRITE_DATA_TO_XML, WRITE_DATA_TO_XML_DESC_KEY);
    validCommands.put(WRITE_DATA_DTD_TO_FILE, WRITE_DATA_DTD_TO_FILE_DESC_KEY);
    validCommands.put(WRITE_SCHEMA_TO_DB, WRITE_SCHEMA_TO_DB_DESC_KEY);
    validCommands.put(WRITE_DATA_TO_DB, WRITE_DATA_TO_DB_DESC_KEY);
    validCommands.put(REPLAY_FAILED_DMLS, REPLAY_FAILED_DMLS_KEY);
  }

  public static void main(String[] args) {
    final GfxdDdlUtils instance = new GfxdDdlUtils();
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
    if (WRITE_SCHEMA_TO_XML.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addWriteSchemaToXMLOptions(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          executeWriteSchemaToXML(cmdLine, cmd, cmdDescKey);
        }
      };
    }
    else if (WRITE_SCHEMA_TO_SQL.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addWriteSchemaToSQLOptions(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          executeWriteSchemaToSQL(cmdLine, cmd, cmdDescKey);
        }
      };
    }
    else if (WRITE_DATA_TO_XML.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addWriteDataToXMLOptions(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          executeWriteDataToXML(cmdLine, cmd, cmdDescKey);
        }
      };
    }
    else if (WRITE_DATA_DTD_TO_FILE.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addWriteDataDtdToFileOptions(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          executeWriteDataDtdToFile(cmdLine, cmd, cmdDescKey);
        }
      };
    }
    else if (WRITE_SCHEMA_TO_DB.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addWriteSchemaToDBOptions(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          executeWriteSchemaToDB(cmdLine, cmd, cmdDescKey);
        }
      };
    }
    else if (WRITE_DATA_TO_DB.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addWriteDataToDBOptions(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          executeWriteDataToDB(cmdLine, cmd, cmdDescKey);
        }
      };
    }
    else if (REPLAY_FAILED_DMLS.equals(cmd)) {
      return new ProcessCommand() {
        @Override
        public final void addCommandOptions(final Options opts) {
          addReplayFailedDMLsOptions(opts);
        }

        @Override
        public final void executeCommand(final CommandLine cmdLine,
            final String cmd, final String cmdDescKey) throws ParseException,
            SQLException {
          executeReplayFailedDMLs(cmdLine, cmd, cmdDescKey);
        }
      };
    }
    else {
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getUsageString(String cmd, String cmdDescKey) {
    return LocalizedResource.getMessage("DDLUTILS_COMMON_DESC");
  }

  protected void addWriteSchemaToXMLOptions(final Options opts) {
    GfxdOption opt;

    FILE_NAME = LocalizedResource.getMessage("TOOLS_FILE");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_PATH_ARG")).hasArg().isRequired(true).withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_SCHEMA_OUT_FILE_MESSAGE")).create(FILE_NAME);
    opts.addOption(opt);

    addCommonTaskOptions(opts);
    addCommonReadTaskOptions(opts);
  }

  protected void addWriteSchemaToSQLOptions(final Options opts) {
    GfxdOption opt;

    FILE_NAME = LocalizedResource.getMessage("TOOLS_FILE");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_PATH_ARG")).hasArg().isRequired(true).withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_SCHEMA_SQL_OUT_FILE_MESSAGE")).create(FILE_NAME);
    opts.addOption(opt);

    XML_SCHEMA_FILES = LocalizedResource
        .getMessage("DDLUTILS_XML_SCHEMA_FILES");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_PATHS_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_XML_SCHEMA_FILES_MESSAGE")).create(XML_SCHEMA_FILES);
    opts.addOption(opt);

    TO_DATABASE_TYPE = LocalizedResource
        .getMessage("DDLUTILS_TO_DATABASE_TYPE");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_TYPE_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_TO_DATABASE_TYPE_MESSAGE")).create(TO_DATABASE_TYPE);
    opts.addOption(opt);

    EXPORT_GENERIC = LocalizedResource.getMessage("DDLUTILS_EXPORT_GENERIC");
    opt = new GfxdOptionBuilder().withDescription(LocalizedResource
        .getMessage("DDLUTILS_EXPORT_GENERIC_MESSAGE")).create(EXPORT_GENERIC);
    opts.addOption(opt);

    EXPORT_ALL = LocalizedResource.getMessage("DDLUTILS_EXPORT_ALL");
    opt = new GfxdOptionBuilder().withDescription(LocalizedResource
        .getMessage("DDLUTILS_EXPORT_ALL_MESSAGE")).create(EXPORT_ALL);
    opts.addOption(opt);

    addCommonTaskOptions(opts);
    addCommonReadTaskOptions(opts);
  }

  protected void addWriteDataToXMLOptions(final Options opts) {
    GfxdOption opt;

    FILE_NAME = LocalizedResource.getMessage("TOOLS_FILE");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_PATH_ARG")).hasArg().isRequired(true).withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_DATA_OUT_FILE_MESSAGE")).create(FILE_NAME);
    opts.addOption(opt);

    addCommonTaskOptions(opts);
    addCommonReadTaskOptions(opts);
  }

  protected void addWriteDataDtdToFileOptions(final Options opts) {
    GfxdOption opt;

    FILE_NAME = LocalizedResource.getMessage("TOOLS_FILE");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_PATH_ARG")).hasArg().isRequired(true).withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_DTD_OUT_FILE_MESSAGE")).create(FILE_NAME);
    opts.addOption(opt);
  }

  protected void addWriteSchemaToDBOptions(final Options opts) {
    GfxdOption opt;

    FILE_NAMES = LocalizedResource.getMessage("DDLUTILS_FILES");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_PATHS_ARG")).hasArg().isRequired(true).withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_SCHEMA_FILES_ONLY_MESSAGE")).create(FILE_NAMES);
    opts.addOption(opt);

    DO_DROPS = LocalizedResource.getMessage("DDLUTILS_DO_DROPS");
    opt = new GfxdOptionBuilder().withDescription(
        LocalizedResource.getMessage("DDLUTILS_DO_DROPS_MESSAGE")).create(
        DO_DROPS);
    opts.addOption(opt);

    ALTER_IDENTITY_COLUMNS = LocalizedResource.getMessage(
        "DDLUTILS_ALTER_IDENTITY_COLUMNS");
    opt = new GfxdOptionBuilder().withDescription(
        LocalizedResource.getMessage("DDLUTILS_ALTER_IDENTITY_COLUMNS_MESSAGE"))
        .create(ALTER_IDENTITY_COLUMNS);
    opts.addOption(opt);

    addCommonTaskOptions(opts);
  }

  protected void addWriteDataToDBOptions(final Options opts) {
    GfxdOption opt;

    SCHEMA_FILE_NAMES = LocalizedResource.getMessage("DDLUTILS_SCHEMA_FILES");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_PATHS_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_SCHEMA_FILES_MESSAGE")).create(SCHEMA_FILE_NAMES);
    opts.addOption(opt);

    FILE_NAMES = LocalizedResource.getMessage("DDLUTILS_FILES");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_PATHS_ARG")).hasArg().isRequired(true).withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_DATA_FILES_MESSAGE")).create(FILE_NAMES);
    opts.addOption(opt);

    BATCH_SIZE = LocalizedResource.getMessage("DDLUTILS_BATCH_SIZE");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_SIZE_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_BATCH_SIZE_MESSAGE")).create(BATCH_SIZE);
    opts.addOption(opt);

    ENSURE_FK_ORDER = LocalizedResource.getMessage("DDLUTILS_ENSURE_FK_ORDER");
    opt = new GfxdOptionBuilder().hasArg().withValueSeparator('=').withDescription(
        LocalizedResource.getMessage("DDLUTILS_ENSURE_FK_ORDER_MESSAGE"))
        .create(ENSURE_FK_ORDER);
    opts.addOption(opt);

    ALTER_IDENTITY_COLUMNS = LocalizedResource.getMessage(
        "DDLUTILS_ALTER_IDENTITY_COLUMNS");
    opt = new GfxdOptionBuilder().withDescription(
        LocalizedResource.getMessage("DDLUTILS_ALTER_IDENTITY_COLUMNS_MESSAGE"))
        .create(ALTER_IDENTITY_COLUMNS);
    opts.addOption(opt);

    addCommonTaskOptions(opts);
  }

  protected void addReplayFailedDMLsOptions(final Options opts) {
    GfxdOption opt;
    
    ERROR_FILE = LocalizedResource.getMessage("DDLUTILS_ERROR_FILE");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_PATH_ARG")).hasArg().isRequired(true).withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_ERROR_FILE_MESSAGE")).create(ERROR_FILE);
    opts.addOption(opt);
    
    addBasicConnectionOptions(opts);
    
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_USERNAME_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "TOOLS_USERNAME_MESSAGE")).create(USERNAME);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "TOOLS_PASSWORD_ARG")).hasOptionalArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "TOOLS_PASSWORD_MESSAGE")).create(PASSWORD);
    opts.addOption(opt);
  }
  
  protected void addCommonTaskOptions(final Options opts) {
    GfxdOption opt;

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_TYPE_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_DATABASE_TYPE_MESSAGE")).create(DATABASE_TYPE);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_PATTERN")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_CATALOG_PATTERN_MESSAGE")).create(CATALOG_PATTERN);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_PATTERN")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_SCHEMA_PATTERN_MESSAGE")).create(SCHEMA_PATTERN);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withDescription(
        LocalizedResource.getMessage("DDLUTILS_DELIMITED_IDENTIFIERS_MESSAGE"))
        .create(DELIMITED_IDENTIFIERS);
    opts.addOption(opt);
  }

  protected void addCommonReadTaskOptions(final Options opts) {
    GfxdOption opt;

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_TABLE_LIST")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_INCLUDE_TABLES_MESSAGE")).create(INCLUDE_TABLES);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_TABLE_FILTER")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_INCLUDE_TABLE_FILTER_MESSAGE"))
            .create(INCLUDE_TABLE_FILTER);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_TABLE_LIST")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_EXCLUDE_TABLES_MESSAGE")).create(EXCLUDE_TABLES);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_TABLE_FILTER")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_EXCLUDE_TABLE_FILTER_MESSAGE"))
            .create(EXCLUDE_TABLE_FILTER);
    opts.addOption(opt);

    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_LEVEL_ARG")).hasArg().withValueSeparator('=').withDescription(
        LocalizedResource.getMessage("DDLUTILS_ISOLATION_LEVEL_MESSAGE"))
        .create(ISOLATION_LEVEL);
    opts.addOption(opt);
  }

  @Override
  protected void addCommonOptions(final Options opts) {
    GfxdOption opt;

    super.addCommonOptions(opts);
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_LEVEL_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_VERBOSITY_MESSAGE")).create(VERBOSITY);
    opts.addOption(opt);
  }

  @Override
  protected void addConnectionOptions(final Options opts) {
    super.addConnectionOptions(opts);
    addBasicConnectionOptions(opts);
  }
  
  protected void addBasicConnectionOptions(final Options opts) {
    GfxdOption opt;
    URL = LocalizedResource.getMessage("DDLUTILS_URL");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_URL_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_URL_MESSAGE")).create(URL);
    opts.addOption(opt);

    DRIVER_CLASS = LocalizedResource.getMessage("DDLUTILS_DRIVER_CLASS");
    opt = new GfxdOptionBuilder().withArgName(LocalizedResource.getMessage(
        "DDLUTILS_DRIVER_CLASS_ARG")).hasArg().withValueSeparator('=')
        .withDescription(LocalizedResource.getMessage(
            "DDLUTILS_DRIVER_CLASS_MESSAGE")).create(DRIVER_CLASS);
    opts.addOption(opt);
  }

  protected void executeWriteSchemaToXML(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey) throws ParseException,
      SQLException {
    String schemaFileName = null;
    final DataSourceOptions dsOpts = new DataSourceOptions();
    TaskOptions resultTaskOpts, taskOpts = null;
    String verbosity = null;
    Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (!handleDataSourceOption(opt, dsOpts)
          && !handleCommonOption(opt, cmd, cmdDescKey)) {
        if ((resultTaskOpts = handleCommonTaskOption(opt, true,
            taskOpts)) == null) {
          if (FILE_NAME.equals(opt.getOpt())) {
            schemaFileName = opt.getValue();
          }
          else {
            verbosity = handleVerbosityOption(opt);
          }
        }
        else if (taskOpts == null) {
          taskOpts = resultTaskOpts;
        }
      }
    }
    BasicDataSource dataSource = handleDataSourceOptions(dsOpts, cmd,
        cmdDescKey);
    final DatabaseToDdlTask fromDBTask = new DatabaseToDdlTask();
    if (verbosity != null) {
      fromDBTask.setVerbosity(new VerbosityLevel(verbosity));
    }
    fromDBTask.addConfiguredDatabase(dataSource);
    setCommonTaskOptions(fromDBTask, taskOpts);
    setCommonReadTaskOptions(fromDBTask, taskOpts);

    WriteSchemaToFileCommand writeSchemaToFile = new WriteSchemaToFileCommand();
    File schemaFile = new File(schemaFileName);
    writeSchemaToFile.setFailOnError(true);
    writeSchemaToFile.setOutputFile(schemaFile);

    fromDBTask.addWriteSchemaToFile(writeSchemaToFile);
    fromDBTask.execute();
  }

  protected void executeWriteSchemaToSQL(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey) throws ParseException,
      SQLException {
    String schemaFileName = null;
    String[] xmlSchemaFiles = null;
    String toDbType = null;
    boolean exportDDLs = true;
    boolean exportAll = false;
    final DataSourceOptions dsOpts = new DataSourceOptions();
    TaskOptions resultTaskOpts, taskOpts = null;
    String verbosity = null;
    Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (!handleDataSourceOption(opt, dsOpts)
          && !handleCommonOption(opt, cmd, cmdDescKey)) {
        if ((resultTaskOpts = handleCommonTaskOption(opt, true,
            taskOpts)) == null) {
          final String optName = opt.getOpt();
          if (FILE_NAME.equals(optName)) {
            schemaFileName = opt.getValue();
          }
          else if (XML_SCHEMA_FILES.equals(optName)) {
            xmlSchemaFiles = opt.getValue().split(",");
          }
          else if (TO_DATABASE_TYPE.equals(optName)) {
            toDbType = opt.getValue();
          }
          else if (EXPORT_GENERIC.equals(optName)) {
            exportDDLs = false;
          }
          else if (EXPORT_ALL.equals(optName)) {
            exportAll = true;
          }
          else {
            verbosity = handleVerbosityOption(opt);
          }
        }
        else if (taskOpts == null) {
          taskOpts = resultTaskOpts;
        }
      }
    }
    BasicDataSource dataSource = handleDataSourceOptions(dsOpts, cmd,
        cmdDescKey);
    final DatabaseTaskBase dbTask;

    WriteSchemaSqlToFileCommand writeSchemaToFile =
        new WriteSchemaSqlToFileCommand();
    File schemaFile = new File(schemaFileName);
    writeSchemaToFile.setFailOnError(true);
    writeSchemaToFile.setDoDrops(true);
    writeSchemaToFile.setAlterDatabase(false);
    writeSchemaToFile.setExportDDLs(exportDDLs);
    writeSchemaToFile.setExportAll(exportAll);
    writeSchemaToFile.setOutputFile(schemaFile);
    if (toDbType != null) {
      writeSchemaToFile.setDatabaseType(toDbType);
    }

    if (xmlSchemaFiles == null) {
      final DatabaseToDdlTask fromDBTask = new DatabaseToDdlTask();
      setCommonReadTaskOptions(fromDBTask, taskOpts);

      fromDBTask.addWriteSchemaSqlToFile(writeSchemaToFile);
      dbTask = fromDBTask;
    }
    else {
      final DdlToDatabaseTask toDBTask = new DdlToDatabaseTask();
      if (xmlSchemaFiles.length == 1) {
        toDBTask.setSchemaFile(new File(xmlSchemaFiles[0]));
      }
      else {
        FileSet schemaFileSet = new FileSet();
        schemaFileSet.appendIncludes(xmlSchemaFiles);
        toDBTask.addConfiguredFileset(schemaFileSet);
      }

      toDBTask.addWriteSchemaSqlToFile(writeSchemaToFile);
      dbTask = toDBTask;
    }

    if (verbosity != null) {
      dbTask.setVerbosity(new VerbosityLevel(verbosity));
    }
    dbTask.addConfiguredDatabase(dataSource);
    setCommonTaskOptions(dbTask, taskOpts);

    dbTask.execute();
  }

  protected void executeWriteDataToXML(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey) throws ParseException,
      SQLException {
    String dataFileName = null;
    final DataSourceOptions dsOpts = new DataSourceOptions();
    TaskOptions resultTaskOpts, taskOpts = null;
    String verbosity = null;
    Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (!handleDataSourceOption(opt, dsOpts)
          && !handleCommonOption(opt, cmd, cmdDescKey)) {
        if ((resultTaskOpts = handleCommonTaskOption(opt, true,
            taskOpts)) == null) {
          if (FILE_NAME.equals(opt.getOpt())) {
            dataFileName = opt.getValue();
          }
          else {
            verbosity = handleVerbosityOption(opt);
          }
        }
        else if (taskOpts == null) {
          taskOpts = resultTaskOpts;
        }
      }
    }
    BasicDataSource dataSource = handleDataSourceOptions(dsOpts, cmd,
        cmdDescKey);
    final DatabaseToDdlTask fromDBTask = new DatabaseToDdlTask();
    if (verbosity != null) {
      fromDBTask.setVerbosity(new VerbosityLevel(verbosity));
    }
    fromDBTask.addConfiguredDatabase(dataSource);
    setCommonTaskOptions(fromDBTask, taskOpts);
    setCommonReadTaskOptions(fromDBTask, taskOpts);

    WriteDataToFileCommand writeDataToFile = new WriteDataToFileCommand();
    File dataFile = new File(dataFileName);
    writeDataToFile.setFailOnError(true);
    writeDataToFile.setOutputFile(dataFile);

    fromDBTask.addWriteDataToFile(writeDataToFile);
    fromDBTask.execute();
  }

  protected void executeWriteDataDtdToFile(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey) throws ParseException,
      SQLException {
    String dtdFileName = null;
    final DataSourceOptions dsOpts = new DataSourceOptions();
    String verbosity = null;
    Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (!handleDataSourceOption(opt, dsOpts)
          && !handleCommonOption(opt, cmd, cmdDescKey)) {
        if (FILE_NAME.equals(opt.getOpt())) {
          dtdFileName = opt.getValue();
        }
        else {
          verbosity = handleVerbosityOption(opt);
        }
      }
    }
    BasicDataSource dataSource = handleDataSourceOptions(dsOpts, cmd,
        cmdDescKey);
    final DatabaseToDdlTask fromDBTask = new DatabaseToDdlTask();
    if (verbosity != null) {
      fromDBTask.setVerbosity(new VerbosityLevel(verbosity));
    }
    fromDBTask.addConfiguredDatabase(dataSource);

    WriteDtdToFileCommand writeDtdToFile = new WriteDtdToFileCommand();
    File dtdFile = new File(dtdFileName);
    writeDtdToFile.setFailOnError(true);
    writeDtdToFile.setOutputFile(dtdFile);

    fromDBTask.addWriteDtdToFile(writeDtdToFile);
    fromDBTask.execute();
  }

  protected void executeWriteSchemaToDB(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey) throws ParseException,
      SQLException {
    String[] schemaFileNames = null;
    boolean doDrops = false;
    boolean alterIdentityColumns = false;
    final DataSourceOptions dsOpts = new DataSourceOptions();
    TaskOptions resultTaskOpts, taskOpts = null;
    String verbosity = null;
    Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (!handleDataSourceOption(opt, dsOpts)
          && !handleCommonOption(opt, cmd, cmdDescKey)) {
        if ((resultTaskOpts = handleCommonTaskOption(opt, false,
            taskOpts)) == null) {
          final String optName = opt.getOpt();
          if (FILE_NAMES.equals(optName)) {
            schemaFileNames = opt.getValue().split(",");
          }
          else if (DO_DROPS.equals(optName)) {
            doDrops = true;
          }
          else if (ALTER_IDENTITY_COLUMNS.equals(optName)) {
            alterIdentityColumns = true;
          }
          else {
            verbosity = handleVerbosityOption(opt);
          }
        }
        else if (taskOpts == null) {
          taskOpts = resultTaskOpts;
        }
      }
    }
    BasicDataSource dataSource = handleDataSourceOptions(dsOpts, cmd,
        cmdDescKey);
    final DdlToDatabaseTask toDBTask = new DdlToDatabaseTask();
    if (verbosity != null) {
      toDBTask.setVerbosity(new VerbosityLevel(verbosity));
    }
    toDBTask.addConfiguredDatabase(dataSource);
    if (schemaFileNames.length == 1) {
      File schemaFile = new File(schemaFileNames[0]);
      toDBTask.setSchemaFile(schemaFile);
    }
    else {
      FileSet schemaFileSet = new FileSet();
      schemaFileSet.appendIncludes(schemaFileNames);
      toDBTask.addConfiguredFileset(schemaFileSet);
    }
    setCommonTaskOptions(toDBTask, taskOpts);

    WriteSchemaToDatabaseCommand writeSchemaToDB =
        new WriteSchemaToDatabaseCommand();
    if (doDrops) {
      writeSchemaToDB.setDoDrops(true);
      writeSchemaToDB.setAlterDatabase(false);
    }
    else {
      writeSchemaToDB.setAlterDatabase(true);
    }
    writeSchemaToDB.setAddIdentityUsingAlterTable(alterIdentityColumns);
    writeSchemaToDB.setFailOnError(true);

    toDBTask.addWriteSchemaToDatabase(writeSchemaToDB);
    toDBTask.execute();
  }

  protected void executeReplayFailedDMLs(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey) throws ParseException,
      SQLException {
    String dataFileName = null;
    final DataSourceOptions dsOpts = new DataSourceOptions();
    TaskOptions resultTaskOpts, taskOpts = null;
    String verbosity = null;
    Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (!handleDataSourceOption(opt, dsOpts)
          && !handleCommonOption(opt, cmd, cmdDescKey)) {
        if ((resultTaskOpts = handleCommonTaskOption(opt, false,
            taskOpts)) == null) {
          final String optName = opt.getOpt();
          if (ERROR_FILE.equals(optName)) {
            dataFileName = opt.getValue();
          }
          else {
            verbosity = handleVerbosityOption(opt);
          }
        }
        else if (taskOpts == null) {
          taskOpts = resultTaskOpts;
        }
      }
    }
    
    BasicDataSource dataSource = handleDataSourceOptions(dsOpts, cmd,
        cmdDescKey);
    ReplayFailedDMLsCommand replayCommand = new ReplayFailedDMLsCommand();
    // set props on the command
    replayCommand.setErrorFileName(dataFileName);
    
    ReplayFailedDMLsTask dbTask = new ReplayFailedDMLsTask();
    dbTask.addReplayFailedDMLs(replayCommand);
    
    if (verbosity != null) {
      dbTask.setVerbosity(new VerbosityLevel(verbosity));
    }
    dbTask.addConfiguredDatabase(dataSource);
    setCommonTaskOptions(dbTask, taskOpts);

    dbTask.execute();
  }
  
  protected void executeWriteDataToDB(final CommandLine cmdLine,
      final String cmd, final String cmdDescKey) throws ParseException,
      SQLException {
    String[] schemaFileNames = null;
    String[] dataFileNames = null;
    int batchSize = DEFAULT_BATCH_SIZE;
    boolean ensureFKOrder = false;
    boolean alterIdentityColumns = false;
    final DataSourceOptions dsOpts = new DataSourceOptions();
    TaskOptions resultTaskOpts, taskOpts = null;
    String verbosity = null;
    Iterator<?> iter = cmdLine.iterator();
    GfxdOption opt;
    while (iter.hasNext()) {
      opt = (GfxdOption)iter.next();
      if (!handleDataSourceOption(opt, dsOpts)
          && !handleCommonOption(opt, cmd, cmdDescKey)) {
        if ((resultTaskOpts = handleCommonTaskOption(opt, false,
            taskOpts)) == null) {
          final String optName = opt.getOpt();
          if (FILE_NAMES.equals(optName)) {
            dataFileNames = opt.getValue().split(",");
          }
          else if (SCHEMA_FILE_NAMES.equals(optName)) {
            schemaFileNames = opt.getValue().split(",");
          }
          else if (BATCH_SIZE.equals(optName)) {
            batchSize = Integer.parseInt(opt.getValue());
          }
          else if (ENSURE_FK_ORDER.equals(optName)) {
            ensureFKOrder = Boolean.parseBoolean(opt.getValue());
          }
          else if (ALTER_IDENTITY_COLUMNS.equals(optName)) {
            alterIdentityColumns = true;
          }
          else {
            verbosity = handleVerbosityOption(opt);
          }
        }
        else if (taskOpts == null) {
          taskOpts = resultTaskOpts;
        }
      }
    }
    BasicDataSource dataSource = handleDataSourceOptions(dsOpts, cmd,
        cmdDescKey);
    WriteDataToDatabaseCommand writeDataToDB = new WriteDataToDatabaseCommand();
    if (batchSize > 1) {
      writeDataToDB.setUseBatchMode(true);
      writeDataToDB.setBatchSize(batchSize);
    }
    if (dataFileNames.length == 1) {
      File dataFile = new File(dataFileNames[0]);
      writeDataToDB.setDataFile(dataFile);
    }
    else {
      FileSet dataFileSet = new FileSet();
      dataFileSet.appendIncludes(dataFileNames);
      writeDataToDB.addConfiguredFileset(dataFileSet);
    }
    writeDataToDB.setEnsureForeignKeyOrder(ensureFKOrder);
    writeDataToDB.setAddIdentityUsingAlterTable(alterIdentityColumns);
    writeDataToDB.setFailOnError(true);

    final DatabaseTaskBase dbTask;
    if (schemaFileNames != null) {
      final DdlToDatabaseTask toDBTask = new DdlToDatabaseTask();
      if (schemaFileNames.length == 1) {
        File schemaFile = new File(schemaFileNames[0]);
        toDBTask.setSchemaFile(schemaFile);
      }
      else {
        FileSet schemaFileSet = new FileSet();
        schemaFileSet.appendIncludes(schemaFileNames);
        toDBTask.addConfiguredFileset(schemaFileSet);
      }
      toDBTask.addWriteDataToDatabase(writeDataToDB);
      dbTask = toDBTask;
    }
    else {
      final DatabaseToDdlTask toDBTask = new DatabaseToDdlTask();
      toDBTask.addWriteDataToDatabase(writeDataToDB);
      dbTask = toDBTask;
    }
    if (verbosity != null) {
      dbTask.setVerbosity(new VerbosityLevel(verbosity));
    }
    dbTask.addConfiguredDatabase(dataSource);
    setCommonTaskOptions(dbTask, taskOpts);

    dbTask.execute();
  }

  protected boolean handleDataSourceOption(final GfxdOption opt,
      final DataSourceOptions dsOpts) {
    String optValue;
    if ((optValue = opt.getOptionValue(URL)) != null) {
      dsOpts.url = optValue;
    }
    else if ((optValue = opt.getOptionValue(DRIVER_CLASS)) != null) {
      dsOpts.driverClass = optValue;
    }
    else {
      return handleConnectionOption(opt, dsOpts);
    }
    return true;
  }

  /**
   * Return a non-null TaskOptions object with the set TaskOptions field if the
   * option is one of those handled by TaskOptions else return null. If incoming
   * taskOpts object is non-null then set the field on that object and return.
   * 
   * Note that you cannot just pass back the result of previous invocation since
   * if the current option is not one handled by TaskOptions then a null will be
   * returned even if previous invocation had set the TaskOptions to non-null.
   */
  protected TaskOptions handleCommonTaskOption(final GfxdOption opt,
      boolean fromDatabase, TaskOptions taskOpts) throws ParseException {
    String optValue;
    if ((optValue = opt.getOptionValue(DATABASE_TYPE)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.dbType = optValue;
    }
    else if ((optValue = opt.getOptionValue(CATALOG_PATTERN)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.catalogPattern = optValue;
    }
    else if ((optValue = opt.getOptionValue(SCHEMA_PATTERN)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.schemaPattern = optValue;
    }
    else if (DELIMITED_IDENTIFIERS.equals(opt.getOpt())) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.useDelimitedIdentifiers = true;
    }
    else if ((optValue = opt.getOptionValue(ISOLATION_LEVEL)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      if (ISOLATION_LEVEL_RU.equalsIgnoreCase(optValue)) {
        taskOpts.isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
      }
      else if (ISOLATION_LEVEL_RC.equalsIgnoreCase(optValue)) {
        taskOpts.isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
      }
      else if (ISOLATION_LEVEL_RR.equalsIgnoreCase(optValue)) {
        taskOpts.isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
      }
      else if (ISOLATION_LEVEL_SER.equalsIgnoreCase(optValue)) {
        taskOpts.isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
      }
      else {
        throw new ParseException(LocalizedResource.getMessage(
            "DDLUTILS_UNKNOWN_ISOLATION_LEVEL", optValue));
      }
    }
    else if (!fromDatabase) {
      return null;
    }
    else if ((optValue = opt.getOptionValue(INCLUDE_TABLES)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.includeTables = optValue;
    }
    else if ((optValue = opt.getOptionValue(INCLUDE_TABLE_FILTER)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.includeTableFilter = optValue;
    }
    else if ((optValue = opt.getOptionValue(EXCLUDE_TABLES)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.excludeTables = optValue;
    }
    else if ((optValue = opt.getOptionValue(EXCLUDE_TABLE_FILTER)) != null) {
      if (taskOpts == null) {
        taskOpts = new TaskOptions();
      }
      taskOpts.excludeTableFilter = optValue;
    }
    else {
      return null;
    }
    return taskOpts;
  }

  protected String handleVerbosityOption(final GfxdOption opt) {
    return opt.getOptionValue(VERBOSITY);
  }

  protected BasicDataSource handleDataSourceOptions(
      final DataSourceOptions dsOpts, final String cmd,
      final String cmdDescKey) {

    if (dsOpts.url != null) {
      // client options should not be present in this case
      if (dsOpts.clientPort >= 0 || dsOpts.clientBindAddress != null) {
        showUsage(LocalizedResource.getMessage(
            "DDLUTILS_BOTH_URL_CLIENT_ERROR"), cmd, cmdDescKey);
        throw new GemFireTerminateError(
            "exiting due to incorrect connection options", 1);
      }
      if (dsOpts.driverClass == null) {
        showUsage(LocalizedResource.getMessage(
            "DDLUTILS_MISSING_DRIVER_CLASS_ERROR"), cmd, cmdDescKey);
        throw new GemFireTerminateError(
            "exiting due to missing driverClass option", 1);
      }
    }
    else {
      handleConnectionOptions(dsOpts, cmd, cmdDescKey);
    }

    final BasicDataSource dataSource = new BasicDataSource();
    final StringBuilder urlBuilder = new StringBuilder();
    if (dsOpts.url != null) {
      dataSource.setDriverClassName(dsOpts.driverClass);
      urlBuilder.append(dsOpts.url);
    }
    else if (dsOpts.clientPort < 0) {
      dataSource.setDriverClassName(GemFireXDPeerPlatform.JDBC_PEER_DRIVER);
      urlBuilder.append(Attribute.PROTOCOL).append(";host-data=false");
    }
    else {
      final String hostName = dsOpts.clientBindAddress != null
          ? dsOpts.clientBindAddress : "localhost";
      dataSource.setDriverClassName(GemFireXDPlatform.JDBC_CLIENT_DRIVER);
      urlBuilder.append(Attribute.DNC_PROTOCOL).append(hostName).append(':')
          .append(dsOpts.clientPort).append('/');
    }
    if (dsOpts.properties.length() > 0) {
      urlBuilder.append(dsOpts.properties);
    }
    dataSource.setUrl(urlBuilder.toString());
    if (dsOpts.user != null) {
      dataSource.setUsername(dsOpts.user);
    }
    if (dsOpts.password != null) {
      dataSource.setPassword(dsOpts.password);
    }
    return dataSource;
  }

  protected void setCommonTaskOptions(final DatabaseTaskBase task,
      final TaskOptions taskOpts) {
    if (taskOpts != null) {
      if (taskOpts.dbType != null) {
        task.setDatabaseType(taskOpts.dbType);
      }
      if (taskOpts.catalogPattern != null) {
        task.setCatalogPattern(taskOpts.catalogPattern);
      }
      if (taskOpts.schemaPattern != null) {
        task.setSchemaPattern(taskOpts.schemaPattern);
      }
      if (taskOpts.isolationLevel >= 0) {
        task.setIsolationLevel(taskOpts.isolationLevel);
      }
      task.setUseDelimitedSqlIdentifiers(taskOpts.useDelimitedIdentifiers);
    }
  }

  protected void setCommonReadTaskOptions(final DatabaseToDdlTask task,
      final TaskOptions taskOpts) {
    if (taskOpts != null) {
      if (taskOpts.includeTables != null) {
        task.setIncludeTables(taskOpts.includeTables);
      }
      if (taskOpts.includeTableFilter != null) {
        task.setIncludeTableFilter(taskOpts.includeTableFilter);
      }
      if (taskOpts.excludeTables != null) {
        task.setExcludeTables(taskOpts.excludeTables);
      }
      if (taskOpts.excludeTableFilter != null) {
        task.setExcludeTableFilter(taskOpts.excludeTableFilter);
      }
    }
  }
}
