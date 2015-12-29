package org.apache.ddlutils.task;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.platform.CreationParameters;
import org.apache.tools.ant.BuildException;

/**
 * Parses the schema XML files specified in the enclosing task, and writes the SQL statements
 * necessary to create this schema in the database, to a file. Note that this SQL is
 * database specific and hence this subtask requires that for the enclosing task, either a
 * data source is specified (via the <code>database</code> sub element) or the
 * <code>databaseType</code> attribute is used to specify the database type.
 * 
 * @version $Revision: 289996 $
 * @ant.task name="writeSchemaSqlToFile"
 */
public class WriteSchemaSqlToFileCommand extends DatabaseCommandWithCreationParameters
{
    /** The file to output the DTD to. */
    private File _outputFile;
    /** Whether to alter or re-set the database if it already exists. */
    private boolean _alterDb = true;
    /** Whether to drop tables and the associated constraints if necessary. */
    private boolean _doDrops = true;

    /**
     * Specifies the name of the file to write the SQL commands to.
     * 
     * @param outputFile The output file
     * @ant.required
     */
    public void setOutputFile(File outputFile)
    {
        _outputFile = outputFile;
    }

    /**
     * Determines whether to alter the database if it already exists, or re-set it.
     * 
     * @return <code>true</code> if to alter the database
     */
    protected boolean isAlterDatabase()
    {
        return _alterDb;
    }

    /**
     * Specifies whether DdlUtils shall generate SQL to alter an existing database rather
     * than SQL for clearing it and creating it new.
     * 
     * @param alterTheDb <code>true</code> if SQL to alter the database shall be created
     * @ant.not-required Per default SQL for altering the database is created
     */
    public void setAlterDatabase(boolean alterTheDb)
    {
        _alterDb = alterTheDb;
    }

    /**
     * Determines whether SQL is generated to drop tables and the associated constraints
     * if necessary.
     * 
     * @return <code>true</code> if drops SQL shall be generated if necessary
     */
    protected boolean isDoDrops()
    {
        return _doDrops;
    }

    /**
     * Specifies whether SQL for dropping tables, external constraints, etc. is created if necessary.
     * Note that this is only relevant when <code>alterDatabase</code> is <code>false</code>.
     * 
     * @param doDrops <code>true</code> if drops shall be performed if necessary
     * @ant.not-required Per default, drop SQL statements are created
     */
    public void setDoDrops(boolean doDrops)
    {
        _doDrops = doDrops;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(DatabaseTaskBase task, Database model) throws BuildException
    {
        if (_outputFile == null)
        {
            throw new BuildException("No output file specified");
        }
        if (_outputFile.exists() && !_outputFile.canWrite())
        {
            throw new BuildException("Cannot overwrite output file "+_outputFile.getAbsolutePath());
        }

        Platform           platform        = getPlatform();
        boolean            isCaseSensitive = platform.isDelimitedIdentifierModeOn();
        CreationParameters params          = getFilteredParameters(model, platform.getName(), isCaseSensitive);
        FileWriter         writer          = null;

        try
        {
            writer = new FileWriter(_outputFile);

            platform.setScriptModeOn(true);
            if (platform.getPlatformInfo().isSqlCommentsSupported())
            {
                // we're generating SQL comments if possible
                platform.setSqlCommentsOn(true);
            }

// GemStone changes BEGIN
            if (isExportDDLs() && platform.getPlatformInfo()
                .isDDLExportSupported()) {
              if (_doDrops) {
                platform.getSqlBuilder().setWriter(writer);
                platform.getSqlBuilder().dropTables(model);
                // TODO: SW: also add drops of all other database objects
                // with "IF EXISTS" when it has been added for all CREATEs
              }
              writer.write(platform.writeAllDDLs(isExportAll()));
              _log.info("Exported schema SQL to "
                  + _outputFile.getAbsolutePath());
              return;
            }
            else if (isExportAll()) {
              // cannot perform export all for this case
              if (!isExportDDLs()) {
                throw new DdlUtilsException(
                    "Cannot export all in generic mode");
              }
              else {
                throw new DdlUtilsException("Platform " + platform.getName()
                    + " does not support full export");
              }
            }
// GemStone changes END
            boolean shouldAlter = isAlterDatabase();

            if (shouldAlter)
            {
                if (getDataSource() == null)
                {
                    shouldAlter = false;
                    _log.warn("Cannot alter the database because no database connection was specified." +
                              " SQL for database creation will be generated instead.");
                }
                else
                {
                    try
                    {
                        Connection connection = getDataSource().getConnection();

                        connection.close();
                    }
                    catch (SQLException ex)
                    {
                        shouldAlter = false;
                        _log.warn("Could not establish a connection to the specified database, " +
                                  "so SQL for database creation will be generated instead.",
                                  ex);
                    }
                }
            }
            if (shouldAlter)
            {
                Database currentModel = (getCatalogPattern() != null) || (getSchemaPattern() != null) ?
                                             platform.readModelFromDatabase("unnamed", getCatalogPattern(), getSchemaPattern(), null) :
                                             platform.readModelFromDatabase("unnamed");

                writer.write(platform.getAlterModelSql(currentModel, model, params));
            }
            else
            {
                writer.write(platform.getCreateModelSql(model, params, _doDrops, !isFailOnError()));
            }
            _log.info("Written schema SQL to " + _outputFile.getAbsolutePath());
        }
        catch (Exception ex)
        {
            handleException(ex, ex.getMessage());
        }
        finally
        {
            if (writer != null)
            {
                try
                {
                    writer.close();
                }
                catch (IOException ex)
                {
                    _log.error("Could not close file " + _outputFile.getAbsolutePath(), ex);
                }
            }
        }
    }
// GemStone changes BEGIN

    /** special mode to get all DDLs directly (only for GemFireXD) */
    private boolean _exportDDLs;

    /**
     * export all database objects including jars/listeners etc (only GemFireXD)
     */
    private boolean _exportAll;

    /**
     * Return true if DDLs have to be exported directly by the target platform
     * database (only for GemFireXD).
     */
    public boolean isExportDDLs() {
      return _exportDDLs;
    }

    /**
     * Return true if all database object have to be exported directly by the
     * target platform database (only for GemFireXD).
     */
    public boolean isExportAll() {
      return _exportAll;
    }

    /**
     * Set the flag for {@link #isExportDDLs()}.
     */
    public void setExportDDLs(boolean exportDDLs) {
      _exportDDLs = exportDDLs;
      if (exportDDLs) {
        // disable alter database and enable drops
        setAlterDatabase(false);
        setDoDrops(true);
      }
    }

    /**
     * Set the flag for {@link #isExportAll()}.
     */
    public void setExportAll(boolean exportAll) {
      _exportAll = exportAll;
    }

    // allow write platform to be different from the read e.g. read from MySQL
    // but write GemFireXD compliant SQL; taken from DatabaseTaskBase

    /**
     * Sets the platform configuration.
     * 
     * @param platformConf
     *          The platform configuration
     */
    @Override
    protected void setPlatformConfiguration(
        final PlatformConfiguration platformConf) {
      // don't overwrite platform configuration if it has been set explicitly
      // see #51732
      if (getDatabaseType() == null || getDataSource() == null) {
        _platformConf = platformConf;
      }
    }

    /**
     * Specifies the database type. You should only need to set this if DdlUtils
     * is not able to derive the setting from the name of the used jdbc driver or
     * the jdbc connection url. If you have to specify this, please post your jdbc
     * driver and connection url combo to the user mailing list so that DdlUtils
     * can be enhanced to support this combo.<br/>
     * Valid values are currently:<br/>
     * <code>axion, cloudscape, db2, derby, firebird, hsqldb, interbase,
     * maxdb, mckoi, mssql, mysql, mysql5, oracle, oracle9, oracle10, oracle11,
     * postgresql, sapdb, gemfirexd, sybase</code>
     * 
     * @param type
     *          The database type
     * @ant.not-required Per default, DdlUtils tries to determine the database
     *                   type via JDBC.
     */
    public void setDatabaseType(String type) {
      if ((type != null) && (type.length() > 0)) {
        _platformConf.setDatabaseType(type);
      }
    }

    /**
     * Adds the data source to use for accessing the database.
     * 
     * @param dataSource
     *          The data source
     */
    public void addConfiguredDatabase(BasicDataSource dataSource) {
      _platformConf.setDataSource(dataSource);
    }

    /**
     * Specifies a pattern that defines which database catalogs to use. For some
     * more info on catalog patterns and JDBC, see <a href=
     * "http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html"
     * >java.sql.DatabaseMetaData</a>.
     * 
     * @param catalogPattern
     *          The catalog pattern
     * @ant.not-required Per default no specific catalog is used.
     */
    public void setCatalogPattern(String catalogPattern) {
      if ((catalogPattern != null) && (catalogPattern.length() > 0)) {
        _platformConf.setCatalogPattern(catalogPattern);
      }
    }

    /**
     * Specifies a pattern that defines which database schemas to use. For some
     * more info on schema patterns and JDBC, see <a href=
     * "http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html"
     * >java.sql.DatabaseMetaData</a>.
     * 
     * @param schemaPattern
     *          The schema pattern
     * @ant.not-required Per default no specific schema is used.
     */
    public void setSchemaPattern(String schemaPattern) {
      if ((schemaPattern != null) && (schemaPattern.length() > 0)) {
        _platformConf.setSchemaPattern(schemaPattern);
      }
    }

    /**
     * Determines whether delimited SQL identifiers shall be used (the default).
     * 
     * @return <code>true</code> if delimited SQL identifiers shall be used
     */
    public boolean isUseDelimitedSqlIdentifiers() {
      return _platformConf.isUseDelimitedSqlIdentifiers();
    }

    /**
     * Specifies whether DdlUtils shall use delimited (quoted) identifiers (such
     * as table and column names). Most databases convert undelimited identifiers
     * to uppercase and ignore the case of identifiers when performing any SQL
     * command. Undelimited identifiers also cannot be reserved words and can only
     * contain alphanumerical characters and the underscore.<br/>
     * These limitations do not exist for delimited identifiers where identifiers
     * have to be enclosed in double quotes. Delimited identifiers can contain
     * unicode characters, and even reserved words can be used as identifiers.
     * Please be aware though, that they always have to enclosed in double quotes,
     * and that the case of the identifier will be important in every SQL command
     * executed against the database.
     * 
     * @param useDelimitedSqlIdentifiers
     *          <code>true</code> if delimited SQL identifiers shall be used
     * @ant.not-required Default is <code>false</code>.
     */
    public void setUseDelimitedSqlIdentifiers(boolean useDelimitedSqlIdentifiers) {
      _platformConf.setUseDelimitedSqlIdentifiers(useDelimitedSqlIdentifiers);
    }

    /**
     * Determines whether a table's foreign keys read from a live database shall
     * be sorted alphabetically. Is <code>false</code> by default.
     * 
     * @return <code>true</code> if the foreign keys shall be sorted
     */
    public boolean isSortForeignKeys() {
      return _platformConf.isSortForeignKeys();
    }

    /**
     * Specifies whether DdlUtils shall sort the foreign keys of a table read from
     * a live database or leave them in the order in which they are returned by
     * the database/JDBC driver. Note that the sort is case sensitive only if
     * delimited identifier mode is on (<code>useDelimitedSqlIdentifiers</code> is
     * set to <code>true</code>).
     * 
     * @param sortForeignKeys
     *          <code>true</code> if the foreign keys shall be sorted
     * @ant.not-required Default is <code>false</code>.
     */
    public void setSortForeignKeys(boolean sortForeignKeys) {
      _platformConf.setSortForeignKeys(sortForeignKeys);
    }

    /**
     * Determines whether the database shall be shut down after the task has
     * finished.
     * 
     * @return <code>true</code> if the database shall be shut down
     */
    public boolean isShutdownDatabase() {
      return _platformConf.isShutdownDatabase();
    }

    /**
     * Specifies whether DdlUtils shall shut down the database after the task has
     * finished. This is mostly useful for embedded databases.
     * 
     * @param shutdownDatabase
     *          <code>true</code> if the database shall be shut down
     * @ant.not-required Default is <code>false</code>.
     */
    public void setShutdownDatabase(boolean shutdownDatabase) {
      _platformConf.setShutdownDatabase(shutdownDatabase);
    }
// GemStone changes END
}
