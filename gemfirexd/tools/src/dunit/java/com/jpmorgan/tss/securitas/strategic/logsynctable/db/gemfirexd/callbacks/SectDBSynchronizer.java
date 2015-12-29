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
package com.jpmorgan.tss.securitas.strategic.logsynctable.db.gemfirexd.callbacks;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.BatchUpdateException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.regex.Pattern;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.internal.ManagerLogWriter;
import com.gemstone.gnu.trove.TObjectIntHashMap;
import com.pivotal.gemfirexd.callbacks.AsyncEventHelper;
import com.pivotal.gemfirexd.callbacks.DBSynchronizer;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.TableMetaData;
import com.pivotal.gemfirexd.execute.QueryObserver;
import com.pivotal.gemfirexd.execute.QueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;

/**
 * SectDBSynchronizer class instance persists the GemFireXD operations to an external
 * database having a JDBC driver.
 * It skips DELETEs from being relayed to backend database.
 */
public class SectDBSynchronizer extends DBSynchronizer {

  // Custom changes for SECURITAS
  private static final Pattern deleteQueryPattern = Pattern.compile(
      "^\\s*DELETE\\s+FROM\\s+.*$", Pattern.CASE_INSENSITIVE
          | Pattern.MULTILINE | Pattern.DOTALL);

  /** file to write errors */
  protected String errorFile;
  /** the output stream associated with {@link #errorFile} */
  protected OutputStream errorStream;
  /** XML writer used to dump errors to {@link #errorFile} */
  protected XMLStreamWriter errorWriter;

  /** maps the tables to the list of all their parents in the FK hierarchy */
  protected HashMap<String, HashSet<String>> parentTables;

  /**
   * the suffix used for the name of file where actual errors go that is built
   * up incrementally while the top-level error file just sources it to remain
   * well-formed
   */
  protected static final String ERROR_ENTRIES_SUFFIX = "_entries";

  /**
   * default number of retries after an error is 3 when {@link #errorFile} is
   * defined
   */
  protected static final int DEFAULT_ERROR_TRIES = 3;
  /** number of retries after an error before dumping to {@link #errorFile} */
  protected int numErrorTries = 0;

  /** keeps track of the retries that have been done for an event */
  protected final ConcurrentHashMap<Event, Object[]> errorTriesMap =
      new ConcurrentHashMap<Event, Object[]>(16, 0.75f, 2);

  // Error file XML tag/attribute names
  protected static final String ERR_XML_ROOT = "failures";
  protected static final String ERR_XML_ENTRIES_ENTITY = "errorEntries";
  protected static final String ERR_XML_FAILURE = "failure";
  protected static final String ERR_XML_SQL = "sql";
  protected static final String ERR_XML_PARAMS = "parameters";
  protected static final String ERR_XML_PARAM = "param";
  protected static final String ERR_XML_ATTR_TYPE = "jtype";
  protected static final String ERR_XML_ATTR_NULL = "isnull";
  protected static final String ERR_XML_EXCEPTION = "exception";
  protected static final String ERR_XML_SQLSTATE = "sqlstate";
  protected static final String ERR_XML_ERRCODE = "errorcode";
  protected static final String ERR_XML_EX_MESSAGE = "message";
  protected static final String ERR_XML_EX_CLASS = "class";
  protected static final String ERR_XML_EX_STACK = "stack";

  // properties below are now also used in inline string
  protected static final String ERRORFILE = "errorfile";
  protected static final String ERRORTRIES = "errortries";

  // Log strings
  protected static final String Gfxd_DB_SYNCHRONIZER__14 = "SectDBSynchronizer::"
      + "close: Error in closing XML stream writer for '%s'";
  protected static final String Gfxd_DB_SYNCHRONIZER__15 = "SectDBSynchronizer::"
      + "close: Error in closing XML stream for '%s'";
  protected static final String Gfxd_DB_SYNCHRONIZER_16 = "SectDBSynchronizer::"
      + "init: could not rename '%s' to '%s'.";

  protected final Logger logger2 = LoggerFactory
      .getLogger("com.pivotal.gemfirexd");

  /**
   * Initialize this {@link SectDBSynchronizer} instance, creating a new JDBC
   * connection to the backend database as per the provided parameter.<BR>
   * 
   * The recommended format of the parameter string is: <BR>
   * 
   * file=&lt;path&gt; <BR>
   * 
   * The file is a properties file specifying the driver, JDBC URL, user and
   * password.<BR>
   * 
   * Driver=&lt;driver-class&gt;<BR>
   * URL=&lt;JDBC URL&gt;<BR>
   * User=&lt;user name&gt;<BR>
   * Password=&lt;password&gt;<BR>
   * Transformation=&lt;transformation for the encryption cipher&gt;<BR>
   * KeySize=&lt;size of the private key to use for encryption&gt;<BR>
   * ErrorFile=&lt;file path&gt;<BR>
   * ErrorTries=&lt;num tries&gt;<BR>
   * 
   * The password provided should be an encrypted one generated using the
   * "gfxd encrypt-password external" command. The "Transformation" and
   * "KeySize" properties optionally specify the transformation and key size
   * used for encryption else the defaults are used ("AES" and 128
   * respectively). User and password are optional and when not provided then
   * JDBC URL will be used as is for connection.
   * 
   * The "ErrorFile" property specifies the file to dump failed DMLs after
   * "ErrorTries" failed attempts. The file format is an XML format specified in
   * dbsyncerr1_0.dtd.
   * 
   * The above properties may also be provided inline like below:<BR>
   * <BR>
   * &lt;driver-class&gt;,&lt;JDBC URL&gt;[,errorfile=&lt;file
   * path&gt;][,errortries=&lt;num tries&gt;][,&lt;user&gt;[,&lt;password&gt]<BR>
   * <BR>
   * The user and password parts are optional and can be possibly embedded in
   * the JDBC URL itself. The password is an encrypted one generated using the
   * "gfxd encrypt-password external" command. Inline initialization does not
   * yet allow for specification of "transformation" and "keysize" properties.
   * 
   * The "errorfile=" and "errortries=" properties have the same meanings as the
   * "ErrorFile" and "ErrorTries" properties mentioned for the separate file
   * specification above.
   */
  public void init(String initParamStr) {
    this.driver = null;
    this.driverClass = null;
    this.dbUrl = null;
    this.userName = null;
    this.passwd = null;
    this.errorFile = null;
    this.errorStream = null;
    this.errorWriter = null;
    this.numErrorTries = 0;
    // check the new "file=<properties file>" option first
    String propsFile = startsWithIgnoreCase(initParamStr, "file=");
    if (propsFile != null) {
      FileInputStream fis = null;
      final Properties props = new Properties();
      try {
        fis = new FileInputStream(propsFile);
        props.load(fis);
      } catch (Exception e) {
        throw helper.newRuntimeException(
            String.format(Gfxd_DB_SYNCHRONIZER__9, propsFile), e);
      } finally {
        try {
          if (fis != null) {
            fis.close();
          }
        } catch (Exception e) {
          // ignored
        }
      }
      try {
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
          String key = ((String)entry.getKey()).trim();
          String value = ((String)entry.getValue()).trim();
          if (DBDRIVER.equalsIgnoreCase(key)) {
            this.driverClass = value;
          }
          else if (DBURL.equalsIgnoreCase(key)) {
            this.dbUrl = value;
          }
          else if (USER.equalsIgnoreCase(key)) {
            this.userName = value;
          }
          else if (ERRORFILE.equalsIgnoreCase(key)) {
            this.errorFile = value;
          }
          else if (ERRORTRIES.equalsIgnoreCase(key)) {
            this.numErrorTries = Integer.parseInt(value);
          }
          else if (PASSWORD.equalsIgnoreCase(key)) {
            this.passwd = value;
          }
          else if (TRANSFORMATION.equalsIgnoreCase(key)) {
            this.transformation = value;
          }
          else if (KEYSIZE.equalsIgnoreCase(key)) {
            this.keySize = Integer.parseInt(value);
          }
          else {
            throw new IllegalArgumentException(String.format(
                Gfxd_DB_SYNCHRONIZER__11, key, propsFile));
          }
          if (this.passwd != null) {
            // check that passwd is encrypted
            AsyncEventHelper.decryptPassword(this.userName, this.passwd,
                this.transformation, this.keySize);
          }
        }
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw helper.newRuntimeException(
            String.format(Gfxd_DB_SYNCHRONIZER__9, propsFile), e);
      }
      if (this.driverClass == null || this.driverClass.length() == 0
          || this.dbUrl == null || this.dbUrl.length() == 0) {
        throw new IllegalArgumentException(String.format(
            Gfxd_DB_SYNCHRONIZER__10, propsFile));
      }
    }
    else {
      inlineInit(initParamStr);
    }

    // set number of tries for error to default if "errorfile" has been set
    // but "errortries" has not been set
    if (this.errorFile != null && this.numErrorTries == 0) {
      this.numErrorTries = DEFAULT_ERROR_TRIES;
    }

    this.initConnection();

    // get the FK relationships both from GemFireXD and external DB
    this.parentTables = new HashMap<String, HashSet<String>>();
    try {
      DatabaseMetaData metaData = this.conn.getMetaData();
      final GemFireStore store = Misc.getMemStoreBooting();
      for (GemFireContainer container : store.getAllContainers()) {
        if (container.isApplicationTable()
            && !container.getRegion().getAsyncEventQueueIds().isEmpty()) {
          String schema = container.getSchemaName();
          String tableName = container.getTableName();
          addParentTables(metaData, schema, tableName,
              container.getQualifiedTableName());
        }
      }
      // finally merge the parents with parents of parents and so on
      HashSet<String> merged = new HashSet<String>();
      for (Map.Entry<String, HashSet<String>> entry : this.parentTables
          .entrySet()) {
        mergeParentTables(entry.getKey(), entry.getValue(), merged);
      }
    } catch (Exception e) {
      String maskedPasswordDbUrl = null;
      if (this.dbUrl != null) {
        maskedPasswordDbUrl = maskPassword(this.dbUrl);
      }
      throw helper.newRuntimeException(String.format(Gfxd_DB_SYNCHRONIZER__6,
          this.driverClass, maskedPasswordDbUrl), e);
    }
  }

  protected void addParentTables(DatabaseMetaData metaData, String schema,
      String tableName, String fullTableName) throws SQLException {
    HashSet<String> parents = this.parentTables.get(fullTableName);
    if (parents == null) {
      parents = new HashSet<String>(5);
      this.parentTables.put(fullTableName, parents);
    }
    ResultSet parentRS = metaData.getImportedKeys(null, schema, tableName);
    while (parentRS.next()) {
      String parentSchema = parentRS.getString("PKTABLE_SCHEM");
      String parentTableName = parentRS.getString("PKTABLE_NAME");
      parents = this.parentTables.get(fullTableName);
      if (parentSchema == null) {
        parentSchema = schema;
      }
      parents.add(parentSchema != null ? parentSchema + '.' + parentTableName
          : parentTableName);
    }
    parentRS.close();
  }

  protected final void mergeParentTables(final String fullTableName,
      HashSet<String> parents, HashSet<String> merged) {
    if (parents != null) {
      // don't consider this table again in recursive merging
      merged.add(fullTableName);
      String[] parentsArray = parents.toArray(new String[parents.size()]);
      for (String parent : parentsArray) {
        HashSet<String> pparents = this.parentTables.get(parent);
        if (pparents != null) {
          if (!merged.contains(parent)) {
            mergeParentTables(parent, pparents, merged);
          }
          parents.addAll(pparents);
        }
      }
    }
  }

  protected void inlineInit(String initParamStr) {
    String[] params = initParamStr.split(",");
    if (params.length < 2) {
      throw new IllegalArgumentException(String.format(Gfxd_DB_SYNCHRONIZER__8,
          initParamStr));
    }
    this.driverClass = params[0].trim();
    this.dbUrl = params[1].trim();

    // search for other attributes
    if (params.length > 2) {
      try {
        String param, paramValue;
        for (int index = 2; index < params.length; index++) {
          param = params[index];
          paramValue = startsWithIgnoreCase(param, ERRORFILE + '=');
          if (paramValue != null) {
            this.errorFile = paramValue.trim();
            continue;
          }
          paramValue = startsWithIgnoreCase(param, ERRORTRIES + '=');
          if (paramValue != null) {
            this.numErrorTries = Integer.parseInt(paramValue.trim());
            continue;
          }
          // none matched so assume remaining to be user+password
          this.userName = param.trim();
          if (params.length > (index + 1)) {
            if (params.length > (index + 2)) {
              // treat all remaining parts as password
              int len = 0;
              for (int i = 0; i <= index; i++) {
                len += (params[i].length() + 1);
              }
              this.passwd = initParamStr.substring(len);
            }
            else {
              this.passwd = params[index + 1];
            }
            // check encrypted
            AsyncEventHelper.decryptPassword(this.userName, this.passwd,
                this.transformation, this.keySize);
          }
          break;
        }
      } catch (Exception e) {
        String maskedPasswdDbUrl = null;
        if (this.dbUrl != null) {
          maskedPasswdDbUrl = maskPassword(this.dbUrl);
        }
        throw helper.newRuntimeException(String.format(Gfxd_DB_SYNCHRONIZER__6,
            this.driverClass, maskedPasswdDbUrl), e);
      }
    }
  }

  protected String startsWithIgnoreCase(String s, String prefix) {
    if (s.length() > prefix.length() && (s.startsWith(prefix) ||
        prefix.equalsIgnoreCase(s.substring(0, prefix.length())))) {
      return s.substring(prefix.length());
    }
    else {
      return null;
    }
  }

  protected synchronized void initErrorFile() throws Exception {
    if (this.errorFile != null && this.errorWriter == null) {
      // first create the top-level XML file that sources the actual errors XML
      int dotIndex = this.errorFile.lastIndexOf('.');
      if (dotIndex <= 0
          || !"xml".equalsIgnoreCase(this.errorFile.substring(dotIndex + 1))) {
        this.errorFile = this.errorFile.concat(".xml");
      }
      String errorFileName = this.errorFile.substring(0,
          this.errorFile.length() - 4);
      String errorEntriesFile = errorFileName + ERROR_ENTRIES_SUFFIX + ".xml";
      String errorRootFile = this.errorFile;
      this.errorFile = errorEntriesFile;
      errorEntriesFile = rollFileIfRequired(errorEntriesFile, this.logger2);
      FileOutputStream xmlStream = new FileOutputStream(errorRootFile);
      final String encoding = "UTF-8";
      final XMLOutputFactory xmlFactory = XMLOutputFactory.newFactory();
      XMLStreamWriter xmlWriter = xmlFactory.createXMLStreamWriter(xmlStream,
          encoding);
      // write the XML header
      xmlWriter.writeStartDocument(encoding, "1.0");
      xmlWriter.writeCharacters("\n");
      xmlWriter.writeDTD("<!DOCTYPE staticinc [ <!ENTITY "
          + ERR_XML_ENTRIES_ENTITY + " SYSTEM \""
          + new File(errorEntriesFile).getName() + "\"> ]>");
      xmlWriter.writeCharacters("\n");
      xmlWriter.writeStartElement(ERR_XML_ROOT);
      xmlWriter.writeCharacters("\n");
      xmlWriter.writeEntityRef(ERR_XML_ENTRIES_ENTITY);
      xmlWriter.writeCharacters("\n");
      xmlWriter.writeEndElement();
      xmlWriter.writeCharacters("\n");
      xmlWriter.writeEndDocument();
      xmlWriter.flush();
      xmlWriter.close();
      xmlStream.flush();
      xmlStream.close();

      this.errorStream = new BufferedOutputStream(new FileOutputStream(
          this.errorFile));
      // disable basic output structure validation done by Woodstox since
      // this XML has multiple root elements by design
      if (xmlFactory
          .isPropertySupported("com.ctc.wstx.outputValidateStructure")) {
        xmlFactory.setProperty("com.ctc.wstx.outputValidateStructure",
            Boolean.FALSE);
      }
      this.errorWriter = xmlFactory.createXMLStreamWriter(this.errorStream,
          encoding);
    }
  }

  /**
   * If there is an existing file with given path, then try to roll it over with
   * a suffix like -01-01 in the name. If rolling fails for some reason then log
   * a warning and try to use rolled over file name.
   */
  public String rollFileIfRequired(String logfile, Logger logger) {
    final File logFile = new File(logfile);
    if (logFile.exists()) {
      final File oldMain = ManagerLogWriter.getLogNameForOldMainLog(logFile,
          false);
      if (!logFile.renameTo(oldMain)) {
        logfile = oldMain.getPath();
        if (logger.isWarnEnabled()) {
          logger.info(Gfxd_DB_SYNCHRONIZER_16, logFile, oldMain);
        }
      }
      else {
        logfile = logFile.getPath();
      }
    }
    return logfile;
  }

  /**
   * Close this {@link SectDBSynchronizer} instance.
   * 
   * To prevent a possible concurrency issue between closing thread & the
   * processor thread, access to this method is synchronized on 'this'
   */
  public synchronized void close() {
    super.close();
    if (this.errorWriter != null) {
      try {
        this.errorWriter.flush();
        this.errorWriter.close();
      } catch (Exception e) {
        this.helper.logFormat(logger, Level.WARNING, e,
            Gfxd_DB_SYNCHRONIZER__14, this.errorFile);
      }
      this.errorWriter = null;
    }
    if (this.errorStream != null) {
      try {
        this.errorStream.flush();
        this.errorStream.close();
      } catch (Exception e) {
        this.helper.logFormat(logger, Level.WARNING, e,
            Gfxd_DB_SYNCHRONIZER__15, this.errorFile);
      }
      this.errorStream = null;
    }
    this.errorFile = null;
  }

  private boolean compareColumns(ResultSetMetaData meta1,
      ResultSetMetaData meta2) throws SQLException {
    final int numColumns = meta1.getColumnCount();
    if (meta2.getColumnCount() != numColumns) {
      return false;
    }
    for (int i = 1; i <= numColumns; i++) {
      if (meta1.getColumnType(i) != meta2.getColumnType(i)
          || !meta1.getColumnName(i).equals(meta2.getColumnName(i))) {
        return false;
      }
    }
    return true;
  }

  // End Custom changes for SECURITAS

  @Override
  public boolean processEvents(List<Event> events) {
    if (this.shutDown) {
      return false;
    }
    final QueryObserver observer = QueryObserverHolder.getInstance();
    final boolean traceDBSynchronizer = this.traceDBSynchronizer = helper
        .traceDBSynchronizer() && logger.isLoggable(Level.INFO);
    boolean completedSucessfully = false;
    String listOfEventsString = null;
    // The retval will be considered true only if the list was iterated
    // completely. If the List iteration was incomplete we will return
    // false so that the events are not removed during failure.
    // As for individual events, they can get exceptions due to constraint
    // violations etc but will not cause return value to be false.
    Statement stmt = null;
    PreparedStatement ps = null;
    Event event = null;
    Event.Type evType = null;

    final Iterator<Event> eventItr = events.iterator();
    // events grouped by batches
    ArrayList<List<Event>> batchEvents = new ArrayList<List<Event>>();
    // holds the (index+1) of last valid batch in batchEvents for given table
    final TObjectIntHashMap tablesSeen = new TObjectIntHashMap();
    while (eventItr.hasNext()) {
      event = eventItr.next();
      evType = event.getType();
      String tableName = event.getTableName();
      int batchIndex = tablesSeen.get(tableName);
      boolean foundExisting = false;
      if (!evType.isBulkOperation()) {
        if (traceDBSynchronizer) {
          logger.info("SectDBSynchronizer::processEvents :processing "
              + "PK based event=" + event + ", type=" + evType);
        }
        switch (evType) {
          case AFTER_INSERT:
            if (batchIndex > 0) {
              // can only batch if the current batch is a PK insert
              List<Event> prevEvents = batchEvents.get(batchIndex - 1);
              if (prevEvents.get(0).getType() == Event.Type.AFTER_INSERT) {
                prevEvents.add(event);
                foundExisting = true;
              }
            }
            break;
          case AFTER_UPDATE:
            if (batchIndex > 0) {
              // can only batch if the current batch is a PK update
              List<Event> prevEvents = batchEvents.get(batchIndex - 1);
              Event prevEvent = prevEvents.get(0);
              try {
                if (prevEvent.getType() == Event.Type.AFTER_UPDATE
                    && compareColumns(event.getNewRowsAsResultSet()
                        .getMetaData(), prevEvent.getNewRowsAsResultSet()
                        .getMetaData())) {
                  prevEvents.add(event);
                  foundExisting = true;
                }
              } catch (SQLException sqle) {
                // ignore
              }
            }
            break;
          case AFTER_DELETE:
            // Custom changes for SECURITAS
            logger.info("SectDBSynchronizer::processEvents: skipping "
                + "the delete operation for event=" + event.toString());
            continue;
            // End custom changes for SECURITAS
          default:
            logger.severe("SectDBSynchronizer::processEvents: unexpected "
                + "eventType " + evType + " for " + event.toString());
            continue;
        }
      }
      else {
        if (traceDBSynchronizer) {
          logger.info("SectDBSynchronizer::processEvents: processing "
              + "Bulk DML Event=" + event);
        }
        // Custom changes for SECURITAS
        String eventDML = event.getDMLString();
        if (eventDML != null && deleteQueryPattern.matcher(eventDML).find()) {
          logger.info("SectDBSynchronizer::processEvents: skipping "
              + "the delete operation for event=" + event.toString());
          continue;
        }
        // End custom changes for SECURITAS
        if (evType == Event.Type.BULK_DML && batchIndex > 0) {
          // can batch only if current list is for the same bulk DML op
          List<Event> prevEvents = batchEvents.get(batchIndex - 1);
          Event prevEv = prevEvents.get(0);
          if (prevEv.getType() == Event.Type.BULK_DML
              && eventDML.equals(prevEv.getDMLString())) {
            prevEvents.add(event);
            foundExisting = true;
          }
        }
      }
      // break the batches for all child tables, if any
      if (tablesSeen.size() > 0) {
        final Object[] tables = tablesSeen.keys();
        DatabaseMetaData metaData = null;
        for (Object childTable : tables) {
          HashSet<String> parents = this.parentTables.get(childTable);
          if (parents == null) {
            // new table attached to DBSync, so populate for this table
            String childTableName = (String)childTable;
            String childSchema = null;
            int dotIndex = childTableName.indexOf('.');
            if (dotIndex > 0) {
              childSchema = childTableName.substring(0, dotIndex);
              childTableName = childTableName.substring(dotIndex + 1);
            }
            try {
              if (metaData == null) {
                metaData = this.conn.getMetaData();
              }
              addParentTables(metaData, childSchema, childTableName,
                  (String)childTable);
              parents = this.parentTables.get(childTable);
            } catch (Exception e) {
              // ignoring exceptions at this point
            }
          }
          if (parents != null && parents.contains(tableName)) {
            tablesSeen.remove(childTable);
          }
        }
      }
      // create a new batch if no existing one found
      if (!foundExisting) {
        if (evType.isBulkInsert()) {
          batchEvents.add(Collections.singletonList(event));
          // we don't batch across batchInserts themselves
          tablesSeen.remove(tableName);
        }
        else {
          ArrayList<Event> eventList = new ArrayList<Event>(4);
          eventList.add(event);
          batchEvents.add(eventList);
          tablesSeen.put(tableName, batchEvents.size());
        }
      }
    }

    final Iterator<List<Event>> batchItr = batchEvents.iterator();
    List<Event> batch = null;
    boolean hasBatch = false;
    String eventString = null;
    event = null;
    evType = null;
    try {
      while (!(completedSucessfully = !batchItr.hasNext())) {
        PreparedStatement prevPS = null;
        batch = batchItr.next();
        final int batchSize = batch.size();
        boolean isPKBased = false;
        eventString = null;

        hasBatch = (batchSize > 1);
        for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
          event = batch.get(batchIndex);
          evType = event.getType();
          if (traceDBSynchronizer) {
            eventString = event.toString();
          }
          else {
            eventString = null;
          }
          // first check if the event is primary key based insert/update/delete
          // operation
          if (!evType.isBulkOperation()) {
            // is PK based
            isPKBased = true;
            try {
              switch (evType) {
                case AFTER_INSERT:
                  ps = getExecutableInsertPrepStmntPKBased(event, prevPS);
                  break;
                case AFTER_UPDATE:
                  ps = getExecutableUpdatePrepStmntPKBased(event, prevPS);
                  break;
                default:
                  logger
                      .severe("SectDBSynchronizer::processEvents: unexpected "
                          + "eventType " + evType + " for " + event);
                  continue;
              }
            } catch (SQLException sqle) {
              SqlExceptionHandler handler = handleSQLException(sqle,
                  Gfxd_DB_SYNCHRONIZER__1, null, event, eventString, logger,
                  true);
              if (handler.breakTheLoop()) {
                break;
              }
            } catch (RegionDestroyedException rde) {
              if (logger.isLoggable(Level.INFO)) {
                logger.info("SectDBSynchronizer::processEvents: WBCLEvent "
                    + event + " will  be discarded as the underlying region "
                    + "for the table has been destroyed");
              }
              continue;
            }
          }
          else {
            isPKBased = false;
            try {
              // prepare the statement for a bulk DML or bulk insert operation;
              // avoid creating a prepared statement if there are no parameters
              // since it is likely to be an unprepared statement that was
              // fired in GemFireXD and we don't want to bloat the backend
              // database's prepared statement cache
              ps = getExecutablePrepStmntBulkOp(event, prevPS);
              if (ps == null) {
                if (stmt == null) {
                  stmt = this.conn.createStatement();
                }
                if (hasBatch) {
                  stmt.addBatch(event.getDMLString());
                }
              }
            } catch (SQLException sqle) {
              SqlExceptionHandler handler = handleSQLException(sqle,
                  Gfxd_DB_SYNCHRONIZER__1, null, event, eventString,
                  this.logger, true);
              if (handler.breakTheLoop()) {
                break;
              }
            }
          }
          prevPS = ps;
        }

        try {
          final int num;
          // check for explicit bulk insert statements first
          if (evType.isBulkInsert()) {
            hasBatch = true;
            if (traceDBSynchronizer) {
              logger.info("SectDBSynchronizer::processEvents executing batch "
                  + "insert for bulk statement=" + ps + " for event="
                  + eventString);
            }
            final int[] res = ps.executeBatch();
            num = res.length;
            // clear event from failure map if present
            helper.removeEventFromFailureMap(event);
            if (traceDBSynchronizer) {
              logger.info("SectDBSynchronizer::processEvents total num rows "
                  + "modified=" + num + " for statement=" + ps + " for event="
                  + eventString);
            }
            if (observer != null) {
              observer.afterBulkOpDBSynchExecution(evType, num, ps,
                  event.getDMLString());
            }
          }
          else {
            if (ps != null) {
              if (hasBatch) {
                if (traceDBSynchronizer) {
                  logger.info("SectDBSynchronizer::processEvents executing "
                      + "batch statement for prepared statement=" + ps
                      + " for batchSize=" + batchSize + ": " + batch);
                }
                // addBatch for the last batch
                ps.addBatch();
                final int[] res = ps.executeBatch();
                num = res.length;
                // clear events from failure map if present
                for (Event ev : batch) {
                  helper.removeEventFromFailureMap(ev);
                }
              }
              else {
                if (traceDBSynchronizer) {
                  logger.info("SectDBSynchronizer::processEvents executing "
                      + "update statement for prepared statement=" + ps
                      + " for event=" + eventString);
                }
                num = ps.executeUpdate();
                // clear event from failure map if present
                helper.removeEventFromFailureMap(event);
              }
              if (observer != null) {
                if (isPKBased) {
                  observer.afterPKBasedDBSynchExecution(evType, num, ps);
                }
                else {
                  observer.afterBulkOpDBSynchExecution(evType, num, ps,
                      event.getDMLString());
                }
              }
            }
            // execute a non-prepared statement
            else {
              if (hasBatch) {
                if (traceDBSynchronizer) {
                  logger.info("SectDBSynchronizer::processEvents executing batch "
                      + "unprepared statement=" + stmt + " for batchSize="
                      + batchSize + ": " + batch);
                }
                final int[] res = stmt.executeBatch();
                num = res.length;
                // clear events from failure map if present
                for (Event ev : batch) {
                  helper.removeEventFromFailureMap(ev);
                  if (observer != null) {
                    observer.afterBulkOpDBSynchExecution(evType, num, stmt,
                        ev.getDMLString());
                  }
                }
              }
              else {
                if (traceDBSynchronizer) {
                  logger.info("SectDBSynchronizer::processEvents executing "
                      + "unprepared statement for event=" + eventString);
                }
                num = stmt.executeUpdate(event.getDMLString());
                // clear event from failure map if present
                helper.removeEventFromFailureMap(event);
                if (observer != null) {
                  observer.afterBulkOpDBSynchExecution(evType, num, stmt,
                      event.getDMLString());
                }
              }
            }
            if (traceDBSynchronizer) {
              logger.info("SectDBSynchronizer::processEvents num rows "
                  + "modified=" + num + " for statement="
                  + (ps != null ? ps : stmt));
            }
          }
        } catch (SQLException sqle) {
          try {
            if (ps != null) {
              if (hasBatch) {
                ps.clearBatch();
              }
              else {
                ps.clearParameters();
              }
            }
            else if (stmt != null && hasBatch) {
              stmt.clearBatch();
            }
          } catch (SQLException e) {
            // ignored
          }
          SqlExceptionHandler handler = handleSQLException(sqle,
              Gfxd_DB_SYNCHRONIZER__3, ps != null ? ps : stmt, event,
              eventString, logger, false);
          if (handler.breakTheLoop()) {
            break;
          }
        }
      } // end of while (event list processing loop)

      // now perform the commit of the above operations
      if (completedSucessfully) {
        try {
          if (helper.logFineEnabled()) {
            if (listOfEventsString == null) {
              listOfEventsString = events.toString();
            }
            logger.fine("SectDBSynchronizer::processEvents: "
                + "before commit of events=" + listOfEventsString);
          }
          this.conn.commit();
          if (observer != null) {
            observer.afterCommitDBSynchExecution(events);
          }
          if (helper.logFineEnabled()) {
            if (listOfEventsString == null) {
              listOfEventsString = events.toString();
            }
            logger.fine("SectDBSynchronizer::processEvents: "
                + "committed successfully for events=" + listOfEventsString);
          }
        } catch (SQLException sqle) {
          try {
            if (ps != null) {
              if (hasBatch) {
                ps.clearBatch();
              }
              else {
                ps.clearParameters();
              }
            }
            else if (stmt != null && hasBatch) {
              stmt.clearBatch();
            }
          } catch (SQLException e) {
            // ignored
          }
          SqlExceptionHandler handler = handleSQLException(sqle,
              Gfxd_DB_SYNCHRONIZER__7, ps != null ? ps : stmt, event,
              eventString, logger, true);
          if (handler != SqlExceptionHandler.IGNORE) {
            completedSucessfully = false;
          }
        }
      }
    } catch (Exception e) {

      if (logger != null && logger.isLoggable(Level.SEVERE)
          && !(event != null && helper.skipFailureLogging(event))) {
        StringBuilder sb = new StringBuilder();
        if (event != null) {
          if (eventString == null) {
            eventString = event.toString();
          }
          sb.append("[FAILED: ").append(eventString).append(" ]");
        }
        while (batchItr.hasNext()) {
          sb.append("[ ").append(batchItr.next().toString()).append(" ]");
        }
        helper.logFormat(logger, Level.SEVERE, e, Gfxd_DB_SYNCHRONIZER__2,
            sb.toString());
      }
      SqlExceptionHandler.CLEANUP.execute(this);
      completedSucessfully = false;
    }

    // Custom changes for SECURITAS
    if (completedSucessfully) {
      // on successful completion, log any pending errors to XML file; when
      // unsuccessful then we know that batch will be retried so don't log in
      // that case else it can get logged multiple times
      // clear event from failure map if present
      if (this.numErrorTries > 0 && this.errorTriesMap.size() > 0) {
        for (Event ev : events) {
          Object[] tries = this.errorTriesMap.remove(ev);
          // log to error file if specified, else the failure would already
          // be logged at warning level in the server logs and above call will
          // clear up errorTriesMap
          if (tries != null && tries[1] != null && this.errorFile != null) {
            try {
              logError(ev, (SQLException)tries[1]);
            } catch (Exception e) {
              // failed to even log the exception
              if (logger.isLoggable(Level.WARNING)) {
                helper.log(logger, Level.WARNING, e, e.getMessage());
              }
            }
          }
        }
      }
    }
    else {
      // rollback since current batch will be retried
      try {
        if (this.conn != null && !this.conn.isClosed()) {
          this.conn.rollback();
        }
      } catch (SQLException sqle) {
        // ignore at this point
      }
    }
    // END Custom changes for SECURITAS

    if (helper.traceExecute()) {
      logger.info("SectDBSynchronizer::processEvents: processed "
          + events.size() + " events, success=" + completedSucessfully);
    }

    return completedSucessfully;
  }

  // Custom changes for SECURITAS
  /**
   * Returns an {@link SqlExceptionHandler} for the given {@link SQLException}.
   */
  protected SqlExceptionHandler handleSQLException(SQLException sqle) {
    String sqlState = sqle.getSQLState();
    // What to do if SQLState is null? Checking through the exception
    // message for common strings for now but DB specific errorCode and other
    // such checks will be better.
    // Below was due to a bug in wrapper OracleDriver being used and normally
    // this can never be null.
    if (sqlState == null) {
      // no SQLState so fallback to string matching in the message
      // for BatchUpdateException it will look at the nextException
      if (sqle instanceof BatchUpdateException
          && sqle.getNextException() != null) {
        // "42Y96" represents an unknown exception but batch exception will
        // look at the nextException in any case
        sqlState = "42Y96";
      }
      else {
        // if connection has been closed then refresh it
        try {
          synchronized (this) {
            if (this.conn == null || this.conn.isClosed()) {
              return SqlExceptionHandler.REFRESH;
            }
          }
        } catch (Exception e) {
          return SqlExceptionHandler.REFRESH;
        }
        // treat like a connection failure by default
        return checkExceptionString(sqle.toString().toLowerCase(),
            SqlExceptionHandler.REFRESH);
      }
    }
    // check for exception type first
    SqlExceptionHandler handler = checkExceptionType(sqle);
    if (handler != null) {
      return handler;
    }
    // next check SQLStates
    if (sqlState.startsWith(AsyncEventHelper.INTEGRITY_VIOLATION_PREFIX)
        || sqlState.startsWith("25")) {
      // constraint violations can happen in retries, so default action is to
      // IGNORE them; when errorFile is provided then it will be logged to
      // that in XML format in any case
      return SqlExceptionHandler.IGNORE;
    }
    else if (sqlState.startsWith(AsyncEventHelper.LSE_COMPILATION_PREFIX)
        || sqlState.startsWith("22") /* for SQLDataExceptions */) {
      // if numErrorTries is defined, then retry some number of times else
      // ignore after having logged warning since retry is not likely to help
      return this.numErrorTries > 0 ? SqlExceptionHandler.IGNORE_BREAK_LOOP
          : SqlExceptionHandler.IGNORE;
    }
    else if (sqlState.startsWith(AsyncEventHelper.CONNECTIVITY_PREFIX)) {
      return SqlExceptionHandler.REFRESH;
    }
    else if (sqlState.startsWith("40")) {
      // these are transient transaction/lock exceptions so retry whole batch
      return SqlExceptionHandler.IGNORE_BREAK_LOOP;
    }
    else {
      if (sqle instanceof BatchUpdateException
          && sqle.getNextException() != null) {
        return handleSQLException(sqle.getNextException());
      }
      // if connection has been closed then refresh it
      try {
        synchronized (this) {
          if (this.conn == null || this.conn.isClosed()) {
            return SqlExceptionHandler.REFRESH;
          }
        }
      } catch (Exception e) {
        return SqlExceptionHandler.REFRESH;
      }
      return checkExceptionString(sqle.toString().toLowerCase(),
          SqlExceptionHandler.REFRESH);
    }
  }

  protected SqlExceptionHandler checkExceptionType(SQLException sqle) {
    if (sqle != null) {
      if (sqle instanceof SQLNonTransientConnectionException) {
        // will need to connect again
        return SqlExceptionHandler.REFRESH;
      }
      if (sqle instanceof SQLIntegrityConstraintViolationException) {
        // constraint violations can happen in retries, so default action is to
        // IGNORE them; when errorFile is provided then it will be logged to
        // that in XML format in any case
        return SqlExceptionHandler.IGNORE;
      }
      if (sqle instanceof SQLNonTransientException) {
        // if numErrorTries is defined, then retry some number of times else
        // ignore after having logged warning since retry is not likely to help
        return this.numErrorTries > 0 ? SqlExceptionHandler.IGNORE_BREAK_LOOP
            : SqlExceptionHandler.IGNORE;
      }
      if (sqle instanceof SQLTransientException) {
        // skip the remaining batch and retry whole batch again
        return SqlExceptionHandler.IGNORE_BREAK_LOOP;
      }
      if (sqle instanceof BatchUpdateException) {
        return checkExceptionType(sqle.getNextException());
      }
    }
    return null;
  }

  protected SqlExceptionHandler checkExceptionString(String message,
      SqlExceptionHandler defaultHandler) {
    if (message.contains("constraint")) {
      // likely a constraint violation
      // constraint violations can happen in retries, so default action is to
      // IGNORE them; when errorFile is provided then it will be logged to
      // that in XML format in any case
      return SqlExceptionHandler.IGNORE;
    }
    else if (message.contains("syntax")) {
      // if numErrorTries is defined, then retry some number of times else
      // ignore after having logged warning since retry is not likely to help
      return this.numErrorTries > 0 ? SqlExceptionHandler.IGNORE_BREAK_LOOP
          : SqlExceptionHandler.IGNORE;
    }
    else if (message.contains("connect")) {
      // likely a connection error
      return SqlExceptionHandler.REFRESH;
    }
    else {
      return defaultHandler;
    }
  }

  /**
   * Log exception including stack traces for fine logging with
   * {@link #traceDBSynchronizer}, and returns an {@link SqlExceptionHandler}
   * for the given {@link SQLException}.
   */
  protected SqlExceptionHandler handleSQLException(SQLException sqle,
      String format, Statement stmt, Event event, String eventString,
      java.util.logging.Logger logger, boolean logWarning) throws SQLException {
    boolean skipLogging = false;
    SqlExceptionHandler handler = handleSQLException(sqle);
    // check if the number of retries for an event have been exceeded
    // and ignore if that is the case
    if (event != null && this.numErrorTries > 0) {
      Object[] tries = this.errorTriesMap.get(event);
      if (tries != null) {
        Integer numTries = (Integer)tries[0];
        if (numTries >= this.numErrorTries) {
          // at this point ignore this exception and move to others in the batch
          handler = SqlExceptionHandler.IGNORE;
          logWarning = false;
        }
        tries[0] = Integer.valueOf(numTries.intValue() + 1);
        tries[1] = sqle;
      }
      else {
        this.errorTriesMap.put(event, new Object[] { 1, sqle });
      }
    }
    if (event != null && (logWarning || traceDBSynchronizer)) {
      skipLogging = helper.skipFailureLogging(event);
      if (eventString == null) {
        eventString = event.toString();
      }
    }
    if (!skipLogging) {
      if (logWarning) {
        if (logger.isLoggable(Level.WARNING)) {
          // don't log exception trace rather just the exception string as
          // the last argument of the "format" string provided
          StringBuilder sqleStr = new StringBuilder();
          sqleStr.append(sqle);
          SQLException next = sqle;
          while ((next = next.getNextException()) != null) {
            sqleStr.append("; ").append(next);
          }
          helper.logFormat(logger, Level.WARNING, null, format, eventString,
              sqleStr.toString());
        }
      }
      if (traceDBSynchronizer) {
        if (logger.isLoggable(Level.WARNING)) {
          String stmtStr = (stmt != null ? ("executing statement=" + stmt)
              : "preparing statement");
          helper.log(logger, Level.WARNING, sqle, "SectDBSynchronizer::"
              + "processEvents: Exception while " + stmtStr + " for event="
              + eventString);
          if (sqle.getNextException() != null) {
            helper.log(logger, Level.WARNING, sqle.getNextException(),
                "SectDBSynchronizer::processEvents: next exception");
          }
        }
      }
    }
    handler.execute(this);
    return handler;
  }

  protected void logError(Event event, SQLException sqle) throws Exception {
    // initialize the error file
    initErrorFile();

    final int indentStep = 2;
    int indent = indentStep;
    this.errorWriter.writeStartElement(ERR_XML_FAILURE);

    final String tableName = event.getTableName();
    String dmlString;
    int numColumns;
    ResultSet rows, pkResultSet;
    ResultSetMetaData metadata, pkMetaData;
    List<String[]> paramsBatch;
    String[] params;
    int[] paramTypes;
    int jdbcType;
    switch (event.getType()) {
      case AFTER_INSERT:
        metadata = event.getResultSetMetaData();
        numColumns = metadata.getColumnCount();
        dmlString = AsyncEventHelper.getInsertString(tableName,
            (TableMetaData)metadata, false);
        params = new String[numColumns];
        paramsBatch = Collections.singletonList(params);
        paramTypes = new int[numColumns];
        rows = event.getNewRowsAsResultSet();
        for (int colIdx = 1; colIdx <= numColumns; colIdx++) {
          jdbcType = metadata.getColumnType(colIdx);
          params[colIdx - 1] = getColumnAsString(rows, colIdx, jdbcType);
          paramTypes[colIdx - 1] = jdbcType;
        }
        break;
      case AFTER_UPDATE:
        rows = event.getNewRowsAsResultSet();
        metadata = rows.getMetaData();
        final int numUpdatedCols = metadata.getColumnCount();
        pkResultSet = event.getPrimaryKeysAsResultSet();
        pkMetaData = pkResultSet.getMetaData();
        final int numKeyCols = pkMetaData.getColumnCount();
        numColumns = numUpdatedCols + numKeyCols;
        dmlString = AsyncEventHelper.getUpdateString(tableName, pkMetaData,
            metadata);
        params = new String[numColumns];
        paramsBatch = Collections.singletonList(params);
        paramTypes = new int[numColumns];
        for (int colIdx = 1; colIdx <= numUpdatedCols; colIdx++) {
          jdbcType = metadata.getColumnType(colIdx);
          params[colIdx - 1] = getColumnAsString(rows, colIdx, jdbcType);
          paramTypes[colIdx - 1] = jdbcType;
        }
        for (int colIdx = 1; colIdx <= numKeyCols; colIdx++) {
          jdbcType = pkMetaData.getColumnType(colIdx);
          params[colIdx + numUpdatedCols - 1] = getColumnAsString(pkResultSet,
              colIdx, jdbcType);
          paramTypes[colIdx + numUpdatedCols - 1] = jdbcType;
        }
        break;
      case AFTER_DELETE:
        pkResultSet = event.getPrimaryKeysAsResultSet();
        pkMetaData = pkResultSet.getMetaData();
        dmlString = AsyncEventHelper.getDeleteString(tableName, pkMetaData);
        numColumns = pkMetaData.getColumnCount();
        params = new String[numColumns];
        paramsBatch = Collections.singletonList(params);
        paramTypes = new int[numColumns];
        for (int colIdx = 1; colIdx <= numColumns; colIdx++) {
          jdbcType = pkMetaData.getColumnType(colIdx);
          params[colIdx - 1] = getColumnAsString(pkResultSet, colIdx, jdbcType);
          paramTypes[colIdx - 1] = jdbcType;
        }
        break;
      case BULK_DML:
        dmlString = event.getDMLString();
        paramsBatch = null;
        paramTypes = null;
        if (event.hasParameters()) {
          rows = event.getNewRowsAsResultSet();
          if (rows != null && (numColumns = (metadata = rows.getMetaData())
              .getColumnCount()) > 0) {
            params = new String[numColumns];
            paramsBatch = Collections.singletonList(params);
            paramTypes = new int[numColumns];
            for (int colIdx = 1; colIdx <= numColumns; colIdx++) {
              jdbcType = metadata.getColumnType(colIdx);
              params[colIdx - 1] = getColumnAsString(rows, colIdx, jdbcType);
              paramTypes[colIdx - 1] = jdbcType;
            }
          }
        }
        break;
      case BULK_INSERT:
        dmlString = event.getDMLString();
        rows = event.getNewRowsAsResultSet();
        metadata = rows.getMetaData();
        numColumns = metadata.getColumnCount();
        paramsBatch = new ArrayList<String[]>();
        paramTypes = new int[numColumns];
        boolean firstIter = true;
        while (rows.next()) {
          params = new String[numColumns];
          paramsBatch.add(params);
          for (int colIdx = 1; colIdx <= numColumns; colIdx++) {
            if (firstIter) {
              jdbcType = metadata.getColumnType(colIdx);
              paramTypes[colIdx - 1] = jdbcType;
            }
            else {
              jdbcType = paramTypes[colIdx - 1];
            }
            params[colIdx - 1] = getColumnAsString(rows, colIdx, jdbcType);
          }
          firstIter = false;
        }
        break;
      default:
        // should never happen
        dmlString = event.toString();
        paramsBatch = null;
        paramTypes = null;
    }

    // write DML and parameters
    indentInErrorXML(indent);
    this.errorWriter.writeStartElement(ERR_XML_SQL);
    this.errorWriter.writeCharacters(dmlString);
    this.errorWriter.writeEndElement();
    if (paramsBatch != null) {
      for (String[] prms : paramsBatch) {
        indentInErrorXML(indent);
        this.errorWriter.writeStartElement(ERR_XML_PARAMS);
        indent += indentStep;
        for (int i = 0; i < prms.length; i++) {
          indentInErrorXML(indent);
          this.errorWriter.writeStartElement(ERR_XML_PARAM);
          this.errorWriter.writeAttribute(ERR_XML_ATTR_TYPE,
              Integer.toString(paramTypes[i]));
          if (prms[i] != null) {
            this.errorWriter.writeCharacters(prms[i]);
          }
          else {
            this.errorWriter.writeAttribute(ERR_XML_ATTR_NULL, "true");
          }
          this.errorWriter.writeEndElement();
        }
        indent -= indentStep;
        indentInErrorXML(indent);
        this.errorWriter.writeEndElement();
      }
    }

    // write exception details at the end
    writeExceptionInErrorXML(sqle, indent, indentStep, true);

    this.errorWriter.writeCharacters("\n");
    this.errorWriter.writeEndElement();
    this.errorWriter.writeCharacters("\n");
    this.errorWriter.flush();
    this.errorStream.flush();
  }

  /**
   * Get the value of a column in current row in ResultSet as a string.
   * 
   * For binary data this returns hex-encoded string of the bytes.
   * 
   * For a java object this returns hex-encoded string of the serialized bytes
   * for the object.
   */
  public static final String getColumnAsString(ResultSet rs, int columnIndex,
      int jdbcType) throws SQLException {
    byte[] bytes;
    switch (jdbcType) {
      case Types.BINARY:
      case Types.BLOB:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        // convert to hex for binary columns
        bytes = rs.getBytes(columnIndex);
        if (bytes != null) {
          return toHexString(bytes, 0, bytes.length);
        }
        else {
          return null;
        }
      case Types.JAVA_OBJECT:
        // for java object type, serialize and then convert to hex
        Object v = rs.getObject(columnIndex);
        if (v != null) {
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(bos);
          try {
            DataSerializer.writeObject(v, dos);
            dos.flush();
          } catch (IOException ioe) {
            // not expected to happen
            throw new SQLException(ioe.getMessage(), "XJ001", 0, ioe);
          }
          bytes = bos.toByteArray();
          return toHexString(bytes, 0, bytes.length);
        }
        else {
          return null;
        }
      default:
        return rs.getString(columnIndex);
    }
  }

  private static char[] hex_table = { '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

  /**
   * Convert a byte array to a String with a hexidecimal format. The String may
   * be converted back to a byte array using fromHexString. <BR>
   * For each byte (b) two characaters are generated, the first character
   * represents the high nibble (4 bits) in hexidecimal (<code>b & 0xf0</code>),
   * the second character represents the low nibble (<code>b & 0x0f</code>). <BR>
   * The byte at <code>data[offset]</code> is represented by the first two
   * characters in the returned String.
   * 
   * @param data
   *          byte array
   * @param offset
   *          starting byte (zero based) to convert.
   * @param length
   *          number of bytes to convert.
   * @return the String (with hexidecimal format) form of the byte array
   */
  public static String toHexString(final byte[] data, final int offset,
      final int length) {
    final char[] chars = new char[length << 1];
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      final int high_nibble = (data[i] & 0xf0) >>> 4;
      final int low_nibble = (data[i] & 0x0f);
      final int j = (i - offset) << 1;
      chars[j] = hex_table[high_nibble];
      chars[j + 1] = hex_table[low_nibble];
    }
    return new String(chars);
  }

  protected void indentInErrorXML(int level) throws XMLStreamException {
    // first begin a newline
    this.errorWriter.writeCharacters("\n");
    final char[] indent = new char[level];
    for (int idx = 0; idx < level; idx++) {
      indent[idx] = ' ';
    }
    this.errorWriter.writeCharacters(indent, 0, indent.length);
  }

  protected void writeExceptionInErrorXML(SQLException sqle, int indent,
      final int indentStep, boolean printStack) throws XMLStreamException {

    indentInErrorXML(indent);
    this.errorWriter.writeStartElement(ERR_XML_EXCEPTION);

    indent += indentStep;
    writeObjectInErrorXML(ERR_XML_SQLSTATE, sqle.getSQLState(), indent);
    writeObjectInErrorXML(ERR_XML_ERRCODE, sqle.getErrorCode(), indent);
    writeObjectInErrorXML(ERR_XML_EX_MESSAGE, sqle.getMessage(), indent);
    if (printStack) {
      StringBuilder sb = new StringBuilder();
      helper.getStackTrace(sqle, sb);
      writeObjectInErrorXML(ERR_XML_EX_STACK, sb.toString(), indent);
    }
    else {
      writeObjectInErrorXML(ERR_XML_EX_STACK, sqle.toString(), indent);
    }

    if ((sqle = sqle.getNextException()) != null) {
      writeExceptionInErrorXML(sqle, indent, indentStep, false);
    }

    indent -= indentStep;
    indentInErrorXML(indent);
    this.errorWriter.writeEndElement();
  }

  protected void writeObjectInErrorXML(final String tag, final Object o,
      int indent) throws XMLStreamException {
    indentInErrorXML(indent);
    if (o != null) {
      this.errorWriter.writeStartElement(tag);
      this.errorWriter.writeCharacters(o.toString());
      this.errorWriter.writeEndElement();
    }
    else {
      this.errorWriter.writeEmptyElement(tag);
    }
  }
  // END Custom changes for SECURITAS
}
