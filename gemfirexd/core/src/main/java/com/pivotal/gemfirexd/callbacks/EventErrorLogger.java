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

package com.pivotal.gemfirexd.callbacks;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.ManagerLogWriter;
import com.gemstone.gemfire.internal.shared.ClientSharedUtils;

/**
 * A simple logger for logging events that failed to be applied by DBSynchronizer.
 * 
 * The output is two XML files, one a root file and another containing the actual XML elements.
 * The root file name is the same as the one provided in the constructor.  If not provided,
 * it is failedevents.xml.  The other file is with _entries appended to its name.
 * 
 * The generated XML can then be provided to replay-failed-dmls GFXD command from the command line.
 * 
 * @author swale
 */
public class EventErrorLogger{

  /** file to write errors */
  protected String errorFile = "failedevents.xml";

  /** the output stream associated with {@link #errorFile} */
  protected OutputStream errorStream;
  /** XML writer used to dump errors to {@link #errorFile} */
  protected XMLStreamWriter errorWriter;
  /**
   * the suffix used for the name of file where actual errors go that is built
   * up incrementally while the top-level error file just sources it to remain
   * well-formed
   */
  protected static final String ERROR_ENTRIES_SUFFIX = "_entries";
  
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
  
  
  protected final Logger logger2 = LoggerFactory
      .getLogger("com.pivotal.gemfirexd");

  protected static final String Gfxd_EVENT_ERROR_LOGGER = "GfxdEventErrorLogger::"
      + "init: could not rename '%s' to '%s'.";

  public EventErrorLogger(String errorFileName) {
    errorFile = errorFileName;
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
          logger.info(Gfxd_EVENT_ERROR_LOGGER, logFile, oldMain);
        }
      }
      else {
        logfile = logFile.getPath();
      }
    }
    return logfile;
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
  
  public void logError(Event event, Exception e) throws Exception {
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
    writeExceptionInErrorXML(e, indent, indentStep, true);

    this.errorWriter.writeCharacters("\n");
    this.errorWriter.writeEndElement();
    this.errorWriter.writeCharacters("\n");
    this.errorWriter.flush();
    this.errorStream.flush();
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

  protected void writeExceptionInErrorXML(Exception sqle, int indent,
      final int indentStep, boolean printStack) throws XMLStreamException {

    indentInErrorXML(indent);
    this.errorWriter.writeStartElement(ERR_XML_EXCEPTION);

    indent += indentStep;
    writeObjectInErrorXML(ERR_XML_SQLSTATE, sqle instanceof SQLException ? ((SQLException)sqle).getSQLState() : "", indent);
    writeObjectInErrorXML(ERR_XML_ERRCODE, sqle instanceof SQLException ? ((SQLException)sqle).getErrorCode() : "", indent);
    writeObjectInErrorXML(ERR_XML_EX_MESSAGE, sqle.getMessage(), indent);
    if (printStack) {
      StringBuilder sb = new StringBuilder();
      //helper.getStackTrace(sqle, sb);
      ClientSharedUtils.getStackTrace(sqle, sb, null);
      writeObjectInErrorXML(ERR_XML_EX_STACK, sb.toString(), indent);
    }
    else {
      writeObjectInErrorXML(ERR_XML_EX_STACK, sqle.toString(), indent);
    }

    if (sqle instanceof SQLException && (sqle = ((SQLException)sqle).getNextException()) != null) {
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
          return ClientSharedUtils.toHexString(bytes, 0, bytes.length);
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
          return ClientSharedUtils.toHexString(bytes, 0, bytes.length);
        }
        else {
          return null;
        }
      default:
        return rs.getString(columnIndex);
    }
  }

  public String toString() {
    return "GFXD_EVENT_ERROR_LOGGER_" + (errorFile == null ? "" : errorFile);
  }
  

}
