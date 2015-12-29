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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.shared.ClientSharedUtils;

/**
 * Executes insert, update or delete operations that failed to be executed and
 * were logged in an error XML file with the EventErrorLogger.
 * 
 * The execution is against a database described by the provided JDBC URL.
 * 
 * This utility is provided as is and may be changed as required.
 * 
 * The error XML file to be provided is generated in two cases: when a DML operation arriving
 * from other WAN site failed to be applied on this WAN site, or when a DML
 * operation failed to be applied from the default AsyncEventListener
 * implementation, the DBSynchronizer.
 * 
 * @author swale
 * 
 */
public class EventErrorFileToDBWriter {

  protected static final String ERR_XML_ROOT = EventErrorLogger.ERR_XML_ROOT;
  protected static final String ERR_XML_ENTRIES_ENTITY = EventErrorLogger.ERR_XML_ENTRIES_ENTITY;
  protected static final String ERR_XML_FAILURE = EventErrorLogger.ERR_XML_FAILURE;
  protected static final String ERR_XML_SQL = EventErrorLogger.ERR_XML_SQL;
  protected static final String ERR_XML_PARAMS = EventErrorLogger.ERR_XML_PARAMS;
  protected static final String ERR_XML_PARAM = EventErrorLogger.ERR_XML_PARAM;
  protected static final String ERR_XML_ATTR_TYPE = EventErrorLogger.ERR_XML_ATTR_TYPE;
  protected static final String ERR_XML_ATTR_NULL = EventErrorLogger.ERR_XML_ATTR_NULL;
  protected static final String ERR_XML_EXCEPTION = EventErrorLogger.ERR_XML_EXCEPTION;
  protected static final String ERR_XML_SQLSTATE = EventErrorLogger.ERR_XML_SQLSTATE;
  protected static final String ERR_XML_ERRCODE = EventErrorLogger.ERR_XML_ERRCODE;
  protected static final String ERR_XML_EX_MESSAGE = EventErrorLogger.ERR_XML_EX_MESSAGE;
  protected static final String ERR_XML_EX_STACK = EventErrorLogger.ERR_XML_EX_STACK;

  public static void main(String[] args) throws Exception {
    if (args.length < 2 || args.length > 4) {
      System.err.println("Usage: WriteErrorsToDB "
          + "<JDBC URL> <file> [<user> <password>]");
      return;
    }
    String connStr = args[0];
    Connection conn;
    if (args.length > 2) {
      String user = args[2];
      String password;
      if (args.length == 4) {
        password = args[3];
      }
      else {
        // get the password if user has been provided
        System.out.print("Password: ");
        System.out.flush();
        char[] passwd = System.console().readPassword();
        if (passwd == null) {
          password = "";
        }
        else {
          password = new String(passwd);
        }
      }
      conn = DriverManager.getConnection(connStr, user, password);
    }
    else {
      conn = DriverManager.getConnection(connStr);
    }
    writeXMLToDB(conn, args[1]);
  }

  public static void writeXMLToDB(Connection conn, String file)
      throws Exception {
    BufferedInputStream xmlStream = new BufferedInputStream(
        new FileInputStream(file));
    XMLInputFactory xmlFactory = XMLInputFactory.newFactory();
    xmlFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES,
        Boolean.TRUE);
    xmlFactory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
    XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(xmlStream);
    String entityName;
    int entityType;
    String sql = null;
    ArrayList<String[]> paramsBatch = new ArrayList<String[]>();
    ArrayList<Integer> paramTypes = new ArrayList<Integer>();
    while (xmlReader.hasNext()) {
      entityType = xmlReader.next();
      switch (entityType) {
        case XMLStreamReader.ENTITY_REFERENCE:
          // check if this is the expected entity
          entityName = xmlReader.getLocalName();
          if (!ERR_XML_ENTRIES_ENTITY.equals(entityName)) {
            throwUnexpectedEntity(xmlReader, entityName, entityType,
                "entityReference");
          }
          break;
        case XMLStreamReader.START_ELEMENT:
          entityName = xmlReader.getLocalName();
          if (ERR_XML_ROOT.equals(entityName)) {
            // root element
          }
          else if (ERR_XML_FAILURE.equals(entityName)) {
            sql = null;
            paramsBatch.clear();
            paramTypes.clear();
          }
          else if (ERR_XML_SQL.equals(entityName)) {
            // go to the nested elements
            entityType = xmlReader.next();
            switch (entityType) {
              case XMLStreamReader.CHARACTERS:
                sql = xmlReader.getText();
                break;
              default:
                throwUnexpectedEntity(xmlReader, xmlReader.getLocalName(),
                    entityType, ERR_XML_SQL);
            }
          }
          else if (ERR_XML_PARAMS.equals(entityName)) {
            // go to the nested elements
            boolean endReached = false;
            ArrayList<String> params = new ArrayList<String>();
            final boolean addParamTypes = paramTypes.isEmpty();
            while (xmlReader.hasNext() && !endReached) {
              entityType = xmlReader.nextTag();
              switch (entityType) {
                case XMLStreamReader.START_ELEMENT:
                  entityName = xmlReader.getLocalName();
                  if (!ERR_XML_PARAM.equals(entityName)) {
                    throwUnexpectedEntity(xmlReader, entityName, entityType,
                        ERR_XML_PARAMS);
                  }
                  boolean isNull = false;
                  // read the attributes
                  for (int i = 0; i < xmlReader.getAttributeCount(); i++) {
                    String attrName = xmlReader.getAttributeLocalName(i);
                    if (ERR_XML_ATTR_NULL.equals(attrName)) {
                      if (Boolean.parseBoolean(xmlReader.getAttributeValue(i))) {
                        params.add(null);
                        isNull = true;
                      }
                    }
                    else if (ERR_XML_ATTR_TYPE.equals(attrName)) {
                      if (addParamTypes) {
                        paramTypes.add(Integer.parseInt(xmlReader
                            .getAttributeValue(i)));
                      }
                    }
                    else {
                      throwUnexpectedEntity(xmlReader, attrName,
                          XMLStreamReader.ATTRIBUTE, ERR_XML_PARAM);
                    }
                  }
                  if (!isNull) {
                    entityType = xmlReader.next();
                    switch (entityType) {
                      case XMLStreamReader.CHARACTERS:
                        params.add(xmlReader.getText());
                        break;
                      case XMLStreamReader.COMMENT:
                        // skip comments
                        break;
                      default:
                        throwUnexpectedEntity(xmlReader,
                            xmlReader.getLocalName(), entityType, ERR_XML_PARAM);
                        break;
                    }
                  }
                  break;
                case XMLStreamReader.END_ELEMENT:
                  entityName = xmlReader.getLocalName();
                  if (ERR_XML_PARAMS.equals(entityName)) {
                    endReached = true;
                  }
                  else if (!ERR_XML_PARAM.equals(entityName)) {
                    throwUnexpectedEntity(xmlReader, entityName, entityType,
                        ERR_XML_PARAMS);
                  }
                  break;
                default:
                  throwUnexpectedEntity(xmlReader, xmlReader.getLocalName(),
                      entityType, ERR_XML_PARAMS);
                  break;
              }
            }
            if (params.size() > 0) {
              paramsBatch.add(params.toArray(new String[params.size()]));
            }
          }
          break;
        case XMLStreamReader.END_ELEMENT:
          entityName = xmlReader.getLocalName();
          if (ERR_XML_ROOT.equals(entityName)) {
            // root element
          }
          else if (ERR_XML_FAILURE.equals(entityName)) {
            // reached the end of one failure
            if (sql == null) {
              throw new XMLStreamException("no SQL found in current "
                  + "'failure' tag at: " + xmlReader.getLocation());
            }
            int[] result = executeStatement(conn, sql, paramsBatch, paramTypes);
            if (result.length == 1) {
              System.out.println("Updated " + result[0] + " rows.");
            }
            else {
              int numAffected = 0;
              String numAffectedStr = null;
              for (int res : result) {
                if (res < 0) {
                  numAffectedStr = "<unknown>";
                }
                else {
                  numAffected += res;
                }
              }
              if (numAffectedStr == null) {
                numAffectedStr = Integer.toString(numAffected);
              }
              System.out.println("Updated " + numAffectedStr
                  + " rows in a batch execution having " + paramsBatch.size()
                  + " elements.");
            }
            sql = null;
            paramsBatch.clear();
            paramTypes.clear();
          }
          else if (ERR_XML_SQL.equals(entityName)) {
            // nothing to do at this point
          }
          break;
        case XMLStreamReader.COMMENT:
          // skip comments
          break;
        default:
          // skip over rest of the elements
          break;
      }
    }
    conn.commit();
    xmlReader.close();
    xmlStream.close();
  }

  protected static int[] executeStatement(Connection conn, String sql,
      ArrayList<String[]> paramsBatch, ArrayList<Integer> paramTypes)
      throws Exception {
    PreparedStatement pstmt = conn.prepareStatement(sql);
    int batchSize = paramsBatch.size();
    if (batchSize > 0) {
      if (batchSize == 1) {
        setParameters(pstmt, paramsBatch.get(0), paramTypes);
      }
      else {
        for (String[] params : paramsBatch) {
          setParameters(pstmt, params, paramTypes);
          pstmt.addBatch();
        }
        return pstmt.executeBatch();
      }
    }
    return new int[] { pstmt.executeUpdate() };
  }

  /**
   * Set the value of a parameter in prepared statement given a string
   * representation as returned by
   */
  public static void setParameters(PreparedStatement pstmt, String[] params,
      ArrayList<Integer> paramTypes) throws Exception {
    for (int index = 0; index < params.length; index++) {
      String param = params[index];
      int paramIndex = index + 1;
      int paramType = paramTypes.get(index);
      byte[] bytes;

      if (param == null) {
        pstmt.setNull(paramIndex, paramType);
        continue;
      }
      switch (paramType) {
        case Types.BIGINT:
          final long longVal = Long.parseLong(param);
          pstmt.setLong(paramIndex, longVal);
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          final boolean boolVal;
          if ("1".equals(param)) {
            boolVal = true;
          }
          else if ("0".equals(param)) {
            boolVal = false;
          }
          else {
            boolVal = Boolean.parseBoolean(param);
          }
          pstmt.setBoolean(paramIndex, boolVal);
          break;
        case Types.DATE:
          final java.sql.Date dateVal = java.sql.Date.valueOf(param);
          pstmt.setDate(paramIndex, dateVal);
          break;
        case Types.DECIMAL:
        case Types.NUMERIC:
          final BigDecimal decimalVal = new BigDecimal(param);
          pstmt.setBigDecimal(paramIndex, decimalVal);
          break;
        case Types.DOUBLE:
          final double doubleVal = Double.parseDouble(param);
          pstmt.setDouble(paramIndex, doubleVal);
          break;
        case Types.FLOAT:
        case Types.REAL:
          final float floatVal = Float.parseFloat(param);
          pstmt.setFloat(paramIndex, floatVal);
          break;
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
          final int intVal = Integer.parseInt(param);
          pstmt.setInt(paramIndex, intVal);
          break;
        case Types.TIME:
          final java.sql.Time timeVal = java.sql.Time.valueOf(param);
          pstmt.setTime(paramIndex, timeVal);
          break;
        case Types.TIMESTAMP:
          final java.sql.Timestamp timestampVal = java.sql.Timestamp
              .valueOf(param);
          pstmt.setTimestamp(paramIndex, timestampVal);
          break;
        case Types.BINARY:
        case Types.BLOB:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
          bytes = ClientSharedUtils.fromHexString(param, 0, param.length());
          pstmt.setBytes(paramIndex, bytes);
          break;
        case Types.JAVA_OBJECT:
          bytes = ClientSharedUtils.fromHexString(param, 0, param.length());
          ByteArrayDataInput in = new ByteArrayDataInput();
          in.initialize(bytes, null);
          pstmt.setObject(paramIndex, DataSerializer.readObject(in));
          break;
        default:
          pstmt.setString(paramIndex, param);
          break;
      }
    }
  }

  protected static void throwUnexpectedEntity(XMLStreamReader xmlReader,
      String name, int type, String currentTag) throws XMLStreamException {
    throw new XMLStreamException("unexpected entity " + name + '[' + type + ']'
        + " inside '" + currentTag + "' at: " + xmlReader.getLocation());
  }
  
  public static void dummy() {
  }
}
