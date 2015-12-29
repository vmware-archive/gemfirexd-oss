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
package cacheperf.comparisons.gemfirexd.useCase1;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The object representation of a parsed backoffice message.<br>
 * A few assumptions:
 * <ul>
 * <li>The data type of all matching data are string</li>
 * <li>20121018 (Elvis+Senthil) BOMessage does not need to be thread-safe
 * because no two threads will access this domain object at any point of time</li>
 * </ul>
 */
public class BOMessage implements Serializable {

  public final static String TABLENAME = "SECL_BO_DATA_STATUS_HIST";

  /**
   * The unique transaction ID.
   */
  private String boTxnId;

  /**
   * Backoffice Code.
   */
  private String boCode;

  /**
   * The file type of the BO instruction.
   */
  //at this moment this field is not populated from the incoming BO message. - Assuming it to be PSEUDO SWIFT for transformation.
  private String fileType = "PSEUDO_SWIFT";

  /**
   * The timestamp when the BO listener first received the backoffice
   * instruction.
   */
  private long messageRecievedTimeInMicroSecs;

  /**
   * SECT_BACKOFFICE_DATA.OFAC_MSG_ID
   */
  private String ofacMsgId;

  /**
   * Holds ALL matching keys data available in this BO instruction in this
   * structure:<br>
   * Map<TABLE_NAME, Map<TABLE_COLUMN, VALUE>>
   */
  private Map<String,Map<String,Object>> boTableData = new HashMap<String,Map<String, Object>>();

  private boolean isLogicalUnMatch;
  private boolean isParseError = false;
  private String channelName;
  private int hitStatus;
  private String entity;

  public String getEntity() {
    return entity;
  }

  public void setEntity(String entity) {
    this.entity = entity;
  }

  public boolean isParseError() {
    return isParseError;
  }

  public void setParseError(boolean parseError) {
    isParseError = parseError;
  }

  public String getBoTxnId() {
    return boTxnId;
  }

  public void setBoTxnId(String boTxnId) {
    this.boTxnId = boTxnId;
  }

  public String getBoCode() {
    return boCode;
  }

  public void setBoCode(String boCode) {
    this.boCode = boCode;
  }

  public String getFileType() {
    return fileType;
  }

  public void setFileType(String fileType) {
    this.fileType = fileType;
  }

  public long getMessageRecievedTimeInMicroSecs() {
    return messageRecievedTimeInMicroSecs;
  }

  public void setMessageRecievedTimeInMicroSecs(long messageRecievedTimeInMicroSecs) {
    this.messageRecievedTimeInMicroSecs = messageRecievedTimeInMicroSecs;
  }

  public String getOfacMsgId() {
    return ofacMsgId;
  }

  public void setOfacMsgId(String ofacMsgId) {
    this.ofacMsgId = ofacMsgId;
  }

  public void setBoTableData(Map<String, Map<String, Object>> boTableData) {
    this.boTableData = boTableData;
  }

  public Map<String, Map<String, Object>> getBoTableData() {
    return boTableData;
  }

  /**
   * @return a set of all matching table names.
   */
  public Set<String> getBoTableNames() {
    return boTableData.keySet();
  }

  /**
   * @param tableName
   * @return a set of all matching column names of the given tableName
   */
  public Set<String> getBoTableColumnNames(final String tableName) {
    Set<String> result = null;
    if (null != tableName && tableName.length() > 0) {
      Map<String, Object> tableMatchingKeys = boTableData.get(tableName);
      if (null != tableMatchingKeys) {
        result = tableMatchingKeys.keySet();
      }
    }
    return result;
  }

  /**
   * @param tableName
   * @param columnName
   * @return the current mapping value associated with
   *         <tt>tableName->columnName</tt>, or <tt>null</tt> if there is no
   *         mapping for <tt>tableName->columnName</tt>
   */
  public Object getBoTableData(String tableName, String columnName) {
    Object result = null;
    if (null != tableName && null != columnName && tableName.length() > 0 && columnName.length() > 0) {
      Map<String, Object> tableMatchingKeys = boTableData.get(tableName);
      if (null != tableMatchingKeys) {
        result = tableMatchingKeys.get(columnName);
      }
    }
    return result;
  }

  /**
   * Put <tt>value</tt> into column <tt>columnName</tt> of backoffice table
   * <tt>tableName</tt>. This operation is not thread-safe.
   * 
   * @param tableName
   * @param columnName
   * @param value
   * @return the previous value associated with <tt>tableName->columnName</tt>
   *         , or <tt>null</tt> if there was no mapping for
   *         <tt>tableName->columnName</tt>.
   */
  public Object putBoTableData(final String tableName, final String columnName, final Object value) {
    Object result = null;
    if (null != tableName && null != columnName && null != value && tableName.length() > 0
        && columnName.length() > 0) {
      Map<String, Object> tableMatchingKeys = boTableData.get(tableName);
      if (null == tableMatchingKeys) {
        tableMatchingKeys = new HashMap<String, Object>();
        boTableData.put(tableName, tableMatchingKeys);
      }
      //hydra.Log.getLogWriter().info("HEY: put " + value + " into " + columnName);
      result = tableMatchingKeys.put(columnName, value);
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BOMessage [boTxnId=").append(boTxnId).append(", boCode=").append(boCode).append(", fileType=")
        .append(fileType).append(", messageRecievedTimeInMicroSecs=").append(messageRecievedTimeInMicroSecs)
        .append(", ofacMsgId=").append(ofacMsgId).append(", boTableData=").append(boTableData).append("]");
    return builder.toString();
  }

  public void setChannelName(String channelName) {
    this.channelName = channelName;
  }

  public String getChannelName() {
    return channelName;
  }

  public boolean isLogicalUnMatch() {
    return isLogicalUnMatch;
  }

  public void setLogicalUnMatch(boolean logicalUnMatch) {
    isLogicalUnMatch = logicalUnMatch;
  }

  public int getHitStatus() {
    return hitStatus;
  }

  public void setHitStatus(int hitStatus) {
    this.hitStatus = hitStatus;
  }
}
