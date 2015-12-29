/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINStatementDescriptor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.pivotal.gemfirexd.internal.impl.sql.catalog;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;

import java.sql.Types;

public class XPLAINStatementDescriptor extends XPLAINTableDescriptor {

  // implementation
  private UUID stmt_id; // the statement UUID

  private String stmt_name; // the statement name, if available

  private String stmt_type; // the statement type, e.g. select,insert, update,
                            // etc.

  private String stmt_text; // the statement text

  private String jvm_id; // the virtual machine identifier, only the code

  private String os_id; // the operating system identifier, from the os.home vm
                        // system variable

  private String current_member_id; // query executing node.

  private String origin_member_id; // query originating node.

  private String locally_executed; // query locally executed true/false.

  private String xplain_mode; // the explain mode, either (F)ull explain or
                              // explain (O)nly

  private Timestamp xplain_time; // the explain timestamp, useful if statistics
                                 // timing was off

  private String thread_id; // the explaining thread identifier

  private String xa_id; // the transaction identifier

  private String session_id; // the session identifier (instance)

  private String db_name; // the database name

  private String drda_id; // the drda identifier

  private Long parse_time;

  private Long bind_time;

  private Long optimize_time;

  private Long routing_info_time;

  private Long generate_time;

  private Long compile_time;

  private Long execute_time;

  private Timestamp begin_comp_time;

  private Timestamp end_comp_time;

  private Timestamp begin_exe_time;

  private Timestamp end_exe_time;
  
  private long begin_exe_time_l;

  private long end_exe_time_l;
  
  public XPLAINStatementDescriptor() {
    
  }

  public XPLAINStatementDescriptor(
      UUID stmt_id,
      String stmt_name,
      String stmt_type,
      String stmt_text,
      String jvm_id,
      String os_id,
      String current_member_id,
      String origin_member_id,
      String locally_executed,
      String xplain_mode,
      Timestamp xplain_time,
      String thread_id,
      String xa_id,
      String session_id,
      String db_name,
      String drda_id,
      Long parse_time,
      Long bind_time,
      Long optimize_time,
      Long routing_info_time,
      Long generate_time,
      Long compile_time,
      Long execute_time,
      Timestamp begin_comp_time,
      Timestamp end_comp_time,
      Timestamp begin_exe_time,
      Timestamp end_exe_time) {

    this.stmt_id = stmt_id;
    this.stmt_name = stmt_name;
    this.stmt_type = stmt_type;
    this.stmt_text = stmt_text;
    this.jvm_id = jvm_id;
    this.os_id = os_id;
    this.current_member_id = current_member_id;
    this.origin_member_id = origin_member_id;
    this.locally_executed = locally_executed;
    this.xplain_mode = xplain_mode;
    this.xplain_time = xplain_time;
    this.thread_id = thread_id;
    this.xa_id = xa_id;
    this.session_id = session_id;
    this.db_name = db_name;
    this.drda_id = drda_id;
    this.parse_time = parse_time;
    this.bind_time = bind_time;
    this.optimize_time =optimize_time; 
    this.routing_info_time = routing_info_time;
    this.generate_time = generate_time;
    this.compile_time = compile_time;
    this.execute_time = execute_time;
    this.begin_comp_time = begin_comp_time;
    this.end_comp_time = end_comp_time;
    this.begin_exe_time = begin_exe_time;
    this.end_exe_time = end_exe_time;
    this.begin_exe_time_l = this.begin_exe_time.getTime();
    this.end_exe_time_l = this.end_exe_time.getTime();
  }

  public void setStatementParameters(
      PreparedStatement ps) throws SQLException {
    int i=0;
    ps.setString(++i, stmt_id.toString());
    ps.setString(++i, stmt_name);
    ps.setString(++i, stmt_type);
    ps.setString(++i, stmt_text);
    ps.setString(++i, jvm_id);
    ps.setString(++i, os_id);
    ps.setString(++i, current_member_id);
    ps.setString(++i, origin_member_id);
    ps.setString(++i, locally_executed);
    ps.setString(++i, xplain_mode);
    ps.setTimestamp(++i, xplain_time);
    ps.setString(++i, thread_id);
    ps.setString(++i, xa_id);
    ps.setString(++i, session_id);
    ps.setString(++i, db_name);
    ps.setString(++i, drda_id);
    ps.setLong(++i, parse_time);
    ps.setLong(++i, bind_time);
    ps.setLong(++i, optimize_time);
    ps.setLong(++i, routing_info_time);
    ps.setLong(++i, generate_time);
    ps.setLong(++i, compile_time);
    ps.setLong(++i, execute_time);
    ps.setTimestamp(++i, begin_comp_time);
    ps.setTimestamp(++i, end_comp_time);
    ps.setTimestamp(++i, begin_exe_time);
    ps.setTimestamp(++i, end_exe_time);
    ps.setLong(++i, begin_exe_time_l);
    ps.setLong(++i, end_exe_time_l);
  }

  public String getCatalogName() {
    return TABLENAME_STRING;
  }

  public static final String TABLENAME_STRING = "SYSXPLAIN_STATEMENTS";

  private static final String[][] indexColumnNames = { { "STMT_ID" } };

  /**
   * Builds a list of columns suitable for creating this Catalog.
   * 
   * @return array of SystemColumn suitable for making this catalog.
   */
  public SystemColumn[] buildColumnList() {
    return new SystemColumn[] {
        SystemColumnImpl.getUUIDColumn("STMT_ID", false),
        SystemColumnImpl.getIdentifierColumn("STMT_NAME", true),
        SystemColumnImpl.getColumn("STMT_TYPE", Types.CHAR, false, 3),
        SystemColumnImpl.getColumn(
            "STMT_TEXT",
            Types.VARCHAR,
            false,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn(
            "JVM_ID",
            Types.VARCHAR,
            false,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn(
            "OS_IDENTIFIER",
            Types.VARCHAR,
            false,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn("CURRENT_MEMBER_ID", 
            Types.VARCHAR, 
            false,
            128),
        SystemColumnImpl.getColumn(
            "ORIGIN_MEMBER_ID",
            Types.VARCHAR,
            false,
            128),
        SystemColumnImpl.getColumn("LOCALLY_EXECUTED", Types.VARCHAR, false,
            6),
        SystemColumnImpl.getColumn("XPLAIN_MODE", Types.CHAR, true, 1),
        SystemColumnImpl.getColumn("XPLAIN_TIME", Types.TIMESTAMP, true),
        SystemColumnImpl.getColumn(
            "XPLAIN_THREAD_ID",
            Types.VARCHAR,
            false,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn(
            "TRANSACTION_ID",
            Types.VARCHAR,
            false,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn(
            "SESSION_ID",
            Types.VARCHAR,
            false,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getIdentifierColumn("DATABASE_NAME", false),
        SystemColumnImpl.getColumn(
            "DRDA_ID",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),

        SystemColumnImpl.getColumn("PARSE_TIME", Types.BIGINT, false),
        SystemColumnImpl.getColumn("BIND_TIME", Types.BIGINT, false),
        SystemColumnImpl.getColumn("OPTIMIZE_TIME", Types.BIGINT, false),
        SystemColumnImpl.getColumn("ROUTING_INFO_TIME", Types.BIGINT, false),
        SystemColumnImpl.getColumn("GENERATE_TIME", Types.BIGINT, false),
        SystemColumnImpl.getColumn("COMPILE_TIME", Types.BIGINT, false),
        SystemColumnImpl.getColumn("EXECUTE_TIME", Types.BIGINT, false),
        SystemColumnImpl.getColumn("BEGIN_COMP_TIME", Types.TIMESTAMP, false),
        SystemColumnImpl.getColumn("END_COMP_TIME", Types.TIMESTAMP, false),
        SystemColumnImpl.getColumn("BEGIN_EXE_TIME", Types.TIMESTAMP, false),
        SystemColumnImpl.getColumn("END_EXE_TIME", Types.TIMESTAMP, false),
        SystemColumnImpl.getColumn("BEGIN_EXE_TIME_L", Types.BIGINT, false),
        SystemColumnImpl.getColumn("END_EXE_TIME_L", Types.BIGINT, false),    };
  }

  @Override
  protected void addConstraints(
      StringBuilder sb) {
    // TODO Auto-generated method stub

  }
  
  public String toString() {
    return super.toString() + " STMT_ID=" + stmt_id //+ " TIMING_ID=" + timing_id
        + " CURRENT_MEMBER_ID=" + current_member_id + " ORIGIN_MEMBER_ID="
        + origin_member_id + " LOCALLY_EXECUTED=" + locally_executed
        + " DRDA_ID=" + drda_id;
  }

  @Override
  protected void createIndex(StringBuilder idx, String schemaName) {
    // TODO Auto-generated method stub
    
  }

}
