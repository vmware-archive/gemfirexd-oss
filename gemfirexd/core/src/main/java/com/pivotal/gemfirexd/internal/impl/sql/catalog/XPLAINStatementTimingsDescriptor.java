/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINStatementTimingsDescriptor

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

import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

public class XPLAINStatementTimingsDescriptor extends XPLAINTableDescriptor 
{

    private UUID timing_id;  // the Timing UUID, which is saved in the xplain_Statements table, if statistics timing is switched on
    private Long parse_time; // the time needed for parsing the stmt text 
    private Long bind_time;  // the time needed for binding the node tree
    private Long optimize_time; // time needed for optimizing the node tree
    private Long routing_info_time;
    private Long generate_time; // time needed for class generation
    private Long compile_time; // time needed for parse+bind+optimize+generate
    private Long execute_time; // time needed for execution of class 
    private Timestamp begin_comp_time; // the begin compilation timestamp
    private Timestamp end_comp_time;   // the end   compilation timestamp
    private Timestamp begin_exe_time;  // the begin execution timestamp
    private Timestamp end_exe_time;    // the end   execution timestamp
    
    protected XPLAINStatementTimingsDescriptor() {}
    public XPLAINStatementTimingsDescriptor
    (
            UUID timing_id,
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
            Timestamp end_exe_time
    )
    {
        this.timing_id       = timing_id;
        this.parse_time      = parse_time;
        this.bind_time       = bind_time;
        this.optimize_time   = optimize_time;
        this.routing_info_time = routing_info_time;
        this.generate_time   = generate_time;
        this.compile_time    = compile_time;
        this.execute_time    = execute_time;
        this.begin_comp_time = begin_comp_time;
        this.end_comp_time   = end_comp_time;
        this.begin_exe_time  = begin_exe_time;
        this.end_exe_time    = end_exe_time;
        
    }
    public void setStatementParameters(PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, timing_id.toString());
        if (parse_time != null)
            ps.setLong(2, parse_time.longValue());
        else
            ps.setNull(2, Types.BIGINT);
        if (bind_time != null)
            ps.setLong(3, bind_time.longValue());
        else
            ps.setNull(3, Types.BIGINT);
        if (optimize_time != null)
            ps.setLong(4, optimize_time.longValue());
        else
            ps.setNull(4, Types.BIGINT);
        if (routing_info_time != null)
          ps.setLong(5, routing_info_time.longValue());
        else
          ps.setNull(5, Types.BIGINT);
        if (generate_time != null)
            ps.setLong(6, generate_time.longValue());
        else
            ps.setNull(6, Types.BIGINT);
        if (compile_time != null)
            ps.setLong(7, compile_time.longValue());
        else
            ps.setNull(7, Types.BIGINT);
        if (execute_time != null)
            ps.setLong(8, execute_time.longValue());
        else
            ps.setNull(8, Types.BIGINT);
        ps.setTimestamp(9, begin_comp_time);
        ps.setTimestamp(10, end_comp_time);
        ps.setTimestamp(11, begin_exe_time);
        ps.setTimestamp(12, end_exe_time);
    }
    
    public String getCatalogName() { return TABLENAME_STRING; }
    static final String             TABLENAME_STRING = "SYSXPLAIN_STATEMENT_TIMINGS";

    private static final String[][] indexColumnNames =
    {
        {"TIMING_ID"}
    };

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList() {
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("TIMING_ID", false),
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
        };
    }
    @Override
    protected void addConstraints(StringBuilder sb) {
      // TODO Auto-generated method stub
      
    }
    
    public long getExecuteTime() {
      return (execute_time != null ? execute_time : 0);
    }
    
    public String toString() {
      return super.toString() + " TIMING_ID=" + timing_id + " COMPILE_TIME="
          + compile_time + " EXECUTE_TIME=" + execute_time
          + " BEGIN_EXECUTE_TIME=" + begin_exe_time + " END_EXECUTE_TIME="
          + end_exe_time;
    }
    @Override
    protected void createIndex(StringBuilder idx, String schemaName) {
    }

}
