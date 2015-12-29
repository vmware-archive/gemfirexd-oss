/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINSortPropsDescriptor

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

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;

import java.sql.Types;

/**
 * This class describes a Tuple for the XPLAIN_SORT_PROPS System Table.
 */
public class XPLAINSortPropsDescriptor extends XPLAINTableDescriptor 
{

    private UUID sort_rs_id;  // the sort props UUID
    protected String sort_type; // the sort type: internal or external
    protected Integer no_input_rows; // the number of input rows of this sort
    protected Integer no_output_rows; // the number of output rows of this sort
    protected Integer no_merge_runs; // the number of merge sort runs
    protected String merge_run_details; // merge run details, internal encoding
    protected String eliminate_dups; // eliminate duplicates during sort
    protected String in_sort_order; // is already in sorted order
    protected String distinct_aggregate; // has distinct aggregates
    
    protected XPLAINSortPropsDescriptor() {}
    public XPLAINSortPropsDescriptor
    (
             final UUID sort_rs_id,
             final String sort_type,
             final Integer no_input_rows,
             final Integer no_output_rows,
             final Integer no_merge_runs,
             final String merge_run_details,
             final String eliminate_dups,
             final String in_sort_order,
             final String distinct_aggregate
    )
    {

        this.sort_rs_id = sort_rs_id;
        this.sort_type = sort_type;
        this.no_input_rows = no_input_rows;
        this.no_output_rows = no_output_rows;
        this.no_merge_runs = no_merge_runs;
        this.merge_run_details = merge_run_details;
        this.eliminate_dups = eliminate_dups;
        this.in_sort_order = in_sort_order;
        this.distinct_aggregate = distinct_aggregate;
        
    }
    public void setStatementParameters(final PreparedStatement ps)
        throws SQLException
    {
        ps.setString(1, sort_rs_id.toString());
        ps.setString(2, sort_type);
        if (no_input_rows != null)
            ps.setInt(3, no_input_rows.intValue());
        else
            ps.setNull(3, Types.INTEGER);
        if (no_output_rows != null)
            ps.setInt(4, no_output_rows.intValue());
        else
            ps.setNull(4, Types.INTEGER);
        if (no_merge_runs != null)
            ps.setInt(5, no_merge_runs.intValue());
        else
            ps.setNull(5, Types.INTEGER);
        ps.setString(6, merge_run_details);
        ps.setString(7, eliminate_dups);
        ps.setString(8, in_sort_order);
        ps.setString(9, distinct_aggregate);
    }
    
    public void setSort_type(final String sort_type) {
        this.sort_type = sort_type;
    }

    public void setNo_input_rows(final Integer no_input_rows) {
        this.no_input_rows = no_input_rows;
    }

    public void setNo_output_rows(final Integer no_output_rows) {
        this.no_output_rows = no_output_rows;
    }

    public void setNo_merge_runs(final Integer no_merge_runs) {
        this.no_merge_runs = no_merge_runs;
    }

    public void setMerge_run_details(final String merge_run_details) {
        this.merge_run_details = merge_run_details;
    }


    public String getCatalogName() { return TABLENAME_STRING; }
    static  final   String  TABLENAME_STRING = "SYSXPLAIN_SORT_PROPS";

    private static final String[][] indexColumnNames =
    {
        {"SORT_RS_ID"}
    };

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList() {
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("SORT_RS_ID", false),
            SystemColumnImpl.getColumn("SORT_TYPE", Types.CHAR, true, 2),
            SystemColumnImpl.getColumn("NO_INPUT_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_OUTPUT_ROWS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("NO_MERGE_RUNS", Types.INTEGER, true),
            SystemColumnImpl.getColumn("MERGE_RUN_DETAILS", Types.VARCHAR, true,
                    TypeId.VARCHAR_MAXWIDTH),
            SystemColumnImpl.getColumn("ELIMINATE_DUPLICATES", Types.CHAR, true, 1),
            SystemColumnImpl.getColumn("IN_SORT_ORDER", Types.CHAR, true, 1),
            SystemColumnImpl.getColumn("DISTINCT_AGGREGATE", Types.CHAR, true, 1),
        };
    }
    @Override
    protected void addConstraints(final StringBuilder sb) {
      // TODO Auto-generated method stub
      
    }
    
    public String toString() {
      return "SortProps@" + System.identityHashCode(this) + " SORT_RS_ID=" + sort_rs_id ;
    }
    
    @Override
    protected void createIndex(StringBuilder idx, String schemaName) {
      // TODO Auto-generated method stub
      
    }
    public UUID getRSID() {
      return sort_rs_id;
    }

}
