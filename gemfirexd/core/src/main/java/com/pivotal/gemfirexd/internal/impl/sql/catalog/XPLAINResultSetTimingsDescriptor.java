/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINResultSetTimingsDescriptor

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

import java.io.Serializable;

import com.pivotal.gemfirexd.internal.catalog.UUID;

public class XPLAINResultSetTimingsDescriptor 
{
  
    private UUID timing_id; // the timuing UUID for the result set timing information
    protected long constructor_time; // the time needed to create an object, through a call of the constructor
    protected long open_time; // the time needed to process all open calls
    protected long next_time; // the time needed to process all next calls
    protected long close_time; // the time needed to close the resultset
    protected long execute_time; // the time needed for overall execution
    protected long avg_next_time_per_row; // the avarage time needed for a next call per row
    protected long projection_time; // the time needed by a ProjectRestrictResultSet to do the projection
    protected long restriction_time; // the time needed by a ProjectRestrictResultSet to do the restriction
    protected long temp_cong_create_time; //  the timestamp of th creation of a temporary conglomerate
    protected long temp_cong_fetch_time; // the time needed to do a fetch from this temporary conglomerate
    

    protected XPLAINResultSetTimingsDescriptor() {}
    public XPLAINResultSetTimingsDescriptor
    (
            UUID timing_id,
            long constructor_time,
            long open_time,
            long next_time,
            long close_time,
            long execute_time,
            long avg_next_time_per_row,
            long projection_time,
            long restriction_time,
            long temp_cong_create_time,
            long temp_cong_fetch_time
    )
    {
        
        this.timing_id = timing_id;
        this.constructor_time = constructor_time;
        this.open_time = open_time;
        this.next_time = next_time;
        this.close_time = close_time;
        this.execute_time = execute_time;
        this.avg_next_time_per_row = avg_next_time_per_row;
        this.projection_time = projection_time;
        this.restriction_time = restriction_time;
        this.temp_cong_create_time = temp_cong_create_time;
        this.temp_cong_fetch_time = temp_cong_fetch_time;
    }
    
    public UUID getTimingID() {
      return timing_id;
    }

    public String toString() {
      return "ResultSetTiming@" + System.identityHashCode(this) + " TIMING_ID=" + timing_id + " EXECUTE_TIME="
          + execute_time;
    }
    
    public void setAvgNextTime(long avg) {
      avg_next_time_per_row = avg;
    }

    public Long getExecuteTime() {
      return this.execute_time;
    }
}
