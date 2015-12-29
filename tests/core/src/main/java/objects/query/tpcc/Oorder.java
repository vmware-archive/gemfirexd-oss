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
package objects.query.tpcc;

import java.io.Serializable;

public class Oorder implements Serializable {
  
  public int  o_id;
  public int  o_w_id;
  public int  o_d_id;
  public int  o_c_id;
  public int  o_carrier_id;
  public int  o_ol_cnt;
  public int  o_all_local;
  public long o_entry_d;

  public String toString()
  {
    java.sql.Timestamp entry_d = new java.sql.Timestamp(o_entry_d);

    return (
      "\n***************** Oorder ********************" +
      "\n*         o_id = " + o_id +
      "\n*       o_w_id = " + o_w_id +
      "\n*       o_d_id = " + o_d_id +
      "\n*       o_c_id = " + o_c_id +
      "\n* o_carrier_id = " + o_carrier_id +
      "\n*     o_ol_cnt = " + o_ol_cnt +
      "\n*  o_all_local = " + o_all_local +
      "\n*    o_entry_d = " + entry_d +
      "\n**********************************************"
      );
  }
}
