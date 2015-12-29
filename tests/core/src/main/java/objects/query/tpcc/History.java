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

public class History implements Serializable {
  
  public int    h_c_id;
  public int    h_c_d_id;
  public int    h_c_w_id;
  public int    h_d_id;
  public int    h_w_id;
  public long   h_date;
  public float  h_amount;
  public String h_data;

  public String toString()
  {
    return (
      "\n***************** History ********************" +
      "\n*   h_c_id = " + h_c_id +
      "\n* h_c_d_id = " + h_c_d_id +
      "\n* h_c_w_id = " + h_c_w_id +
      "\n*   h_d_id = " + h_d_id +
      "\n*   h_w_id = " + h_w_id +
      "\n*   h_date = " + h_date +
      "\n* h_amount = " + h_amount +
      "\n*   h_data = " + h_data +
      "\n**********************************************"
      );
  }
}
