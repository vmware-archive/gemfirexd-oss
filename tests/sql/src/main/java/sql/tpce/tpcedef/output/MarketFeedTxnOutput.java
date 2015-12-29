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
package sql.tpce.tpcedef.output;

import sql.tpce.tpcedef.TPCETxnOutput;

public class MarketFeedTxnOutput implements TPCETxnOutput {
  private static final long serialVersionUID = 100020002;
  
  private int num_updated;
  private int send_len;
  private int status;
  
  public int getNumUpdated() {
    return num_updated;
  }
  
  public int getSendLen() {
    return send_len;
  }
  
  public void setNumUpdated(int num_updated) {
    this.num_updated = num_updated;
  }
  
  public void setSendLen(int send_len) {
    this.send_len = send_len;
  }

  public int getStatus() {
    return this.status;
  }
  
  public void setStatus(int status) {
    this.status = status;
  }
}
