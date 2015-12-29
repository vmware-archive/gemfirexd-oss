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
package sql.sqlutil;

import java.sql.Connection;

public class IsolationRRConnectionCustomizer extends
    MyConnectionCustomizer {
  @Override
  public void onAcquire(Connection c, String parentDataSourceIdentityToken) 
    throws Exception {
    super.onAcquire(c, parentDataSourceIdentityToken);
    c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
  }
  
  @Override
  public void onCheckOut(Connection c, String parentDataSourceIdentityToken)
    throws java.lang.Exception {
    c.setAutoCommit(false);
  }
  
}
