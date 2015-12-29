/*

   Derby - Class com.pivotal.gemfirexd.internal.client.am.Savepoint

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

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.client.am;

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

import java.sql.SQLException;

public class Savepoint implements java.sql.Savepoint {
    // ----------------- internals -----------------------------------------------

    int savepointId_ = 0;
    String savepointName_ = null;
    Agent agent_;

    //---------------------constructors/finalizer---------------------------------

    // create a named savepoint.
    Savepoint(Agent agent, String savepointName) {
        agent_ = agent;
        savepointName_ = savepointName;
    }

    // create an un-named savepoint.
    Savepoint(Agent agent, int savepointId) {
        agent_ = agent;
        savepointId_ = savepointId;
    }

    // ----------------- externals -----------------------------------------------

    public int getSavepointId() throws SQLException {
        if (savepointId_ != 0) {
            return savepointId_;
        } else {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.NO_ID_FOR_NAMED_SAVEPOINT)).getSQLException(
                    agent_ /* GemStoneAddition */);
        }
    }

    public String getSavepointName() throws SQLException {
        if (savepointName_ != null) {
            return savepointName_;
        } else {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.NO_NAME_FOR_UNNAMED_SAVEPOINT)).getSQLException(
                    agent_ /* GemStoneAddition */);
        }
    }
}
