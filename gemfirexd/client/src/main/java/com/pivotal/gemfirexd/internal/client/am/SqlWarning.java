/*

   Derby - Class com.pivotal.gemfirexd.internal.client.am.SqlWarning

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

import java.sql.SQLWarning;

/**
 * This represents a warning versus a full exception.  As with
 * SqlException, this is an internal representation of java.sql.SQLWarning.
 *
 * Public JDBC methods need to convert an internal SqlWarning to a SQLWarning
 * using <code>getSQLWarning()</code>
 */
public class SqlWarning extends SqlException implements Diagnosable {

    protected SqlWarning nextWarning_;
    
    public SqlWarning(LogWriter logwriter, 
        ClientMessageId msgid, Object[] args, Throwable cause)
    {
        super(logwriter, msgid, args, cause);
    }
    
    public SqlWarning(LogWriter logwriter, ClientMessageId msgid, Object[] args)
    {
        this(logwriter, msgid, args, null);
    }
    
    public SqlWarning (LogWriter logwriter, ClientMessageId msgid)
    {
        super(logwriter, msgid);
    }
    
    public SqlWarning(LogWriter logwriter, ClientMessageId msgid, Object arg1)
    {
        super(logwriter, msgid, arg1);
    }
    
    public SqlWarning(LogWriter logwriter,
        ClientMessageId msgid, Object arg1, Object arg2)
    {
        super(logwriter, msgid, arg1, arg2);
    }
    
    public SqlWarning(LogWriter logwriter,
        ClientMessageId msgid, Object arg1, Object arg2, Object arg3)
    {
        super(logwriter, msgid, arg1, arg2, arg3);
    }
    
    public SqlWarning(LogWriter logWriter, Sqlca sqlca)
    {
        super(logWriter, sqlca);
    }
    
// GemStone changes BEGIN
    protected SqlWarning(LogWriter logWriter, Throwable throwable,
        String reason, String sqlState, int errorCode) {
      super(logWriter, throwable, reason, sqlState, errorCode, null, null);
    }

// GemStone changes END
    public void setNextWarning(SqlWarning warning)
    {
        // Add this warning to the end of the chain
        SqlWarning theEnd = this;
        while (theEnd.nextWarning_ != null) {
            theEnd = theEnd.nextWarning_;
        }
        theEnd.nextWarning_ = warning;
    }
    
    public SqlWarning getNextWarning()
    {
        return nextWarning_;
    }
    
    /**
     * Get the java.sql.SQLWarning for this SqlWarning
     */
// GemStone changes BEGIN
    public SQLWarning getSQLWarning(final Agent agent) {
        final String message;
        final String sqlState = getSQLState();
        final int errorCode = getErrorCode();
        // fetching exception should not change inUnitOfWork (#44311)
        Connection conn = null;
        boolean savedInUnitOfWork = false;
        if (agent != null) {
          if ((conn = agent.connection_) != null) {
            savedInUnitOfWork = conn.inUnitOfWork_;
          }
          message = getMessage() + " (SQLState=" + sqlState + ",Severity="
              + errorCode + ",Server=" + agent.getServerLocation() + ')';
        }
        else {
          message = getMessage();
        }
        SQLWarning sqlw = new SQLWarning(message, sqlState,
            errorCode);
    /* (original code)
    public SQLWarning getSQLWarning()
    {
        SQLWarning sqlw = new SQLWarning(getMessage(), getSQLState(), 
            getErrorCode());
    */
// GemStone changes END

        sqlw.initCause(this);

        // Set up the nextException chain
        if ( nextWarning_ != null )
        {
            // The exception chain gets constructed automatically through 
            // the beautiful power of recursion
            //
            // We have to use the right method to convert the next exception
            // depending upon its type.  Luckily with all the other subclasses
            // of SQLException we don't have to make our own matching 
            // subclasses because 
            sqlw.setNextException(
                nextException_ instanceof SqlWarning ?
                    ((SqlWarning)nextException_).getSQLWarning(null /* GemStoneAddition */) :
                    nextException_.getSQLException(null /* GemStoneAddition */));
        }
// GemStone changes BEGIN
        // restore inUnitOfWork
        if (conn != null) {
          conn.setInUnitOfWork(savedInUnitOfWork);
        }
// GemStone changes END
        
        return sqlw;
        
    }
}

