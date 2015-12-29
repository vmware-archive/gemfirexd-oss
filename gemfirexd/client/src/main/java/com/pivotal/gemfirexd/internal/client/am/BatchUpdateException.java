/*

   Derby - Class com.pivotal.gemfirexd.internal.client.am.BatchUpdateException

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

import com.pivotal.gemfirexd.internal.iapi.services.info.JVMInfo;
import com.pivotal.gemfirexd.internal.shared.common.error.ExceptionUtil;
import com.pivotal.gemfirexd.internal.shared.common.i18n.MessageUtil;

public class BatchUpdateException extends java.sql.BatchUpdateException {

    /** 
     *  The message utility instance we use to find messages
     *  It's primed with the name of the client message bundle so that
     *  it knows to look there if the message isn't found in the
     *  shared message bundle.
     */
    private static MessageUtil msgutil_ = 
        SqlException.getMessageUtil();

    public BatchUpdateException(LogWriter logWriter, ClientMessageId msgid,
        Object[] args, int[] updateCounts, SqlException cause)
    {
        super(
            msgutil_.getCompleteMessage(
                msgid.msgid,
                args),
            ExceptionUtil.getSQLStateFromIdentifier(msgid.msgid),
            ExceptionUtil.getSeverityFromIdentifier(msgid.msgid),
            updateCounts);

        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }

        if (cause != null) {
            initCause(cause);
            setNextException(cause.getSQLException(
                null /* GemStoneAddition */));
        }
    }
    
    // Syntactic sugar constructors to make it easier to create
    // a BatchUpdateException with substitution parameters
    public BatchUpdateException(LogWriter logWriter, ClientMessageId msgid,
        Object[] args, int[] updateCounts) {
        this(logWriter, msgid, args, updateCounts, null);
    }

    public BatchUpdateException(LogWriter logWriter, ClientMessageId msgid,
        int[] updateCounts)
    {
        this(logWriter, msgid, (Object [])null, updateCounts);
    }
    
    public BatchUpdateException(LogWriter logWriter, ClientMessageId msgid,
        Object arg1, int[] updateCounts)
    {
        this(logWriter, msgid, new Object[] {arg1}, updateCounts);
    }
    
    //-----------------old constructors - to be removed when i18n is complete
    //-----------------------------------------------

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter) {
        this(logWriter, null, null, SqlException.DEFAULT_ERRCODE, null);
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, int[] updateCounts) {
        this(logWriter, null, null, SqlException.DEFAULT_ERRCODE, updateCounts);
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, String reason, int[] updateCounts) {
        this(logWriter, reason, null, SqlException.DEFAULT_ERRCODE, updateCounts);
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, String reason, String sqlState, int[] updateCounts) {
        this(logWriter, reason, sqlState, SqlException.DEFAULT_ERRCODE, updateCounts);
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, String reason, String sqlState, int errorCode, int[] updateCounts) {
// GemStone changes BEGIN
      this(logWriter, reason, sqlState, errorCode, updateCounts, null);
    }

  public BatchUpdateException(LogWriter logWriter, String reason,
      String sqlState, int errorCode, int[] updateCounts, SqlException cause) {
// GemStone changes END
        super(reason, sqlState, errorCode, updateCounts);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
// GemStone changes BEGIN
        if (cause != null) {
          initCause(cause);
          setNextException(cause.getSQLException(null));
        }
// GemStone changes END
    }
}

