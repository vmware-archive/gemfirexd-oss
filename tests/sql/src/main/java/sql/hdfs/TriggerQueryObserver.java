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
package sql.hdfs;

import hydra.Log;

import java.io.Serializable;

import sql.SQLBB;

import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TriggerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.GenericParameterValueSet;

public class TriggerQueryObserver extends GemFireXDQueryObserverAdapter implements Serializable {
  
 
  
  public void beforeRowTrigger(LanguageConnectionContext lcc, ExecRow execRow,
      ExecRow newRow) {
    SQLBB.getBB().getSharedCounters().incrementAndRead(SQLBB.triggerInvocationCounter);
  }

  public void afterRowTrigger(TriggerDescriptor trigD,
      GenericParameterValueSet gpv) {   
    String data = " ";

    String[] columns;
    try {
      columns = trigD.getTableDescriptor().getColumnNamesArray();
      for (int i = 0; i < gpv.getParameterCount() ; i++) {
        if ( ! columns[i].equalsIgnoreCase("companyinfo"))
        data += columns[i] + ": " + gpv.getParameter(i) + " ";
      }
    } catch (StandardException e) {
      // TODO Auto-generated catch block
       Log.getLogWriter().info("error from TriggerQueryObserver- " + e.getMessage());
    }

    Log.getLogWriter().info(trigD.toString() + " invoked with " 
        + data + " completed successfully");
    long numOfInvocation = SQLBB.getBB().getSharedCounters().decrementAndRead(SQLBB.triggerInvocationCounter);
  }

}
