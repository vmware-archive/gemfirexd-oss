/*

   Derby - Class com.pivotal.gemfirexd.internal.jdbc.ResourceAdapterImpl

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

package com.pivotal.gemfirexd.internal.jdbc;



//import com.pivotal.gemfirexd.internal.iapi.jdbc.XATransactionResource;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ResourceAdapter;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.services.info.JVMInfo;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.access.AccessFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.xa.XAResourceManager;
import com.pivotal.gemfirexd.internal.iapi.store.access.xa.XAXactId;
import com.pivotal.gemfirexd.internal.iapi.store.raw.GlobalTransactionId;
import com.pivotal.gemfirexd.internal.impl.store.raw.xact.TransactionTableEntry;
import com.pivotal.gemfirexd.internal.impl.store.raw.xact.Xact;


import java.util.Properties;
import java.util.Hashtable;
import java.util.Enumeration;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


public class ResourceAdapterImpl
		implements ResourceAdapter, ModuleControl
{
	private boolean active;

	// the real resource manager 
	private XAResourceManager rm;	

	// maps Xid to XATransationResource for run time transactions
	private Hashtable connectionTable;

	/*
	 * Module control
	 */

	public void boot(boolean create, Properties properties)
		throws StandardException
	{
		// we can only run on jdk1.2 or beyond with JTA and JAVA 20 extension
		// loaded.

		connectionTable = new Hashtable();

		AccessFactory af = 
			(AccessFactory)Monitor.findServiceModule(this, AccessFactory.MODULE);

		rm = (XAResourceManager) af.getXAResourceManager();

		active = true;
	}

	public void stop()
	{
		active = false;

		for (Enumeration e = connectionTable.elements(); e.hasMoreElements(); ) {

			XATransactionState tranState = (XATransactionState) e.nextElement();

			try {
				tranState.conn.close();
			} catch (java.sql.SQLException sqle) {
			}
		}

		active = false;
	}

	public boolean isActive()
	{
		return active;
	}

	/*
	 * Resource Adapter methods 
	 */

	public synchronized Object findConnection(XAXactId xid) {

		return connectionTable.get(xid);
	}

	public synchronized boolean addConnection(XAXactId xid, Object conn) {
		if (connectionTable.get(xid) != null)
			return false;

		// put this into the transaction table, if the xid is already
		// present as an in-doubt transaction, we need to remove it from
		// the run time list
		connectionTable.put(xid, conn);
		return true;
	}

	public synchronized Object removeConnection(XAXactId xid) {

		return connectionTable.remove(xid);

	}


	/**
		Return the XA Resource manager to the XA Connection
	 */
	public XAResourceManager getXAResourceManager()
	{
		return rm;
	}

  // Gemstone changes BEGIN
  @Override
  public Xid[] recover(int flags) throws StandardException {
    XAXactId[] ret_xid_list;

    if ((flags & XAResource.TMSTARTRSCAN) != 0) {
      XAXactId[] xid_list = new XAXactId[connectionTable.size()];
      int num_prepared = 0;

      // Need to hold sync while linear searching the hash table.
      synchronized (connectionTable) {
        int i = 0;

        for (Enumeration e = connectionTable.elements(); e.hasMoreElements(); i++) {
          XATransactionState xaTxnState = (XATransactionState)e.nextElement();

          if (xaTxnState.isPrepared()) {
            xid_list[i] = xaTxnState.getXId();
            num_prepared++;
          }
        }
      }

      // now need to squish the nulls out of the array to return.
      ret_xid_list = new XAXactId[num_prepared];
      int ret_index = 0;
      for (int i = xid_list.length; i-- > 0;) {
        if (xid_list[i] != null)
          ret_xid_list[ret_index++] = xid_list[i];
      }

      if (SanityManager.DEBUG) {
        SanityManager.ASSERT(ret_index == num_prepared);
      }
    }
    else {
      ret_xid_list = new XAXactId[0];
    }

    return (ret_xid_list);
  }
  // Gemstone changes END
}
