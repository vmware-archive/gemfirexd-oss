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
/*
 * Test TransactionImpl methods not tested by UserTransactionImpl
 * 
 * @author Mitul Bid
 */
package com.gemstone.gemfire.internal.jta;

import java.util.Properties;
import junit.framework.TestCase;
import javax.transaction.*;
import com.gemstone.gemfire.distributed.DistributedSystem;

public class TransactionImplTest extends TestCase {

  private static TransactionManagerImpl tm = TransactionManagerImpl
      .getTransactionManager();
  private UserTransaction utx = null;
  static {
    try {
      Properties props = new Properties();
//      props.setProperty("mcast-port", "10340");
      DistributedSystem.connect(props);
    } catch (Exception e) {
      fail("Unable to create Distributed System. Exception=" + e);
    }
  }

  public TransactionImplTest(String name) {
    super(name);
  }

  protected void setUp() {
    try {
      utx = new UserTransactionImpl();
    } catch (Exception e) {
      fail("set up failed due to " + e);
      e.printStackTrace();
    }
  }

  protected void tearDown() {
  }

  public void testRegisterSynchronization() {
    try {
      utx.begin();
      TransactionImpl txn = (TransactionImpl) tm.getTransaction();
      Synchronization sync = new SyncImpl();
      txn.registerSynchronization(sync);
      if (!txn.getSyncList().contains(sync))
          fail("Synchronization not registered succesfully");
      utx.commit();
    } catch (Exception e) {
      fail("exception in testRegister Synchronization due to " + e);
      e.printStackTrace();
    }
  }

  public void testNotifyBeforeCompletion() {
    try {
      utx.begin();
      TransactionImpl txn = (TransactionImpl) tm.getTransaction();
      SyncImpl sync = new SyncImpl();
      txn.registerSynchronization(sync);
      txn.notifyBeforeCompletion();
      if (!sync.befCompletion)
          fail("Notify before completion not executed succesfully");
      utx.commit();
    } catch (Exception e) {
      fail("exception in testNotifyBeforecompletion due to " + e);
      e.printStackTrace();
    }
  }

  public void testNotifyAfterCompletion() {
    try {
      utx.begin();
      TransactionImpl txn = (TransactionImpl) tm.getTransaction();
      SyncImpl sync = new SyncImpl();
      txn.registerSynchronization(sync);
      txn.notifyAfterCompletion(1);
      if (!sync.aftCompletion)
          fail("Notify after completion not executed succesfully");
      utx.commit();
    } catch (Exception e) {
      fail("exception in testNotifyAfterCompletion due to " + e);
      e.printStackTrace();
    }
  }
}
