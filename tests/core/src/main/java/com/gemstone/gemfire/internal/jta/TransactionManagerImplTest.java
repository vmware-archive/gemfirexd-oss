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
package com.gemstone.gemfire.internal.jta;

/*
 * Test TransactionManagerImpl methods not tested by UserTransactionImplTest
 * 
 * @author Mitul Bid
 */
import java.util.Properties;
import junit.framework.TestCase;
import javax.transaction.*;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;

public class TransactionManagerImplTest extends TestCase {

  private static TransactionManagerImpl tm = TransactionManagerImpl
      .getTransactionManager();
  protected UserTransaction utx = null;
  static {
    try {
      Properties props = new Properties();
//      props.setProperty("mcast-port", "10340");
      DistributedSystem.connect(props);
    } catch (Exception e) {
      fail("Unable to create Distributed System. Exception=" + e);
    }
  }

  public TransactionManagerImplTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() {
    try {
      utx = new UserTransactionImpl();
    } catch (Exception e) {
      fail("set up failed due to " + e);
      e.printStackTrace();
    }
  }

  @Override
  protected void tearDown() {
  }

  public void testGetTransaction() {
    try {
      assertEquals(Status.STATUS_NO_TRANSACTION, tm.getStatus());
      utx.begin();
      assertEquals(Status.STATUS_ACTIVE, tm.getStatus());
      Transaction txn1 = TXManagerImpl.getCurrentJTA();
      Transaction txn2 = tm.getTransaction();
      if (txn1 != txn2)
          fail("GetTransaction not returning the correct transaction");
      utx.commit();
      assertEquals(Status.STATUS_NO_TRANSACTION, tm.getStatus());
    } catch (Exception e) {
      fail("exception in testGetTransaction due to " + e);
      e.printStackTrace();
    }
  }

  public void testGetTransactionImpl() {
    try {
      utx.begin();
      Transaction txn1 = TXManagerImpl.getCurrentJTA();
      Transaction txn2 = tm.getTransaction();
      if (txn1 != txn2)
          fail("GetTransactionImpl not returning the correct transaction");
      utx.commit();
    } catch (Exception e) {
      fail("exception in testGetTransactionImpl due to " + e);
      e.printStackTrace();
    }
  }

  public void testGetGlobalTransaction() {
    try {
      utx.begin();
      Transaction txn = TXManagerImpl.getCurrentJTA();
      GlobalTransaction gTxn1 = (GlobalTransaction) tm
          .getGlobalTransactionMap().get(txn);
      GlobalTransaction gTxn2 = tm.getGlobalTransaction();
      if (gTxn1 != gTxn2)
          fail("Get Global Transaction not returning the correct global transaction");
      utx.commit();
    } catch (Exception e) {
      fail("exception in testGetGlobalTransaction due to " + e);
      e.printStackTrace();
    }
  }

  //Asif : Test Notify BeforeCompletion Exception handling
  public void testNotifyBeforeCompletionException() {
    try {
      utx.begin();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    Transaction txn = TXManagerImpl.getCurrentJTA();
    try {
      txn.registerSynchronization(new Synchronization() {

        public void beforeCompletion() {
          throw new RuntimeException("MyException");
        }

        public void afterCompletion(int status) {
          assertTrue(status == Status.STATUS_ROLLEDBACK);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    try {
      utx.commit();
      fail("The commit should have thrown RolledBackException");
    } catch (RollbackException re) {
      System.out.println("SucessfulTest:Exception String =" + re);
      assertTrue(true);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  //	Asif : Test Exception when marking transaction for rollbackonly
  public void testSetRollBackOnly() {
    try {
      utx.begin();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    Transaction txn = TXManagerImpl.getCurrentJTA();
    try {
      txn.registerSynchronization(new Synchronization() {

        public void beforeCompletion() {
          try {
            utx.setRollbackOnly();
          } catch (Exception se) {
            fail("Set Roll Back only caused failure.Exception =" + se);
          }
        }

        public void afterCompletion(int status) {
          assertTrue(status == Status.STATUS_ROLLEDBACK);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    try {
      utx.commit();
      fail("The commit should have thrown RolledBackException");
    } catch (RollbackException re) {
      System.out.println("SucessfulTest:Exception String =" + re);
      assertTrue(true);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  public void testXAExceptionInCommit() {
    try {
      utx.begin();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    Transaction txn = TXManagerImpl.getCurrentJTA();
    try {
      txn.registerSynchronization(new Synchronization() {

        public void beforeCompletion() {
        }

        public void afterCompletion(int status) {
          assertTrue(status == Status.STATUS_ROLLEDBACK);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    try {
      txn.enlistResource(new XAResourceAdaptor() {

        @Override
        public void commit(Xid arg0, boolean arg1) throws XAException {
          throw new XAException(5);
        }

        @Override
        public void rollback(Xid arg0) throws XAException {
          throw new XAException(6);
        }
      });
    } catch (Exception e) {
      fail(e.toString());
    }
    try {
      utx.commit();
      fail("The commit should have thrown SystemException");
    } catch (SystemException se) {
      System.out.println("SucessfulTest:Exception String =" + se);
      assertTrue(true);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  public void testXAExceptionRollback() {
    try {
      utx.begin();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    Transaction txn = TXManagerImpl.getCurrentJTA();
    try {
      txn.registerSynchronization(new Synchronization() {

        public void beforeCompletion() {
          fail("Notify Before Completion should not be called in rollback");
        }

        public void afterCompletion(int status) {
          assertTrue(status == Status.STATUS_ROLLEDBACK);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    try {
      txn.enlistResource(new XAResourceAdaptor() {

        @Override
        public void commit(Xid arg0, boolean arg1) throws XAException {
        }

        @Override
        public void rollback(Xid arg0) throws XAException {
          throw new XAException(6);
        }
      });
    } catch (Exception e) {
      fail(e.toString());
    }
    try {
      utx.rollback();
      fail("The rollback should have thrown SystemException");
    } catch (SystemException se) {
      System.out.println("SucessfulTest:Exception String =" + se);
      assertTrue(true);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  public void testRollback() {
    try {
      utx.begin();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    Transaction txn = TXManagerImpl.getCurrentJTA();
    try {
      txn.registerSynchronization(new Synchronization() {

        public void beforeCompletion() {
          fail("Notify Before Completion should not be called in rollback");
        }

        public void afterCompletion(int status) {
          assertTrue(status == Status.STATUS_ROLLEDBACK);
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    try {
      txn.enlistResource(new XAResourceAdaptor() {
      });
    } catch (Exception e) {
      fail(e.toString());
    }
    try {
      utx.rollback();
      assertTrue(tm.getGlobalTransactionMap().isEmpty());
      System.out.println("RolledBack successfully");
    } catch (SystemException se) {
      fail("Exception String =" + se);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }
}
//Asif :Added helper class to test XAException situations while
// commit/rollback etc

class XAResourceAdaptor implements XAResource {

  public int getTransactionTimeout() throws XAException {
    return 0;
  }

  public boolean setTransactionTimeout(int arg0) throws XAException {
    return false;
  }

  public boolean isSameRM(XAResource arg0) throws XAException {
    return false;
  }

  public Xid[] recover(int arg0) throws XAException {
    return null;
  }

  public int prepare(Xid arg0) throws XAException {
    return 0;
  }

  public void forget(Xid arg0) throws XAException {
  }

  public void rollback(Xid arg0) throws XAException {
  }

  public void end(Xid arg0, int arg1) throws XAException {
  }

  public void start(Xid arg0, int arg1) throws XAException {
  }

  public void commit(Xid arg0, boolean arg1) throws XAException {
  }
}
