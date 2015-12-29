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
 * Check if the correct excpetions are being thrown when they have to be thrown *
 * @author Mitul bid
 */
import java.util.Properties;
import junit.framework.TestCase;
import javax.transaction.*;

import com.gemstone.gemfire.distributed.DistributedSystem;

public class ExceptionTest extends TestCase {

  private static TransactionManagerImpl tm = TransactionManagerImpl
      .getTransactionManager();
  private UserTransaction utx = null;
  static {
    try {
      Properties props = new Properties();
//      props.setProperty("mcast-port", "10340");
      DistributedSystem.connect(props);
    } catch (Exception e) {
      fail("test set up failed");
      e.printStackTrace();
    }
  }

  public ExceptionTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() {
    try {
      utx = new UserTransactionImpl();
    } catch (Exception e) {
      fail("test set up failed");
      e.printStackTrace();
    }
  }

  @Override
  protected void tearDown() {
  }

  public void testNestedTransactionNotSupported() {
    boolean result = true;
    try {
      utx.begin();
      utx.begin();
      utx.commit();
    } catch (NotSupportedException e) {
      result = false;
      try {
        utx.commit();
      } catch (Exception ex) {
        fail("testNestedTransaction exception in catch block" + ex);
        ex.printStackTrace();
      }
    } catch (Exception e) {
      fail("testNestedTransaction exception occured" + e);
      e.printStackTrace();
    }
    if (result) fail("Exception on nested transaction not thrown");
  }

  public void testCommitSystemException() {
    boolean result = true;
    try {
      utx.commit();
    } catch (IllegalStateException e) {
      result = false;
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (result) fail("IllegalStateException not thrown on commit");
  }

  public void testRollbackIllegalStateException() {
    boolean result = true;
    try {
      utx.begin();
      GlobalTransaction gtx = tm.getGlobalTransaction();
      gtx.setStatus(Status.STATUS_UNKNOWN);
      utx.rollback();
    } catch (IllegalStateException e) {
      result = false;
      try {
        GlobalTransaction gtx = tm.getGlobalTransaction();
        gtx.setStatus(Status.STATUS_ACTIVE);
        utx.commit();
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Exception thrown in Catch of  Illegal State Exception in testRollbackIllegal");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception thrown in Illegal State Exception in testRollbackIllegalState");
    }
    if (result) fail("Illegal State Exception not thrown on rollback");
  }

  public void testAddNullTransaction() {
    boolean result = true;
    try {
      utx.begin();
      GlobalTransaction gtx = tm.getGlobalTransaction();
      Transaction txn = null;
      gtx.addTransaction(txn);
      utx.commit();
    } catch (SystemException e) {
      result = false;
      try {
        utx.commit();
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Exception thrown in catch of Illegal State Exception testAddNullTransaction");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception thrown in  Illegal State Exception testAddNullTransaction");
    }
    if (result) fail("SystemException not thrown on adding null transaction");
  }
}
