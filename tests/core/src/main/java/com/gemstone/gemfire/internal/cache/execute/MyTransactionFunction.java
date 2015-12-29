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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import util.TestException;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.ConflictException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.TXEntryState;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXRegionState;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.execute.PRTransactionDUnitTest.TransactionListener2;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;

public class MyTransactionFunction implements Function {

  public void execute(FunctionContext context) {
    RegionFunctionContext ctx = (RegionFunctionContext)context;
    verifyExecutionOnPrimary(ctx);
    ArrayList args = (ArrayList)ctx.getArguments();
    Integer testOperation = (Integer)args.get(0);
    int op = testOperation.intValue();
    switch (op) {
    case PRTransactionDUnitTest.VERIFY_TX:
      verifyTransactionExecution(ctx);
      ctx.getDataSet().getCache().getLogger().info(
          "verifyTransactionExecution Passed");
      break;
    case PRTransactionDUnitTest.VERIFY_ROLLBACK:
      verifyTransactionRollback(ctx);
      ctx.getDataSet().getCache().getLogger().info(
          "verifyTransactionRollback Passed");
      break;
    case PRTransactionDUnitTest.VERIFY_DESTROY:
      verifyDestroyOperation(ctx, true);
      ctx.getDataSet().getCache().getLogger().info("verifyDestroy Passed");
      break;
    case PRTransactionDUnitTest.VERIFY_INVALIDATE:
      verifyInvalidateOperation(ctx, true);
      ctx.getDataSet().getCache().getLogger().info("verifyInvalidate Passed");
      break;
    case PRTransactionDUnitTest.VERIFY_NON_COLOCATION:
      verifyNonCoLocatedSuccess(ctx);
      ctx.getDataSet().getCache().getLogger().info(
          "verifyNonCoLocatedOpsRejection Passed");
      break;
    case PRTransactionDUnitTest.VERIFY_LISTENER_CALLBACK:
      verifyListenerCallback(ctx);
      break;
    case PRTransactionDUnitTest.VERIFY_TXSTATE_CONFLICT:
      verifyTxStateAndConflicts(ctx);
      break;
    case PRTransactionDUnitTest.VERIFY_REP_READ:
      verifyRepeatableRead(ctx);
      break;
    }
    context.getResultSender().lastResult(null);
  }  

  public String getId() {
    return "txFuntion";
  }

  private void verifyTransactionExecution(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache()
        .getCacheTransactionManager();
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    Customer newCus = (Customer)args.get(2);
    OrderId orderId = (OrderId)args.get(3);
    Order order = (Order)args.get(4);
    mgr.begin();
    custPR.put(custId, newCus);
    Assert.assertTrue(custPR.containsKey(custId));
    Assert.assertTrue(custPR.containsValueForKey(custId));
    orderPR.put(orderId, order);
    Assert.assertTrue(orderPR.containsKey(orderId));
    Assert.assertTrue(orderPR.containsValueForKey(orderId));
    mgr.commit();
    Customer commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(newCus.equals(commitedCust), "Expected Customer to be:"
        + newCus + " but was:" + commitedCust);
    Order commitedOrder = (Order)orderPR.get(orderId);
    Assert.assertTrue(order.equals(commitedOrder), "Expected Order to be:"
        + order + " but was:" + commitedOrder);
    //put a never before put key
    OrderId newOrderId = new OrderId(4000,custId);
    Order newOrder = new Order("NewOrder");
    mgr.begin();
    custPR.put(custId, newCus);
    orderPR.put(newOrderId, newOrder);
    mgr.commit();    
    commitedOrder = (Order)orderPR.get(newOrderId);
    Assert.assertTrue(newOrder.equals(commitedOrder), "Expected Order to be:"
        + order + " but was:" + commitedOrder);
  }

  @SuppressWarnings("serial")
  private void verifyDestroyOperation(RegionFunctionContext ctx, boolean remote) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRColocationDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache()
        .getCacheTransactionManager();
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    Customer newCus = (Customer)args.get(2);
    OrderId orderId = (OrderId)args.get(3);
    Order order = (Order)args.get(4);
    Customer oldCustomer = (Customer)custPR.get(custId);
    Customer commitedCust = null;
    // test destroy rollback
    mgr.begin();
    custPR.put(custId, newCus);
    custPR.destroy(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:"
            + commitedCust);
    // test destroy rollback on unmodified entry
    mgr.begin();
    custPR.destroy(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:"
            + commitedCust);
    // test remote destroy
    if (remote) {
      final boolean hasData = custPR.containsKey(new CustId(1));
      mgr.begin();
      Customer cust = new Customer("foo", "bar");
      Assert.assertTrue(oldCustomer.equals(custPR.get(custId)));
      custPR.put(custId, cust);
      Assert.assertTrue(cust.equals(custPR.get(custId)));
      custPR.destroy(custId);
      Assert.assertTrue(custPR.get(custId) == null);
      custPR.putIfAbsent(custId, cust);
      Assert.assertTrue(cust.equals(custPR.get(custId)));
      custPR.remove(custId, cust);
      Assert.assertTrue(custPR.get(custId) == null);

      if (hasData) {
        final Customer expectedCust = new Customer("name1", "Address1");
        custPR.putIfAbsent(new CustId(1), cust);
        Assert.assertTrue(expectedCust.equals(custPR.get(new CustId(1))),
            "expected [" + expectedCust + "] but got: "
            + custPR.get(new CustId(1)));
      }
      custPR.put(new CustId(1), cust);
      Assert.assertTrue(cust.equals(custPR.get(new CustId(1))));
      custPR.remove(new CustId(1), cust);
      Assert.assertTrue(custPR.get(new CustId(1)) == null);
      custPR.putIfAbsent(new CustId(1), cust);
      Assert.assertTrue(cust.equals(custPR.get(new CustId(1))));
      Assert.assertTrue(custPR.get(new CustId(13)) == null);
      try {
        custPR.destroy(new CustId(13));
        throw new TestException("expected EntryNotFoundException");
      } catch (EntryNotFoundException enfe) {
        // expected
      }
      Assert.assertTrue(custPR.get(new CustId(13)) == null);
      Assert.assertTrue(cust.equals(custPR.get(new CustId(1))));
      custPR.destroy(new CustId(1));
      Assert.assertTrue(custPR.get(new CustId(1)) == null);

      if (hasData) {
        Assert.assertTrue(new Customer("name7", "Address7").equals(custPR
            .get(new CustId(7))));
        custPR.destroy(new CustId(7));
      }
      Assert.assertTrue(custPR.get(new CustId(7)) == null);
      try {
        custPR.destroy(new CustId(7));
        throw new TestException("expected EntryNotFoundException");
      } catch (EntryNotFoundException enfe) {
        // expected
      }
      mgr.commit();
      custPR.put(custId, cust);
      if (hasData) {
        custPR.putIfAbsent(new CustId(1), new Customer("name1", "Address1"));
        custPR.put(new CustId(7), new Customer("name7", "Address7"));
      }
    }
    // test destroy on unmodified entry
    mgr.begin();
    custPR.destroy(custId);
    orderPR.put(orderId, order);
    mgr.commit();
    commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(commitedCust == null,
        "Expected Customer to be null but was:" + commitedCust);
    Order commitedOrder = (Order)orderPR.get(orderId);
    Assert.assertTrue(order.equals(commitedOrder), "Expected Order to be:"
        + order + " but was:" + commitedOrder);
    //put the customer again for invalidate verification
    mgr.begin();
    custPR.putIfAbsent(custId, newCus);
    mgr.commit();
    //test destroy on new entry
    //TODO: This throws EntryNotFound
    OrderId newOrderId = new OrderId(5000,custId);
    mgr.begin();
    Order newOrder = new Order("New Order to be destroyed");
    orderPR.put(newOrderId, newOrder);
    orderPR.destroy(newOrderId);
    mgr.commit();
    Assert.assertTrue(orderPR.get(newOrderId)==null,"Did not expect orderId to be present");

    // test ConcurrentMap operations
    mgr.begin();
    Order order1 = new Order("New Order to be replaced");
    Order order2 = new Order("New Order to be destroyed");
    orderPR.putIfAbsent(newOrderId, order1);
    Assert.assertTrue(order1.equals(orderPR.replace(newOrderId, order2)));
    mgr.commit(); // value is order2
    Assert.assertTrue(order2.equals(orderPR.get(newOrderId)));
    mgr.begin();
    Assert.assertTrue(orderPR.replace(newOrderId, order2, order1));
    mgr.commit(); // value is order1
    Assert.assertTrue(orderPR.get(newOrderId).equals(order1));
    mgr.begin();
    // this should return false since the value is order1
    Assert.assertTrue(!orderPR.remove(newOrderId, new java.io.Serializable() {
    }));
    mgr.commit();
    Assert.assertTrue(orderPR.get(newOrderId).equals(order1));
    mgr.begin();
    Assert.assertTrue(orderPR.remove(newOrderId, order1));
    mgr.commit(); // gone now
    Assert.assertTrue(orderPR.get(newOrderId) == null);
  }

  private void verifyInvalidateOperation(RegionFunctionContext ctx,
      boolean remote) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache()
        .getCacheTransactionManager();
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    Customer newCus = (Customer)args.get(2);
    OrderId orderId = (OrderId)args.get(3);
    Order order = (Order)args.get(4);
    Customer oldCustomer = (Customer)custPR.get(custId);
    Customer commitedCust = null;
    // test destroy rollback
    mgr.begin();
    custPR.put(custId, newCus);
    custPR.invalidate(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:"
            + commitedCust);
    // test destroy rollback on unmodified entry
    mgr.begin();
    custPR.invalidate(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:"
            + commitedCust);
    // test remote invalidate
    if (remote) {
      final boolean hasData = custPR.containsKey(new CustId(1));
      mgr.begin();
      Customer cust = new Customer("foo", "bar");
      Assert.assertTrue(oldCustomer.equals(custPR.get(custId)));
      custPR.put(custId, cust);
      Assert.assertTrue(cust.equals(custPR.get(custId)));
      custPR.invalidate(custId);
      Assert.assertTrue(custPR.get(custId) == null);
      custPR.put(custId, cust);
      Assert.assertTrue(cust.equals(custPR.get(custId)));
      custPR.invalidate(custId);
      Assert.assertTrue(custPR.get(custId) == null);

      custPR.put(new CustId(1), cust);
      Assert.assertTrue(cust.equals(custPR.get(new CustId(1))));
      custPR.invalidate(new CustId(1));
      Assert.assertTrue(custPR.get(new CustId(1)) == null);
      custPR.put(new CustId(1), cust);
      Assert.assertTrue(cust.equals(custPR.get(new CustId(1))));
      Assert.assertTrue(custPR.get(new CustId(13)) == null);
      try {
        custPR.invalidate(new CustId(13));
        throw new TestException("expected EntryNotFoundException");
      } catch (EntryNotFoundException enfe) {
        // expected
      }
      Assert.assertTrue(custPR.get(new CustId(13)) == null);
      Assert.assertTrue(cust.equals(custPR.get(new CustId(1))));
      custPR.invalidate(new CustId(1));
      Assert.assertTrue(custPR.get(new CustId(1)) == null);

      if (hasData) {
        Assert.assertTrue(new Customer("name7", "Address7").equals(custPR
            .get(new CustId(7))));
        custPR.invalidate(new CustId(7));
      }
      Assert.assertTrue(custPR.get(new CustId(7)) == null);
      mgr.commit();
      custPR.put(custId, cust);
      if (hasData) {
        custPR.put(new CustId(1), new Customer("name1", "Address1"));
        custPR.put(new CustId(7), new Customer("name7", "Address7"));
      }
    }
    // test invalidate on unmodified entry
    mgr.begin();
    custPR.invalidate(custId);
    orderPR.put(orderId, order);
    mgr.commit();
    commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(commitedCust == null,
        "Expected Customer to be null but was:" + commitedCust);
    Order commitedOrder = (Order)orderPR.get(orderId);
    Assert.assertTrue(order.equals(commitedOrder), "Expected Order to be:"
        + order + " but was:" + commitedOrder);
    //test destroy on new entry
    //TODO: This throws EntryNotFound
    /*OrderId newOrderId = new OrderId(5000,custId);
    mgr.begin();
    orderPR.put(newOrderId, new Order("New Order to be destroyed"));
    orderPR.invalidate(newOrderId);
    mgr.commit();
    Assert.assertTrue(orderPR.get(newOrderId)==null,"Did not expect orderId to be present");*/
  }

  private void verifyTransactionRollback(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache()
        .getCacheTransactionManager();
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    Customer newCus = (Customer)args.get(2);
    OrderId orderId = (OrderId)args.get(3);
    Order order = (Order)args.get(4);
    Customer oldCustomer = (Customer)custPR.get(custId);
    Order oldOrder = (Order)orderPR.get(orderId);
    mgr.begin();
    custPR.put(custId, newCus);
    Customer txCust = (Customer)custPR.get(custId);
    orderPR.put(orderId, order);
    Order txOrder = (Order)orderPR.get(orderId);
    Assert.assertTrue(newCus.equals(txCust), "Expected Customer to be:"
        + newCus + " but was:" + txCust);
    Assert.assertTrue(txOrder.equals(order), "Expected Order to be:" + order
        + " but was:" + txOrder);
    mgr.rollback();
    Customer commitedCust = (Customer)custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected Customer to be:" + oldCustomer + " but was:" + commitedCust);
    Order commitedOrder = (Order)orderPR.get(orderId);
    Assert.assertTrue(oldOrder.equals(commitedOrder), "Expected Order to be:"
        + oldOrder + " but was:" + commitedOrder);
    
    mgr.begin();
    Assert.assertTrue(custPR.remove(custId, oldCustomer));
    orderPR.replace(orderId, order);
    mgr.rollback();
    
    Assert.assertTrue(oldCustomer.equals(custPR.get(custId)));
    Assert.assertTrue(oldOrder.equals(orderPR.get(orderId)));
    
    mgr.begin();
    Assert.assertTrue(custPR.replace(custId, oldCustomer, newCus));
    orderPR.remove(orderId, oldOrder);
    Assert.assertTrue(null == orderPR.putIfAbsent(orderId, order));
    mgr.rollback();
    Assert.assertTrue(oldCustomer.equals(custPR.get(custId)));
    Assert.assertTrue(oldOrder.equals(orderPR.get(orderId)));
    
  }

  private void verifyNonCoLocatedSuccess(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache()
        .getCacheTransactionManager();
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    Customer newCus = (Customer)args.get(2);
    OrderId orderId = (OrderId)args.get(3);
    Order order = (Order)args.get(4);
    mgr.begin();
    //try {
      custPR.put(custId, newCus);
      custPR.put(new CustId(4), "foo4");
      custPR.put(new CustId(5), "foo5");
      custPR.put(new CustId(6), "foo6");
      orderPR.put(orderId, order);
    /*
      Assert.assertTrue(false);
    } finally {
      mgr.rollback();
    }
    */
    mgr.commit();
  }

  private void verifyListenerCallback(RegionFunctionContext ctx) {
    verifyTransactionExecution(ctx);
    TransactionListener2 listener = (TransactionListener2) ctx.getDataSet().getAttributes().getCacheListeners()[0];
    Assert.assertTrue(listener.getNumberOfPutCallbacks() == 2,"Expected 2 put callback, but " +
    		"got "+listener.getNumberOfPutCallbacks());
    verifyDestroyOperation(ctx, false);
    Assert.assertTrue(listener.getNumberOfDestroyCallbacks() == 1,"Expected 1 destroy callbacks, but " +
    		"got "+listener.getNumberOfDestroyCallbacks());
    verifyInvalidateOperation(ctx, false);
    Assert.assertTrue(listener.getNumberOfInvalidateCallbacks() == 1,"Expected 1 invalidate callbacks, but " +
                "got "+listener.getNumberOfInvalidateCallbacks());
  }

  private void verifyExecutionOnPrimary(RegionFunctionContext ctx) {
    PartitionedRegion pr = (PartitionedRegion)ctx.getDataSet();
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, custId, null, null);
    DistributedMember primary = pr.getRegionAdvisor()
        .getPrimaryMemberForBucket(bucketId);
    DistributedMember me = pr.getCache().getDistributedSystem()
        .getDistributedMember();
    Assert.assertTrue(me.equals(primary),
        "Function should have been executed on primary:" + primary
            + " but was executed on member:" + me);
  }
  
  private void verifyTxStateAndConflicts(RegionFunctionContext ctx){
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    OrderId vOrderId = new OrderId(3000,custId);
    Order vOrder = new Order("vOrder");
    TXManagerImpl mImp = (TXManagerImpl)mgr;
    mImp.begin();
    orderPR.put(vOrderId, vOrder);
    TXStateInterface txState = mImp.internalSuspend();
    final Collection<LocalRegion> regions = txState.getLocalTXState()
        .getRegions();
    Iterator<LocalRegion> it = regions.iterator();
    Assert.assertTrue(regions.size() == 1, "Expected 1 region; " + "found:"
        + regions);
    LocalRegion lr = it.next();
    Assert.assertTrue(lr instanceof BucketRegion);
    TXRegionState txRegion = txState.readRegion(lr);
    TXEntryState txEntry = (TXEntryState)txRegion.readEntry(txRegion
        .getEntryKeys().iterator().next());
    mImp.resume(txState);
    orderPR.put(vOrderId, new Order("foo"));
    txState = mImp.internalSuspend();
    //since both puts were on same key, verify that
    //TxRegionState and TXEntryState are same 
    LocalRegion lr1 = txState.getLocalTXState().getRegions().iterator().next();
    Assert.assertTrue(lr == lr1);
    TXRegionState txRegion1 = txState.readRegion(lr);
    TXEntryState txEntry1 = (TXEntryState)txRegion1.readEntry(txRegion
        .getEntryKeys().iterator().next());
    Assert.assertTrue(txEntry == txEntry1);
    //to check for conflicts, start a new transaction, operate on same key,
    //commit the second and expect the first to fail
    mImp.begin();
    try {
      orderPR.put(vOrderId, new Order("foobar"));
      throw new TestException("An expected exception was not thrown");
    } catch (ConflictException ce) {
      // expected
      mImp.rollback();
    }
    //now begin the first and expect successful commit
    mImp.resume(txState);
    mImp.commit();
  }

  private void verifyRepeatableRead(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    custPR.getCache().
          getRegion(PRColocationDUnitTest.OrderPartitionedRegionName);
    ArrayList args = (ArrayList)ctx.getArguments();
    CustId custId = (CustId)args.get(1);
    Customer cust = (Customer)args.get(2);
    Assert.assertTrue(custPR.get(custId) == null);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    TXManagerImpl mImp = (TXManagerImpl)mgr;
    mImp.begin();
    custPR.put(custId, cust);
    Assert.assertTrue(cust.equals(custPR.get(custId)));
    TXStateInterface txState = mImp.internalSuspend();
    Assert.assertTrue(custPR.get(custId) == null);
    mImp.resume(txState);
    mImp.commit();
    //change value
    mImp.begin();
    Customer oldCust = (Customer)custPR.get(custId);
    Assert.assertTrue(oldCust.equals(cust));
    txState = mImp.internalSuspend();
    Customer newCust = new Customer("fooNew","barNew");
    custPR.put(custId, newCust);
    mImp.resume(txState);
    Assert.assertTrue(oldCust.equals(custPR.get(custId)));
    mImp.commit();
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean isHA() {
    return false;
  }
}
