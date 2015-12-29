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
/**
 * 
 */
package transaction;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import static com.gemstone.gemfire.cache.RegionShortcut.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.execute.FunctionService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

/**
 * An example for using GemFire Transactions.
 * Creates a Customer and Order partitioned region, and then
 * updates the customer and one of his/her order in a transaction.
 * This example uses a cacheListener to print outputs on the VM
 * where the data resides.
 * To see proxied transactions and function execution in action,
 * use more than one VM
 * @since 6.5
 */
public class TransactionalPeer {

  /** The region for storing customer information*/
  private Region<CustomerId, String> custRegion;
  
  /** The region for storing order information of a customer*/
  private Region<OrderId, String> orderRegion;
  
  /** The cache used in the example */
  private Cache cache;
  
  private static final int MAX_KEYS_IN_REGION = 10;

  public TransactionalPeer(boolean isEmpty) {
    createCache();
    createRegions(isEmpty);
  }

  /**
   * Connects to the distributed system and creates the cache
   */
  protected void createCache() {
    Properties props = new Properties();
    cache = new CacheFactory(props).create();
  }
  
  /**
   * Creates the customer and order regions
   * @param isEmpty true if these regions should not store data locally, false otherwise
   */
  protected void createRegions(boolean isEmpty) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory<CustomerId, String>();
    paf.setPartitionResolver(new CustomerOrderResolver());
    if (isEmpty) {
      paf.setLocalMaxMemory(0);
    }
    
    RegionFactory rf = cache.createRegionFactory(PARTITION);
    rf.setPartitionAttributes(paf.create());
    rf.addCacheListener(new LoggingCacheListener());
    custRegion = rf.create("customer");
    
    paf = new PartitionAttributesFactory<OrderId, String>();
    paf.setColocatedWith("customer");
    paf.setPartitionResolver(new CustomerOrderResolver());
    if (isEmpty) {
      paf.setLocalMaxMemory(0);
    }
    
    rf = cache.createRegionFactory(PARTITION);
    rf.addCacheListener(new LoggingCacheListener());
    rf.setPartitionAttributes(paf.create());
    orderRegion = rf.create("order");
  }
  
  /**
   * Runs the example by asking for user input for the type of transaction
   * to be run
   * @throws IOException if there is Exception while reading user input 
   * 
   */
  private void runExample() throws IOException {
    System.out.println("Please start the other VM and press enter to populate regions");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    reader.readLine();
    if (custRegion.size() == 0) {
      System.out.println("Populating region...");
      populateRegion();
      System.out.println("Complete");
    } else {
      System.out.println("Regions already populated");
    }
    Random random = new Random();
    while (true) {
      System.out.println("Press 1 to run a transaction, 2 to run a transactional function");
      String input = reader.readLine();
      CustomerId custToUpdate = new CustomerId(random.nextInt(MAX_KEYS_IN_REGION+1));
      OrderId orderToUpdate = new OrderId(random.nextInt(100), custToUpdate);
      if ("1".equals(input.trim())) {
        CacheTransactionManager mgr = CacheFactory.getAnyInstance().getCacheTransactionManager();
        System.out.println("Starting a transaction...");
        mgr.begin();
        int randomInt = random.nextInt(1000);
        System.out.println("for customer region updating "+custToUpdate);
        custRegion.put(custToUpdate, "updatedCustomer_"+randomInt);
        System.out.println("for order region updating "+orderToUpdate);
        orderRegion.put(orderToUpdate, "newOrder_"+randomInt);
        mgr.commit();
        System.out.println("transaction completed");
      } else if ("2".equals(input.trim())) {
        System.out.println("Executing Function");
        Set filter = new HashSet();
        filter.add(custToUpdate);
        System.out.println("Invoking Function");
        //please refer to the function service documentation for more information
        FunctionService.onRegion(custRegion).withFilter(filter).withArgs(
            orderToUpdate).execute(new TransactionalFunction()).getResult();
        System.out.println("Function invocation completed");
      } else {
        continue;
      }
    }
  }
  
  /**
   * gets us started by putting some data in the customer
   * and order regions
   */
  private void populateRegion() {
    for (int i=0; i<MAX_KEYS_IN_REGION/2; i++) {
      CustomerId custId = new CustomerId(i);
      OrderId orderId = new OrderId(i, custId);
      custRegion.put(custId, "customer_"+i);
      orderRegion.put(orderId, "order_"+i);
    }
  }
  public static void main(String[] args) throws IOException {
    boolean isEmpty = false;
    if (args.length > 1) {
      showUsage();
      System.exit(1);
    } else if (args.length == 1) {
      if ("empty".equalsIgnoreCase(args[0])) {
        isEmpty = true;
      } else {
        showUsage();
        System.exit(1);
      }
    }
    TransactionalPeer peer = new TransactionalPeer(isEmpty);
    peer.runExample();
  }

  /**
   * prints the usage
   */
  private static void showUsage() {
    System.out.println("java TransactionalPeer [empty]");
  }
}
