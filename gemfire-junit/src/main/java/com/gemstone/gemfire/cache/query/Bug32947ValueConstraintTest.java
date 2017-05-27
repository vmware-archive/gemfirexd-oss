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

package com.gemstone.gemfire.cache.query;

import junit.framework.TestCase;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.cache.query.data.Address;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Data;
import com.gemstone.gemfire.cache.query.data.Employee;
import com.gemstone.gemfire.cache.query.data.Manager;
import java.util.HashSet;
import java.util.Set;

/**
 * @author kdeshpan
 * Junit test for checking the value constraint region attributes.
 */

public class Bug32947ValueConstraintTest extends TestCase {
  
  DistributedSystem distributedSystem = null;
  Cache cache = null;
  
  protected void setUp() throws java.lang.Exception {
    Properties properties = new Properties();
    distributedSystem = DistributedSystem.connect(properties);
    cache = CacheFactory.create(distributedSystem);
  }

  public void tearDown() {
    cache.close();
    distributedSystem.disconnect(); 
  }

  public void testBug32947ValueConstraints() throws Exception {
    
    boolean flag = false; 
    AttributesFactory factory = new AttributesFactory();
    factory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = factory.create();
    Region portolioRegion = cache.createRegion("portfolios", regionAttributes);

    portolioRegion.put("key1", new Portfolio(1));
    try {
      portolioRegion.put("key2", new Data());
    } catch (ClassCastException e) {
        flag = true;
    }
    
    if (!flag) {
      fail("Expected ClassCastException after put as valueConstraint is set to Portfolio.class");
      return;
    }
    
    Set address1 = new HashSet();
    Set address2 = new HashSet();
    address1.add(new Address("Hp3 9yf", "Apsley"));
    address1.add(new Address("Hp4 9yf", "Apsleyss"));
    address2.add(new Address("Hp3 8DZ", "Hemel"));
    address2.add(new Address("Hp4 8DZ", "Hemel"));
    
    // Note that Manager extends Employee
    
    Manager manager = new Manager("aaa", 27, 270, "QA", 1800, address1, 2701);
    Employee employee = new Employee("bbb", 28, 280, "QA", 1900, address2);
    
    factory.setValueConstraint(Manager.class);
    regionAttributes = factory.create();
    Region managerRegion = cache.createRegion("managers", regionAttributes);
    
    factory.setValueConstraint(Employee.class);
    regionAttributes = factory.create();
    Region employeeRegion = cache.createRegion("employees", regionAttributes);
    
    employeeRegion.put("key1", manager); //This is perfectly valid, as Manager is Derived from Employee
    
    flag = false;
    
    try {
      managerRegion.put("key1", employee);
    } catch (ClassCastException e) {
        flag = true;
    }
    
    if (!flag) {
      fail("Expected ClassCastException after put as valueConstraint is set to Manager.class");
      return;
    }

    // Test passed successfully.
  
  }
}
