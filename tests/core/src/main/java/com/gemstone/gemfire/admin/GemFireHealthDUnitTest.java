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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import dunit.*;

/**
 * Tests the functionality of the {@link GemFireHealth} administration
 * API.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class GemFireHealthDUnitTest extends AdminDUnitTestCase {

  /**
   * Creates a new <code>GemFireHealthDUnitTest</code>
   */
  public GemFireHealthDUnitTest(String name) {
    super(name);
  }

  ////////  Helper methods

  /**
   * Since the default sleep time for the health monitor is 30 seconds,
   * we need to wait a bit longer for the status to change
   */
  private static final int HEALTH_TIME = 45 * 1000;
  
  /**
   * Waits a given number of milliseconds for GemFire's health to
   * change to a {@link GemFireHealth#GOOD_HEALTH given state}. 
   */
  private void waitForHealthChange(final GemFireHealth health,
                                   final GemFireHealth.Health desiredHealth) {
//    long start = System.currentTimeMillis();
    GemFireHealth.Health current = health.getHealth();
    if (current == desiredHealth) {
      return;
    }
    else {
      getLogWriter().info("waiting for health to change from " + current + " to " + desiredHealth);
    }
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return health.getHealth() == desiredHealth;
      }
      public String description() {
        return "health never became " + desiredHealth;
      }
    };
    DistributedTestCase.waitForCriterion(ev, HEALTH_TIME, 200, true);
  }

  ////////  Test Methods

  /**
   * Tests getting/setting the {@link
   * DistributedSystemHealthConfig#setMaxDepartedApplications
   * maxDepartedApplications} attribute.
   */
  public void testSetGetMaxDepartedApplications() throws Exception {
    AdminDistributedSystem system = this.tcSystem;
    GemFireHealth health = system.getGemFireHealth();
    DistributedSystemHealthConfig config =
      health.getDistributedSystemHealthConfig();
    assertEquals(DistributedSystemHealthConfig.DEFAULT_MAX_DEPARTED_APPLICATIONS, config.getMaxDepartedApplications());
    long newValue = 42;
    config.setMaxDepartedApplications(newValue);
    assertEquals(newValue, config.getMaxDepartedApplications());
  }

  /**
   * Tests that a slow cache loader causes the health to change to
   * {@link GemFireHealth#POOR_HEALTH}.
   *
   * @see CacheHealthConfig#getMaxLoadTime
   */
  public void testMaxLoadTime() throws Exception {
    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;
    GemFireHealth health = system.getGemFireHealth();
    waitForHealthChange(health, GemFireHealth.GOOD_HEALTH);
//    assertEquals(GemFireHealth.GOOD_HEALTH, health.getHealth());

    GemFireHealthConfig config =
      health.getDefaultGemFireHealthConfig();
    config.setHealthEvaluationInterval(1);
    config.setMaxLoadTime(50);
    health.setDefaultGemFireHealthConfig(config);
    pause(1000);
    assertEquals(GemFireHealth.GOOD_HEALTH, health.getHealth());

    final String name = this.getUniqueName();
    
    // This should cause a member to enter the distributed system
    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory();
          factory.setCacheLoader(new CacheLoader() {
              public Object load(LoaderHelper helper)
                throws CacheLoaderException {
                return "Loaded value";
              }

              public void close() { }
            });

          createRegion(name, factory.create());
        }
      });

    pause(1500);
    assertEquals("Diagnosis of poor health: " + health.getDiagnosis(),
                 GemFireHealth.GOOD_HEALTH, health.getHealth());

    vm.invoke(new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 100; i++) {
            region.get("Test1 " + i);
          }
        }
      });
    pause(1000);
    assertEquals(GemFireHealth.GOOD_HEALTH, health.getHealth());

    vm.invoke(new CacheSerializableRunnable("Slow cache loader") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.getAttributesMutator().setCacheLoader(new
            CacheLoader() {
              public Object load(LoaderHelper helper)
                throws CacheLoaderException {
                try {
                  Thread.sleep(100);
                  
                } catch (InterruptedException ex) {
                  fail("Why was I interrupted?");
                }
                return "Slowly loaded value";
              }
              
              public void close() { }
            });
          for (int i = 0; i < 50; i++) {
            region.get("Test2 " + i);
          }
        }
      });
    
    waitForHealthChange(health, GemFireHealth.OKAY_HEALTH);

    String diagnosis = health.getDiagnosis();
    assertNotNull(diagnosis);

    String s = "The average duration of a Cache load";
    assertTrue(diagnosis.indexOf(s) != -1);

    // wait for poor loadTime stats to be old enough so they no longer
    // change the health to ok
    pause(1500);
    
    health.resetHealth();
    assertEquals("Diagnosis is: " + health.getDiagnosis(),
                 GemFireHealth.GOOD_HEALTH, health.getHealth());

    // make sure the health stays good for 2 seconds
    GemFireHealth.Health newHealth = health.getHealth();
    long totalTime = 2 * 1000;
    for (long begin = System.currentTimeMillis();
         System.currentTimeMillis() - begin < totalTime; ) {
      pause(100);
      newHealth = health.getHealth();
      if (!newHealth.equals(GemFireHealth.GOOD_HEALTH)) {
        break;
      }
    }
    assertEquals("Didn't become healthy after " + totalTime + " ms: "
                 + health.getDiagnosis(),
                 GemFireHealth.GOOD_HEALTH, health.getHealth());
    
  }
  
  /**
   * Test that shows when there is a region missing a required role
   * and congfigured with LossAction.NO_ACCESS then the health
   * of the gemfire cache is poor {@link GemFireHealth.H}.
   * */
  
  public void testHealthWithRoleLossNoAccess()throws Exception{
	  
	  final String rr1 = "RoleA";
	  final String[] requiredRoles = { rr1 };
	  final String regionName = "MyRegion";
	  final AdminDistributedSystem system = this.tcSystem;
	  VM vm = Host.getHost(0).getVM(0);
	  
	  SerializableRunnable roleLoss = new CacheSerializableRunnable(
	    "RoleMissingAndPoorHealth") {
		  public void run2(){
			  MembershipAttributes ra = new MembershipAttributes(requiredRoles,
				        LossAction.NO_ACCESS, ResumptionAction.NONE);
				    
			  AttributesFactory fac = new AttributesFactory();
			  fac.setMembershipAttributes(ra);
			  fac.setScope(Scope.DISTRIBUTED_ACK);
			  fac.setDataPolicy(DataPolicy.REPLICATE);
			  RegionAttributes attr = fac.create();
			  try{
				  createRootRegion(regionName, attr);
			  }catch(Exception ex){
				  ex.printStackTrace();
				  fail("Exception in VM0 : ", ex);
			  }
			  pause(1000);
			  }
	  	};
	vm.invoke(roleLoss); 
	GemFireHealth health = system.getGemFireHealth();
        waitForHealthChange(health, GemFireHealth.POOR_HEALTH);
	
	getLogWriter().info("THE MEMBER HEALTH 1 : "+health.getHealth());
        assertEquals(GemFireHealth.POOR_HEALTH, health.getHealth());
  }
  
  /**
   * Test that shows when there is a region missing a required role
   * and congfigured with LossAction.LIMITED_ACCESS then the health
   * of the system is poor.
   * */
  
  public void testHealthWithRoleLossLimitedAccess()throws Exception{
	  
	  final String rr1 = "RoleA";
	  final String[] requiredRoles = { rr1 };
	  final String regionName = "MyRegion";
	  final AdminDistributedSystem system = this.tcSystem;
	  VM vm = Host.getHost(0).getVM(0);
	  
	  SerializableRunnable roleLoss = new CacheSerializableRunnable(
	    "RoleMissingAndPoorHealth") {
		  public void run2(){
			  MembershipAttributes ra = new MembershipAttributes(requiredRoles,
				        LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
				    
			  AttributesFactory fac = new AttributesFactory();
			  fac.setMembershipAttributes(ra);
			  fac.setScope(Scope.DISTRIBUTED_ACK);
			  fac.setDataPolicy(DataPolicy.REPLICATE);
			  RegionAttributes attr = fac.create();
			  try{
				  createRootRegion(regionName, attr);
			  }catch(Exception ex){
				  ex.printStackTrace();
				  fail("Exception in VM0 : ", ex);
			  }
			  pause(1000);
			  }
	  	};
	vm.invoke(roleLoss); 
	GemFireHealth health = system.getGemFireHealth();
        waitForHealthChange(health, GemFireHealth.POOR_HEALTH);
	
        getLogWriter().info("THE MEMBER HEALTH POOR2 : "+health.getHealth());
	assertEquals(GemFireHealth.POOR_HEALTH, health.getHealth());
		  
  }
  
  /**
   * Test that shows when there is a region missing a required role
   * and congfigured with LossAction.FULL_ACCESS then the health
   * of the gemfire cache is Okay.
   * */
  
  public void testHealthWithRoleLossFullAccess()throws Exception{
	  
	  final String rr1 = "RoleA";
	  final String[] requiredRoles = { rr1 };
	  final String regionName = "MyRegion";
	  final AdminDistributedSystem system = this.tcSystem;
	  VM vm = Host.getHost(0).getVM(0);
	  
	  SerializableRunnable roleLoss = new CacheSerializableRunnable(
	    "RoleMissingAndPoorHealth") {
		  public void run2(){
			  MembershipAttributes ra = new MembershipAttributes(requiredRoles,
				        LossAction.FULL_ACCESS, ResumptionAction.NONE);
				    
			  AttributesFactory fac = new AttributesFactory();
			  fac.setMembershipAttributes(ra);
			  fac.setScope(Scope.DISTRIBUTED_ACK);
			  fac.setDataPolicy(DataPolicy.REPLICATE);
			  RegionAttributes attr = fac.create();
			  try{
				  createRootRegion(regionName, attr);
			  }catch(Exception ex){
				  ex.printStackTrace();
				  fail("Exception in VM0 : ", ex);
			  }
			  pause(1000);
			  		  
			  }
	  	};
	vm.invoke(roleLoss); 
	GemFireHealth health = system.getGemFireHealth();
        waitForHealthChange(health, GemFireHealth.OKAY_HEALTH);
	
        getLogWriter().info("THE MEMBER HEALTH OKAY : "+health.getHealth());
	assertEquals(GemFireHealth.OKAY_HEALTH, health.getHealth());
		  
  }

}
