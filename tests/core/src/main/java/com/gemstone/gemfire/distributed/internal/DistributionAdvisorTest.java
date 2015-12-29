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

package com.gemstone.gemfire.distributed.internal;

import dunit.*;
import java.util.*;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.membership.*;


/**
 *
 * @author Eric Zoerner
 *
 */
public class DistributionAdvisorTest extends DistributedTestCase {
  private transient DistributionAdvisor.Profile profiles[];
  protected transient DistributionAdvisor advisor;
  
  public DistributionAdvisorTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    // connect to distributed system in every VM
    invokeInEveryVM(new SerializableRunnable("DistributionAdvisorTest: SetUp") {
      public void run() {
        getSystem();
      }
    });
    
    // reinitialize the advisor
    this.advisor = DistributionAdvisor.createDistributionAdvisor(new DistributionAdvisee() {
        public DistributionAdvisee getParentAdvisee() { return null; }
        public InternalDistributedSystem getSystem() { return DistributionAdvisorTest.this.getSystem(); }
        public String getName() {return "DistributionAdvisorTest";}
        public String getFullPath() {return getName();}
        public DM getDistributionManager() {return getSystem().getDistributionManager();}
        public DistributionAdvisor getDistributionAdvisor() {return DistributionAdvisorTest.this.advisor;}
        public DistributionAdvisor.Profile getProfile() {return null;}
        public void fillInProfile(DistributionAdvisor.Profile profile) {}
        public int getSerialNumber() {return 0;}
        public CancelCriterion getCancelCriterion() {
          return DistributionAdvisorTest.this.getSystem().getCancelCriterion();
        }
      });
    Set ids = getSystem().getDistributionManager().getOtherNormalDistributionManagerIds();
    assertEquals(4, ids.size());
    List profileList = new ArrayList();
    
    int i = 0;
    for (Iterator itr = ids.iterator(); itr.hasNext(); i++) {
      InternalDistributedMember id = (InternalDistributedMember)itr.next();
      DistributionAdvisor.Profile profile = new DistributionAdvisor.Profile(id, 0);      

      // add profile to advisor
      advisor.putProfile(profile);
      profileList.add(profile);
    }
    this.profiles = (DistributionAdvisor.Profile[])profileList.toArray(
                    new DistributionAdvisor.Profile[profileList.size()]);
  }
    
  public void tearDown2() throws Exception {
    this.advisor.close();
    super.tearDown2();
  }
  
    
  public void testGenericAdvice() {
    Set expected = new HashSet();
    for (int i = 0; i < profiles.length; i++) {
      expected.add(profiles[i].getDistributedMember());
    }
    assertEquals(expected, advisor.adviseGeneric());
  }
}
