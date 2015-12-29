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
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.List;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;

public class ParallelGatewaySenderCreation extends AbstractGatewaySender implements GatewaySender{

  public ParallelGatewaySenderCreation(Cache cache, GatewaySenderAttributes attrs) {
    super(cache, attrs);
  }

  @Override
  public void distribute(EnumListenerEvent operation, EntryEventImpl event, List<Integer> remoteDSIds) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void destroy() {
  }

  public void fillInProfile(Profile profile) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DM getDistributionManager() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getFullPath() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Profile getProfile() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getSerialNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender#setModifiedEventId(com.gemstone.gemfire.internal.cache.EntryEventImpl)
   */
  @Override
  protected void setModifiedEventId(EntryEventImpl clonedEvent) {
    // TODO Auto-generated method stub
    
  }

}
