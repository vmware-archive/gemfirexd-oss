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
package com.gemstone.gemfire.internal.cache.tier.sockets;


/**
 * Class <code>DurableClientCrashDUnitTest</code> tests durable client
 * functionality when clients are disconnected from servers.
 * 
 * @author Abhijit Bhaware
 * 
 * @since 5.2
 */
public class DurableClientNetDownDUnitTest extends DurableClientCrashDUnitTest {

  public DurableClientNetDownDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    //ensure that the test flag is no longer set in this vm
    this.durableClientVM.invoke(CacheServerTestUtil.class, "reconnectClient");
    super.tearDown2();
  }

  public void setPrimaryRecoveryCheck() {}
  
  public void checkPrimaryRecovery() {}
  
  public void configureClientStop1() {}
  
  public void configureClientStop2() {}
  
  public void closeDurableClient()
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "reconnectClient");
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  public void disconnectDurableClient()
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disconnectClient");
  }

  public void disconnectDurableClient(boolean keepAlive)
  {
    this.disconnectDurableClient();
  }
  
  public void restartDurableClient(Object[] args)
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "reconnectClient");  
  }
  
  public void verifyListenerUpdatesDisconnected(int numberOfEntries)
  {
    this.verifyListenerUpdates(numberOfEntries);
  }

  public void verifyListenerUpdates(int numEntries, int numEntriesBeforeDisconnect)
  {
    this.verifyListenerUpdatesEntries(numEntries, numEntriesBeforeDisconnect);
  }
  
  public void testDurableNonHAFailover() throws InterruptedException
  {
    super.testDurableNonHAFailover();
  }

  public void testDurableHAFailover() throws InterruptedException
  {
    super.testDurableHAFailover();
  }
  
  
}
