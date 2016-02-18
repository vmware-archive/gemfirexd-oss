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
package com.gemstone.gemfire.cache;

import java.io.*;
import junit.framework.TestCase;

/** Tests classes of Bug36619 to make sure they are Serializable */
public class Bug36619JUnitTest extends TestCase {
  
  public Bug36619JUnitTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Assert that MembershipAttributes are serializable.
   */
  public void testMembershipAttributesAreSerializable() throws Exception {
    String[] roles = {"a", "b", "c"};
    MembershipAttributes outMA = new MembershipAttributes(roles);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(outMA);
    
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    MembershipAttributes inMA = (MembershipAttributes) ois.readObject();
    assertEquals(outMA, inMA);
  }
  /**
   * Assert that SubscriptionAttributes are serializable.
   */
  public void testSubscriptionAttributesAreSerializable() throws Exception {
    SubscriptionAttributes outSA = new SubscriptionAttributes();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(outSA);
    
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    SubscriptionAttributes inSA = (SubscriptionAttributes) ois.readObject();
    assertEquals(outSA, inSA);
  }
}

