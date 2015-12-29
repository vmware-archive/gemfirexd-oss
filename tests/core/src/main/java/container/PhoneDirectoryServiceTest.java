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
package container;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import container.app.phonedirectory.domain.Address;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.phonedirectory.domain.PhoneNumber;

/**
 * The PhoneDirectoryServiceTest class is a container test suite testing GemFire inside OSGi-based containers, such as
 * Apache Felix or Eclipse Virgo, in order to ensure GemFire can be successfully deployed as an OSGi Bundle with a
 * proper MANIFEST inside an OSGi container and continue to function properly.  This container-based test uses a simple
 * RMI-based Java application using GemFire to test inside both Apache Felix and Eclipse Virgo.
 * <p/>
 * @author John Blum
 * @see container.AbstractPhoneDirectoryServiceTestCase
 * @since 6.6.x
 */
public class PhoneDirectoryServiceTest extends AbstractPhoneDirectoryServiceTestCase {

  public PhoneDirectoryServiceTest(final String testName) {
    super(testName);
  }

  public static Test suite() {
    final TestSuite suite = new TestSuite();
    suite.addTestSuite(PhoneDirectoryServiceTest.class);
    //suite.addTest(new PhoneDirectoryServiceTest("testFromCacheToApp"));
    //suite.addTest(new PhoneDirectoryServiceTest("testFromAppToCache"));
    return new PhoneDirectoryServiceCacheTestSetup(suite);
  }

  public void testFromCacheToApp() throws Exception {
    final Person expectedPerson = createPerson("Jon", "Doe");
    final Address expectedAddress = createAddress("100 Main St", "Portland", "OR", "97205");
    final PhoneNumber expectedPhoneNumber = createPhoneNumber("503", "555", "1234");

    final PhoneDirectoryEntry expectedEntry = createPhoneDirectoryEntry(
      expectedPerson, expectedAddress, expectedPhoneNumber);

    getRegion().put("jon doe", expectedEntry);

    final PhoneDirectoryEntry actualEntry = getPhoneDirectoryService().getEntry(expectedPerson);

    Assert.assertNotNull(actualEntry);
    assertEquals(expectedEntry, actualEntry);
  }

  public void testFromAppToCache() throws Exception {
    final Person expectedPerson = createPerson("Jane", "Doe");
    final Address expectedAddress = createAddress("1221 Yamhill", "Portland", "OR", "97205");
    final PhoneNumber expectedPhoneNumber = createPhoneNumber("503", "555", "9876");

    final PhoneDirectoryEntry expectedEntry = createPhoneDirectoryEntry(
        expectedPerson, expectedAddress, expectedPhoneNumber);

    getPhoneDirectoryService().add(expectedPerson, expectedAddress, expectedPhoneNumber);

    final PhoneDirectoryEntry actualEntry = getRegion().get("jane doe");

    Assert.assertNotNull(actualEntry);
    assertEquals(expectedEntry, actualEntry);
  }

}
