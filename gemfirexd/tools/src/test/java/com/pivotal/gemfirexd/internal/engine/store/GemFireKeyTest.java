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
package com.pivotal.gemfirexd.internal.engine.store;

import junit.framework.*;
import junit.textui.*;

import com.gemstone.gemfire.internal.util.BlobHelper;
import com.pivotal.gemfirexd.internal.engine.store.CompositeRegionKey;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.types.DataType;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;

/**
 *
 *
 * @author Eric Zoerner
 */

public class GemFireKeyTest extends TestCase {
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(GemFireKeyTest.class));
  }
  
  public GemFireKeyTest(String name) {
    super(name);
  }

  public void testSerializeHash() throws Exception {
    // Register DVD deserialization helper
    DataType.init();

    DataValueDescriptor[] dvds = new DataValueDescriptor[] { new SQLVarchar(
        "Instrument45312") };

    RegionKey gfkey = new CompositeRegionKey(dvds);
    int beforeHash = gfkey.hashCode();

    // serialize and deserialize
    byte[] bytes = BlobHelper.serializeToBlob(gfkey);

    RegionKey newKey = (RegionKey)BlobHelper.deserializeBlob(bytes);
    assertEquals(gfkey, newKey);
    assertEquals(beforeHash, newKey.hashCode());

    System.out.println("hash=" + beforeHash);
  }
}
