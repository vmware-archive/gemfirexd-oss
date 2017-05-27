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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

public class GFKeyJUnitTest extends TestCase {
  public void testSerde() throws Exception {
    String str = "str";
    GFKey key = new GFKey();
    key.setKey(str);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    key.write(dos);
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    key.readFields(dis);
    
    assertEquals(str, key.getKey());
  }
  
  public void testCompare() {
    GFKey keya = new GFKey();
    keya.setKey("a");
    
    GFKey keyb = new GFKey();
    keyb.setKey("b");
    
    assertEquals(-1, keya.compareTo(keyb));
    assertEquals(1, keyb.compareTo(keya));
  }
}
