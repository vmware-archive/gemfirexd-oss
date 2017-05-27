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
package com.gemstone.gemfire.internal;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.shared.Version;

import junit.framework.TestCase;


/**
 * Test of methods on HeapDataOutputStream
 * 
 * TODO right now this just tests the new
 * write(ByteBuffer) method. We might want
 * to add some unit tests for the existing methods.
 *
 */
public class HeapDataOutputStreamJUnitTest extends TestCase {
  
  public void testWriteByteBuffer() {
    HeapDataOutputStream out = new HeapDataOutputStream(32, Version.CURRENT);
    
    byte[] bytes = "1234567890qwertyuiopasdfghjklzxcvbnm,./;'".getBytes();
    out.write(ByteBuffer.wrap(bytes, 0, 2));
    out.write(ByteBuffer.wrap(bytes, 2, bytes.length - 2));
    
    byte[] actual = out.toByteArray();
    
    assertEquals(new String(bytes) , new String(actual));
  }

}
