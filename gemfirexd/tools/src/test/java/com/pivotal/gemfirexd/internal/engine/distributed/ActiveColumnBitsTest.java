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
package com.pivotal.gemfirexd.internal.engine.distributed;

import com.pivotal.gemfirexd.internal.engine.distributed.ActiveColumnBits;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * Tests the behaviour of the bits set 
 * @author ashahid
 *
 */
public class ActiveColumnBitsTest extends TestCase{
  
  
  public ActiveColumnBitsTest(String name) {
    super(name);
    // assert that assertions are enabled
    boolean assertionsEnabled = false;
    assert (assertionsEnabled = true) == true;
    assertTrue("assertions are not enabled", assertionsEnabled);
  }

  public static void main(String[] args)
  {
    TestRunner.run(new TestSuite(ActiveColumnBitsTest.class));
  }
  
  public void testActiveColumnBehaviour() {
    
    byte flag =0x00;
    //Validate that all the eight bits representing the 8 columns  are not have any data 
    
    for(int i=0 ; i <8 ; ++i) {
      assertFalse(ActiveColumnBits.isNormalizedColumnOn(i, flag));
    }
    
    
    //Activated the 2nd Column
    assertEquals(flag=ActiveColumnBits.setFlagForNormalizedColumnPosition(1, true, flag),2);
    //  Activated the 2nd &  3rd Column
    assertEquals(flag = ActiveColumnBits.setFlagForNormalizedColumnPosition(2, true, flag),6);
    
    //  Activated the 2nd , 3rd &  6th Column
    assertEquals(flag = ActiveColumnBits.setFlagForNormalizedColumnPosition(5, true, flag),38);
    
    //activated the 2nd, 3rd, 6th & 8th col
    
    flag = ActiveColumnBits.setFlagForNormalizedColumnPosition(7, true, flag);
    
    //1st coulumn is off
    assertFalse(ActiveColumnBits.isNormalizedColumnOn(0, flag));
    
    //  2 coulumn is on
    assertTrue(ActiveColumnBits.isNormalizedColumnOn(1, flag));
    
    //  3 coulumn is on
    assertTrue(ActiveColumnBits.isNormalizedColumnOn(2, flag));

    //  4 coulumn is off
    assertFalse(ActiveColumnBits.isNormalizedColumnOn(3, flag));
    
    //  5 coulumn is off
    assertFalse(ActiveColumnBits.isNormalizedColumnOn(4, flag));
    
    //  6 coulumn is on
    assertTrue(ActiveColumnBits.isNormalizedColumnOn(5, flag));
    
    //  7 coulumn is off
    assertFalse(ActiveColumnBits.isNormalizedColumnOn(6, flag));
    
    //  8 coulumn is on
    assertTrue(ActiveColumnBits.isNormalizedColumnOn(7, flag));
    
    
    //Check if only the eight column is on, the rest should be correctly off
    
    flag = 0x00;
    flag = ActiveColumnBits.setFlagForNormalizedColumnPosition(7, true, flag);
    
    for(int i =0; i < 7; ++i) {
      assertFalse(ActiveColumnBits.isNormalizedColumnOn(i, flag));
    }
    
    assertTrue(ActiveColumnBits.isNormalizedColumnOn(7, flag));
    
    
    
  }

  
}
