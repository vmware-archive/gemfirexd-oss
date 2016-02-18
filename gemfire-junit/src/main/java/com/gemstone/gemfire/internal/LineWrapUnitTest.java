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

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
public class LineWrapUnitTest extends TestCase {
  
  public void test() {
    String test = new String("aaa aaaaa  aaa aaaa");
    
    assertEquals(list("aaa", "aaaaa", "aaa", "aaaa"), SystemAdmin.lineWrapOut(test, 3));
    assertEquals(list("aaa", "aaaaa", "aaa aaaa"), SystemAdmin.lineWrapOut(test, 8));

    assertEquals(list("aaa aaaaa", "aaa aaaa"), SystemAdmin.lineWrapOut(test, 9));
    assertEquals(list("aaa aaaaa  aaa",  "aaaa"), SystemAdmin.lineWrapOut(test, 14));
    
    String test2 = new String("aaa\n aaaaa  aaa aaaa");
    assertEquals(list("aaa", " aaaaa  aaa",  "aaaa"), SystemAdmin.lineWrapOut(test2, 14));
  }

  private List<String> list(String  ...strings) {
    return Arrays.asList(strings);
  }

}
