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
package com.gemstone.gemfire.management.internal.cli.parser;

import junit.framework.TestCase;

import com.gemstone.gemfire.management.internal.cli.parser.ParserUtils;

/**
 * Includes tests for all utility methods in {@link ParserUtils}
 * 
 * @author njadhav
 * 
 */
public class ParserUtilsTest extends TestCase {

  /**
   * Test for {@link ParserUtils#split(String, String)}
   */
  public void testSplit() {
    String input = "something::{::}::nothing";
    String[] split = ParserUtils.split(input, "::");
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "something", split[0]);
    assertEquals("Second string", "{::}", split[1]);
    assertEquals("Third string", "nothing", split[2]);
  }

  /**
   * Test for {@link ParserUtils#splitValues(String, String)}
   */
  public void testSplitValues() {
    String input = "something::{::}::nothing::";
    String[] split = ParserUtils.splitValues(input, "::");
    assertEquals("Size of the split", 4, split.length);
    assertEquals("First string", "something", split[0]);
    assertEquals("Second string", "{::}", split[1]);
    assertEquals("Third string", "nothing", split[2]);
    assertEquals("Fourth string", "", split[3]);
  }

  /**
   * Test for {@link ParserUtils#contains(String, String)}
   */
  public void testContains() {
    String input = "something::{::}::nothing::";
    assertTrue("Check Boolean", ParserUtils.contains(input, "::"));
    input = "{something::{::}::nothing::}";
    assertFalse("Check Boolean", ParserUtils.contains(input, "::"));
  }

  /**
   * Test for {@link ParserUtils#lastIndexOf(String, String)}
   */
  public void testLastIndexOf() {
    String input = "something::{::}::nothing::";
    assertEquals("lastIndex", 24, ParserUtils.lastIndexOf(input, "::"));
    input = "something::{::}::\"nothing::\"";
    assertEquals("lastIndex", 15, ParserUtils.lastIndexOf(input, "::"));
    input = "{something::{::}::\"nothing::\"}";
    assertEquals("lastIndex", -1, ParserUtils.lastIndexOf(input, "::"));
  }

}
