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
package com.gemstone.gemfire.management.internal.cli.parser.preprocessor;

import junit.framework.TestCase;

import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.TrimmedInput;

/**
 * Includes tests for all utility methods in {@link PreprocessorUtils}
 * 
 * @author njadhav
 * 
 */
public class PreprocessorUtilsTest extends TestCase {

  /**
   * Test for {@link PreprocessorUtils#simpleTrim(String)}
   */
  public void testSimpleTrim() {
    String input = " 1 2 3 ";
    TrimmedInput simpleTrim = PreprocessorUtils.simpleTrim(input);
    assertEquals("No of spaces removed", 1, simpleTrim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "1 2 3", simpleTrim.getString());

    input = " 1 2 3      ";
    simpleTrim = PreprocessorUtils.simpleTrim(input);
    assertEquals("No of spaces removed", 1, simpleTrim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "1 2 3", simpleTrim.getString());
  }

  /**
   * Test for {@link PreprocessorUtils#trim(String)}
   */
  public void testTrim() {
    String input = " command argument1 argument2 ";
    TrimmedInput trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 1, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "   command   argument1   argument2 ";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 7, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "command argument1 argument2 -- -- - - - -- -- -- -- -- --- --------- - - - --- --";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 0, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "command argument1 argument2 --";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 0, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "command argument1 argument2 -";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 0, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());
  }

  /**
   * Test for {@link PreprocessorUtils#removeWhiteSpaces(String)}
   */
  public void testRemoveWhiteSpaces() {
    String input = "1 2 3   ";
    String output = PreprocessorUtils.removeWhiteSpaces(input);
    assertEquals("Output after removing white spaces", "123", output);
  }

  /**
   * Test for {@link PreprocessorUtils#isSyntaxValid(String)}
   */
  public void testIsSyntaxValid() {
    assertTrue(PreprocessorUtils.isSyntaxValid("{}"));
    assertFalse(PreprocessorUtils.isSyntaxValid("{{]}"));
    assertTrue(PreprocessorUtils.isSyntaxValid("\"\""));
    assertTrue(PreprocessorUtils.isSyntaxValid("\"{\'[]\'}\""));
    assertFalse(PreprocessorUtils.isSyntaxValid("{\"}\""));
  }

  /**
   * Test for {@link PreprocessorUtils#containsOnlyWhiteSpaces(String)}
   */
  public void testContainsOnlyWhiteSpaces() {
    assertTrue(PreprocessorUtils
        .containsOnlyWhiteSpaces("                                                  "));
    assertFalse(PreprocessorUtils
        .containsOnlyWhiteSpaces("              d       "));
  }

  /**
   * Test for {@link PreprocessorUtils#isWhitespace(char)}
   */
  public void testIsWhitespace() {
    assertTrue(PreprocessorUtils.isWhitespace(' '));
    assertTrue(PreprocessorUtils.isWhitespace('\t'));
    assertTrue(PreprocessorUtils.isWhitespace('\n'));
    assertFalse(PreprocessorUtils.isWhitespace('\r'));
  }

}
