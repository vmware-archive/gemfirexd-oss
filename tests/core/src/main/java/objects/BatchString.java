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

package objects;

/**
 * An object that is a String prefixed by a batch number such that the first
 * {@link BatchStringPrms#batchSize} instances are in batch 0, the second are
 * in batch 1, etc.
 */
public class BatchString {

  public static final String PREFIXER = "_";

  public static String init(int index) {
    int batchSize = BatchStringPrms.getBatchSize();
    return PREFIXER + index/batchSize + PREFIXER + index;
  }
  public static int getIndex(String str) {
    int marker = str.indexOf(PREFIXER);
    if (marker == -1) {
      throw new ObjectAccessException("Should not happen");
    }
    String index = str.substring(marker + 1, str.length());
    try {
      return (new Integer(index)).intValue();
    } catch (NumberFormatException e) {
      throw new ObjectAccessException("Should not happen");
    }
  }
  public static void validate(int index, String str) {
    int encodedIndex = getIndex(str);
    if (encodedIndex != index) {
      throw new ObjectValidationException("Expected index " + index + ", got " + encodedIndex);
    }
  }
  /**
   * Return a regular expression that matches the keys in the given batch.
   */
  public static String getRegex(int batchNum) {
    return PREFIXER + batchNum + PREFIXER + "[0-9]+";
  }
}
