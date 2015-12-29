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
package util;

import util.RandomValues;
import util.ValueHolder;

/** Class to compile outside the test tree so it will not be included in the
 *  jvm classpath of a GemFire member. This class can be placed in a jar and 
 *  deployed with Gfsh.
 * 
 * @author lynng
 *
 */
public class ValueHolder1 extends ValueHolder {

  /** No-arg constructor
   * 
   */
  public ValueHolder1() {
    myVersion = "util.ValueHolder1";
  }

  /**
   * @param key
   * @param randomValues
   * @param integer
   */
  public ValueHolder1(Object key, RandomValues randomValues, Integer integer) {
    super(key, randomValues, integer);
    myVersion = "util.ValueHolder1";
  }

  /**
   * @param key
   * @param randomValues
   */
  public ValueHolder1(String key, RandomValues randomValues) {
    super(key, randomValues);
    myVersion = "util.ValueHolder1";
  }

  /**
   * @param key
   * @param randomValues
   */
  public ValueHolder1(Object key, RandomValues randomValues) {
    super(key, randomValues);
    myVersion = "util.ValueHolder1";
  }

  /**
   * @param randomValues
   */
  public ValueHolder1(RandomValues randomValues) {
    super(randomValues);
    myVersion = "util.ValueHolder1";
  }

}
