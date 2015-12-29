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
package regions.validate;

import hydra.blackboard.*;

/**
 * An RMI-based Hydra <code>Blackboard</code> whose shared map mirrors
 * the region being tested.  The keys of the blackboard are the same
 * as in the region and the values are instances of {@link Value}.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class ValidateBlackboard extends Blackboard {

  /** The singleton instance of ValidateBlackboard */
  private static ValidateBlackboard singleton;

  /** Should we debug the blackboard? */
  private static boolean DEBUG = false;

  //////////////////////  Static Methods  //////////////////////

  /**
   * Returns the singleton instance of
   * <code>ValidateBlackboard</code>, creating if it necessary.
   */
  public static ValidateBlackboard getInstance() {
    if (singleton == null) {
      synchronized (ValidateBlackboard.class) {
        if (singleton == null) {
          singleton = new ValidateBlackboard("ValidateBlackboard");
//           DEBUG = ValidatePrms.isDebug();
        }
      }
    }

    return singleton;
  }

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Zero-argument constructor for remote method invocations
   */
  private ValidateBlackboard() {
    super();
  }

  /**
   * Creates the singleton instance of <code>ValidateBlackboard</code>
   * with the given name.
   */
  private ValidateBlackboard(String name) {
    super(name, RMI, ValidateBlackboard.class);
  }

  ////////////////////  Instance Methods  ////////////////////

  /** 
   * Returns the value of the given key in the blackboard
   */
  public Value get(Object key) {
    return (Value) this.getSharedMap().get(key);
  }

  /**
   * Atomically replaces the value associated with the given key if
   * and only if the current value is equal to
   * <code>currentValue</code>.
   *
   * @see hydra.blackboard.SharedMap#replace
   *
   * @return Whether or not the value was actually replaced
   */
  public boolean replace(Object key, Value expectedValue,
                         Value newValue) {
    boolean replaced = 
      this.getSharedMap().replace(key, expectedValue, newValue);
    if (DEBUG) {
      String s = "Replace " + key + " -> (" + expectedValue + " -> " +
        newValue + ") " + replaced;
      hydra.Log.getLogWriter().info(s);
    }
    return replaced;
  }

  /**
   * Returns a randomly chosen key from the blackboard
   *
   * @see hydra.blackboard.SharedMap#getRandomKey
   */
  public Object getRandomKey() {
    return this.getSharedMap().getRandomKey();
  }

}
