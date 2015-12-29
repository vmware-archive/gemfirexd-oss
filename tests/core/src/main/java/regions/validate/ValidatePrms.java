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

import hydra.*;
import objects.Message;

/**
 * Hydra parameters for the region validation test.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class ValidatePrms extends BasePrms {

  static {
    BasePrms.setValues(ValidatePrms.class);
  }

  /** (String) The type of region value object to create
   *
   * @see #getObjectType */
  public static Long objectType;

  /**
   * (ONEOF) The Region entry operations to perform.
   *
   * @see #getEntryOperation
   */
  public static Long entryOperations;

  /**
   * (int) The number of seconds for which entry operations should be
   * performed. 
   *
   * @see #getEntryOperationsDuration
   */
  public static Long entryOperationsDuration;

  /** (boolean) Should the test print out debugging information?
   *
   * @see #isDebug */
  public static Long debug;

  /** (boolean) Should the test receive values as invalidates. 
   *
   * @see #getReceiveValuesAsInvalidates */
  public static Long receiveValuesAsInvalidates;

  /**
   * (long) The number of milliseconds of latency we can tolerate for
   * distributed updates.
   *
   * @see #getDistributionLatency
   */
  public static Long distributionLatency;

  //////////////////////  Static Methods  //////////////////////

  /**
   * Returns the type (class) of object to use as region values.  By
   * default, the type is {@link objects.Message}.
   *
   * @see objects.ObjectHelper#createObject
   */
  public static String getObjectType() {
    String d = Message.class.getName();
    return TestConfig.tab().stringAt(objectType, d);
  }

  /**
   * Returns whether or not the test should print out debugging
   * information.  Default is <code>false</code>.
   */
  public static boolean isDebug() {
    return TestConfig.tab().booleanAt(debug, false);
  }

  public static boolean getReceiveValuesAsInvalidates() {
    return TestConfig.tab().booleanAt(receiveValuesAsInvalidates, false);
  }

  /**
   * Returns the (randomly selected) Region entry operation to
   * perform.  Valid values are "create", "load", "get", "update",
   * "invalidate" (distributed), and "destroy" (distributed).
   *
   * @throws HydraConfigException
   *         If no operation was configured
   */
  public static String getEntryOperation() {
    String op = TestConfig.tab().stringAt(entryOperations, null);
    if (op == null) {
      String s = "No entry operation specified";
      throw new HydraConfigException(s);
    }

    return op;
  }
  
  /**
   * Returns the number of seconds for which entry operations should
   * be performed.  The default is <code>60</code>.
   *
   * @see ValidateTasks#doEntryOperations
   */
  public static int getEntryOperationsDuration() {
    return TestConfig.tab().intAt(entryOperationsDuration, 60);
  }

  /**
   * Returns the (maximum) number of milliseconds of latency that we
   * should expect distributed operations to be propagated.  The
   * default is <code>500</code>.
   */
  public static long getDistributionLatency() {
    return TestConfig.tab().longAt(distributionLatency, 500);
  }

}
