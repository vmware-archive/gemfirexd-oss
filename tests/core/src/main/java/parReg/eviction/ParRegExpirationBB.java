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
package parReg.eviction;

// import java.util.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;

// import com.gemstone.gemfire.cache.*;

/**
 * Blackboard defining counters for events. This blackboard has counters with
 * names used by the increment methods in {@link util#AbstractListener}. Any
 * subclass of AbstractListener that wants to use AbstractListener's counter
 * increment methods can use this blackboard for that purpose. Note that the
 * names of the blackboard counters defined here must be the same names as
 * defined in AbstractListener.
 */
public class ParRegExpirationBB extends Blackboard {

  // Blackboard variables
  static String BB_NAME = "IdleTODestroyBB_Blackboard";

  static String BB_TYPE = "RMI";

  // singleton instance of blackboard
  private static ParRegExpirationBB bbInstance = null;

  // Event counters
  // afterCreate events
  public static int numAfterCreateEvents_TTLDestroy;

  public static int numAfterCreateEvents_TTLInvalidate;

  public static int numAfterCreateEvents_IdleTODestroy;

  public static int numAfterCreateEvents_IdleTOInvalidate;

  public static int numAfterCreateEvents_CustomTTLDestroy;

  public static int numAfterCreateEvents_CustomExpiryTTLDestroy;

  public static int numAfterCreateEvents_CustomNoExpiryTTLDestroy;

  public static int numAfterCreateEvents_CustomTTLInvalidate;

  public static int numAfterCreateEvents_CustomExpiryTTLInvalidate;

  public static int numAfterCreateEvents_CustomNoExpiryTTLInvalidate;

  public static int numAfterCreateEvents_CustomIdleTODestroy;

  public static int numAfterCreateEvents_CustomExpiryIdleTODestroy;

  public static int numAfterCreateEvents_CustomNoExpiryIdleTODestroy;

  public static int numAfterCreateEvents_CustomIdleTOInvalidate;

  public static int numAfterCreateEvents_CustomExpiryIdleTOInvalidate;

  public static int numAfterCreateEvents_CustomNoExpiryIdleTOInvalidate;

  // afterDestroy events
  public static int numAfterDestroyEvents_TTLDestroy;

  public static int numAfterDestroyEvents_IdleTODestroy;

  public static int numAfterDestroyEvents_CustomTTLDestroy;

  public static int numAfterDestroyEvents_CustomIdleTODestroy;

  // afterInvalidate events
  public static int numAfterInvalidateEvents_TTLInvalidate;

  public static int numAfterInvalidateEvents_IdleTOInvalidate;

  public static int numAfterInvalidateEvents_CustomTTLInvalidate;

  public static int numAfterInvalidateEvents_CustomIdleTOInvalidate;
  
  //Counter to mention that populating is complete
  public static int numPopulationTask_Completed;

  /**
   * Get the ParRegExpirationBB
   */
  public static ParRegExpirationBB getBB() {
    if (bbInstance == null) {
      synchronized (ParRegExpirationBB.class) {
        if (bbInstance == null)
          bbInstance = new ParRegExpirationBB(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public ParRegExpirationBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public ParRegExpirationBB(String name, String type) {
    super(name, type, ParRegExpirationBB.class);
  }

  /**
   * Check the value of all event counters.
   * 
   * @param expectedValues
   *                An array of the expected counter values, in the same order
   *                as they are defined in this class. If any value is < 0, then
   *                don't check it.
   * 
   * @throws TestException
   *                 if any counter does not have the expected value.
   */
  public void checkEventCounters(long[] expectedValues) {
    Log.getLogWriter().info(
        "Checking event counters in " + this.getClass().getName());
    Blackboard BB = getBB();
    String[] counterNames = BB.getCounterNames();

    // make sure expectedValues is same length as number of counters in this BB
    if (counterNames.length != expectedValues.length) {
      StringBuffer aStr = new StringBuffer();
      for (int i = 0; i < counterNames.length; i++)
        aStr.append("   counterNames[" + i + "] is " + counterNames[i] + "\n");
      for (int i = 0; i < expectedValues.length; i++)
        aStr.append("   expectedValues[" + i + "] is " + expectedValues[i]
            + "\n");
      Log.getLogWriter().info(aStr.toString());
      throw new TestException("Expected length of expectedValues "
          + expectedValues.length + " to be = length of counterNames "
          + counterNames.length);
    }

    SharedCounters counters = BB.getSharedCounters();
    for (int i = 0; i < expectedValues.length; i++) {
      if (expectedValues[i] >= 0)
        try {
          TestHelper.waitForCounter(BB, counterNames[i], BB
              .getSharedCounter(counterNames[i]), expectedValues[i], true,
              event.EventTest.MILLIS_TO_WAIT);
        }
        catch (TestException e) {
          BB.printSharedCounters();
          throw e;
        }
    }
    Log.getLogWriter().info(BB.getClass().getName() + ", all counters are OK");
  }

  /**
   * Zero all counters in this blackboard.
   */
  public void zeroAllCounters() {
    SharedCounters sc = getSharedCounters();
    sc.zero(numAfterCreateEvents_TTLDestroy);
    sc.zero(numAfterCreateEvents_TTLInvalidate);
    sc.zero(numAfterCreateEvents_IdleTODestroy);
    sc.zero(numAfterCreateEvents_IdleTOInvalidate);
    sc.zero(numAfterDestroyEvents_TTLDestroy);
    sc.zero(numAfterDestroyEvents_IdleTODestroy);
    sc.zero(numAfterInvalidateEvents_TTLInvalidate);
    sc.zero(numAfterInvalidateEvents_IdleTOInvalidate);

    sc.zero(numAfterCreateEvents_CustomTTLDestroy);
    sc.zero(numAfterCreateEvents_CustomTTLInvalidate);
    sc.zero(numAfterCreateEvents_CustomIdleTODestroy);
    sc.zero(numAfterCreateEvents_CustomIdleTOInvalidate);
    sc.zero(numAfterDestroyEvents_CustomTTLDestroy);
    sc.zero(numAfterDestroyEvents_CustomIdleTODestroy);
    sc.zero(numAfterInvalidateEvents_CustomTTLInvalidate);
    sc.zero(numAfterInvalidateEvents_CustomIdleTOInvalidate);
  }

}
