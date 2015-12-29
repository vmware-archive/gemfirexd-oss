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

import hydra.RemoteTestModule;
import util.TestException;

import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.control.MemoryEvent;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds.MemoryState;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;

public class EvictionThresholdListener implements ResourceListener<MemoryEvent> {
  private static int normalCalls = 0;
  
  private static int criticalThresholdCalls = 0;

  private static int evictionThresholdCalls = 0;

  private static int disabledCalls = 0;
  
  private static int allCalls = 0;
  
  private static long bytesFromThreshold = 0;

  private static float currentPercentage = 0;

  public static final float UPPER_LIMIT_EVICTION_TOLERANCE = System.getProperty("java.vm.vendor").startsWith("IBM") ? 25:25;

  public static final float LOWER_LIMIT_EVICTION_TOLERANCE = System.getProperty("java.vm.vendor").startsWith("IBM") ? 25:2;

  public static final String CRITICAL_HEAP_PERCENTAGE = "Critical Heap Percentage";
  public static final String EVICTION_HEAP_PERCENTAGE = "Eviction Heap Percentage";
  public static final String CRITICAL_OFF_HEAP_PERCENTAGE = "Critical Off-Heap Percentage";
  public static final String EVICTION_OFF_HEAP_PERCENTAGE = "Eviction Off-Heap Percentage";

  public static final String EVICTION_EVENT = "Eviction";
  
  public static final String NORMAL_EVENT = "Normal";

  public static final String NO_EVENT_RECEIVED = "No Event Received";

  private static String lastEventReceived = NO_EVENT_RECEIVED;

  public EvictionThresholdListener() {
    hydra.Log.getLogWriter().info(
        "Creating instance of EvictionThresholdListener");
  } 

  public static long getBytesFromThreshold() {
    synchronized (EvictionThresholdListener.class) {
      return bytesFromThreshold;
    }
  }

  public static float getCurrentPercentage() {
    synchronized (EvictionThresholdListener.class) {
      return currentPercentage;
    }
  }

  public static int getAllCalls() {
    synchronized (EvictionThresholdListener.class) {
      return allCalls;
    }
  }

  public static int getNormalCalls() {
    synchronized (EvictionThresholdListener.class) {
      return normalCalls;
    }
  }
  
  public static int getCriticalThresholdCalls() {
    synchronized (EvictionThresholdListener.class) {
      return criticalThresholdCalls;
    }
  }

  public static int getEvictionThresholdCalls() {
    synchronized (EvictionThresholdListener.class) {
      return evictionThresholdCalls;
    }
  }


  public static int getDisabledCalls() {
    synchronized (EvictionThresholdListener.class) {
      return disabledCalls;
    }
  }

  public static void resetThresholdCalls() {
    synchronized (EvictionThresholdListener.class) {
      normalCalls = 0;
      criticalThresholdCalls = 0;
      evictionThresholdCalls = 0;
      disabledCalls = 0;
      allCalls = 0;
    }
  }

  public static String asString() {
    return new StringBuilder("TestListenerStatus:").append(
        " normalCalls :" + normalCalls).append(
        " criticalThresholdCalls :" + criticalThresholdCalls).append(
        " evictionThresholdCalls :" + evictionThresholdCalls).append(
        " disabledCalls :" + disabledCalls).append(
        " allCalls :" + allCalls).toString();
  }

  /**
   * Convert a percentage as a double to an integer e.g. 0.09 => 9
   * also legal is 0.095 => 9
   * @param percent a percentage value expressed as a double e.g. 9.5% => 0.095
   * @return the calculated percent as an integer >= 0 and <= 100
   */
  private int convertToIntPercent(final double percent) {
    assert percent >= 0.0 && percent <= 1.0;
    int ret = (int) Math.ceil(percent * 100.0);
    assert ret >= 0 && ret <= 100;
    return ret;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.cache.management.ResourceListener#onEvent(java.lang.Object)
   */
  @Override
  public void onEvent(MemoryEvent event) {
    hydra.Log.getLogWriter().info("In EvictionThresholdListener.onEvent with " + event);
    EvictionBB.getBB().printSharedMap();
    synchronized (EvictionThresholdListener.class) {
      if (event.isLocal()) {
        if (event.getState().isCritical()) {
          this.bytesFromThreshold = event.getBytesUsed() - event.getThresholds().getCriticalThresholdBytes();
        } else if (event.getState().isEviction()) {
          if (event.getPreviousState().isCritical()) {
            this.bytesFromThreshold = event.getThresholds().getCriticalThresholdBytes() - event.getBytesUsed();
          } else {
            this.bytesFromThreshold = event.getBytesUsed() - event.getThresholds().getEvictionThresholdBytes();
          }
        } else {
          this.bytesFromThreshold = event.getThresholds().getEvictionThresholdBytes() - event.getBytesUsed();
        }

        if (event.getThresholds().getMaxMemoryBytes() == 0) {
          this.currentPercentage = 0;
        } else if (event.getBytesUsed() > event.getThresholds().getMaxMemoryBytes()) {
          this.currentPercentage = 1;
        } else {
          this.currentPercentage = convertToIntPercent((double)(event.getBytesUsed()) / (double)(event.getThresholds().getMaxMemoryBytes()));
        }

        if (event.getState().isNormal()) {
          normalCalls++;
          verifyNormalEvent(event);
          lastEventReceived = NORMAL_EVENT;
          hydra.Log.getLogWriter().info("Received normal event number " + normalCalls + " bytes used " + event.getBytesUsed());
        }
        if (event.getState().isCritical() && event.getState() != event.getPreviousState()) {
          this.criticalThresholdCalls++;
        }
        if (event.getState().isEviction() && event.getState() != event.getPreviousState()) {
          evictionThresholdCalls++;
          verifyEvictionEvent(event);
          lastEventReceived = EVICTION_EVENT;
          hydra.Log.getLogWriter().info(
              "Received eviction event number " + evictionThresholdCalls + " bytes used " + event.getBytesUsed());
        }
        if (event.getState() == MemoryState.DISABLED) {
          disabledCalls++;
        }

        allCalls++;
      }
    }
  }

  private void verifyEvictionEvent(MemoryEvent event) {
    float evictionPercentage = 0;
    float criticalPercentage = 0;
    if (event.getType() == ResourceType.HEAP_MEMORY) {
      evictionPercentage = (Float)EvictionBB.getBB().getSharedMap().get(EVICTION_HEAP_PERCENTAGE);
      criticalPercentage = (Float)EvictionBB.getBB().getSharedMap().get(CRITICAL_HEAP_PERCENTAGE);
    } else if (event.getType() == ResourceType.OFFHEAP_MEMORY) {
      evictionPercentage = (Float)EvictionBB.getBB().getSharedMap().get(EVICTION_OFF_HEAP_PERCENTAGE);
      criticalPercentage = (Float)EvictionBB.getBB().getSharedMap().get(CRITICAL_OFF_HEAP_PERCENTAGE);
    } else {
      throw new TestException("Test cannot handle type " + event.getType());
    }

    if (currentPercentage > criticalPercentage) {
      String errorString = "For the Vm " + RemoteTestModule.getMyVmid()
          + " :Eviction event raised when the currentPercentage "
          + currentPercentage
          + " is greater than critical percentage "
          + criticalPercentage;
      hydra.Log.getLogWriter().info(errorString);
      throwException(errorString);
    }

    float upperLimitTolerance = evictionPercentage + UPPER_LIMIT_EVICTION_TOLERANCE;
    float lowerLimitTolerance = evictionPercentage - LOWER_LIMIT_EVICTION_TOLERANCE;

    if (currentPercentage > upperLimitTolerance) {
      String errorString = "For the Vm " + RemoteTestModule.getMyVmid()
          + " :Eviction up event raised too late - the currentPercentage "
          + currentPercentage + " eviction percentage "
          + evictionPercentage + " (test considered tolerence up to "
          + upperLimitTolerance + "%)";
      hydra.Log.getLogWriter().info(errorString);
      throwException(errorString);
    }

    if (currentPercentage < lowerLimitTolerance) {
      String errorString = "For the Vm " + RemoteTestModule.getMyVmid()
          + " :Eviction up event raised too early - the currentPercentage "
          + currentPercentage + " eviction percentage "
          + evictionPercentage + " (test considered tolerence up to "
          + lowerLimitTolerance + "%)";
      hydra.Log.getLogWriter().info(errorString);
      throwException(errorString);
    }

    if ((lastEventReceived != NORMAL_EVENT)
        && (lastEventReceived != NO_EVENT_RECEIVED)
        && (lastEventReceived != EVICTION_EVENT)) {
      String errorString = "For the Vm " + RemoteTestModule.getMyVmid()
          + " event preceding eviction up event is " + event.toString();
      hydra.Log.getLogWriter().info(errorString);
      throwException(errorString);
    }

  }

  private void verifyNormalEvent(MemoryEvent event) {
    float evictionPercentage = 0;
    float criticalPercentage = 0;
    if (event.getType() == ResourceType.HEAP_MEMORY) {
      evictionPercentage = (Float)EvictionBB.getBB().getSharedMap().get(EVICTION_HEAP_PERCENTAGE);
      criticalPercentage = (Float)EvictionBB.getBB().getSharedMap().get(CRITICAL_HEAP_PERCENTAGE);
    } else if (event.getType() == ResourceType.OFFHEAP_MEMORY) {
      evictionPercentage = (Float)EvictionBB.getBB().getSharedMap().get(EVICTION_OFF_HEAP_PERCENTAGE);
      criticalPercentage = (Float)EvictionBB.getBB().getSharedMap().get(CRITICAL_OFF_HEAP_PERCENTAGE);
    } else {
      throw new TestException("Test cannot handle type " + event.getType());
    }

    if (criticalPercentage != 0) { // critical percentage is set; check it
      if (currentPercentage > criticalPercentage) {
        String errorString = "For the Vm " + RemoteTestModule.getMyVmid()
            + " :Normal event raised when the currentPercentage "
            + currentPercentage
            + " is greater than critical percentage "
            + criticalPercentage;
        hydra.Log.getLogWriter().info(errorString);
        throwException(errorString);
      }
    }

    float upperLimitTolerance = evictionPercentage
        - UPPER_LIMIT_EVICTION_TOLERANCE;
    float lowerLimitTolerance = evictionPercentage
        + LOWER_LIMIT_EVICTION_TOLERANCE;
    if (evictionPercentage != 0) { // eviction percentage is set; check it
      if (currentPercentage < upperLimitTolerance) {
        String errorString = "For the Vm "
            + RemoteTestModule.getMyVmid()
            + " :Eviction down event raised too late - the currentPercentage "
            + currentPercentage + " eviction percentage "
            + evictionPercentage + " (test considered tolerence up to "
            + upperLimitTolerance + "%)";
        hydra.Log.getLogWriter().info(errorString);
        throwException(errorString);
      }

      if (currentPercentage > lowerLimitTolerance) {
        String errorString = "Eviction down event raised too early - the currentPercentage "
            + currentPercentage
            + " eviction percentage "
            + evictionPercentage
            + " (test considered tolerence up to "
            + lowerLimitTolerance + "%)";
        hydra.Log.getLogWriter().info(errorString);
        throwException(errorString);
      }
    }

    if ((lastEventReceived != EVICTION_EVENT)) {
      String errorString = "For the Vm " + RemoteTestModule.getMyVmid()
          + " event preceding eviction down event is " + event.toString();
      hydra.Log.getLogWriter().info(errorString);
      throwException(errorString);
    }

  }

  public void throwException(String reason) {

    hydra.Log.getLogWriter().info("Adding exception :" + reason);
    long exceptionNumber = EvictionBB.getBB().getSharedCounters()
        .incrementAndRead(EvictionBB.NUM_EXCEPTION);

    EvictionBB.getBB().getSharedMap().put(new Long(exceptionNumber), reason);

    EvictionBB.getBB().getSharedCounters().incrementAndRead(
        EvictionBB.NUM_COMPLETED_EXCEPTION_LOGGING);
  }
}
