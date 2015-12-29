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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.util.ArrayList;
import java.util.EnumSet;

import com.gemstone.gnu.trove.THashMap;
import com.gemstone.gnu.trove.THashSet;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * @author vivekb Only a wrapper class for purpose of handling @see
 *         DMLQueryInfo.ncjMetaData
 */
public abstract class NcjHashMapWrapper {
  private NcjHashMapWrapper() {
  }

  public static int MINUS_ONE = -1;

  private static enum Local {
    ZERO(0), ONE(1), MAXTABLES(4), TOTALTABLES(8), FIRSTTABLE(-91), SECONDTABLE(
        -92), PULLTABLES(-99), HIGHERJOINORDER(-100), BATCHSIZE(500), CACHESIZE(
        501);
    private int value;

    private Local(int value) {
      this.value = value;
    }
  };

  public static String getEnumValues() {
    StringBuilder strBldr = new StringBuilder();
    strBldr.append(" { ");
    for (Local lenum : EnumSet.allOf(Local.class)) {
      strBldr.append(lenum);
      strBldr.append(":");
      strBldr.append(lenum.value);
      strBldr.append(", ");
    }
    strBldr.append("}");
    return strBldr.toString();
  }

  public static int getSecondTablePositionIndex() {
    return Local.ONE.value;
  }

  public static int getFirstTablePositionIndex() {
    return Local.ZERO.value;
  }

  static int getMaxTableAllowed() {
    return Local.MAXTABLES.value;
  }

  static int getTotalTableAllowed() {
    return Local.TOTALTABLES.value;
  }

  private static void addValue(THashMap ncjMetaData, int index, String val) {
    if (val == null) {
      SanityManager.THROWASSERT("Null not allowed for index=" + index);
    }
    ncjMetaData.put(index, val);
  }

  private static String getValue(THashMap ncjMetaData, int index) {
    Object val = ncjMetaData.get(index);
    if (val != null) {

      return (String)val;
    }
    return null;
  }

  private static boolean isEquals(THashMap ncjMetaData, int index,
      String otherVal) {
    if (otherVal == null) {
      return false;
    }

    Object val = ncjMetaData.get(index);
    if (val != null && val instanceof String) {
      String strVal = (String)val;
      return strVal == otherVal || strVal.equals(otherVal);
    }
    return false;
  }

  private static void addToArrayList(THashMap ncjMetaData, int index, String val) {
    if (val == null) {
      SanityManager.THROWASSERT("Null not allowed for index=" + index);
    }
    final ArrayList<String> valList;
    Object obj = ncjMetaData.get(index);
    if (obj == null) {
      valList = new ArrayList<String>(Local.MAXTABLES.value);
      ncjMetaData.put(index, valList);
    }
    else {
      valList = (ArrayList<String>)obj;
    }
    valList.add(val);
  }

  private static ArrayList<String> getArrayList(THashMap ncjMetaData, int index) {
    Object val = ncjMetaData.get(index);
    if (val != null && val instanceof ArrayList) {
      return (ArrayList<String>)val;
    }
    return null;
  }

  private static void addToTHashSet(THashMap ncjMetaData, int index, String val) {
    if (val == null) {
      SanityManager.THROWASSERT("Null not allowed for index=" + index);
    }
    final THashSet valSet;
    Object obj = ncjMetaData.get(index);
    if (obj == null) {
      valSet = new THashSet(Local.MAXTABLES.value);
      ncjMetaData.put(index, valSet);
    }
    else {
      valSet = (THashSet)obj;
    }
    valSet.add(val);
  }

  private static THashSet getTHashSet(THashMap ncjMetaData, int index) {
    Object val = ncjMetaData.get(index);
    if (val != null && val instanceof THashSet) {
      return (THashSet)val;
    }
    return null;
  }

  static void addTableAtFirstPosition(THashMap ncjMetaData, String strVal,
      boolean isPull, TableQueryInfo logTqi) {
    if (isPull) {
      addToTHashSet(ncjMetaData, Local.PULLTABLES.value, strVal);
    }
    addValue(ncjMetaData, Local.FIRSTTABLE.value, strVal);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER, "Added Table "
            + strVal + " at first Position. isRemotePull= " + isPull + " ,tqi="
            + logTqi);
      }
    }
    return;
  }

  static void addTableAtSecondPosition(THashMap ncjMetaData, String strVal,
      boolean isPull, TableQueryInfo logTqi) {
    if (isPull) {
      addToTHashSet(ncjMetaData, Local.PULLTABLES.value, strVal);
    }
    addValue(ncjMetaData, Local.SECONDTABLE.value, strVal);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER, "Added Table "
            + strVal + " at second Position. isRemotePull= " + isPull
            + " ,tqi=" + logTqi);
      }
    }
    return;
  }

  static void addTableAtHigherPosition(THashMap ncjMetaData, String strVal,
      boolean isPull, TableQueryInfo logTqi) {
    if (isPull) {
      addToTHashSet(ncjMetaData, Local.PULLTABLES.value, strVal);
    }
    addToArrayList(ncjMetaData, Local.HIGHERJOINORDER.value, strVal);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER, "Added Table "
            + strVal + " at next Position. isRemotePull= " + isPull + " ,tqi="
            + logTqi);
      }
    }
    return;
  }

  public static boolean isTabForPull(THashMap ncjMetaData, String strVal) {
    if (strVal != null) {
      THashSet tSet = getTHashSet(ncjMetaData, Local.PULLTABLES.value);
      if (tSet != null && tSet.size() > 0) {
        return tSet.contains(strVal);
      }
    }
    return false;
  }

  public static boolean isTabAtSecondPosition(THashMap ncjMetaData,
      String strVal) {
    SanityManager.ASSERT(strVal != null);
    return isEquals(ncjMetaData, Local.SECONDTABLE.value, strVal);
  }

  public static boolean isTabAtFirstPosition(THashMap ncjMetaData, String strVal) {
    SanityManager.ASSERT(strVal != null);
    return isEquals(ncjMetaData, Local.FIRSTTABLE.value, strVal);
  }

  public static int indexOfHigherLevelJoinOrder(THashMap ncjMetaData,
      String strVal) {
    ArrayList<String> tList = getArrayList(ncjMetaData,
        Local.HIGHERJOINORDER.value);
    if (tList != null && tList.size() > 0) {
      SanityManager.ASSERT(strVal != null);
      return tList.indexOf(strVal);
    }
    return NcjHashMapWrapper.MINUS_ONE;
  }

  public static int sizeOfHigherLevelJoinOrder(THashMap ncjMetaData) {
    ArrayList<String> tList = getArrayList(ncjMetaData,
        Local.HIGHERJOINORDER.value);
    if (tList != null) {
      return tList.size();
    }
    return 0;
  }

  public static int getNoOfTabsForPull(THashMap ncjMetaData) {
    THashSet tSet = getTHashSet(ncjMetaData, Local.PULLTABLES.value);
    if (tSet != null) {
      return tSet.size();
    }
    return 0;
  }

  public static int getNoOfTabsForPullAtFirstLevel(THashMap ncjMetaData) {
    int count = 0;
    if (isTabForPull(ncjMetaData, getValue(ncjMetaData, Local.FIRSTTABLE.value))) {
      count++;
    }

    if (isTabForPull(ncjMetaData,
        getValue(ncjMetaData, Local.SECONDTABLE.value))) {
      count++;
    }

    return count;
  }

  private static void addIntValue(THashMap ncjMetaData, int index, int val) {
    ncjMetaData.put(index, val);
  }

  private static Integer getIntValue(THashMap ncjMetaData, int index) {
    Object val = ncjMetaData.get(index);
    if (val instanceof Integer) {
      return (Integer)val;
    }
    return null;
  }

  public static void setBatchSize(THashMap ncjMetaData, int intVal) {
    addIntValue(ncjMetaData, Local.BATCHSIZE.value, intVal);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
            "Added Batch Size = " + intVal);
      }
    }
    return;
  }

  public static int getBatchSize(THashMap ncjMetaData) {
    Integer value = getIntValue(ncjMetaData, Local.BATCHSIZE.value);
    if (value != null) {
      return value;
    }
    return 0;
  }

  public static void setCacheSize(THashMap ncjMetaData, int intVal) {
    addIntValue(ncjMetaData, Local.CACHESIZE.value, intVal);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
            "Added Cache Size = " + intVal);
      }
    }
    return;
  }

  public static int getCacheSize(THashMap ncjMetaData) {
    Integer value = getIntValue(ncjMetaData, Local.CACHESIZE.value);
    if (value != null) {
      return value;
    }
    return 0;
  }
}
