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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.pivotal.gemfirexd.internal.engine.distributed.message.ProjectionRow;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapByteSource;

/**
 * 
 * @author asifs
 * 
 */
public class OffHeapReleaseUtil {

  public static void freeOffHeapReference(@Released ResultHolder holder) {
    if (holder != null) {
      ((ResultHolder)holder).freeOffHeapForCachedRowsAndCloseResultSet();
    }
  }

  public static void freeOffHeapReference(@Released Object offheapHolder) {
    if (offheapHolder != null) {
      final Class<?> oclass = offheapHolder.getClass();
      if (oclass == ResultHolder.class) {
        ((ResultHolder)offheapHolder)
            .freeOffHeapForCachedRowsAndCloseResultSet();
      }
      else if (oclass == byte[].class || oclass == byte[][].class) {
        return;
      }
      else if (oclass == ProjectionRow.class) {
        ProjectionRow projRow = (ProjectionRow)offheapHolder;
        Object rawVal = projRow.getRawValue();
        OffHeapHelper.release(rawVal);
      }
      else if (OffHeapByteSource.isOffHeapBytesClass(oclass)) {
        ((OffHeapByteSource)offheapHolder).release();
      }
      else if (oclass == GfxdListResultCollector.ListResultCollectorValue.class) {
        Object result = ((GfxdListResultCollector.ListResultCollectorValue)
            offheapHolder).resultOfSingleExecution;
        if (result instanceof List<?>) {
          freeOffHeapReference((List<?>)result);
        }
        else {
          freeOffHeapReference(result);
        }
      }
      else if (List.class.isAssignableFrom(oclass)) {
        freeOffHeapReference((List<?>)offheapHolder);
      }
    }
  }

  public static void freeOffHeapReference(List<?> resultList) {
    Iterator<?> itr = resultList.iterator();
    while (itr.hasNext()) {
      Object result = itr.next();
      freeOffHeapReference(result);
    }
  }
}
