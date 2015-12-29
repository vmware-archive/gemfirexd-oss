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
package com.gemstone.gemfire.internal.size;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteWeakHashMap;

/**
 * @author dsmith
 *
 */
public class CachingSingleObjectSizer implements SingleObjectSizer {
  private final Map<Class, Long> sizeCache = new CopyOnWriteWeakHashMap<Class, Long>();
  private final SingleObjectSizer wrappedSizer;

  public CachingSingleObjectSizer(SingleObjectSizer sizer) {
    this.wrappedSizer = sizer;
  }
  public long sizeof(Object object) {
    Class clazz = object.getClass();
    if(clazz.isArray()) {
      return wrappedSizer.sizeof(object);
    }
    else {
      Long size = sizeCache.get(clazz);
      if(size != null) {
        return size.longValue();
      }
      size = Long.valueOf(wrappedSizer.sizeof(object));
      sizeCache.put(clazz, size);
      return size.longValue();
    }
  }

  public boolean isAgentAttached() {
    return wrappedSizer.isAgentAttached();
  }

}
