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
package splitBrain;

import java.util.*;
import java.io.Serializable;

import util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;

/**
 * Used by the mlRio tests to get accurate sizes when using
 * memory controllers.
 */
public class MLRioObjectSizer extends ObjectSizerImpl implements Serializable, Declarable {
  public int sizeof( Object o ) {
    if (o instanceof BaseValueHolder) {
      int result = Sizeable.PER_OBJECT_OVERHEAD;
      result += 3/*ValueHolder.numFields*/ * 4/*fieldSize*/;
      BaseValueHolder vh = (BaseValueHolder)o;
      if (vh.myValue != null) {
        result += Sizeable.PER_OBJECT_OVERHEAD;
        if (vh.myValue instanceof Long) {
          result += 8;
        } else if (vh.myValue instanceof String) {
          result += 4/*for the string length*/ + (((String)vh.myValue).length() * 2);
        } else {
          result += com.gemstone.gemfire.internal.util.Sizeof.sizeof(vh.myValue);
        }
      }
      if (vh.extraObject != null) {
        result += Sizeable.PER_OBJECT_OVERHEAD;
        result += 4/*for the array length*/ + ((byte[])vh.extraObject).length;
      }
      if (vh.modVal != null) {
        result += Sizeable.PER_OBJECT_OVERHEAD + 4 /* Integer */;
      }
      return result;
    } else {
      return super.sizeof(o);
    }
  }

  // Declarable implementation
  public void init(Properties props) {
  }
}
