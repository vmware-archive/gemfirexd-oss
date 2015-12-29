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

import java.util.Properties;

import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region.Entry;

public class ParRegCustomExpiry implements CustomExpiry, Declarable {

  public ParRegCustomExpiry() {
  }

  public ExpirationAttributes getExpiry(Entry entry) {
    if (entry.getKey() instanceof String) {
      String key = (String)entry.getKey();
      if (key.startsWith("Expire_")) {
        if (key.startsWith("Expire_CustomTTLDestroy")
            || key.startsWith("Expire_CustomIdleTODestroy")) {
          return new ExpirationAttributes(420, ExpirationAction.DESTROY);
        }
        else if (key.startsWith("Expire_CustomTTLInval")
            || key.startsWith("Expire_CustomIdleTOInval")) {
          return new ExpirationAttributes(420, ExpirationAction.INVALIDATE);
        }
        else {
          return null;
        }
      }
      else {
        return null;
      }
    }
    return null;
  }

  public void init(Properties props) {
  }

  public void close() {
  }

}
