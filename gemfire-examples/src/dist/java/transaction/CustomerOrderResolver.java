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
package transaction;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;

import java.io.Serializable;

/**
 * This resolver ensures that customer and their order are colocated by
 * always returning {@link CustomerId}
 */
public class CustomerOrderResolver implements PartitionResolver {

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.PartitionResolver#getName()
   */
  public String getName() {
    return "customResolver";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.PartitionResolver#getRoutingObject(com.gemstone.gemfire.cache.EntryOperation)
   */
  public Serializable getRoutingObject(EntryOperation opDetails) {
    Serializable key = (Serializable)opDetails.getKey();
    if (key instanceof CustomerId) {
      return key;
    } else if (key instanceof OrderId) {
      return ((OrderId)key).getCustId();
    }
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheCallback#close()
   */
  public void close() {
  }

}
