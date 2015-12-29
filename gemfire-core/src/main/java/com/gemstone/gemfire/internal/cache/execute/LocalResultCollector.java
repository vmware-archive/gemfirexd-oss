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

package com.gemstone.gemfire.internal.cache.execute;

/**
 * Extends {@link InternalResultCollector} interface to provide for methods that
 * are required internally by the product and synchronization necessary for
 * thread-safe usage.
 * 
 * @author swale
 */
public interface LocalResultCollector<T, S> extends
    InternalResultCollector<T, S> {

  /** set any exception during execution of a Function in the collector */
  void setException(Throwable exception);
}
