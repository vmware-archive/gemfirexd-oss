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
package com.gemstone.gemfire.management.internal.cli.remote;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @param <T>
 *          Type of ThreadLocal variable
 * 
 * @since 7.0
 */
public abstract class WrapperThreadLocal<T> extends ThreadLocal<T> {
  
  public T getAndCreateIfAbsent() {
    if (!isSet()) {
      set(createWrapped());
    }
    return get();
  }
  
  protected abstract T createWrapped();

  public boolean isSet() {
    return get() != null;
  }
}
