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
package com.gemstone.gemfire;

import com.gemstone.gemfire.cache.CacheException;

/**
 * An <code>GemFireCacheException</code> is used to wrap a
 * {@link CacheException}. This is needed in contexts that can
 * not throw the cache exception directly because of it being
 * a typed exception.
 */
public class GemFireCacheException extends GemFireException {
private static final long serialVersionUID = -2844020916351682908L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireCacheException</code>.
   */
  public GemFireCacheException(String message, CacheException ex) {
    super(message, ex);
  }
  /**
   * Creates a new <code>GemFireCacheException</code>.
   */
  public GemFireCacheException(CacheException ex) {
    super(ex);
  }
  /**
   * Gets the wrapped {@link CacheException}
   */
  public CacheException getCacheException() {
    return (CacheException)getCause();
  }
}
