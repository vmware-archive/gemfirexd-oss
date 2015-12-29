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
package cacheRunner;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheLoader</code> that simply returns the 
 * value of the <code>key</code> that is
 * loaded. 
 *
 * @author GemStone Systems, Inc.
 * @since 3.5
 */
public class StringLoader implements CacheLoader, Declarable {

  /**
   * Simply return the string value of the key being loaded
   *
   * @see LoaderHelper#getKey
   * @see String#valueOf(Object)
   */
  public Object load(LoaderHelper helper)
    throws CacheLoaderException {

    return String.valueOf(helper.getKey());
  }

  /**
   * Nothing to initialize
   */
  public void init(java.util.Properties props) {

  }

  public void close() {

  }
}
