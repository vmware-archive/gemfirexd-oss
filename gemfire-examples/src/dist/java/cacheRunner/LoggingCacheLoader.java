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
 * A <code>CacheLoader</code> that logs information about the events
 * it receives.
 *
 * @see LoggingCacheWriter
 *
 * @author GemStone Systems, Inc.
 * @since 4.0
 */
public class LoggingCacheLoader extends LoggingCacheCallback
  implements CacheLoader {

  /**
   * Before creating a new value for the requested key, this loader
   * will do a {@link LoaderHelper#netSearch} to look for the value in
   * another member of the distributed system.
   */
  public final Object load(LoaderHelper helper)
    throws CacheLoaderException {

    StringBuffer sb = new StringBuffer();
    sb.append("Loader invoked for key: ");
    sb.append(format(helper.getKey()));
    sb.append("\n");

    if (helper.getArgument() != null) {
      sb.append("  With argument: ");
      sb.append(format(helper.getArgument()));
      sb.append("\n");
    }
              
    Object value;
    try {
      value = helper.netSearch(false /* netLoad */);

    } catch (TimeoutException ex) {
      String s = "Timed out while performing net search";
      throw new CacheLoaderException(s, ex);
    }

    if (value != null) {
      sb.append("  Net search found value: ");
      sb.append(format(value));
      sb.append("\n");

    } else {
      sb.append("  Net search did not find a value");
      value = "Loader Invoked by Thread ID " + Thread.currentThread();
    }

    log(sb.toString(), helper.getRegion());

    return value;
  }

}
