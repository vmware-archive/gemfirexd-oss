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
package quickstart;

import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.LoaderHelper;
import java.util.Properties;

/**
 * A simple CacheLoader implementation.
 * 
 * @author GemStone Systems, Inc.
 */
public class SimpleCacheLoader implements CacheLoader<String, String>, Declarable {

  @Override
  public String load(LoaderHelper<String, String> helper) {
    String key = helper.getKey();
    System.out.println("    Loader called to retrieve value for " + key);
    
    // Create a value using the suffix number of the key (key1, key2, etc.)
    return "LoadedValue" + (Integer.parseInt(key.substring(3)));
  }
  
  @Override
  public void close() {
    // do nothing
  }
  
  @Override
  public void init(Properties props) {
    // do nothing
  }
}
