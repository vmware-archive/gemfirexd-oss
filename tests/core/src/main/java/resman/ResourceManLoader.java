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
package resman;

import hydra.Log;
import util.*;
import com.gemstone.gemfire.cache.*;

public class ResourceManLoader implements CacheLoader, Declarable {

protected static RandomValues randomValues = new RandomValues();

public Object load(LoaderHelper helper) throws CacheLoaderException {
   BaseValueHolder anObj = new ValueHolder(helper.getKey(), randomValues);
//   Log.getLogWriter().info("In ResourceManLoader for key " + helper.getKey() + ", returning " + anObj);
   return anObj;
}

public void close() {
   Log.getLogWriter().info("In ResourceManLoader, close");
}

public void init(java.util.Properties prop) {
   Log.getLogWriter().info("In ResourceManLoader, init");
}

}
