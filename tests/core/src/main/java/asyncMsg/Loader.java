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
package asyncMsg;

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

public class Loader implements CacheLoader {

public Object load(LoaderHelper helper) throws CacheLoaderException {
   Object key = helper.getKey();
   Object anObj = AsyncMsgTest.asyncTest.getNewValue(key);
   Log.getLogWriter().info("In Loader, returning " + ((BaseValueHolder)anObj).toString());
//   Log.getLogWriter().info("In Loader, returning " + ((ValueHolder)anObj).toString() + TestHelper.getStackTrace());
   return anObj;
}

public void close() {
}

}
