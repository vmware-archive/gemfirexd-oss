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
package capCon;

import util.*;
//import hydra.*;
import com.gemstone.gemfire.cache.*;

public class CapConLoader implements CacheLoader {

static CapConTest previousTestInstance;

public Object load(LoaderHelper helper) throws CacheLoaderException {
   CapConTest testInstance = (CapConTest)helper.getArgument();
   if (testInstance == null) {
      throw new TestException("Unexpected callback object " + testInstance + ", callback for previous call to loader is " + previousTestInstance);
   }
   previousTestInstance = testInstance;
   Object anObj = testInstance.getNewValue();
   return anObj;
}

public void close() {
}

}
