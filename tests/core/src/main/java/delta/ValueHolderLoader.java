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
package delta;

import hydra.Log;
import util.*;
import com.gemstone.gemfire.cache.*;

public class ValueHolderLoader implements CacheLoader, Declarable {

public Object load(LoaderHelper helper) throws CacheLoaderException {
  String valueClass = DeltaPropagationPrms.getValueClass();
  try {
    Class cls = Class.forName(valueClass);
    BaseValueHolder value = (BaseValueHolder)cls.newInstance();
    value.myValue = DeltaPropagationPrms.getPretendSize();
    value.extraObject = new byte[DeltaPropagationPrms.getPayloadSize()];
    return value;
  } catch (ClassNotFoundException e) {
    throw new TestException(TestHelper.getStackTrace(e));
  } catch (InstantiationException e) {
    throw new TestException(TestHelper.getStackTrace(e));
  } catch (IllegalAccessException e) {
    throw new TestException(TestHelper.getStackTrace(e));
  }
}

public void close() {
   Log.getLogWriter().info("In ValueHolderLoader, close");
}

public void init(java.util.Properties prop) {
   Log.getLogWriter().info("In ValueHolderLoader, init");
}

}
