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
   
package util;

import hydra.Log;
import hydra.TestConfig;

public class RandomRangeValues extends RandomValues {

public RandomRangeValues(Long objectType_paramKey) {
   super(objectType_paramKey);
}

/** Return a random object of the type specified with the given hydra config
 *  param.
 *
 *  @return Return a random object of the specified type.
 */
public Object getRandomObject() {
   int randInt = TestConfig.tab().getRandGen().nextInt(0, objectTypeVec.size() - 1);
   String objectType = (String)objectTypeVec.elementAt(randInt);
   Object anObj = getRandomObjectForType(objectType);
   Log.getLogWriter().fine("getRandomObject, returning " + TestHelper.toString(anObj));
   return anObj;
}

/** Return a random object of the type specified with the given class name.
 *
 *  @param dataTypeName A fully qualified class name.
 *
 *  @return Return a random object of the specified type.
 */
public Object getRandomObjectForType(String dataTypeName) {
   Object returnObject = null;
   if (dataTypeName.equals(Integer.class.getName())) {
      returnObject = new Integer(getRandom_int());
   } else if (dataTypeName.equals(Long.class.getName())) {
      returnObject = new Long(getRandom_long());
   } else {
      throw new TestException("Test does not support: " + dataTypeName); 
   }
   return returnObject;
}

/** Return a random int in the range specified by randomValueRange.
 *
 *  @return A random int.
 */
public int getRandom_int() {
   int randomValueRange = TestConfig.tab().intAt(RandomValuesPrms.randomValueRange);
   if (randomValueRange <= 0)
      return TestConfig.tab().getRandGen().nextInt();
   else
      return TestConfig.tab().getRandGen().nextInt(randomValueRange);
}

/** Return a random long in the range specified by randomValueRange.
 *
 *  @return A random long.
 */
public long getRandom_long() {
   int randomValueRange = TestConfig.tab().intAt(RandomValuesPrms.randomValueRange);
   if (randomValueRange <= 0)
      return TestConfig.tab().getRandGen().nextLong();
   else
      return TestConfig.tab().getRandGen().nextLong(randomValueRange);
}

}
