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
/**
 * 
 */
package util;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;

/**
 * @author lynn
 *
 */
public class VHDataSerializableInstantiator extends VHDataSerializable {

  static {
    Instantiator.register(new Instantiator(VHDataSerializableInstantiator.class, 88889999) {
      public DataSerializable newInstance() {
        return new VHDataSerializableInstantiator();
      }
    });
  }

  public VHDataSerializableInstantiator() {
    super();
  }

  /**
   * @param anObj
   * @param randomValues
   */
  public VHDataSerializableInstantiator(Object anObj, RandomValues randomValues) {
    super(anObj, randomValues);
  }

  /**
   * @param randomValues
   */
  public VHDataSerializableInstantiator(RandomValues randomValues) {
    super(randomValues);
  }

  /**
   * @param nameFactoryName
   * @param randomValues
   */
  public VHDataSerializableInstantiator(String nameFactoryName,
      RandomValues randomValues) {
    super(nameFactoryName, randomValues);
  }

  /**
   * @param anObj
   * @param randomValues
   * @param initModVal
   */
  public VHDataSerializableInstantiator(Object anObj,
      RandomValues randomValues, Integer initModVal) {
    super(anObj, randomValues, initModVal);
  }

  /**
   * @param nameFactoryName
   * @param randomValues
   * @param initModVal
   */
  public VHDataSerializableInstantiator(String nameFactoryName,
      RandomValues randomValues, Integer initModVal) {
    super(nameFactoryName, randomValues, initModVal);
  }

}
