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
package management.operations;

import hydra.Log;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import objects.ObjectHelper;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;

import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TimeoutException;

public class DefaultRegionKeyValueConfig implements RegionKeyValueConfig {

  protected RandomValues randomValues = null;

  public DefaultRegionKeyValueConfig() {
    randomValues = new RandomValues();
  }

  @Override
  public List<String> getValueColumns() {
    String type = OperationPrms.getObjectType();
    if ("valueHolder".equals(type)) {
      List<String> list = new ArrayList<String>();
      list.add("myVersion");
      list.add("myValue");
      list.add("extraObject");
      list.add("modVal");
      return list;
    }
    return null;
  }

  @Override
  public Object getNextKey() {

    return NameFactory.getNextPositiveObjectName();
  }

  @Override
  public Object getValueForKey(Object key) {
    String type = OperationPrms.getObjectType();
    if ("valueHolder".equals(type)) {
      ValueHolder anObj = new ValueHolder(key, randomValues);
      return anObj;
    } else {
      int index = -1;
      String name = (String) key;
      String array[] = name.split("_");
      index = Integer.parseInt(array[1]);
      Object typedObject = ObjectHelper.createObject(type, index);
      return typedObject;
    }
  }

  @Override
  public Object getUpdatedValueForKey(Region region, Object key) {
    String type = OperationPrms.getObjectType();
    if ("valueHolder".equals(type)) {
      ValueHolder anObj = null;
      ValueHolder newObj = null;
      try {
        anObj = (ValueHolder) region.get(key);
      } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      newObj = (ValueHolder) ((anObj == null) ? new ValueHolder(key, randomValues) : anObj
          .getAlternateValueHolder(randomValues));
      return newObj;
    } else {
      Object val = ObjectHelper.createObject(type, 0);
      return val;
    }
  }

  @Override
  public Object getUsedKey(Region region) {
    Set set = region.keys();
    if (set.size() == 0) {
      Log.getLogWriter().info("getExistingKey: No names in region");
      return null;
    }
    long maxNames = NameFactory.getPositiveNameCounter();
    if (maxNames <= 0) {
      Log.getLogWriter().info("getExistingKey: max positive name counter is " + maxNames);
      return null;
    }
    String name = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int) maxNames));
    return name;
  }

  @Override
  public int getMaxKeySize() {

    return 0;
  }

}
