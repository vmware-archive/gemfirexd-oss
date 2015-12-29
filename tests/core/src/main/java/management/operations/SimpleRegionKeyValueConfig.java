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

import hydra.TestConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Region;

public class SimpleRegionKeyValueConfig implements RegionKeyValueConfig {

  AtomicInteger counter = new AtomicInteger();
  private String keyPrefix = null;
  private int maxKeys;
  private List<String> columnList;

  public SimpleRegionKeyValueConfig(String keyPrefix, int maxKeys) {
    this.keyPrefix = keyPrefix;
    this.maxKeys = maxKeys;

    columnList = new ArrayList<String>();
    columnList.add("prop1");
    columnList.add("prop2");
    columnList.add("prop3");
    columnList.add("prop4");
    columnList.add("prop5");
    columnList.add("prop6");
    columnList.add("prop7");
    columnList.add("prop8");
    columnList.add("prop9");
    columnList.add("prop10");
    columnList.add("prop11");
    columnList.add("prop12");
    columnList.add("prop13");
    columnList.add("prop14");
    columnList.add("prop15");
    columnList.add("prop16");
    columnList.add("prop17");
    columnList.add("prop18");

  }

  @Override
  public List<String> getValueColumns() {
    return columnList;
  }

  @Override
  public Object getNextKey() {
    // TODO check with bonudry
    return (keyPrefix + "_" + counter.incrementAndGet());
  }

  @Override
  public Object getValueForKey(Object key) {
    int basevalue;
    String a[] = ((String) key).split("_");
    basevalue = Integer.parseInt(a[1]);
    return new RegionObject(basevalue);
  }

  @Override
  public Object getUpdatedValueForKey(Region region, Object key) {
    int basevalue;
    String a[] = ((String) key).split("_");
    basevalue = Integer.parseInt(a[1]) * 100;
    return new RegionObject(basevalue);
  }

  @Override
  public Object getUsedKey(Region region) {
    int random = TestConfig.tab().getRandGen().nextInt(counter.get());
    return (keyPrefix + "_" + random);
  }

  @Override
  public int getMaxKeySize() {
    return this.maxKeys;
  }

}
