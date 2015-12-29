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
package objects.query;

import com.gemstone.gemfire.cache.Region;

public abstract class BaseGFEQueryFactory extends BaseQueryFactory implements GFEQueryFactory {

  //direct get
  public Object directGet(Object key, Region region) {
    return region.get(key);
  }
  
  //direct put
  public void directPut(Object key, Region region, Object value) {
    region.put(key, value);
  }
}
