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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.cache.*;
import java.util.Properties;

/**
 * A program that populates a {@link Region} with a {@link
 * CacheLoader} that gets progressively slower.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class PopulateRegion {

  /** How long we sleep before loading data */
  protected static int pause;

  public static void main(String[] args) throws Throwable {
    Properties props = new Properties();
//     props.setProperty("log-level", getGemFireLogLevel());

    DistributedSystem system = DistributedSystem.connect(props);
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper)
          throws CacheLoaderException {

          try {
            Thread.sleep(pause);
            return "Loaded value";

          } catch (InterruptedException ex) {
            return ex.toString(); // FIXME
          }
        }

        public void close() { }
      });
    RegionAttributes attrs = factory.create();

    pause = 100;
    Region region = cache.createRegion("Root", attrs);
    for (int i = 0; i < 10; i++) {
      System.out.print(pause);
      System.out.flush();

      for (int j = 0; j < 100; j++) {
        region.get("Data-" + i + "-" + j);
        System.out.print('.');
      }

      pause += 100;
    }
  }

}
