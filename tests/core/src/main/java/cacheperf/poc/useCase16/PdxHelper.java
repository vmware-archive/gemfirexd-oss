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
package cacheperf.poc.useCase16;

import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;
import hydra.Log;
import java.util.ArrayList;
import java.util.List;

public class PdxHelper {

  public static PdxSerializer instantiatePdxSerializer() {
    List<String> l = new ArrayList();
    l.add("objects.PdxObject#identity=timestamp#identity=field1#identity=field2");
    ReflectionBasedAutoSerializer pdx = new ReflectionBasedAutoSerializer(l);
    Log.getLogWriter().info("Created PDX serializer " + pdx.getClass().getName() + " with args: " + l);
    return pdx;
  }
}
