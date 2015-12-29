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
package hydratest.version.partition;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;

import hydra.Log;
import java.util.List;
import java.util.Properties;

public class TestAsyncEventListener implements
    AsyncEventListener<Object, Object>, Declarable {

  public TestAsyncEventListener() {
    Log.getLogWriter().info("Instantiated a TestAsyncEventListener");
  }

  public void init(Properties p) {
  }

  public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
    Log.getLogWriter().info("TestAsyncEventListener processEvents was invoked");
    return true;
  }

  public void close() {
    Log.getLogWriter().info("TestAsyncEventListener close was invoked");
  }
}
