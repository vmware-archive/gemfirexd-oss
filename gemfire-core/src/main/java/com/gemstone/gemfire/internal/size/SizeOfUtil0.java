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
package com.gemstone.gemfire.internal.size;

/**
 * A Size of util class which does nothing. This is useful for running the test
 * with jprobe, because jprobe doesn't play nicely with the -javaagent flag. If
 * we implement a 1.4 SizeOfUtil class, then we probably don't need this one.
 * 
 * @author dsmith
 * 
 */
public class SizeOfUtil0 implements SingleObjectSizer {

  public long sizeof(Object object) {
    return 2;
  }

  public boolean isAgentAttached() {
    return false;
  }
}
