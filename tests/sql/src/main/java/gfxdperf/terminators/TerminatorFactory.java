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

package gfxdperf.terminators;

import gfxdperf.PerfTestException;

/**
 * Instantiates a subclass of an AbstractTerminator.
 */
public class TerminatorFactory {

  public static enum TerminatorName {
    IterationTerminator,
    TimeTerminator
    ;
  }

  public static AbstractTerminator create(TerminatorName name) {
    switch (name) {
      case IterationTerminator: return new IterationTerminator();
      case TimeTerminator: return new TimeTerminator();
    }
    String s = "Terminator " + name + " not found in enum TerminatorName";
    throw new PerfTestException(s);
  }
}
