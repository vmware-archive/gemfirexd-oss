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

package perffmwk;

import java.io.*;

/**
 *  An instance of a trim interval.  It has a start and end time.
 *  Used for caching and reporting trim intervals.
 */

public class TrimInterval implements Serializable {

  /** Optional name (used in new gfxdperf framework) */
  private String name;

  /** Keeps the max start time for this logical trim spec. */
  private long start = -1;

  /** Keeps the min end time for this logical trim spec. */
  private long end = -1;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public TrimInterval() {
  }

  public TrimInterval(String s) {
    this.name = s;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE METHODS                                                  ////
  //////////////////////////////////////////////////////////////////////////////

  public String getName() {
    return this.name;
  }

  public long getStart() {
    return this.start;
  }
  public void setStart( long t ) {
    this.start = t;
  }
  public long getEnd() {
    return this.end;
  }
  public void setEnd( long t ) {
    this.end = t;
  }
}
