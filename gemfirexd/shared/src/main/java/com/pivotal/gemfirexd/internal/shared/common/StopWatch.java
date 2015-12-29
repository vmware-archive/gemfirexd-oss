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

package com.pivotal.gemfirexd.internal.shared.common;

/**
 * A simple stop watch that can be stopped/started multiple times to record the
 * total time elapsed. A new instance is created by
 * {@link SharedUtils#newTimer(long)} where the timer is started by default.
 * 
 * @author swale
 * @since 7.0
 */
public final class StopWatch {
  private long startTime;
  private long elapsedTime;

  StopWatch(long startTime) {
    this.startTime = startTime;
  }

  public void start() {
    this.startTime = System.currentTimeMillis();
  }

  public void stop() {
    this.elapsedTime += (System.currentTimeMillis() - this.startTime);
    this.startTime = 0;
  }

  public long getElapsedMillis() {
    return this.elapsedTime;
  }
}
