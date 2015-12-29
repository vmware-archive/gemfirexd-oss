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

package security;

import com.gemstone.gemfire.cache.EntryEvent;
import hydra.*;

/**
 * 
 * @author Aneesh Karayil
 * @since 5.5
 * 
 */
public class SecurityListener extends hct.EventListener {

  static Object lock = new Object();

  public static int create;

  public static int invalidate;

  public static int update;

  public static int destroy;

  public void afterCreate(EntryEvent event) {
    // super.afterCreate(event);
    synchronized (lock) {
      create++;
    }
    Log.getLogWriter().info(
        "Invoked the SecurityListerer for afterCreate for key "
            + event.getKey());

  }

  public void afterUpdate(EntryEvent event) {
    // super.afterUpdate(event);
    synchronized (lock) {
      update++;
    }
    Log.getLogWriter().info(
        "Invoked the SecurityListerer for afterUpdate for key "
            + event.getKey() + " New value " + event.getNewValue()
            + " Old value " + event.getOldValue());

  }

  public void afterInvalidate(EntryEvent event) {
    // super.afterInvalidate(event);
    synchronized (lock) {
      invalidate++;
    }
  }

  public void afterDestroy(EntryEvent event) {
    // super.afterDestroy(event);
    synchronized (lock) {
      destroy++;
    }
  }

}
