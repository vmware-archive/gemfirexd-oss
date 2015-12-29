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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.query.internal.Support;

/**
 * This class is intended to hold a single 'observer' which will receive
 * callbacks. There can be only one such observer at a time. If no observer is
 * needed, this member variable should point to an object with 'do-nothing'
 * methods, such as BridgeObserverAdapter.
 * 
 * @author Yogesh Mahajan
 * @since 5.1
 */
public class BridgeObserverHolder
  {

  /**
   * The default 'do-nothing' bridge observer *
   */
  private static final BridgeObserver NO_OBSERVER = new BridgeObserverAdapter();

  /**
   * The current observer which will be notified of all query events.
   */
  private static BridgeObserver _instance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of events. Returns the current
   * observer.
   */
  public static final BridgeObserver setInstance(BridgeObserver observer)
  {
    Support.assertArg(observer != null,
        "setInstance expects a non-null argument!");
    BridgeObserver oldObserver = _instance;
    _instance = observer;
    return oldObserver;
  }

  /** Return the current BridgeObserver instance */
  public static final BridgeObserver getInstance()
  {
    return _instance;
  }

}
