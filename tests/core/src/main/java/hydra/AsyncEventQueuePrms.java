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
package hydra;

/**
 * A class used to store keys for async event queue configuration
 * settings.  The settings are used to create instances of {@link
 * AsyncEventQueueDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values of a parameter can be set to {@link #DEFAULT}, except where noted.
 * This uses the product default, except where noted.
 * <p>
 * Values and fields can be set to {@link #NONE} where noted, with the
 * documented effect.
 * <p>
 * Values of a parameter can use oneof, range, or robing except where noted, but
 * each description created will use a fixed value chosen at test configuration
 * time.  Use as a task attribute is illegal.
 */
public class AsyncEventQueuePrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the async event descriptions and actual ids of the
   * async event queues.  Each name must be unique.  Defaults to null.  Not
   * for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (String(s))
   * Class name of async event listener for each async event queue.  A new
   * instance of the class is created each time a given logical async event
   * queue description is used to create an async event queue. This is a
   * required parameter.
   */
  public static Long asyncEventListener;

  /**
   * (boolean(s))
   * Batch conflation enabled for each async event queue.
   */
  public static Long batchConflationEnabled;

  /**
   * (int(s))
   * Batch size for each async event queue.
   */
  public static Long batchSize;

  /**
   * (int(s))
   * Batch time interval for each async event queue.
   */
  public static Long batchTimeInterval;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * for each async event queue, as found in {@link DiskStorePrms#names}.
   * This is a required parameter if {@link #persistent} is true.
   */
  public static Long diskStoreName;

  /**
   * (boolean(s))
   * Disk synchronous for each async event queue.
   */
  public static Long diskSynchronous;

  /**
   * (int(s))
   * Dispatcher threads for each async event queue.
   */
  public static Long dispatcherThreads;

  /**
   * (int(s))
   * Maximum queue memory for each async event queue.
   */
  public static Long maximumQueueMemory;

  /**
   * (String(s))
   * Order policy for each async event queue.
   */
  public static Long orderPolicy;

  /**
   * (boolean(s))
   * Whether to send in parallel for each async event queue.
   */
  public static Long parallel;

  /**
   * (boolean(s))
   * Persistent for each async event queue. When true, requires {@link
   * #diskStoreName} to be set as well.
   */
  public static Long persistent;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(AsyncEventQueuePrms.class);
  }
}
