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

package hydra.gemfirexd;

import hydra.BasePrms;

/**
 * A class used to store keys for GemFireXD connection configuration properties.
 * The settings are used by {@link GfxdHelper} when fetching connection
 * properties.
 */
public class GfxdHelperPrms extends BasePrms {

  static {
    setValues(GfxdHelperPrms.class);
  }

  /**
   * (boolean)
   * Whether to persist the DDL via the "persist-dd" connection property.
   * Defaults to true.
   * <p>
   * If true, the persistence files are written to the gemfire system directory
   * via the "sys-disk-dir" connection property.
   */
  public static Long persistDD;
  public static boolean persistDD() {
    Long key = persistDD;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (boolean)
   * Whether the test intends to create persistent queues.
   * Defaults to false.
   * <p>
   * If true, the persistence files are written to a disk directory named for
   * the logical VM ID via the "sys-disk-dir" connection property, for example
   * /export/.../test-0328-112342/vm_3_client2_disk_1.
   */
  public static Long persistQueues;
  public static boolean persistQueues() {
    Long key = persistQueues;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether the test intends to create persistent tables.
   * Defaults to false.
   * <p>
   * If true, the persistence files are written to a disk directory named for
   * the logical VM ID via the "sys-disk-dir" connection property, for example
   * /export/.../test-0328-112342/vm_3_client2_disk_1.
   */
  public static Long persistTables;
  public static boolean persistTables() {
    Long key = persistTables;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to create the disk store for overflow disk etc
   * Defaults to false.
   * <p>
   * If true, the overflow files are written to the gemfire system directory
   * via the "sys-disk-dir" connection property.
   */
  public static Long createDiskStore;
  public static boolean createDiskStore() {
    Long key = createDiskStore;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

}
