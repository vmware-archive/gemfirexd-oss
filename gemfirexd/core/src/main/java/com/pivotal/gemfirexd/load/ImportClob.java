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

package com.pivotal.gemfirexd.load;

import java.sql.Clob;

/**
 * Simple {@link Clob} implementation for a string that can be used in custom
 * overrides to {@link Import} class for
 * SYSCS_UTIL.IMPORT_TABLE_EX/IMPORT_DATA_EX procedures.
 */
public class ImportClob extends
    com.pivotal.gemfirexd.internal.impl.load.ImportClobBase {

  /**
   * Create a Clob object, whose value is given as string.
   * 
   * @param data
   *          String that contains the clob data.
   */
  public ImportClob(String data) {
    super(data);
  }
}
