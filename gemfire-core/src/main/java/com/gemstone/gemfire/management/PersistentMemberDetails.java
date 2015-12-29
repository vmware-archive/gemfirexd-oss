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
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;

/**
 * Composite data type used to distribute attributes for the missing disk
 * store of a persistent member.
 *
 * @author rishim
 * @since 7.0
 *
 */
public class PersistentMemberDetails {

  private final String host;
  private final String directory;
  private final String diskStoreId;

  @ConstructorProperties( { "host", "directory", "diskStoreId" })
  public PersistentMemberDetails(final String host, final String directory, final String diskStoreId) {
    this.host = host;
    this.directory = directory;
    this.diskStoreId = diskStoreId;
  }

  /**
   * Returns the name or IP address of the host on which the member is
   * running.
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Returns the directory in which the <code>DiskStore</code> is saved.
   */
  public String getDirectory() {
    return this.directory;
  }

  /**
   * Returns the ID of the <code>DiskStore</code>.
   */
  public String getDiskStoreId() {
    return this.diskStoreId;
  }
}
