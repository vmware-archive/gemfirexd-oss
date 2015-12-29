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

import com.gemstone.gemfire.cache.Region;

/**
 * Composite data type used to distribute the results of a disk backup
 * operation.
 * 
 * @author rishim
 * @since 7.0
 */
public class DiskBackupResult {

  /**
   * Returns the name of the directory
   */
  private String diskDirectory;

  /**
   * whether the bacup operation was successful or not
   */
  private boolean offilne;

  @ConstructorProperties( { "diskDirectory", "offilne"
    
  })
  public DiskBackupResult(String diskDirectory, boolean offline) {
    this.diskDirectory = diskDirectory;
    this.offilne = offline;
  }

  /**
   * Returns the name of the directory where the files for this backup
   * were written.
   */
  public String getDiskDirectory() {
    return diskDirectory;
  }

  /**
   * Returns whether the backup was successful.
   * 
   * @return True if the backup was successful, false otherwise.
   */
  public boolean isOffilne() {
    return offilne;
  }
}
