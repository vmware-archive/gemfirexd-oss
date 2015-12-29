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
package cacheperf.comparisons.gemfirexd.useCase1;

public enum FsOutStatus {

  OUT_FAILED(3) ,
  OUT_ERROR(100), 
  OUT_CANCELLED(9), 
  OUT_UNQUEUE(10), 
  OUT_RECHECK(5), 
  OUT_PENDING(12),
  OUT_PASSED(2); 
  
  int fsOutStatusCode;
  
  FsOutStatus(int fsOutStatusCode) {
    this.fsOutStatusCode = fsOutStatusCode;
  }
  
  /**
   * Given the fsAckStatusCode find the database code used to store it in GemFireXD.
   * @param fsAckStatusCode
   * @return
   */
  public static int getFsOutStatusCode(FsOutStatus fsOutStatus) {
    return fsOutStatus.fsOutStatusCode;
  }
  
  /**
   * Given the fsAckStatusCode find the FsAckStatus.
   * @param fsAckStatusCode
   * @return
   */
  public static FsOutStatus getFsOutStatus(int fsOutStatusCode) {
    FsOutStatus fsOutStatus = null;
    for (FsOutStatus tempFsOutStatus : values()) {
      if (tempFsOutStatus.fsOutStatusCode == fsOutStatusCode) {
        fsOutStatus = tempFsOutStatus;
        break;
      }
    }
    return fsOutStatus;
  }
}
