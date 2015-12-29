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

public enum FsAckStatus {

  ACK_HIT(1),
  ACK_ERROR(100),
  ACK_BYPASS(4),
  ACK_UNKNOWN(6),
  ACK_NON_BLOCKING(7),
  ACK_NON_CHECKING(8),
  ACK_NO_HIT(10);
  
  int fsAckStatusCode;
  
  FsAckStatus(int fsAckStatusCode) {
    this.fsAckStatusCode = fsAckStatusCode;
  }
  
  /**
   * Given the fsAckStatusCode find the database code used to store it in GemFireXD.
   * @param fsAckStatusCode
   * @return
   */
  public static int getFsAckStatusCode(FsAckStatus fsAckStatus) {
    return fsAckStatus.fsAckStatusCode;
  }
  
  /**
   * Given the fsAckStatusCode find the FsAckStatus.
   * @param fsAckStatusCode
   * @return
   */
  public static FsAckStatus getFsAckStatus(int fsAckStatusCode) {
    FsAckStatus fsAckStatus = null;
    for (FsAckStatus tempFsAckStatus : values()) {
      if (tempFsAckStatus.fsAckStatusCode == fsAckStatusCode) {
        fsAckStatus = tempFsAckStatus;
        break;
      }
    }
    return fsAckStatus;
  }
}
