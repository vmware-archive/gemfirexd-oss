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

/**
 * FsMessageSendStatus defines the fircosoft message send status. 
 * Based on different enum values, the client can interpret at which stage the message failed.
 * The message can fail because 
 *    -  it could not be formatted.
 *    -  The message chunker failed to chunk the message
 *    -  The message sender failed sending the chunked messages as the MULE ESB is not functionaing properly. 
 */
public enum FsMessageSendStatus {
  FS_FORMATTER_ERROR(0), FS_CHUNKER_ERROR(1), NOT_YET_SENT(2), FS_MSG_SEND_ERROR(3), FS_MSG_SENT(4);
  
  int fsMessageSendStatusCode;
  
  FsMessageSendStatus(int fsMessageSendStatusCode) {
    this.fsMessageSendStatusCode = fsMessageSendStatusCode;
  }
  
  /**
   * Given the fsMessageSendStatus find the database code used to store it in GemFireXD.
   * @param fsMessageSendStatus
   * @return
   */
  public static int getFsMessageSendStatusCode(FsMessageSendStatus fsMessageSendStatus) {
    return fsMessageSendStatus.fsMessageSendStatusCode;
  }
  
  /**
   * Given the fsMessageSendStatusCode find the FsMessageSendStatus.
   * @param fsMessageSendStatusCode
   * @return
   */
  public static FsMessageSendStatus getFsMessageSendStatus(int fsMessageSendStatusCode) {
    FsMessageSendStatus fsMessageSendStatus = null;
    for (FsMessageSendStatus tempFsMessageSendStatus : values()) {
      if (tempFsMessageSendStatus.fsMessageSendStatusCode == fsMessageSendStatusCode) {
        fsMessageSendStatus = tempFsMessageSendStatus;
        break;
      }
    }
    return fsMessageSendStatus;
  }
}
