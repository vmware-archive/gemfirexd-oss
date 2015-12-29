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
 * FsDataLifeStatus
 * 
 * Possible list of data life statuses are,
 * 0 - Back office instruction is loaded into Securitas, the initial status of the instruction
 * 1 - Back office is marked as unmatched after matching is done and no matched channel data is found (Unmatched)
 * 2 - Single matched after matching is done and the back office instruction matches one channel data (Single matched)
 * 3 - Multi-matched after matching is done and multiple channel data are matched with it (Multi-matched)
 * 4 - Sent for sanction screening
 * 5 - Get sanction screening response from Fircosoft
 * 6 - Send the sanction screening result to back office and OFAC is done
 */
public enum FsDataLifeStatus {
  BO_INST_LOADED(0),UNMATCHED(1),MATCHED(2), MULTI_MATCHED(3), SENT_SS(4), GOT_SS_RESPONSE(5), SENT_SS_TO_BO_AND_OFAC(6);
  
  int fsDataLifeStatusCode;
  FsDataLifeStatus(int fsDataLifeStatus) {
    this.fsDataLifeStatusCode = fsDataLifeStatus;
  }
  
  public static int getFsDataLifeStatus(FsDataLifeStatus fsDataLifeStatus) {
    return fsDataLifeStatus.fsDataLifeStatusCode;
  }
  
  public static FsDataLifeStatus getFsDataLifeStatus(int fsDataLifeStatusCode) {
    FsDataLifeStatus fsDataLifeStatus = null;
    for (FsDataLifeStatus tempFsDataLifeStatus : values()) {
      if (tempFsDataLifeStatus.fsDataLifeStatusCode == fsDataLifeStatusCode) {
        fsDataLifeStatus = tempFsDataLifeStatus;
      }
    }
    return fsDataLifeStatus;
  }

  public int getValue() {
    return fsDataLifeStatusCode;
  }
  
  @Override
  public String toString(){
    return Integer.toString(fsDataLifeStatusCode);
  }
}
