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

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * OFACMessage POJO to represent the ofac message.
 */
public class OFACMessage implements Comparable<OFACMessage>, Serializable {

  /**
   * the unique id
   */
  private String messageGuId;

  /**
   * The fs message id
   */
  private String fsMessageId;
  
  /**
   * The back office transaction ID.
   */
  private String boTranId;
  /**
   * The channel transaction id.
   */
  private String chnTxnId;
  
  /**
   * The OFAC hit status.
   */
  private FsHitStatus fsHitStatus;
  
  /**
   * The last updated time
   */
  private Timestamp lastUpdatedTime;
  
  /**
   * The no of chunks in the ofac message
   */
  private int noOfChunks;
  
  /**
   * What the data life status.
   */
  private FsDataLifeStatus fsDataLifeStatus;
  
  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(OFACMessage fsChunkedMessage) {
    return this.messageGuId.compareTo(fsChunkedMessage.getMessageGuId());
  }
  
  /**
   * @return the messageGuId
   */
  public String getMessageGuId() {
    return messageGuId;
  }

  /**
   * @param messageGuId the messageGuId to set
   */
  public void setMessageGuId(String messageGuId) {
    this.messageGuId = messageGuId;
  }

  /**
   * @return the messageId
   */
  public String getFsMessageId() {
    return fsMessageId;
  }

  /**
   * @param messageId the messageId to set
   */
  public void setFsMessageId(String fsMessageId) {
    this.fsMessageId = fsMessageId;
  }

  /**
   * @return the boTranId
   */
  public String getBoTranId() {
    return boTranId;
  }

  /**
   * @param boTranId the boTranId to set
   */
  public void setBoTranId(String boTranId) {
    this.boTranId = boTranId;
  }

  /**
   * @return the chnTxnId
   */
  public String getChnTxnId() {
    return chnTxnId;
  }

  /**
   * @param chnTxnId the chnTxnId to set
   */
  public void setChnTxnId(String chnTxnId) {
    this.chnTxnId = chnTxnId;
  }

  /**
   * @return the fsHitStatus
   */
  public FsHitStatus getFsHitStatus() {
    return fsHitStatus;
  }

  /**
   * @param fsHitStatus the fsHitStatus to set
   */
  public void setFsHitStatus(FsHitStatus fsHitStatus) {
    this.fsHitStatus = fsHitStatus;
  }

  /**
   * @return the lastUpdatedTime
   */
  public Timestamp getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  /**
   * @param lastUpdatedTime the lastUpdatedTime to set
   */
  public void setLastUpdatedTime(Timestamp lastUpdatedTime) {
    this.lastUpdatedTime = lastUpdatedTime;
  }

  /**
   * @return the noOfChunks
   */
  public int getNoOfChunks() {
    return noOfChunks;
  }

  /**
   * @param noOfChunks the noOfChunks to set
   */
  public void setNoOfChunks(int noOfChunks) {
    this.noOfChunks = noOfChunks;
  }

  /**
   * @return the fsDataLifeStatus
   */
  public FsDataLifeStatus getFsDataLifeStatus() {
    return fsDataLifeStatus;
  }

  /**
   * @param fsDataLifeStatus the fsDataLifeStatus to set
   */
  public void setFsDataLifeStatus(FsDataLifeStatus fsDataLifeStatus) {
    this.fsDataLifeStatus = fsDataLifeStatus;
  }
}
