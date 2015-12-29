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
 * FsChunkedMessage POJO to represent the chunked message.
 */
public class FsChunkedMessage implements Comparable<FsChunkedMessage>, Serializable {

  /**
   * The generated chunkId. 
   */
  private String chunkId;
  
  /**
   * Generated messageId for each chunk   
   */
  private String messageId;

  /**
   * The back office transaction ID.
   */
  private String boTranId;
  /**
   * The channel transaction id.
   */
  private String chnTxnId;
  
  /**
   * The chunk sequence
   */
  private int chunkSequence;
  
  /**
   * The chunked messages. stored in the CHUNKED_MESSAGE field.
   */
  private byte[] chunkedMessage;
  
  /**
   * The OFAC out status.
   */
  private FsOutStatus fsOutStatus;
  
  /**
   * The OFAC ack status.
   */
  private FsAckStatus fsAckStatus;
  
  /**
   * The date and time when the message was sent. 
   */
  private Timestamp sentDate;
  
  /**
   * The date and time when the message reply was received.
   */
  private Timestamp receivedDate;
  
  /**
   * Comment provided by the OFAC sanction screening. 
   */
  private String ofacComment;
  
  /**
   * For Matched, Unmatched, Multi-matched messages the ofac message id.
   * For orphan messages the chnTxnId.
   */
  private String fsMessageId;
  
  /**
   * What the message send status.
   */
  private FsMessageSendStatus fsMessageSendStatus;
  
  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(FsChunkedMessage fsChunkedMessage) {
    return this.chunkSequence - fsChunkedMessage.getChunkSequence();
  }

  /**
   * @return the chunkedMessage
   */
  public byte[] getChunkedMessage() {
    return chunkedMessage;
  }

  /**
   * @param chunkedMessage the chunkedMessage to set
   */
  public void setChunkedMessage(byte[] chunkedMessage) {
    this.chunkedMessage = chunkedMessage.clone();
  }


  /**
   * @return the chunkSequence
   */
  public int getChunkSequence() {
    return chunkSequence;
  }

  /**
   * @param chunkSequence the chunkSequence to set
   */
  public void setChunkSequence(int chunkSequence) {
    this.chunkSequence = chunkSequence;
  }

  /**
   * @return the v
   */
  public String getChunkId() {
    return chunkId;
  }

  /**
   * @param chunkId the chunkId to set
   */
  public void setChunkId(String chunkId) {
    this.chunkId = chunkId;
  }

  /**
   * @return the messageId
   */
  public String getMessageId() {
    return messageId;
  }

  /**
   * @param messageId the messageId to set
   */
  public void setMessageId(String messageId) {
    this.messageId = messageId;
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
   * @return the sentDate
   */
  public Timestamp getSentDate() {
    return sentDate;
  }

  /**
   * @param sentDate the sentDate to set
   */
  public void setSentDate(Timestamp sentDate) {
    this.sentDate = sentDate;
  }

  /**
   * @return the receivedDate
   */
  public Timestamp getReceivedDate() {
    return receivedDate;
  }

  /**
   * @param receivedDate the receivedDate to set
   */
  public void setReceivedDate(Timestamp receivedDate) {
    this.receivedDate = receivedDate;
  }

  /**
   * @return the ofacComment
   */
  public String getOfacComment() {
    return ofacComment;
  }

  /**
   * @param ofacComment the ofacComment to set
   */
  public void setOfacComment(String ofacComment) {
    this.ofacComment = ofacComment;
  }

  /**
   * @return the fsMessageId
   */
  public String getFsMessageId() {
    return fsMessageId;
  }

  /**
   * @param fsMessageId the fsMessageId to set
   */
  public void setFsMessageId(String fsMessageId) {
    this.fsMessageId = fsMessageId;
  }

  /**
   * @return the fsMessageSendStatus
   */
  public FsMessageSendStatus getFsMessageSendStatus() {
    return fsMessageSendStatus;
  }

  /**
   * @param fsMessageSendStatus the fsMessageSendStatus to set
   */
  public void setFsMessageSendStatus(FsMessageSendStatus fsMessageSendStatus) {
    this.fsMessageSendStatus = fsMessageSendStatus;
  }

  /**
   * @return the fsOutStatus
   */
  public FsOutStatus getFsOutStatus() {
    return fsOutStatus;
  }

  /**
   * @param fsOutStatus the fsOutStatus to set
   */
  public void setFsOutStatus(FsOutStatus fsOutStatus) {
    this.fsOutStatus = fsOutStatus;
  }

  /**
   * @return the fsAckStatus
   */
  public FsAckStatus getFsAckStatus() {
    return fsAckStatus;
  }

  /**
   * @param fsAckStatus the fsAckStatus to set
   */
  public void setFsAckStatus(FsAckStatus fsAckStatus) {
    this.fsAckStatus = fsAckStatus;
  }
}
