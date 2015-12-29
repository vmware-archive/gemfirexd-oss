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

import cacheperf.CachePerfException;
import java.io.UnsupportedEncodingException;

public class BORawData {

  /**
   * The unique transaction ID.
   */
  private String boTxnId;

  /**
   * Message ID.
   */
  private String msgId;

  /**
   * Raw data/message from queue.
   */
  private byte[] rawData;
  
  /**
   * Incoming queue name of the raw message
   */
  private String incomingQueueName;

  /**
   * @return the boTxnId
   */
  public String getBoTxnId() {
    return boTxnId;
  }

  /**
   * @param boTxnId the boTxnId to set
   */
  public void setBoTxnId(String boTxnId) {
    this.boTxnId = boTxnId;
  }

  /**
   * @return the msgId
   */
  public String getMsgId() {
    return msgId;
  }

  /**
   * @param msgId the msgId to set
   */
  public void setMsgId(String msgId) {
    this.msgId = msgId;
  }

  /**
   * @return the rawData
   */
  public byte[] getRawData() {
    return rawData;
  }


  public String getRawDataString() {
    try {
      return new String (rawData,"UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new CachePerfException("Bad encoding for boTxnId " + boTxnId, e);
    }
  }

  public void setRawData(byte[] rawData) {
    this.rawData = rawData.clone();
  }

  public void setRawData(String rawDataString)
  throws UnsupportedEncodingException {
    if (rawDataString != null)  {
      try {
        this.rawData = rawDataString.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new CachePerfException("Bad encoding for boTxnId " + boTxnId, e);
      }
    }
  }

  /**
   * @return the incomingQueueName
   */
  public String getIncomingQueueName() {
    return incomingQueueName;
  }

  /**
   * @param incomingQueueName the incomingQueueName to set
   */
  public void setIncomingQueueName(String incomingQueueName) {
    this.incomingQueueName = incomingQueueName;
  }
}
