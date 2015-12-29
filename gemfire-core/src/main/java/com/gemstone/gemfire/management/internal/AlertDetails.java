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
package com.gemstone.gemfire.management.internal;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.LogWriterImpl;

public class AlertDetails {

  private int alertLevel;

  private String connectionName;
  private String threadName;
  private long tid;
  private String msg;
  private String exceptionText;
  private Date msgDate;
  private final String sourceId;
  private final String message;

  private InternalDistributedMember sender;

  public AlertDetails(int alertLevel, Date msgDate, String connectionName,
      String threadName, long tid, String msg, String exceptionText,
      InternalDistributedMember sender) {

    this.alertLevel = alertLevel;
    this.connectionName = connectionName;
    this.threadName = threadName;
    this.tid = tid;
    this.msg = msg;
    this.exceptionText = exceptionText;
    this.msgDate = msgDate;
    this.sender = sender;

    {
      StringBuffer tmpSourceId = new StringBuffer();

      tmpSourceId.append(threadName);
      if (tmpSourceId.length() > 0) {
        tmpSourceId.append(' ');
      }
      tmpSourceId.append("tid=0x");
      tmpSourceId.append(Long.toHexString(tid));
      this.sourceId = tmpSourceId.toString();
    }
    {
      StringBuffer tmpMessage = new StringBuffer();
      tmpMessage.append(msg);
      if (tmpMessage.length() > 0) {
        tmpMessage.append('\n');
      }
      tmpMessage.append(exceptionText);
      this.message = tmpMessage.toString();
    }
  }

  public int getAlertLevel() {
    return alertLevel;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public String getThreadName() {
    return threadName;
  }

  public long getTid() {
    return tid;
  }

  public String getMsg() {
    return msg;
  }

  public String getExceptionText() {
    return exceptionText;
  }

  public Date getMsgTime() {
    return msgDate;
  }

  /**
   * Returns the sender of this message. Note that this value is not set until
   * this message is received by a distribution manager.
   */
  public InternalDistributedMember getSender() {
    return this.sender;
  }

  public String toString() {
    final SimpleDateFormat timeFormatter = new SimpleDateFormat(
        LogWriterImpl.FORMAT);
    java.io.StringWriter sw = new java.io.StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.print('[');
    pw.print(LogWriterImpl.levelToString(alertLevel));
    pw.print(' ');
    pw.print(timeFormatter.format(msgDate));
    pw.print(' ');
    pw.print(connectionName);
    pw.print(' ');
    pw.print(sourceId);
    pw.print("] ");
    pw.print(message);

    pw.close();
    try {
      sw.close();
    } catch (java.io.IOException ignore) {
    }
    return sw.toString();
  }

}
