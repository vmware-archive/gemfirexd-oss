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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.distributed.DistributedSystem;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

public final class TailLogResponse extends AdminResponse {
  private String tail;
  private String childTail;

  public static TailLogResponse create(DistributionManager dm, InternalDistributedMember recipient){
    TailLogResponse m = new TailLogResponse();
    m.setRecipient(recipient);
    try {
      InternalDistributedSystem sys = dm.getSystem();
      LogWriterI18n logger = sys.getLogWriterI18n();
      if (logger instanceof ManagerLogWriter) {
        ManagerLogWriter mlw = (ManagerLogWriter)logger;
        m.childTail = tailSystemLog(mlw.getChildLogFile());
        m.tail = tailSystemLog(sys.getConfig());
        if (m.tail == null) {
          m.tail = LocalizedStrings.TailLogResponse_NO_LOG_FILE_WAS_SPECIFIED_IN_THE_CONFIGURATION_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT.toLocalizedString(); 
        }
      } else {
        Assert.assertTrue(false, "TailLogRequest/Response processed in application vm with shared logging.");
      }
    } catch (IOException e){
      dm.getLoggerI18n().warning(LocalizedStrings.TailLogResponse_ERROR_OCCURRED_WHILE_READING_SYSTEM_LOG__0, e);
      m.tail = "";
    }
    return m;
  }

  public int getDSFID() {
    return TAIL_LOG_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(tail, out);
    DataSerializer.writeString(childTail, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    tail = DataSerializer.readString(in);
    childTail = DataSerializer.readString(in);
  }

  public String getTail(){
    return tail;
  }

  public String getChildTail() {
    return childTail;
  }

  @Override
  public String toString(){
    return "TailLogResponse from " + this.getRecipient() + " <TAIL>" + getTail() + "</TAIL>";
  }

  public static String tailSystemLog(File logFile) throws IOException {
    if (logFile == null || logFile.equals(new File(""))) {
      return null;
    }
    int numLines = 30;
    int maxBuffer = 65500; //DataOutput.writeUTF will only accept 65535 bytes
    long fileLength = logFile.length();
    byte[] buffer = (fileLength > maxBuffer) ? new byte[maxBuffer] : new byte[(int)fileLength];
    int readSize = buffer.length;
    RandomAccessFile f = new RandomAccessFile(logFile, "r");
    f.seek(fileLength - readSize);
    f.read(buffer, 0, readSize);
    f.close();
    
    String messageString = new String( buffer );
    char[] text = messageString.toCharArray();
    for (int i=text.length-1,j=0; i>=0; i--){
      if (text[i] == '[')
        j++;
      if (j == numLines){
        messageString = messageString.substring(i);
        break;
      }
    }    
    return messageString.trim();
  }

//  private static String readSystemLog(File logFile) throws IOException {
//    if (logFile == null || logFile.equals(new File(""))) {
//      return null;
//    }
//    long fileLength = logFile.length();
//    byte[] buffer = new byte[(int)fileLength];
//    BufferedInputStream in = new BufferedInputStream(new FileInputStream(logFile));
//    in.read(buffer, 0, buffer.length);
//    return new String(buffer).trim();
//  }

//  private static String readSystemLog(DistributionConfig sc) throws IOException {
//    File logFile = sc.getLogFile();
//    if (logFile == null || logFile.equals(new File(""))) {
//      return null;
//    }
//    if (!logFile.isAbsolute()) {
//      logFile = new File(logFile.getAbsolutePath());
//    }    
//    return readSystemLog(logFile);
//  }  
  
  private static String tailSystemLog(DistributionConfig sc) throws IOException {
    File logFile = sc.getLogFile();
    if (logFile == null || logFile.equals(new File(""))) {
      return null;
    }
    if (!logFile.isAbsolute()) {
      logFile = new File(logFile.getAbsolutePath());
    }    
    return tailSystemLog(logFile);
  }

}
