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
package batterytest.greplogs;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.List;
import java.util.regex.Pattern;

import com.gemstone.gemfire.InternalGemFireError;

/**
 * An output stream that writes data to an underlying output stream, while
 * also grepping for suspect strings.
 * 
 * Callers can find what suspect strings were detected using {@link #getAndClearSuspects()}
 * 
 * @author dsmith
 *
 */
public class SuspectGrepOutputStream extends FilterOutputStream {
  private final LogConsumer suspectGrepper;
  private final String lineSeparator = System.getProperty("line.separator");
  private final Pattern newLinePattern = Pattern.compile(lineSeparator);
  CharBuffer charBuf = CharBuffer.allocate(8192);
  private final CharsetDecoder decoder;
  private StringBuilder suspectStrings;
  private StringBuilder currentLine;
  private ByteBuffer leftover;

  public SuspectGrepOutputStream(OutputStream out, String fileName, int repeatLimit, String type, Charset charset) {
    super(out);
    boolean skipLogMsgs = ExpectedStrings.skipLogMsgs(type);
    List testExpectStrs = ExpectedStrings.create(type);
    suspectGrepper = new LogConsumer(skipLogMsgs, testExpectStrs, fileName, repeatLimit);
    decoder = charset.newDecoder();
  }

  /**
   * Grep for suspect strings in a whole line of the input 
   */
  private void consumeLn(CharSequence x) {
    StringBuilder suspect = suspectGrepper.consume(x);
    if(suspect != null) {
      if(suspectStrings == null) {
        suspectStrings = new StringBuilder();
      }
      suspectStrings.append(suspect);
      
      suspectStrings.append("\n\toccurred at ").append(getStackTrace(new Exception()));
    }
  }
  
  
  
  
  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(int b) throws IOException {
    super.write(b);
    consumeBytes(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    //We can't call super.write here, because that will just
    //write each individual byte to the stream.
    out.write(buf, off, len);
    consumeBytes(buf, off, len);
  }

  /**
   * Grep for suspect strings in a sequence of bytes.
   * 
   */
  private void consumeBytes(byte[] buf, int off, int len) {
    charBuf.clear();
    
    ByteBuffer bytesIn;
    
    if(leftover != null) {
      //We may have leftover bytes from the previous bytes that 
      //could not be decoded into characters. Build a new byte
      //buffer that includes both the leftovers and the new bytes
      bytesIn = ByteBuffer.allocate(leftover.capacity() + len);
   
      
      bytesIn.put(leftover);
      bytesIn.put(buf, off, len);
      leftover = null;
    } else {
      //Construct a byte buffer just from the new bytes
      bytesIn = ByteBuffer.wrap(buf, off, len);
    }
    
    //The decoder returns either overflow, underflow, or error.
    //Overflow means there are more chars to decode, so we will loop
    //until there are no more chars to decode.
    CoderResult result = CoderResult.OVERFLOW;
    while(result == CoderResult.OVERFLOW) {
      //Decode characters from bytesIn into charBuf
      result = decoder.decode(bytesIn, charBuf, false);
      if(result.isError()) {
        throw new InternalGemFireError("Unable to decode system out to check for suspect strings: " + result);
      }

      //Consume the characters that we read
      charBuf.flip();
      consumeString(charBuf.slice());

      //If all the possible characters are decoded from this
      //byte array, we're done. Save any leftover bytes.
      if(result.isUnderflow()) {
        if(bytesIn.remaining() > 0) {
          leftover = bytesIn.slice();
        }

      }
    }
    
  }

  /**
   * Consume a string printed to this print writer. The string may contain line breaks,
   * or no line breaks at all. This method looks for line breaks and passes whole lines
   * to the consumeLn method.
   */
  private void consumeString(CharSequence sequence) {

    //See if the sequence ends with a line feed
    boolean endsWithLF = endsWithLF(sequence);
    
    //Split the sequence into lines
    String[] lines = newLinePattern.split(sequence);
    
    
    for(int i =0; i < lines.length; i++) {
      if(i == lines.length - 1 && !endsWithLF) {
        //If this is the last line, and the total sequence did
        //not end with a line feed, we need to save results 
        if(currentLine == null) {
          currentLine = new StringBuilder();
        }
        currentLine.append(lines[i]);
      } else if(currentLine != null) {
        //If we have previously decoded
        //characters that are part of this line, append these chars
        //and consume the whole line
        currentLine.append(lines[i]);
        consumeLn(currentLine);
        currentLine = null;
      } else {
        //if this is a whole line, consume it.
        consumeLn(lines[i]);
      }
    }
  }

  private boolean endsWithLF(CharSequence sequence) {
    if(sequence.length() < lineSeparator.length()) {
      return false;
    }
    for(int i = 1; i <= lineSeparator.length(); i++) {
      char lsChar = lineSeparator.charAt(lineSeparator.length() - i);
      char seqChar = sequence.charAt(sequence.length() - i);
      if(lsChar != seqChar) {
        return false;
      }
    }
    return true;
  }

  public String getAndClearSuspects() {
    if(suspectStrings == null) {
      return null;
    } 
    String result = suspectStrings.toString();
    suspectStrings = null;
    return result;
  }
  

  public static String getStackTrace(Throwable x) {
    if(x == null)
      return null;
    else {
      ByteArrayOutputStream bout=new ByteArrayOutputStream();
      PrintStream writer=new PrintStream(bout);
      x.printStackTrace(writer);
      String result=new String(bout.toByteArray());
      return result;
    }
  }

}
