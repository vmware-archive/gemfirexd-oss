/*
 * Copyright (c) 2002-2006, Marc Prud'hommeaux <mwp1@cornell.edu>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with
 * the distribution.
 *
 * Neither the name of JLine nor the names of its contributors
 * may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
 * OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * This is a re-write of UnixTerminal with stty process spawn
 * removed for Windows.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. licensed under
 * the same license as the original file. All rights reserved.
 */
package com.gemstone.gemfire.management.internal.cli.shell.jline;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import jline.UnixTerminal;


/**
 * This is re-write of UnixTerminal with stty process spawn removed.
 * There is no process named stty in windows (non-cygwin process) so
 * that part is commented, also since erase is already applied within
 * gfsh script when running under cygwin backspaceDeleteSwitched is
 * hard-coded as true
 * 
 * To know exact changes please see UnixTerminal code.
 * 
 * @author tushark
 *
 */
public class CygwinMinttyTerminal extends UnixTerminal {
  
  
  String encoding = System.getProperty("input.encoding", "UTF-8");
  ReplayPrefixOneCharInputStream replayStream = new ReplayPrefixOneCharInputStream(encoding);
  InputStreamReader replayReader;

  public CygwinMinttyTerminal() {
      try {
          replayReader = new InputStreamReader(replayStream, encoding);
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
  }

  @Override
  public void initializeTerminal() throws IOException, InterruptedException {

  }

  @Override
  public void restoreTerminal() throws Exception {
    resetTerminal();
  }

  @Override
  public int readVirtualKey(InputStream in) throws IOException {
    int c = readCharacter(in);

    //if (backspaceDeleteSwitched)
        if (c == DELETE)
            c = BACKSPACE;
        else if (c == BACKSPACE)
            c = DELETE;

    // in Unix terminals, arrow keys are represented by
    // a sequence of 3 characters. E.g., the up arrow
    // key yields 27, 91, 68
    if (c == ARROW_START && in.available() > 0) {
        // Escape key is also 27, so we use InputStream.available()
        // to distinguish those. If 27 represents an arrow, there
        // should be two more chars immediately available.
        while (c == ARROW_START) {
            c = readCharacter(in);
        }
        if (c == ARROW_PREFIX || c == O_PREFIX) {
            c = readCharacter(in);
            if (c == ARROW_UP) {
                return CTRL_P;
            } else if (c == ARROW_DOWN) {
                return CTRL_N;
            } else if (c == ARROW_LEFT) {
                return CTRL_B;
            } else if (c == ARROW_RIGHT) {
                return CTRL_F;
            } else if (c == HOME_CODE) {
                return CTRL_A;
            } else if (c == END_CODE) {
                return CTRL_E;
            } else if (c == DEL_THIRD) {
                c = readCharacter(in); // read 4th
                return DELETE;
            }
        } 
    } 
    // handle unicode characters, thanks for a patch from amyi@inf.ed.ac.uk
    if (c > 128) {      
      // handle unicode characters longer than 2 bytes,
      // thanks to Marc.Herbert@continuent.com        
        replayStream.setInput(c, in);
//      replayReader = new InputStreamReader(replayStream, encoding);
        c = replayReader.read();
        
    }
    return c;
  }
  
  /**
   * This is awkward and inefficient, but probably the minimal way to add
   * UTF-8 support to JLine
   *
   * @author <a href="mailto:Marc.Herbert@continuent.com">Marc Herbert</a>
   */
  static class ReplayPrefixOneCharInputStream extends InputStream {
      byte firstByte;
      int byteLength;
      InputStream wrappedStream;
      int byteRead;

      final String encoding;
      
      public ReplayPrefixOneCharInputStream(String encoding) {
          this.encoding = encoding;
      }
      
      public void setInput(int recorded, InputStream wrapped) throws IOException {
          this.byteRead = 0;
          this.firstByte = (byte) recorded;
          this.wrappedStream = wrapped;

          byteLength = 1;
          if (encoding.equalsIgnoreCase("UTF-8"))
              setInputUTF8(recorded, wrapped);
          else if (encoding.equalsIgnoreCase("UTF-16"))
              byteLength = 2;
          else if (encoding.equalsIgnoreCase("UTF-32"))
              byteLength = 4;
      }
          
          
      public void setInputUTF8(int recorded, InputStream wrapped) throws IOException {
          // 110yyyyy 10zzzzzz
          if ((firstByte & (byte) 0xE0) == (byte) 0xC0)
              this.byteLength = 2;
          // 1110xxxx 10yyyyyy 10zzzzzz
          else if ((firstByte & (byte) 0xF0) == (byte) 0xE0)
              this.byteLength = 3;
          // 11110www 10xxxxxx 10yyyyyy 10zzzzzz
          else if ((firstByte & (byte) 0xF8) == (byte) 0xF0)
              this.byteLength = 4;
          else
              throw new IOException("invalid UTF-8 first byte: " + firstByte);
      }

      public int read() throws IOException {
          if (available() == 0)
              return -1;

          byteRead++;

          if (byteRead == 1)
              return firstByte;

          return wrappedStream.read();
      }

      /**
      * InputStreamReader is greedy and will try to read bytes in advance. We
      * do NOT want this to happen since we use a temporary/"losing bytes"
      * InputStreamReader above, that's why we hide the real
      * wrappedStream.available() here.
      */
      public int available() {
          return byteLength - byteRead;
      }
  }

}
