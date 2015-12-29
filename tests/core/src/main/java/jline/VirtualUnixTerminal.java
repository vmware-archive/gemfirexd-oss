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
 * Extension of UnixTerminal for background test runs.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. licensed under
 * the same license as the original file. All rights reserved.
 */
package jline;

import java.io.IOException;
import java.io.InputStream;

import management.util.HydraUtil;

public class VirtualUnixTerminal extends UnixTerminal {

   public VirtualUnixTerminal() {
   }

   public void initializeTerminal() throws IOException, InterruptedException {

	   HydraUtil.logInfo("Using virtual UnixTerminal");
       // save the initial tty configuration
       /*ttyConfig = stty("-g");

       // sanity check
       if ((ttyConfig.length() == 0)
               || ((ttyConfig.indexOf("=") == -1)
                      && (ttyConfig.indexOf(":") == -1))) {
           throw new IOException("Unrecognized stty code: " + ttyConfig);
       }

       checkBackspace();

       // set the console to be character-buffered instead of line-buffered
       stty("-icanon min 1");

       // disable character echoing
       stty("-echo");
       echoEnabled = false;

       // at exit, restore the original tty configuration (for JDK 1.3+)
       try {
           Runtime.getRuntime().addShutdownHook(new Thread() {
                   public void start() {
                       try {
                           restoreTerminal();
                       } catch (Exception e) {
                           consumeException(e);
                       }
                   }
               });
       } catch (AbstractMethodError ame) {
           // JDK 1.3+ only method. Bummer.
           consumeException(ame);
       }*/
   }

   /** 
    * Restore the original terminal configuration, which can be used when
    * shutting down the console reader. The ConsoleReader cannot be
    * used after calling this method.
    */
   public void restoreTerminal() throws Exception {
       /*if (ttyConfig != null) {
           stty(ttyConfig);
           ttyConfig = null;
       }*/
       resetTerminal();
   }

   
   
   public int readVirtualKey(InputStream in) throws IOException {
       int c = readCharacter(in);

       /*if (backspaceDeleteSwitched)
           if (c == DELETE)
               c = BACKSPACE;
           else if (c == BACKSPACE)
               c = DELETE;*/

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
    	   /*
         // handle unicode characters longer than 2 bytes,
         // thanks to Marc.Herbert@continuent.com
           replayStream.setInput(c, in);
//           replayReader = new InputStreamReader(replayStream, encoding);
           c = replayReader.read();
    	   //fork to unixTerminal
           c = super.readCharacter(in);*/
           
           replayStream.setInput(c, in);
//         replayReader = new InputStreamReader(replayStream, encoding);
           c = replayReader.read();
           
       }
       return c;
   } 
   
   public boolean isANSISupported() {
     return !Boolean.getBoolean("gfsh.disable.color");
   }
}
