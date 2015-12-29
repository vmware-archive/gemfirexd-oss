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
package com.pivotal.gemfirexd.internal.iapi.tools.i18n;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.gemstone.gemfire.internal.shared.NativeCalls;

/**
 * This helps in redirecting an output to a file and then get piped with 'less'
 * unix shell utility.
 * 
 * TODO:SB: It can be made more generic to redirect to any stream.
 * 
 * @author soubhikc
 * 
 */
public class RedirectedLocalizedOutput extends LocalizedOutput {
  private final File file;
  
  @SuppressWarnings("unused")
  private static final class lesspWaiter extends Thread {
    private Process lessp ;
    private int exitStatus;
    private volatile boolean isWaiting = false;
    
    public lesspWaiter(Process p, String name) {
      super(name);
      this.lessp = p;
    }
    
    public void run() {
      try {
        isWaiting = true;
        exitStatus = this.lessp.waitFor();
        if ( exitStatus != 0)
          System.err.println("SB: ERROR: " + exitStatus);
      } catch (InterruptedException e) {
        e.printStackTrace(System.err);
      }
      finally {
        isWaiting = false;
      }
    }

    public boolean isDone() {
      return !isWaiting;
    }
    
  }

  public static RedirectedLocalizedOutput getNewInstance() {
    
    if (!NativeCalls.getInstance().isTTY()) {
      return null; // indicating we cannot redirect to less.
    }
    
    File f = new File(".plan");
    try {
      f.createNewFile();
      f.deleteOnExit();
      return new RedirectedLocalizedOutput(f);
    } catch (IOException e) {
      System.err.println("Error: ");
      e.printStackTrace(System.err);
    }
    return null;
  }
  
  private RedirectedLocalizedOutput(File f) throws IOException {
    super(new FileOutputStream(f));
    this.file = f;
  }

  public int waitForCompletion() {
    
    File workingDir = this.file.getParentFile();
    
    try {
      Process p = Runtime.getRuntime().exec(
          new String[] { "sh", "-c",
              "LESSOPEN=\"|color %s\" less -SR " + this.file.getName() + " < /dev/tty > /dev/tty " },
          null, workingDir);
      int exitStatus = p.waitFor();
      if (exitStatus != 0) {
        System.err.println("ERROR opening less... " + exitStatus);
      }
      
      return exitStatus;
    } catch (IOException e) {
      e.printStackTrace(System.err);
    } catch (InterruptedException e) {
      e.printStackTrace(System.err);
    } finally {
      this.file.delete();
    }
    return -1;
    
//    try {
//      do {
//        Thread.sleep(100);
//      } while (!this.waitOn.isDone());
//    } catch (InterruptedException e) {
//      e.printStackTrace(System.err);
//      this.waitOn.interrupt();
//      return;
//    } finally {
//      //this.file.delete();
//    }
  }
}
