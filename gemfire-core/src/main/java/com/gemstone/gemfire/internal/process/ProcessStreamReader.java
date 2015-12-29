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
package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;

/**
 * Reads the output stream of a Process.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class ProcessStreamReader implements Runnable {
 
  private final LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);

  private final InputStream inputStream;
  private final InputListener listener;

  private Thread thread;

  public ProcessStreamReader(final InputStream inputStream) {
    this.inputStream = inputStream;
    this.listener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        // do nothing
      }
      @Override
      public String toString() {
        return "NullInputListener";
      }
    };
  }

  public ProcessStreamReader(final InputStream inputStream, final InputListener listener) {
    this.inputStream = inputStream;
    this.listener = listener;
  }

  @Override
  public void run() {
    this.logWriter.fine("Running " + this);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while ((line = reader.readLine()) != null) {
        this.listener.notifyInputLine(line);
      }
    } catch (IOException e) {
      this.logWriter.fine("Failure reading from buffered input stream: " + e.getMessage(), e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        this.logWriter.fine("Failure closing buffered input stream reader: " + e.getMessage(), e);
      }
      this.logWriter.fine("Terminating " + this);
    }
  }

  public ProcessStreamReader start() {
    synchronized (this) {
      if (this.thread == null) {
        this.thread = new Thread(this, createThreadName());
        this.thread.start();
      } else if (this.thread.isAlive()){
        throw new IllegalStateException(this + " has already started");
      } else {
        throw new IllegalStateException(this + " was stopped and cannot be restarted");
      }
    }
    return this;
  }

  public ProcessStreamReader stop() {
    synchronized (this) {
      if (this.thread != null && this.thread.isAlive()) {
        this.thread.interrupt();
      } else if (this.thread != null){
        this.logWriter.fine(this + " has already been stopped");
      } else {
        this.logWriter.fine(this + " has not been started");
      }
    }
    return this;
  }
  
  public boolean isRunning() {
    synchronized (this) {
      if (this.thread != null) {
        return this.thread.isAlive();
      }
    }
    return false;
  }
  
  public void join() throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join();
    }
  }
  
  public void join(final long millis) throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join(millis);
    }
  }
  
  public void join(final long millis, final int nanos) throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join(millis, nanos);
    }
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append(" Thread").append(" #").append(System.identityHashCode(this));
    sb.append(" alive=").append(isRunning()); //this.thread == null ? false : this.thread.isAlive());
    sb.append(" listener=").append(this.listener);
    return sb.toString();
  }
  
  private String createThreadName() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
  }
  
  /**
   * Defines the callback for  lines of output found in the stream.
   */
  public static interface InputListener {
    public void notifyInputLine(String line);
  }
}
