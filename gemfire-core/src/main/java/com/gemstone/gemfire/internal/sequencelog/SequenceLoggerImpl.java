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
package com.gemstone.gemfire.internal.sequencelog;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.sequencelog.io.OutputStreamAppender;

/**
 * @author dsmith
 *
 */
public class SequenceLoggerImpl implements SequenceLogger {

  private static final SequenceLogger INSTANCE;
  
  public static final String ENABLED_TYPES_PROPERTY = "gemfire.GraphLoggerImpl.ENABLED_TYPES";
  
  private final EnumSet<GraphType> enabledTypes;
  
  static {
    SequenceLoggerImpl logger = new SequenceLoggerImpl();
    logger.start();
    INSTANCE = logger;
  }
  
  //TODO - this might be too much synchronization for recording all region
  //operations. Maybe we should use a ConcurrentLinkedQueue instead?
  private final LinkedBlockingQueue<Transition> edges = new LinkedBlockingQueue<Transition>();
  
  private volatile OutputStreamAppender appender;

  private ConsumerThread consumerThread;
  
  public static SequenceLogger getInstance() {
    return INSTANCE;
  }

  public boolean isEnabled(GraphType type) {
    return enabledTypes.contains(type);
  }

  public void logTransition(GraphType type, Object graphName, Object edgeName,
      Object state, Object source, Object dest) {
    if(isEnabled(type)) {
      Transition edge = new Transition(type, graphName, edgeName, state, source, dest);
      edges.add(edge);
    }
  }
  
  public void flush() throws InterruptedException {
    FlushToken token = new FlushToken();
    edges.add(token);
    token.cdl.await();
  }

  private SequenceLoggerImpl() {
    String enabledTypesString = System.getProperty(ENABLED_TYPES_PROPERTY, "");
    this.enabledTypes = GraphType.parse(enabledTypesString);
    if(!enabledTypes.isEmpty()) {
      try {
        String name = "states" + OSProcess.getId() + ".graph";
        appender = new OutputStreamAppender(new File(name));
      } catch (FileNotFoundException e) {
      }
    }
  }
  
  private void start() {
    consumerThread = new ConsumerThread();
    consumerThread.start();
  }

  private class ConsumerThread extends Thread {
    
    public ConsumerThread() {
      super("State Logger Consumer Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      Transition edge;
      while(true) {
        try {
          edge = edges.take();
          if(edge instanceof FlushToken) {
            ((FlushToken) edge).cdl.countDown();
            continue;
          }

          if(appender != null) {
            appender.write(edge);
          }
        } catch (InterruptedException e) {
          //do nothing
        } catch (Throwable t) {
          t.printStackTrace();
          appender.close();
          appender = null;
        }
      }
    }
  }
  
  private static class FlushToken extends Transition {
    CountDownLatch cdl = new CountDownLatch(1);
    
    public FlushToken() {
      super(null, null, null, null, null, null);
    }
  }
}
