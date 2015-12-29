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
package com.gemstone.gemfire.internal.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class StatMonitorHandler implements SampleHandler {

  static final String ENABLE_MONITOR_THREAD = "gemfire.stats.enableMonitorThread";
  static final boolean enableMonitorThread = Boolean.getBoolean(ENABLE_MONITOR_THREAD);
  
  /** Log writer to write log messages to */
  private final LogWriterI18n log;

  /** The registered monitors */
  private volatile List<StatisticsMonitor> monitors = 
      Collections.<StatisticsMonitor>emptyList();
  
  /** Protected by synchronization on this handler instance */
  private volatile StatMonitorNotifier notifier;

  /** Constructs a new StatMonitorHandler instance */
  public StatMonitorHandler(LogWriterI18n log) {
    this.log = log;
  }
  
  /** Adds a monitor which will be notified of samples */
  public boolean addMonitor(StatisticsMonitor monitor) {
    synchronized (this) {
      boolean added = false;
      List<StatisticsMonitor> oldMonitors = this.monitors;
      if (!oldMonitors.contains(monitor)) {
        List<StatisticsMonitor> newMonitors = new ArrayList<StatisticsMonitor>(oldMonitors);
        added = newMonitors.add(monitor);
        this.monitors = Collections.unmodifiableList(newMonitors);
      }
      if (enableMonitorThread && !this.monitors.isEmpty() && this.notifier == null) {
        this.notifier = new StatMonitorNotifier();
        this.notifier.start();
      }
      return added;
    }
  }

  /** Removes a monitor that will no longer be used */
  public boolean removeMonitor(StatisticsMonitor monitor) {
    synchronized (this) {
      boolean removed = false;
      List<StatisticsMonitor> oldMonitors = this.monitors;
      if (oldMonitors.contains(monitor)) {
        List<StatisticsMonitor> newMonitors = new ArrayList<StatisticsMonitor>(oldMonitors);
        removed = newMonitors.remove(monitor);
        this.monitors = Collections.unmodifiableList(newMonitors);
      }
      if (enableMonitorThread && this.monitors.isEmpty() && this.notifier != null) {
        this.notifier.stop();
        this.notifier = null;
      }
      return removed;
    }
  }
  
  /**
   * Stops the notifier thread if one exists.
   */
  public void close() {
    synchronized (this) {
      if (enableMonitorThread && this.notifier != null) {
        this.notifier.stop();
      }
    }
  }
  
  @Override
  public void sampled(long nanosTimeStamp, List<ResourceInstance> resourceInstances) {
    synchronized (this) {
      if (enableMonitorThread) {
        final StatMonitorNotifier thread = this.notifier;
        if (thread != null) {
          try {
            thread.monitor(new MonitorTask(System.currentTimeMillis(), resourceInstances));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      } else {
        monitor(System.currentTimeMillis(), resourceInstances);
      }
    }
  }
  
  private void monitor(final long sampleTimeMillis, final List<ResourceInstance> resourceInstance) {
    List<StatisticsMonitor> currentMonitors = StatMonitorHandler.this.monitors;
    for (StatisticsMonitor monitor : currentMonitors) {
      try {
        monitor.monitor(sampleTimeMillis, resourceInstance);
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Error e) {
        SystemFailure.checkFailure();
        getLogWriter().warning("StatisticsMonitor " + monitor + " threw " + e.getClass().getSimpleName(), e);
      }
      catch (RuntimeException e) {
        getLogWriter().warning("StatisticsMonitor " + monitor + " threw " + e.getClass().getSimpleName(), e);
      }
    }
  }

  @Override
  public void allocatedResourceType(ResourceType resourceType) {
  }

  @Override
  public void allocatedResourceInstance(ResourceInstance resourceInstance) {
  }

  @Override
  public void destroyedResourceInstance(ResourceInstance resourceInstance) {
  }

  LogWriter getLogWriter() {
    return getLogWriterI18n().convertToLogWriter();
  }
  
  LogWriterI18n getLogWriterI18n() {
    return this.log;
  }
  
  /** For testing only */
  List<StatisticsMonitor> getMonitorsSnapshot() {
    return Collections.unmodifiableList(this.monitors);
  }
  
  /** For testing only */
  StatMonitorNotifier getStatMonitorNotifier() {
    synchronized (this) {
      return this.notifier;
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  class StatMonitorNotifier implements Runnable {
    
    /** True while this notifier's thread is running */
    private volatile boolean alive;
    
    /** Protected by synchronization on this notifier instance */
    private Thread consumer;
    
    /** Protected by synchronization on this notifier instance */
    private boolean waiting;
    
    /** Protected by synchronization on this notifier instance */
    private Thread producer;

    /** Used to hand-off from producer to consumer */
    private final SynchronousQueue<MonitorTask> task = new SynchronousQueue<MonitorTask>();

    StatMonitorNotifier() {
    }
    
    @Override
    public void run() {
      getLogWriter().fine("StatMonitorNotifier is starting " + this);
      try {
        work();
      } finally {
        synchronized (this) {
          this.alive = false;
          if (this.producer != null) {
            this.producer.interrupt();
          }
        }
      }
      getLogWriter().fine("StatMonitorNotifier is stopping " + this);
    }
    
    private void work() {
      boolean working = true;
      while (working) {
        try {
          MonitorTask latestTask = null;
          synchronized (this) {
            working = this.alive;
            if (working) {
              this.waiting = true;
            }
          }
          if (working) {
            try {
              latestTask = this.task.take(); // blocking
            } finally {
              synchronized (this) {
                this.waiting = false;
                working = this.alive;
              }
            }
          }
          if (working && latestTask != null) {
            List<StatisticsMonitor> currentMonitors = StatMonitorHandler.this.monitors;
            for (StatisticsMonitor monitor : currentMonitors) {
              try {
                monitor.monitor(latestTask.getSampleTimeMillis(), 
                                latestTask.getResourceInstances());
              } catch (Throwable t) {
                Error err;
                if (t instanceof Error
                    && SystemFailure.isJVMFailureError(err = (Error)t)) {
                  SystemFailure.initiateFailure(err);
                  // If this ever returns, rethrow the error. We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                }
                // Whenever you catch Error or Throwable, you must also
                // check for fatal JVM error (see above). However, there is
                // _still_ a possibility that you are dealing with a cascading
                // error condition, so you also need to check to see if the JVM
                // is still usable:
                SystemFailure.checkFailure();
                getLogWriter().warning("StatisticsMonitor " + monitor + " threw "
                    + t.getClass().getSimpleName(), t);
              }
            }
          }
        } catch (InterruptedException e) {
          synchronized (this) {
            working = false;
          }
        }
      }
    }
      
    void start() {
      synchronized (this) {
        if (this.consumer == null) {
          this.consumer = new Thread(this, toString());
          this.consumer.setDaemon(true);
          this.alive = true;
          this.consumer.start();
        }
      }
    }

    void stop() {
      synchronized (this) {
        if (this.consumer != null) {
          this.alive = false;
          this.consumer.interrupt();
          this.consumer = null;
        }
      }
    }
    
    void monitor(MonitorTask task) throws InterruptedException {
      boolean isAlive = false;
      synchronized (this) {
        if (this.alive) {
          isAlive = true;
          this.producer = Thread.currentThread();
        }
      }
      if (isAlive) {
        try {
          this.task.put(task);
        } catch (InterruptedException e) {
          // fall through and return
        } finally {
          synchronized (this) {
            this.producer = null;
          }
        }
      }
    }
    
    boolean isWaiting() {
      synchronized (this) {
        return this.waiting;
      }
    }

    boolean isAlive() {
      synchronized (this) {
        return this.alive;
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append(" Thread").append(" #").append(System.identityHashCode(this));
      return sb.toString();
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  static class MonitorTask {
    private final long sampleTimeMillis; 
    private final List<ResourceInstance> resourceInstances;
    
    MonitorTask(long sampleTimeMillis, List<ResourceInstance> resourceInstances) {
      this.sampleTimeMillis = sampleTimeMillis;
      this.resourceInstances = resourceInstances;
    }
    
    long getSampleTimeMillis() {
      return this.sampleTimeMillis;
    }
    
    List<ResourceInstance> getResourceInstances() {
      return this.resourceInstances;
    }
  }
}
