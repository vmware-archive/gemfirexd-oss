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

package com.pivotal.gemfirexd.internal.impl.services.stream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.GFToSlf4jBridge;
import com.gemstone.gemfire.internal.PureLogWriter;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.ExceptionSeverity;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleFactory;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.services.stream.HeaderPrintWriter;
import com.pivotal.gemfirexd.internal.iapi.services.stream.PrintWriterGetHeader;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.GfxdHeaderPrintWriter;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * An GemFireXD specific extension to {@link BasicHeaderPrintWriter} to enable
 * wrapping the GemFire {@link LogWriter} after store boot is complete or when
 * {@link GfxdConstants#GFXD_LOG_FILE} has been specified.
 * 
 * @author swale
 */
public final class GfxdHeaderPrintWriterImpl extends BasicHeaderPrintWriter
    implements GfxdHeaderPrintWriter {

  /** The wrapped GemFire {@link LogWriter}, if any. */
  private LogWriter logger;

  /**
   * Skip println() when using {@link #logger} since it was added previously by
   * {@link LogWriter} methods.
   */
  private volatile boolean skipPrintLnForLogger;

  /** The wrapped GemFire {@link PureLogWriter}, if any. */
  private PureLogWriter pureLogger;

  private boolean usingSLF4JBridge;
  /**
   * This constructor sets up the HeaderPrintWriter.
   * 
   * @param writeTo
   *          Where to write to.
   * @param headerGetter
   *          Object to get headers for output lines.
   * @param canClose
   *          If true, {@link #complete} will also close writeTo
   * @param streamName
   *          Name of writeTo, e.g. a file name
   * 
   * @see PrintWriterGetHeader
   */
  public GfxdHeaderPrintWriterImpl(OutputStream writeTo, String fileName,
      PrintWriterGetHeader headerGetter, boolean canClose, String streamName) {
    super(writeTo, headerGetter, canClose, streamName);
    // If the property is set as slf4, use slf4 bridge. 
    if (PropertyUtil.getSystemBoolean(
        InternalDistributedSystem.ENABLE_SLF4J_LOG_BRIDGE, true)) {
      this.usingSLF4JBridge = true;
      this.logger = new GFToSlf4jBridge("SNAPPY", fileName);
    }
  }

  /**
   * @see HeaderPrintWriter#printlnWithHeader(String)
   */
  @Override
  public void printlnWithHeader(String message) {
    put(SanityManager.DEBUG_LEVEL_STRING, message, null);
  }

  /**
   * @see PrintWriter#write(char[], int, int)
   */
  @Override
  public void write(char[] buf, int off, int len) {
    final LogWriter log;
    if ((log = this.logger) != null) {
      log.info(new String(buf, off, len));
      this.skipPrintLnForLogger = true;
    }
    else {
      moveToEndOfStream();
      super.write(buf, off, len);
    }
  }

  /**
   * @see PrintWriter#write(String, int, int)
   */
  @Override
  public void write(String s, int off, int len) {
    final LogWriter log;
    if ((log = this.logger) != null) {
      log.info(s.substring(off, off + len));
      this.skipPrintLnForLogger = true;
    }
    else {
      moveToEndOfStream();
      super.write(s, off, len);
    }
  }

  /**
   * @see PrintWriter#println()
   */
  @Override
  public void println() {
    final LogWriter log;
    if ((log = this.logger) != null) {
      if (!this.skipPrintLnForLogger) {
        log.info(SanityManager.lineSeparator);
      }
      else {
        this.skipPrintLnForLogger = false;
      }
    }
    else {
      moveToEndOfStream();
      super.println();
    }
  }

  /**
   * @see PureLogWriter#put(int, String, Throwable)
   */
  @Override
  public void put(String flag, String message, Throwable t) {
    final PureLogWriter pureLog;
    final LogWriter log;
    final GFToSlf4jBridge slf4logger;
    if ((pureLog = this.pureLogger) != null) {
      int level = -1;
      final int defaultLevel = PureLogWriter.INFO_LEVEL;
      String levelName = PureLogWriter.levelToString(defaultLevel);
      int colonIndex;
      if (flag != null && ((colonIndex = flag.indexOf(':')) > 0)) {
        levelName = flag.substring(0, colonIndex);
        try {
          flag = flag.substring(colonIndex + 1);
          if (!levelName.equalsIgnoreCase("dump")) {
            level = PureLogWriter.levelNameToCode(levelName);
          }
          else {
            level = PureLogWriter.NONE_LEVEL;
          }
        } catch (Exception ex) {
          // ignored
        }
      }
      boolean doLog;
      if (level == -1) {
        if (pureLog.getLevel() > defaultLevel) {
          levelName = SanityManager.DEBUG_LEVEL_STRING;
        }
        doLog = true;
      }
      else {
        doLog = (pureLog.getLevel() <= level);
      }
      if (doLog) {
        final String formattedMessage = SanityManager
            .formatMessage(levelName, flag, message, t)
            .append(SanityManager.lineSeparator).toString();
        pureLog.writeFormattedMessage(formattedMessage);
      }
    } else if (this.usingSLF4JBridge &&
        (slf4logger = (GFToSlf4jBridge)this.logger) != null) {
      // GFToSlf4jBridge is found when logs should go to spark/hadoop logs
      int level = PureLogWriter.INFO_LEVEL;
      int colonIndex;
      if (flag != null && ((colonIndex = flag.indexOf(':')) > 0)) {
        String levelName = flag.substring(0, colonIndex);
        try {
          flag = flag.substring(colonIndex + 1);
          if (!levelName.equalsIgnoreCase("dump")) {
            level = PureLogWriter.levelNameToCode(levelName);
          }
          else {
            level = PureLogWriter.NONE_LEVEL;
          }
        } catch (Exception ex) {
          // ignored
        }
      }
      if (slf4logger.getLevel() <= level) {
        final StringBuilder sb = new StringBuilder();
        if (flag != null) {
          sb.append(flag).append(": ");
        }
        sb.append(message);
        slf4logger.put(level, sb.toString(), t);
      }
    }
    else if ((log = this.logger) != null) {
      int colonIndex = -1;
      boolean forceLog = flag != null && (colonIndex = flag.indexOf(':')) > 0
          && flag.substring(0, colonIndex).equalsIgnoreCase("dump");
      if (log.infoEnabled() && !forceLog) {
        final StringBuilder sb = new StringBuilder("GFXD");
        if (flag != null) {
          sb.append(':').append(flag);
        }
        sb.append(": ").append(message);
        log.info(sb.toString(), t);
      }
      else if (forceLog) {
        flag = flag.substring(colonIndex + 1);
        log.warning("GFXD:" + flag + ": " + message, t);
      }
    }
    else {
      final String formattedMessage = SanityManager
          .formatMessage(null, flag, message, t)
          .append(SanityManager.lineSeparator).toString();
      synchronized (this.lock) {
        moveToEndOfStream();
        super.write(formattedMessage, 0, formattedMessage.length());
        super.flush();
      }
    }
  }

  protected final void moveToEndOfStream() {
    ModuleFactory mf = Monitor.getMonitor();
    SingleStream ss;
    FileOutputStream fos;
    if (mf != null && (ss = (SingleStream)mf.getSystemStreams()) != null
        && (fos = ss.fileStream) != null) {
      try {
        FileChannel fch = fos.getChannel();
        long pos = fch.position();
        long size = fch.size();
        if (pos < size) {
          fch.position(size);
        }
      } catch (IOException ioe) {
        // ignore
      }
    }
  }

  @Override
  public final boolean isFineEnabled() {
    final PureLogWriter pureLog;
    final LogWriter log;
    if ((pureLog = this.pureLogger) != null) {
      return pureLog.fineEnabled();
    } else
      return (log = this.logger) != null && log.fineEnabled();
  }

  @Override
  public final boolean isFinerEnabled() {
    final PureLogWriter pureLog;
    final LogWriter log;
    if ((pureLog = this.pureLogger) != null) {
      return pureLog.finerEnabled();
    } else
      return (log = this.logger) != null && log.finerEnabled();
  }

  /**
   * Refresh the given debug flag to enable/disable it.
   * 
   * @see SanityManager#DEBUG_ON(String)
   * @see SanityManager#DEBUG_CLEAR(String)
   */
  @Override
  public void refreshDebugFlag(String debugFlag, boolean enable) {
    // currently just blindly refresh everything
    GemFireXDUtils.initFlags();
  }

  /**
   * Set the GemFire {@link LogWriter} to the given one. This method is not
   * thread-safe and should be invoked by the main thread.
   */
  public void setLogWriter(LogWriter logger) {
    assert logger != null;
    // close the current stream first
    // cannot do this at least when it points to standard output (#42371)
    //super.close();
    if (logger instanceof PureLogWriter) {
      this.pureLogger = (PureLogWriter)logger;
      this.logger = null;
      this.out = this.pureLogger.getPrintWriter();
      GfxdLogWriter.getInstance().setLogWriter(this.pureLogger);
    } else if (logger instanceof GFToSlf4jBridge) {
      this.usingSLF4JBridge = true;
      this.pureLogger = null;
      this.logger = logger;
    } else {
      this.pureLogger = null;
      this.logger = logger;
    }
    this.lock = logger;
  }

  /**
   * @see PrintWriter#flush()
   */
  @Override
  public void flush() {
    if (this.logger == null) {
      super.flush();
    }
  }

  /**
   * @see PrintWriter#close()
   */
  @Override
  public void close() {
    if (this.pureLogger == null && this.logger == null) {
      super.close();
    }
  }

  @Override
  public int getLogSeverityLevel() {
    final LogWriter log = getLogger();
    if (log != null) {
      // report everything for fine-level and higher
      if (log.fineEnabled()) {
        return ExceptionSeverity.NO_APPLICABLE_SEVERITY;
      }
      // for info-level, only report STATEMENT_SEVERITY or higher
      if (log.infoEnabled()) {
        return ExceptionSeverity.STATEMENT_SEVERITY;
      }
      // for warning-level, only report SESSION_SEVERITY or higher
      if (log.warningEnabled()) {
        return ExceptionSeverity.SESSION_SEVERITY;
      }
      // for severe-level, only report DATABASE_SEVERITY or higher
      if (log.severeEnabled()) {
        return ExceptionSeverity.DATABASE_SEVERITY;
      }
      // if none of the above then only report SYSTEM_SEVERITY errors
      return ExceptionSeverity.SYSTEM_SEVERITY;
    }
    return super.getLogSeverityLevel();
  }

  private LogWriter getLogger() {
    final PureLogWriter pureLog;
    if ((pureLog = this.pureLogger) != null) {
      return pureLog;
    }
    else {
      return this.logger;
    }
  }

  @Override
  public boolean printStackTrace(Throwable error, int logSeverityLevel) {
    if (logSeverityLevel >= ExceptionSeverity.STATEMENT_SEVERITY) {
      // avoid logging stack traces for constraint violations, conflicts, syntax
      // errors etc.
      if (error instanceof StandardException) {
        StandardException se = (StandardException)error;
        if (se.getSQLState().startsWith(SQLState.INTEGRITY_VIOLATION_PREFIX)
            || se.getSQLState().startsWith(
                SQLState.GFXD_OPERATION_CONFLICT_PREFIX)) {
          return false;
        }
        // also skip stacks for all syntax errors at log-level < fine
        final LogWriter log = getLogger();
        if (log != null && !log.fineEnabled()) {
          if (se.getSQLState().startsWith(SQLState.LSE_COMPILATION_PREFIX)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * This class is necessary to avoid too many changes to PureLogWriter. Also,
   * formatDate is synchronized on timeformatter and thus formatLogLine needs an
   * instance.
   * 
   * As timeformatter is the only sync point, synchronizing on _self should be
   * acceptable.
   * 
   * @author soubhikc
   * @author swale
   */
  public static final class GfxdLogWriter implements PrintWriterGetHeader {

    private final static GfxdLogWriter _self = new GfxdLogWriter();

    private PureLogWriter pureLogger;

    public static GfxdLogWriter getInstance() {
      return _self;
    }

    void setLogWriter(PureLogWriter logger) {
      this.pureLogger = logger;
    }

    public String getHeader() {
      String levelStr = null;
      String connectionName = null;
      if (this.pureLogger != null) {
        int level = PureLogWriter.INFO_LEVEL;
        if (this.pureLogger.getLevel() > level) {
          level = this.pureLogger.getLevel();
        }
        levelStr = PureLogWriter.levelToString(level);
        connectionName = this.pureLogger.getConnectionName();
      }
      return SanityManager.formatHeader(
          levelStr, connectionName != null ? connectionName
              : SanityManager.DEBUG_LEVEL_STRING).toString();
    }
  }
}
