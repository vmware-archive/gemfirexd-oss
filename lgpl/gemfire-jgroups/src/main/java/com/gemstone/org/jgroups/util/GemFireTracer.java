/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.gemstone.org.jgroups.util;

import java.io.StringWriter;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * GemFireTracer wraps a GemFire InternalLogWriter and adapts it to the
 * log4j Log interface.
 * 
 * @author bruce schuchardt
 * @since 5.0
 *
 */
public class GemFireTracer  {
    /** whether jgroups debugging should be turned on */
    public static boolean DEBUG = Boolean.getBoolean("JGroups.DEBUG");

    private static Logger logger = LogManager.getLogger(GemFireTracer.class);

    private GFLogWriter logWriter = new GFLogWriterImpl(logger);

    private GFLogWriter securityLogWriter = new GFLogWriterImpl(logger);


// threadlocals were holding onto logwriters refering to distributedsystems,
// causing a memory leak
//        new InheritableThreadLocal() {
//            protected Object initialValue() {
//                return defaultLogWriter;
//            }
//        };

    /** <code>ThreadGroup</code> to which JGroups threads belong
     */
    // soubhik: removed final qualifier. (#41438)
    // added initialization in JGroupMemberShipManager#createChannel().
    // [sumedh] removed initialization as mentioned above to prevent leaks
    // and instead added back final with daemon property explicitly as false
    // to avoid it being auto-cleaned if the parent group has that as true
    public static final ThreadGroup GROUP;

    static {
      GROUP = new ThreadGroup("JGroups Threads") {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          StringWriter sw = new StringWriter();
          sw.write("Uncaught Exception in thread ");
          sw.write(t.getName());
          GemFireTracer.getLog(GemFireTracer.class).error(
              "ThreadGroup: " + sw.toString(), e);
        }
      };
      // this is a static thread group so do not exit when last thread exits
      // (the property is inherited from parent so explicitly set to false)
      GROUP.setDaemon(false);
    }

    /** the getLog method currently returns the default instance of
        GemFireTracer */
    private static GemFireTracer defaultInstance = new GemFireTracer();
    
    /** jgroups uses this method to get a tracer */
    public static GemFireTracer getLog(Class clz) {
        return defaultInstance;
    }
    
    /** default constructor */
    public GemFireTracer() {
        super();
    }
    
    public void setLogger(Logger log) {
      this.logger = log;
    }
    
    /** gemfire's GossipServer startup code uses this method to establish
        the log writer */
    public void setLogWriter(GFLogWriter writer) {
        setLogWriter(writer, DEBUG);
    }

    /** gemfire uses this to establish a thread's log writer.  For this
        to work properly, it must be done before the channel is created */
    public void setLogWriter(GFLogWriter writer, boolean debug) {
        DEBUG = debug;
        logWriter = writer;
        //logWriter.set(writer);
    }

    /**
     * gemfire uses this to establish a thread's security log writer.
     */
    public void setSecurityLogWriter(GFLogWriter writer) {
      securityLogWriter = writer;
    }

    /** returns the InternalLogWriter for this tracer */
    public GFLogWriter getLogWriter() {
      return this.logWriter;
    }

    /** returns the security LogWriter for this tracer */
    public GFLogWriter getSecurityLogWriter() {
      return this.securityLogWriter;
    }

    /** returns the log writer for this thread */
    private final Logger _log() {
        //return (InternalLogWriter)logWriter.get();
        return logger;
    }

    public void debug(Object arg0, Throwable arg1) {
        if (DEBUG) _log().info(arg0, arg1);
    }

    public void debug(Object arg0) {
        if (DEBUG) _log().info(arg0);
    }

    public void debug(StringId arg0, Object[] arg1, Throwable arg2) {
        if (DEBUG) _log().info(arg0.toLocalizedString(arg1), arg2);
    }

    public void debug(StringId arg0, Throwable arg1) {
        if (DEBUG) _log().info(arg0.toLocalizedString(), arg1);
    }

    public void debug(StringId arg0, Object[] arg1) {
        if (DEBUG) _log().info(arg0.toLocalizedString(arg1));
    }

    public void debug(StringId arg0) {
        if (DEBUG) _log().info(arg0.toLocalizedString());
    }

    public void error(Object arg0, Throwable arg1) {
        _log().error(arg0,arg1);
    }

    public void error(Object arg0) {
      _log().error(arg0);
    }

    public void error(StringId arg0, Object[] arg1, Throwable arg2) {
        if (DEBUG) _log().error(arg0.toLocalizedString(arg1), arg2);
    }

    public void error(StringId arg0, Object arg1, Throwable arg2) {
      if (DEBUG) _log().error(arg0.toLocalizedString(arg1), arg2);
    }

    public void error(StringId arg0, Object[] arg1) {
      if (DEBUG) _log().error(arg0.toLocalizedString(arg1));
    }
    
    public void error(StringId arg0, Object arg1) {
        if (DEBUG) _log().error(arg0.toLocalizedString(arg1));
    }

    public void error(StringId arg0, Throwable arg1) {
        if (DEBUG) _log().error(arg0.toLocalizedString(), arg1);
    }

    public void error(StringId arg0) {
        if (DEBUG) _log().error(arg0.toLocalizedString());
    }

    public void fatal(Object arg0, Throwable arg1) {
        _log().fatal(arg0, arg1);
    }

    public void fatal(Object arg0) {
        _log().fatal(arg0);
    }

    public void fatal(StringId arg0, Object[] arg1) {
      if (DEBUG) _log().fatal(arg0.toLocalizedString(arg1));
    }

    public void fatal(StringId arg0, Object[] arg1, Throwable arg2) {
        if (DEBUG) _log().fatal(arg0.toLocalizedString(arg1), arg2);
    }

    public void fatal(StringId arg0, Throwable arg1) {
        if (DEBUG) _log().fatal(arg0.toLocalizedString(), arg1);
    }

    public void fatal(StringId arg0) {
        if (DEBUG) _log().fatal(arg0.toLocalizedString());
    }

    public void info(Object arg0, Throwable arg1) {
        if (DEBUG)
          _log().info(arg0, arg1);
        else
          _log().debug(""+arg0, arg1);        
    }

    public void info(Object arg0) {
        if (DEBUG)
          _log().info(""+arg0);
        else
          _log().debug(""+arg0);
    }

    public void info(StringId arg0, Object[] arg1, Throwable arg2) {
        if (DEBUG)
          _log().info(arg0.toLocalizedString(arg1), arg2);
        else
          _log().debug(arg0.toLocalizedString(arg1), arg2);        
    }

    public void info(StringId arg0, Throwable arg1) {
        if (DEBUG)
          _log().info(arg0.toLocalizedString(), arg1);
        else
          _log().debug(arg0.toLocalizedString(), arg1);        
    }

    public void info(StringId arg0, Object[] arg1) {
      if (DEBUG)
        _log().info(arg0.toLocalizedString(arg1));
      else
        _log().debug(arg0.toLocalizedString(arg1));
    }

    public void info(StringId arg0, Object arg1) {
      this.info(arg0.toLocalizedString(new Object[] {arg1}));        
    }

    public void info(StringId arg0) {
        if (DEBUG)
          _log().info(arg0.toLocalizedString());
        else
          _log().debug(arg0.toLocalizedString());
    }

    public boolean isDebugEnabled() {
        return DEBUG;
    }

    public boolean isErrorEnabled() {
        return _log().isEnabledFor(Level.ERROR);
    }

    public boolean isFatalEnabled() {
        return _log().isEnabledFor(Level.FATAL);
    }

    public boolean isInfoEnabled() {
        if (DEBUG)
          return _log().isInfoEnabled();
        else
          return _log().isDebugEnabled();
    }

    public boolean isTraceEnabled() {
        return DEBUG; //_log().finerEnabled();
    }

    public boolean isWarnEnabled() {
        if (DEBUG)
          return _log().isEnabledFor(Level.WARN);
        else
          return _log().isDebugEnabled();  // we use fine level logging for jgroups since it's pretty verbose otherwise
    }

    public void trace(Object arg0, Throwable arg1) {
        if (DEBUG) {
          _log().info(arg0, arg1);
        }
        else {
          _log().trace(""+arg0, arg1);
        }
    }

    public void trace(Object arg0) {
        if (DEBUG) {
          _log().info(""+arg0);
        }
        else {
          _log().trace(""+arg0);
        }
    }

    public void warn(Object arg0, Throwable arg1) {
        if (DEBUG) {
          _log().warn(arg0, arg1);
        }
        else {
          _log().debug(""+arg0,arg1);
        }
    }

    public void warn(Object arg0) {
        if (DEBUG) {
          _log().warn(arg0);
        }
        else {
          _log().debug(""+arg0);
        }
    }


    /**
     * Copied from Javagroups' Trace class from v2.0.3<br>
     * Converts an exception stack trace into a java.lang.String
     * @param x an exception
     * @return a string containg the stack trace, null if the exception parameter was null
     */
    public static String getStackTrace(Throwable x) {
        if(x == null)
            return null;
        else {
            java.io.ByteArrayOutputStream bout=new java.io.ByteArrayOutputStream();
            java.io.PrintStream writer=new java.io.PrintStream(bout);
            x.printStackTrace(writer);
            String result=new String(bout.toByteArray());
            return result;
        }
    }
    
    private static class GFLogWriterImpl implements GFLogWriter {

      Logger logger;

      GFLogWriterImpl(Logger logger) {
        this.logger = logger;
      }

      @Override
      public boolean fineEnabled() {
        return this.logger.isDebugEnabled();
      }

      @Override
      public boolean infoEnabled() {
        return this.logger.isInfoEnabled();
      }

      @Override
      public boolean warningEnabled() {
        return this.logger.isEnabledFor(Level.WARN);
      }

      @Override
      public boolean severeEnabled() {
        return this.logger.isEnabledFor(Level.FATAL);
      }

      @Override
      public void fine(String string) {
        this.logger.debug(string);
      }

      @Override
      public void fine(String string, Throwable e) {
        this.logger.debug(string, e);
      }

      @Override
      public void info(StringId str) {
        this.logger.info(str.toLocalizedString());
      }

      @Override
      public void info(StringId str, Object arg) {
        this.logger.info(str.toLocalizedString(arg));
      }

      @Override
      public void info(StringId str, Object[] args) {
        this.logger.info(str.toLocalizedString(args));
      }

      @Override
      public void info(StringId str, Object arg, Throwable thr) {
        this.logger.info(str.toLocalizedString(arg), thr);
      }

      @Override
      public void info(StringId str, Object[] args, Throwable thr) {
        this.logger.info(str.toLocalizedString(args), thr);
      }

      @Override
      public void warning(StringId str) {
        this.logger.warn(str.toLocalizedString());
      }

      @Override
      public void warning(StringId str, Object arg) {
        this.logger.warn(str.toLocalizedString(arg));
      }

      @Override
      public void warning(StringId str, Object[] args) {
        this.logger.warn(str.toLocalizedString(args));
      }

      @Override
      public void warning(StringId str, Object arg, Throwable thr) {
        this.logger.warn(str.toLocalizedString(arg), thr);
      }

      @Override
      public void warning(StringId str, Object[] args, Throwable thr) {
        this.logger.warn(str.toLocalizedString(args), thr);
      }

      @Override
      public void severe(StringId str) {
        this.logger.fatal(str.toLocalizedString());
      }

      @Override
      public void severe(StringId str, Object arg) {
        this.logger.fatal(str.toLocalizedString(arg));
      }

      @Override
      public void severe(StringId str, Object[] args) {
        this.logger.fatal(str.toLocalizedString(args));
      }

      @Override
      public void severe(StringId str, Object arg, Throwable thr) {
        this.logger.fatal(str.toLocalizedString(arg), thr);
      }

      @Override
      public void severe(StringId str, Object[] args, Throwable thr) {
        this.logger.fatal(str.toLocalizedString(args), thr);
      }
    }
}
