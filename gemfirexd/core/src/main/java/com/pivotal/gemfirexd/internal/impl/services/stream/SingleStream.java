/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.stream.SingleStream

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
// GemStone changes BEGIN
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.ManagerLogWriter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
// GemStone changes END


import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleSupportable;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.stream.HeaderPrintWriter;
import com.pivotal.gemfirexd.internal.iapi.services.stream.InfoStreams;
import com.pivotal.gemfirexd.internal.iapi.services.stream.PrintWriterGetHeader;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.io.Writer;
import java.util.Properties;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Member;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * The Basic Services provide InfoStreams for reporting
 * information. Two streams are provided: trace and error.
 * It is configurable where these streams are directed.
 * <p>
 * Errors will be printed to the error stream in addition
 * to being sent to the client.
 * <p>
 * By default both streams are sent to an error log
 * for the system. When creating a message for a stream,
 * you can create an initial entry with header information
 * and then append to it as many times as desired.
 * <p>
 * Note: if character encodings are needed, the use of
 * java.io.*OutputStream's should be replaced with
 * java.io.*Writer's (assuming the Writer interface
 * remains stable in JDK1.1)
 *
 */
public final class SingleStream
implements InfoStreams, ModuleControl, java.security.PrivilegedAction
{

	/*
	** Instance fields
	*/
	private HeaderPrintWriter theStream;


	/**
		  The no-arg public constructor for ModuleControl's use.
	 */
	public SingleStream() {
	}

	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl#boot
	 */
	public void boot(boolean create, Properties properties) {
		theStream = makeStream();
	}


	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl#stop
	 */
	public void stop()	{
		((BasicHeaderPrintWriter) theStream).complete();
	}

	/*
	 * InfoStreams interface
	 */

	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.services.stream.InfoStreams#stream
	 */
	public HeaderPrintWriter stream() {
		return theStream;
	}

	//
	// class interface
	//

	/**
		Make the stream; note that service properties override
		application and system properties.

	 */
	private HeaderPrintWriter makeStream() {

		// get the header
		PrintWriterGetHeader header = makeHeader();
		HeaderPrintWriter hpw = makeHPW(header);

		// If hpw == null then no properties were specified for the stream
		// so use/create the default stream.
		if (hpw == null)
			hpw = createDefaultStream(header);
		return hpw;
	}

	/**
		Return a new header object.
	*/
	private PrintWriterGetHeader makeHeader() {

// GemStone changes BEGIN
	  return GfxdHeaderPrintWriterImpl.GfxdLogWriter.getInstance();
	  /* (original code)
		return new BasicGetLogHeader(true, true, (String) null);
	  */
// GemStone changes END
	}

	/**
		create a HeaderPrintWriter based on the header.
		Will still need to determine the target type.
	 */
	private HeaderPrintWriter makeHPW(PrintWriterGetHeader header) {

		// the type of target is based on which property is used
		// to set it. choices are file, method, field, stream

		String target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_FILE_PROPERTY);
		if (target!=null)
			return makeFileHPW(target, header);

		target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_METHOD_PROPERTY);
		if (target!=null) 
			return makeMethodHPW(target, header);

		target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_FIELD_PROPERTY);
		if (target!=null) 
			return makeFieldHPW(target, header);

		return null;
	}

	/**
		Make a header print writer out of a file name. If it is a relative
		path name then it is taken as relative to gemfirexd.system.home if that is set,
		otherwise relative to the current directory. If the path name is absolute
		then it is taken as absolute.
	*/
	private HeaderPrintWriter PBmakeFileHPW(String fileName,
											PrintWriterGetHeader header) {

		boolean appendInfoLog = PropertyUtil.getSystemBoolean(Property.LOG_FILE_APPEND);

		File streamFile = new File(fileName);

		// See if this needs to be made relative to something ...
		if (!streamFile.isAbsolute()) {
			Object monitorEnv = Monitor.getMonitor().getEnvironment();
			if (monitorEnv instanceof File)
				streamFile = new File((File) monitorEnv, fileName);
		}

		FileOutputStream	fos;

		try {

			if (streamFile.exists() && appendInfoLog)
				fos = new FileOutputStream(streamFile.getPath(), true);
			else
				fos = new FileOutputStream(streamFile);
// GemStone changes BEGIN
			this.fileStream = fos;
// GemStone changes END
		} catch (IOException ioe) {
			return useDefaultStream(header, ioe);
		} catch (SecurityException se) {
			return useDefaultStream(header, se);
		}

// GemStone changes BEGIN
		return new GfxdHeaderPrintWriterImpl(new BufferedOutputStream(fos),
				streamFile.getPath(), header,
		/* (original code)
		return new BasicHeaderPrintWriter(new BufferedOutputStream(fos), header,
		*/
// GemStone changes END
			true, streamFile.getPath());
	}

	private HeaderPrintWriter makeMethodHPW(String methodInvocation,
											PrintWriterGetHeader header) {

		int lastDot = methodInvocation.lastIndexOf('.');
		String className = methodInvocation.substring(0, lastDot);
		String methodName = methodInvocation.substring(lastDot+1);

		Throwable t;
		try {
			Class theClass = Class.forName(className);

			try {
				Method theMethod = theClass.getMethod(methodName,  new Class[0]);

				if (!Modifier.isStatic(theMethod.getModifiers())) {
					HeaderPrintWriter hpw = useDefaultStream(header);
					hpw.printlnWithHeader(theMethod.toString() + " is not static");
					return hpw;
				}

				try {
					return makeValueHPW(theMethod, theMethod.invoke((Object) null, 
						new Object[0]), header, methodInvocation);
				} catch (IllegalAccessException iae) {
					t = iae;
				} catch (IllegalArgumentException iarge) {
					t = iarge;
				} catch (InvocationTargetException ite) {
					t = ite.getTargetException();
				}

			} catch (NoSuchMethodException nsme) {
				t = nsme;
			}
		} catch (ClassNotFoundException cnfe) {
			t = cnfe;
		} catch (SecurityException se) {
			t = se;
			
		}
		return useDefaultStream(header, t);

	}


	private HeaderPrintWriter makeFieldHPW(String fieldAccess,
											PrintWriterGetHeader header) {

		int lastDot = fieldAccess.lastIndexOf('.');
		String className = fieldAccess.substring(0, lastDot);
		String fieldName = fieldAccess.substring(lastDot+1,
							  fieldAccess.length());

		Throwable t;
		try {
			Class theClass = Class.forName(className);

			try {
				Field theField = theClass.getField(fieldName);
		
				if (!Modifier.isStatic(theField.getModifiers())) {
					HeaderPrintWriter hpw = useDefaultStream(header);
					hpw.printlnWithHeader(theField.toString() + " is not static");
					return hpw;
				}

				try {
					return makeValueHPW(theField, theField.get((Object) null), 
						header, fieldAccess);
				} catch (IllegalAccessException iae) {
					t = iae;
				} catch (IllegalArgumentException iarge) {
					t = iarge;
				}

			} catch (NoSuchFieldException nsfe) {
				t = nsfe;
			}
		} catch (ClassNotFoundException cnfe) {
			t = cnfe;
		} catch (SecurityException se) {
			t = se;
		}
		return useDefaultStream(header, t);

		/*
			If we decide it is a bad idea to use reflect and need
			an alternate implementation, we can hard-wire those
			fields that we desire to give configurations access to,
			like so:

		if ("java.lang.System.out".equals(fieldAccess))
		 	os = System.out;
		else if ("java.lang.System.err".equals(fieldAccess))
		 	os = System.err;
		*/
	}

	private HeaderPrintWriter makeValueHPW(Member whereFrom, Object value,
		PrintWriterGetHeader header, String name) {

		if (value instanceof OutputStream)
			 return new BasicHeaderPrintWriter((OutputStream) value, header, false, name);
		else if (value instanceof Writer)
			 return new BasicHeaderPrintWriter((Writer) value, header, false, name);
		
		HeaderPrintWriter hpw = useDefaultStream(header);

		if (value == null)
			hpw.printlnWithHeader(whereFrom.toString() + "=null");
		else
			hpw.printlnWithHeader(whereFrom.toString() + " instanceof " + value.getClass().getName());

		return hpw;
	}
 

	/**
		Used when no configuration information exists for a stream.
	*/
	private HeaderPrintWriter createDefaultStream(PrintWriterGetHeader header) {
// GemStone changes BEGIN
    String sysLogFile = getGFXDLogFile();
		if (sysLogFile == null) {
      return new GfxdHeaderPrintWriterImpl(System.out, null, header, false,
          "System.out");
    }
    String logfile = sysLogFile;
    final File logFile = new File(sysLogFile);
    String firstMsg = null;
    boolean append = Boolean.getBoolean(InternalDistributedSystem.APPEND_TO_LOG_FILE);
    if (logFile.exists() && !append) {
      final File oldMain = ManagerLogWriter.getLogNameForOldMainLog(logFile,
          false);
      if (!logFile.renameTo(oldMain)) {
        logfile = oldMain.getPath();
        firstMsg = LocalizedStrings
          .InternalDistributedSystem_COULD_NOT_RENAME_0_TO_1
            .toLocalizedString(new Object[] { logFile, oldMain });
      }
      else {
        logfile = logFile.getPath();
      }
    }
    Object oldSetting = System.getProperties().get(Property.LOG_FILE_APPEND);
    try {
      if (append) {
        // pass on log-file-append setting to the print writer creation code
        System.setProperty(Property.LOG_FILE_APPEND, "true");
      }
      final HeaderPrintWriter writer = makeFileHPW(logfile, header);
      if (firstMsg != null) {
        writer.println(firstMsg);
      }
      return writer;
    } finally {
      if (append) {
        if (oldSetting != null) {
          System.getProperties().put(Property.LOG_FILE_APPEND, oldSetting);
        } else {
          System.getProperties().remove(Property.LOG_FILE_APPEND);
        }
      }
    }
  }

  public FileOutputStream fileStream;

  public static String getGFXDLogFile() {
    String logFile = PropertyUtil
        .getSystemProperty(GfxdConstants.GFXD_LOG_FILE);
    if (logFile != null && logFile.length() == 0) {
      logFile = null;
    }
    // if slf4 bridge is being used, don't create a file here
    // but promote GFXD property to GemFire system property so that
    // GemFire layer can create appropriate file
    if (PropertyUtil.getSystemBoolean(
	InternalDistributedSystem.ENABLE_SLF4J_LOG_BRIDGE, true)) {
      if (logFile != null) {
	PropertyUtil.setSystemProperty(DistributionConfigImpl.GEMFIRE_PREFIX +
	    DistributionConfigImpl.LOG_FILE_NAME, logFile);
      }
      return null;
    }
    return logFile;
  }
    /* (original code)
	return makeFileHPW("gemfirexd.log", header);
	}
    */

// GemStone changes END
	/**
		Used when creating a stream creates an error.
	*/
	private HeaderPrintWriter useDefaultStream(PrintWriterGetHeader header) {

		return new BasicHeaderPrintWriter(System.err, header, false, "System.err");
	}

	private HeaderPrintWriter useDefaultStream(PrintWriterGetHeader header, Throwable t) {

		HeaderPrintWriter hpw = useDefaultStream(header);
		hpw.printlnWithHeader(t.toString());
		return hpw;
	}

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

    private String PBfileName;
    private PrintWriterGetHeader PBheader;

	private HeaderPrintWriter makeFileHPW(String fileName, PrintWriterGetHeader header)
    {
        this.PBfileName = fileName;
        this.PBheader = header;
        return (HeaderPrintWriter) java.security.AccessController.doPrivileged(this);
    }


    public final Object run()
    {
        // SECURITY PERMISSION - OP4, OP5
        return PBmakeFileHPW(PBfileName, PBheader);
    }
}

