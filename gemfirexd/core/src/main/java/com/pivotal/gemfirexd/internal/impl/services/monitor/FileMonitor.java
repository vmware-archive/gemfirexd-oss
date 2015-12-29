/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.monitor.FileMonitor

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

package com.pivotal.gemfirexd.internal.impl.services.monitor;


import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.services.info.ProductGenusNames;
import com.pivotal.gemfirexd.internal.iapi.services.info.ProductVersionHolder;
import com.pivotal.gemfirexd.internal.iapi.services.io.FileUtil;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.services.stream.HeaderPrintWriter;
import com.pivotal.gemfirexd.internal.impl.services.stream.GfxdHeaderPrintWriterImpl;

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
	Implementation of the monitor that uses the class loader
	that the its was loaded in for all class loading.

*/

public final class FileMonitor extends BaseMonitor implements java.security.PrivilegedExceptionAction<Object>
{

	/* Fields */
	private File home;

	private ProductVersionHolder engineVersion;

	public FileMonitor() {
		initialize(true);
		applicationProperties = readApplicationProperties();
	}

	public FileMonitor(java.util.Properties properties, java.io.PrintStream log) {
		runWithState(properties, log);
	}

	private InputStream PBapplicationPropertiesStream(BaseMonitor m)
	  throws IOException {

          //GemStone changes BEGIN
          {
            String fileName = PBgetJVMProperty(com.pivotal.gemfirexd.Property.PROPERTIES_FILE);
            //SQLF:BC
            if (fileName == null) {
              fileName = PBgetJVMProperty(com.pivotal.gemfirexd.Property.SQLF_PROPERTIES_FILE);
            }
            //if system property is not defined & there ain't any 'gemfirexd.properties'
            // file in gemfirexd.system.home or user.home
            if(fileName != null) {
                File sr = FileUtil.newFile(null, fileName);
                if (!sr.exists()) {
                  final String errorStr = "[warning] "+ fileName + " gemfirexd.properties not found to read ";
                  HeaderPrintWriter hpw = Monitor.getStream();
                  if(hpw == null) {
                    new GfxdHeaderPrintWriterImpl(System.err, null,
                        GfxdHeaderPrintWriterImpl.GfxdLogWriter.getInstance(), true,
                        "System.err").println(errorStr);
                    this.getTempWriter().append(errorStr);
                  }
                  throw new IOException(errorStr);
                }
                
                return new FileInputStream(sr);
            }
         }
          //GemStone changes END
		File sr = FileUtil.newFile(home, com.pivotal.gemfirexd.Property.PROPERTIES_FILE);

// GemStone changes BEGIN
                //SQL:BC
                File sqlfsr = FileUtil.newFile(home,
                    com.pivotal.gemfirexd.Property.SQLF_PROPERTIES_FILE);

                if (!sr.exists()) {
                  File userhome = new File(System.getProperty("user.home"));
                  if (!sqlfsr.exists()) {
                    if (!userhome.exists() || !userhome.isDirectory()) {
                      return null;
                    }
                    sr = FileUtil.newFile(userhome,
                        com.pivotal.gemfirexd.Property.PROPERTIES_FILE);
                    if (!sr.exists()) {
                      sqlfsr = sr = FileUtil.newFile(userhome,
                          com.pivotal.gemfirexd.Property.SQLF_PROPERTIES_FILE);
                      if (!sqlfsr.exists()) {
                        return null;
                      }
                      else {
                        PBsetJVMProperty(Property.PROP_SQLF_BC_MODE_INDICATOR, Boolean.TRUE.toString());
                        PropertyUtil.setSQLFire();
                      }
                    }
                  }
                  else {
                    PBsetJVMProperty(Property.PROP_SQLF_BC_MODE_INDICATOR, Boolean.TRUE.toString());
                    PropertyUtil.setSQLFire();
                  }
                }
		
		/* (before SQLF:BC
                if (!sr.exists()) {
                  File userhome = new File(System.getProperty("user.home"));
                  if (!userhome.exists() || !userhome.isDirectory()) {
                    return null;
                  }
                  sr = FileUtil.newFile(userhome, com.pivotal.gemfirexd.Property.PROPERTIES_FILE);
                  if (!sr.exists()) {
                    return null;
                  }
                }*/
                /* (original code)
                  if (!sr.exists())
                  return null;
                 */
// GemStone changes END

		return new FileInputStream(PropertyUtil.isSQLFire ? sqlfsr : sr);
	}

	public Object getEnvironment() {
		return home;
	}



	/**
		SECURITY WARNING.

		This method is run in a privileged block in a Java 2 environment.

		Set the system home directory.  Returns false if it couldn't for
		some reason.

	**/
	private boolean PBinitialize(boolean lite)
	{
		if (!lite) {
			try {
				// Create a ThreadGroup and set the daemon property to
				// make sure the group is destroyed and garbage
				// collected when all its members have finished (i.e.,
				// when the driver is unloaded).
				daemonGroup = new ThreadGroup("gemfirexd.daemons");
				daemonGroup.setDaemon(true);
			} catch (SecurityException se) {
			}
		}

		InputStream versionStream = getClass().getResourceAsStream(ProductGenusNames.DBMS_INFO);

		engineVersion = ProductVersionHolder.getProductVersionHolderFromMyEnv(versionStream);

		String systemHome;
		// create the system home directory if it doesn't exist
		try {
			// SECURITY PERMISSION - OP2
			systemHome = System.getProperty(Property.SYSTEM_HOME_PROPERTY);
			//SQLF:BC
			if (systemHome == null) {
			  systemHome = System.getProperty("sqlfire.system.home");
			}
		} catch (SecurityException se) {
			// system home will be the current directory
			systemHome = null;
		}

		if (systemHome != null) {
			home = new File(systemHome);

			// SECURITY PERMISSION - OP2a
			if (home.exists()) {
				if (!home.isDirectory()) {
					report(Property.SYSTEM_HOME_PROPERTY + "=" + systemHome
						+ " does not represent a directory");
					return false;
				}
			} else if (!lite) {

				try {
					// SECURITY PERMISSION - OP2b
                    // Attempt to create just the folder initially
                    // which does not require read permission on
                    // the parent folder. This is to allow a policy
                    // file to limit file permissions for gemfirexd.jar
                    // to be contained under gemfirexd.system.home.
                    // If the folder cannot be created that way
                    // due to missing parent folder(s) 
                    // then mkdir() will return false and thus
                    // mkdirs will be called to create the
                    // intermediate folders. This use of mkdir()
                    // and mkdirs() retains existing (pre10.3) behaviour
                    // but avoids requiring read permission on the parent
                    // directory if it exists.
					boolean created = home.mkdir() || home.mkdirs();
				} catch (SecurityException se) {
					return false;
				}
			}
		}

		return true;
	}

	/**
		SECURITY WARNING.

		This method is run in a privileged block in a Java 2 environment.

		Return a property from the JVM's system set.
		In a Java2 environment this will be executed as a privileged block
		if and only if the property starts with 'derby.'.
		If a SecurityException occurs, null is returned.
	*/
	private String PBgetJVMProperty(String key) {

		try {
			// SECURITY PERMISSION - OP1
			return System.getProperty(key);
		} catch (SecurityException se) {
			return null;
		}
	}
	
	//GemStone changes BEGIN
        private String PBsetJVMProperty(String key, String value) {
      
          try {
            // SECURITY PERMISSION - OP1
            return System.setProperty(key, value);
          } catch (SecurityException se) {
            return null;
          }
        }
        
        private String PBclearJVMProperty(String key) {
          
          try {
            // SECURITY PERMISSION - OP1
            return System.clearProperty(key);
          } catch (SecurityException se) {
            return null;
          }
        }
	//GemStone changes END

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

	private int action;
	private String key3;
	//GemStone changes BEGIN
        private String value3;
        //GemStone changes END
	private Runnable task;
	private int intValue;

	/**
		Initialize the system in a privileged block.
	**/
	synchronized final boolean initialize(boolean lite)
	{
		action = lite ? 0 : 1;
		try {
			Object ret = java.security.AccessController.doPrivileged(this);

			return ((Boolean) ret).booleanValue();
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}

	synchronized final Properties getDefaultModuleProperties() {
		action = 2;
 		try {
			return (Properties) java.security.AccessController.doPrivileged(this);
        } catch (java.security.PrivilegedActionException pae) {
           throw (RuntimeException) pae.getException();
        }
    }

	public synchronized final String getJVMProperty(String key) {
		if (!key.startsWith("gemfirexd.") && !key.startsWith("derby."))
			return PBgetJVMProperty(key);

		try {

			action = 3;
			key3 = key;
			String value  = (String) java.security.AccessController.doPrivileged(this);
			key3 = null;
			return value;
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}
	
	//GemStone changes BEGIN
        public synchronized final String setJVMProperty(String key, String value) {
          if (!key.startsWith("gemfirexd.") || !key.startsWith("derby."))
            return PBsetJVMProperty(key, value);
      
          try {
      
            action = 7;
            key3 = key;
            value3 = value;
            String rValue = (String) java.security.AccessController.doPrivileged(this);
            key3 = null;
            value3 = null;
            return rValue;
          } catch (java.security.PrivilegedActionException pae) {
            throw (RuntimeException)pae.getException();
          }
        }
        
        public synchronized final String clearJVMProperty(String key) {
          if (!key.startsWith("gemfirexd.") || !key.startsWith("derby."))
            return PBclearJVMProperty(key);
      
          try {
      
            action = 8;
            key3 = key;
            String rValue = (String) java.security.AccessController.doPrivileged(this);
            key3 = null;
            return rValue;
          } catch (java.security.PrivilegedActionException pae) {
            throw (RuntimeException)pae.getException();
          }
        }
	//GemStone changes END

	public synchronized final Thread getDaemonThread(Runnable task, String name, boolean setMinPriority) {

		action = 4;
		key3 = name;
		this.task = task;
		this.intValue = setMinPriority ? 1 : 0;

		try {

			Thread t = (Thread) java.security.AccessController.doPrivileged(this);

			key3 = null;
			task = null;

			return t;
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}

	public synchronized final void setThreadPriority(int priority) {
		action = 5;
		intValue = priority;
		try {
			java.security.AccessController.doPrivileged(this);
        } catch (java.security.PrivilegedActionException pae) {
			throw (RuntimeException) pae.getException();
		}
	}

	synchronized final InputStream applicationPropertiesStream()
	  throws IOException {
		action = 6;
		try {
			// SECURITY PERMISSION - OP3
			return (InputStream) java.security.AccessController.doPrivileged(this);
		}
        catch (java.security.PrivilegedActionException pae)
        {
			throw (IOException) pae.getException();
		}
	}


	public synchronized final Object run() throws IOException {
		switch (action) {
		case 0:
		case 1:
			// SECURITY PERMISSION - OP2, OP2a, OP2b
			return Boolean.valueOf(PBinitialize(action == 0));
		case 2: 
			// SECURITY PERMISSION - IP1
			return super.getDefaultModuleProperties();
		case 3:
			// SECURITY PERMISSION - OP1
			return PBgetJVMProperty(key3);
		case 4:
			return super.getDaemonThread(task, key3, intValue != 0);
		case 5:
			super.setThreadPriority(intValue);
			return null;
		case 6:
			// SECURITY PERMISSION - OP3
			return PBapplicationPropertiesStream(this);

               // GemStone changes BEGIN
		case 7:
                  // SECURITY PERMISSION - OP1
		  return PBsetJVMProperty(key3, value3);
                case 8:
                  // SECURITY PERMISSION - OP1
                  return PBclearJVMProperty(key3);
               // GemStone changes END
		default:
			return null;
		}
	}

	public final ProductVersionHolder getEngineVersion() {
		return engineVersion;
	}
}
