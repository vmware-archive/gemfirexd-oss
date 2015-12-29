/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.reflect.JarLoader

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

package com.pivotal.gemfirexd.internal.impl.services.reflect;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.GeneralSecurityException;
import java.security.PrivilegedActionException;
import java.security.SecureClassLoader;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import com.pivotal.gemfirexd.internal.catalog.DependableFinder;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.MessageId;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.io.AccessibleByteArrayOutputStream;
import com.pivotal.gemfirexd.internal.iapi.services.io.InputStreamUtil;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.stream.HeaderPrintWriter;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Dependency;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Dependent;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Provider;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdAsyncEventListenerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DDdependableFinder;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary;
import com.pivotal.gemfirexd.internal.impl.sql.depend.BasicDependencyManager;
import com.pivotal.gemfirexd.internal.io.StorageFile;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;


//GemStone changes BEGIN
//added implements provider
final public class JarLoader extends SecureClassLoader implements Provider {
//GemStone changes END 
    
    /**
     * Two part name for the jar file.
     */
    /* GemStone change private */ final String[] name;
    
    /**
     * Handle to the installed jar file.
     */
    private StorageFile installedJar;
    
    /**
     * When the jar file can be manipulated as a java.util.JarFile
     * this holds the reference to the open jar. When the jar can
     * only be manipulated as an InputStream (because the jar is itself
     * in a database jar) then this will be null.
     */
    private JarFile jar;
    
    /**
     * True if the jar can only be accessed using a stream, because
     * the jar is itself in a database jar. When fals the jar is accessed
     * using the jar field.
     */
    private boolean isStream;

	private UpdateLoader updateLoader;
	private final HeaderPrintWriter vs;
	//GemStone changes BEGIN
	  private UUID oid;
	//GemStone changes END
	JarLoader(UpdateLoader updateLoader, String[] name, HeaderPrintWriter vs) {

		this.updateLoader = updateLoader;
        this.name = name;
		this.vs = vs;
// GemStone changes BEGIN
                GfxdDataDictionary gfxddd = Misc.getMemStore()
                    .getDatabase().getDataDictionary();
                this.oid = gfxddd.getUUIDFactory().createUUID();
		String schemaName = name[IdUtil.DBCP_SCHEMA_NAME];
		String sqlName = name[IdUtil.DBCP_SQL_JAR_NAME];
		try {
		  installedJar = updateLoader.getJarReader().getJarFile(
		      schemaName, sqlName);
		} catch (StandardException se) {
		  if (vs != null) {
		    vs.println(MessageService.getTextMessage(
		        MessageId.CM_LOAD_JAR_EXCEPTION, getJarName(), se));
		  }
		}
// GemStone changes END
	}

	/**
	 *  Initialize the class loader so it knows if it
	 *  is loading from a ZipFile or an InputStream
	 */
	void initialize() {

// GemStone changes BEGIN
	  if (jar != null) {
	    return;
	  }
// GemStone changes END
		String schemaName = name[IdUtil.DBCP_SCHEMA_NAME];
		String sqlName = name[IdUtil.DBCP_SQL_JAR_NAME];

		Exception e;
		try {
// GemStone changes BEGIN
		  if (installedJar == null)
// GemStone changes END
			installedJar =
				updateLoader.getJarReader().getJarFile(
					schemaName, sqlName);

			if (installedJar instanceof File) {
                try {
                    jar = (JarFile) AccessController.doPrivileged
                    (new java.security.PrivilegedExceptionAction(){

                        public Object run() throws IOException {
                        return new JarFile((File) installedJar);

                        }

                    }
                     );
                 // GemStone changes BEGIN
                    if (GemFireXDUtils.TraceApplicationJars) {
                      StringBuilder msg = new StringBuilder();
                      msg.append("JarLoader#initialize Loaded jar sqlName=")
                          .append(sqlName).append(" schemaName=").append(schemaName)
                          .append(" installedJar=")
                          .append(installedJar.getClass().getSimpleName());
                      msg.append(" with entries:").append(SanityManager.lineSeparator);
                      Enumeration<?> jarEntries = jar.entries();
                      while (jarEntries.hasMoreElements()) {
                        Object entry = jarEntries.nextElement();
                        msg.append(entry).append(SanityManager.lineSeparator);
                      }
                      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
                          msg.toString());
                    }
                 // GemStone changes END
                } catch (PrivilegedActionException pae) {
                    throw (IOException) pae.getException();
                }
				return;
			}

			// Jar is only accessible as an InputStream,
			// which means we need to re-open the stream for
			// each access.

			isStream = true;
			// GemStone changes BEGIN
                        if (GemFireXDUtils.TraceApplicationJars) {
                          StringBuilder msg = new StringBuilder();
                          msg.append("JarLoader#initialize Loaded jar as stream sqlName=")
                              .append(sqlName).append(" schemaName=").append(schemaName)
                              .append(" installedJar=")
                              .append(installedJar.getClass().getSimpleName());
                          msg.append(" with entries:").append(SanityManager.lineSeparator);
                  
                          JarInputStream jarIn = new JarInputStream(installedJar.getInputStream());
                          JarEntry entry = null;
                          while ((entry = jarIn.getNextJarEntry()) != null) {
                            msg.append(entry).append(SanityManager.lineSeparator);
                          }
                          if (entry == null) {
                            jarIn.close();
                          }
                          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
                              msg.toString());
                        }
                        // GemStone changes END
			return;

		} catch (IOException ioe) {
			e = ioe;
		} catch (StandardException se) {
			e = se;
		}

		if (vs != null)
			vs.println(MessageService.getTextMessage(
					MessageId.CM_LOAD_JAR_EXCEPTION, getJarName(), e));

		// No such zip.
		setInvalid();
	}

	/**
	 * Handle all requests to the top-level loader.
	 * 
	 * @exception ClassNotFoundException
	 *                Class can not be found
	 */
	protected Class loadClass(String className, boolean resolve) 
		throws ClassNotFoundException {
        
        // Classes in installed jars cannot reference
        // Derby internal code. This is to avoid
        // code in installed jars bypassing SQL
        // authorization by calling Derby's internal methods.
        //
        // Any classes in the com.pivotal.gemfirexd.internal.jdbc package
        // are allowed as it allows routines to make JDBC
        // connections to other databases. This does expose
        // public classes in that package that are not part
        // of the public api to attacks. One could attempt
        // further limiting allowed classes to those starting
        // with Embedded (and Client) but when fetching the
        // default connection in a routine (jdbc:default:connection)
        // the DriverManager attempts a load of the already loaded
        // AutoloadDriver, I think to establish the calling class
        // has access to the driver.
        //
        // This check in addition to the one in UpdateLoader
        // that prevents restricted classes from being loaded
        // from installed jars. The checks should be seen as
        // independent, ie. the restricted load check should
        // not make assumptions about this check reducing the
        // number of classes it has to check for.
        if (className.startsWith("com.pivotal.gemfirexd.internal.")
                && !className.startsWith("com.pivotal.gemfirexd.internal.jdbc.")
                && !className.startsWith("com.pivotal.gemfirexd.internal.vti."))
        {
            ClassNotFoundException cnfe = new ClassNotFoundException(className);
            //cnfe.printStackTrace(System.out);
            throw cnfe;
        }

		// we attempt the system class load even if we
		// are stale because otherwise we will fail
		// to load java.* classes which confuses some VMs
		try {
			return Class.forName(className);
		} catch (ClassNotFoundException cnfe) {

			if (updateLoader == null)
				throw new ClassNotFoundException(MessageService.getTextMessage(MessageId.CM_STALE_LOADER, className));

			Class c = updateLoader.loadClass(className, resolve);
			if (c == null)
				throw cnfe;
			return c;
		}
	}

	/**
		
	*/
	public InputStream getResourceAsStream(String name) {
		if (updateLoader == null)
			return null;
		return updateLoader.getResourceAsStream(name);
	}

    /**
     * Return the SQL name for the installed jar.
     * Used for error and informational messages.
     */
    final String getJarName() {
        return IdUtil.mkQualifiedName(name);
    }
// GemStone changes BEGIN
    final String getJarNameAsString() {
      return name[0] + "." + name[1];
  }
// GemStone changes END
   public Class loadClassData(String className, String jvmClassName, boolean resolve) {

		if (updateLoader == null)
			return null;

		try {
			if (jar != null)
				return loadClassDataFromJar(className, jvmClassName, resolve);

			if (isStream) {
				// have to use a new stream each time
				return loadClassData(installedJar.getInputStream(),
						className, jvmClassName, resolve);
			}

			return null;
		} catch (FileNotFoundException fnfe) {
			// No such entry.
			return null;
		} catch (IOException ioe) {
			if (vs != null)
				vs.println(MessageService.getTextMessage(MessageId.CM_CLASS_LOAD_EXCEPTION, className, getJarName(), ioe));
			return null;
		}	
	}

	/**
		Get an InputStream for the given resource.
	*/
	InputStream getStream(String name) {

		if (updateLoader == null)
			return null;
     
		if (jar != null)
			return getRawStream(name);

		if (isStream) {
			try {
				return getRawStream(installedJar.getInputStream(), name);
			} catch (FileNotFoundException e) {
				// no such entry
			}
		}
		return null;
	}


	/*
	** Private api
	*/


    /**
     * Load the class data when the installed jar is accessible
     * as a java.util.jarFile.
     */
	private Class loadClassDataFromJar(
            String className, String jvmClassName, boolean resolve) 
		throws IOException {

		JarEntry e = jar.getJarEntry(jvmClassName);
		if (e == null)
			return null;

		InputStream in = jar.getInputStream(e);

		try {
			return loadClassData(e, in, className, resolve);
		} finally {
			in.close();
		}
	}

    /**
     * Load the class data when the installed jar is accessible
     * only as an input stream (the jar is itself in a database jar).
     */
	private Class loadClassData(
		InputStream in, String className, String jvmClassName, boolean resolve) 
		throws IOException {

        if (GemFireXDUtils.TraceApplicationJars) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
              "JarLoader#loadClassData Attempting to load class " + className + " jvmClassName "
                  + jvmClassName + " name " + Arrays.toString(name));
        }
        JarInputStream jarIn = new JarInputStream(in);

		for (;;) {

			JarEntry e = jarIn.getNextJarEntry();
			if (e == null) {
				jarIn.close();
				return null;
			}
			if (e.getName().equals(jvmClassName)) {
                                if (GemFireXDUtils.TraceApplicationJars) {
                                  SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
                                      "JarLoader#loadClassData found jar entry " + e.getName()
                                          + " attempting to load classData ");
                                }
				Class c = loadClassData(e, jarIn, className, resolve);
				jarIn.close();
				return c;
			}
		}
		
	}

    /**
     * Load and optionally resolve the class given its
     * JarEntry and an InputStream to the class fiel format.
     * This is common code for when the jar is accessed
     * directly using JarFile or through InputStream.
     */
	private Class loadClassData(JarEntry e, InputStream in,
		String className, boolean resolve) throws IOException {

		byte[] data = readData(e, in, className);

		Certificate[] signers = getSigners(className, e);

		synchronized (updateLoader) {
			// see if someone else loaded it while we
			// were getting the bytes ...
			Class c = updateLoader.checkLoaded(className, resolve);
			if (c == null) {
			    try {
				c = defineClass(className, data, 0, data.length, (CodeSource) null);
				if (signers != null) {
					setSigners(c, signers);
				}
				if (resolve) {
	                                if (GemFireXDUtils.TraceApplicationJars) {
	                                  SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
	                                      "JarLoader#loadClassData resolving defined class " + c.getClass().getCanonicalName());
	                                }
					resolveClass(c);
				}
			    } catch(Throwable t) {
                                if (GemFireXDUtils.TraceApplicationJars) {
                                  SanityManager
                                      .DEBUG_PRINT(
                                          GfxdConstants.TRACE_APP_JARS,
                                          "JarLoader#loadClassData error while defining or resolving class from stream for "
                                              + className, t);
                                }
                                throw new IOException(
                                    "JarLoader#loadClassData error while defining or resolving class from stream for "
                                        + className, t);
			    }
			}
			return c;

		}
	}

	Class checkLoaded(String className, boolean resolve) {
		if (updateLoader == null)
			return null;

		Class c = findLoadedClass(className);
		if ((c != null) && resolve) {
			resolveClass(c);
		}
		return c;
	}

    /**
     * Set this loader to be invaid so that it will not
     * resolve any classes or resources.
     *
     */
	void setInvalid() {
		updateLoader = null;
        if (jar != null) {
            try {
                jar.close();
            } catch (IOException ioe) {
            }
            jar = null;

        }
        isStream = false;
	}

// GemStone changes BEGIN
	boolean isInvalid() {
	  return updateLoader == null;
	}
// GemStone changes END
	/*
	** Routines to get an InputStream for a namedResource
	*/

	/**
		Get a stream for a resource directly from a JarFile.
		In this case we can safely return the stream directly.
		It's a new stream set up by the zip code to read just
		the contents of this entry.
	*/
	private InputStream getRawStream(String name) {

		try {
			JarEntry e = jar.getJarEntry(name);
			if (e == null)
				return null;

			return jar.getInputStream(e);
		} catch (IOException ioe) {
			return null;
		}
	}

	/**
		Get a stream from a zip file that is itself a stream.
        We copy to the contents to a byte array and return a
        stream around that to the caller. Though a copy is
        involved it has the benefit of:
        <UL>
        <LI> Isolating the application from the JarInputStream, thus
        denying any possibility of the application reading more of the
        jar that it should be allowed to. E.g. the contents class files are not
        exposed through getResource.
        <LI> Avoids any possibility of the application holding onto
        the open stream beyond shutdown of the database, thus leading
        to leaked file descriptors or inability to remove the jar.
        </UL>
	*/
	private InputStream getRawStream(InputStream in, String name) { 

		JarInputStream jarIn = null;
		try {
			jarIn = new JarInputStream(in);

		    JarEntry e;
			while ((e = jarIn.getNextJarEntry()) != null) {

				if (e.getName().equals(name)) {
                    int size = (int) e.getSize();
                    if (size == -1)
                    {
                        // unknown size so just pick a good buffer size.
                        size = 8192;
                    }
                    return AccessibleByteArrayOutputStream.copyStream(jarIn, size);
				}
			}

		} catch (IOException ioe) {
            // can't read the jar file just assume it doesn't exist.
		}
        finally {
            if (jarIn != null) {
                try {
                    jarIn.close();
                } catch (IOException ioe2) {
                }
            }            
        }
		return null;
	}
    
    /**
     * Read the raw data for the class file format
     * into a byte array that can be used for loading the class.
     * If this is a signed class and it has been compromised then
     * a SecurityException will be thrown.
     */
    byte[] readData(JarEntry ze, InputStream in, String className)
            throws IOException {

        try {
            int size = (int) ze.getSize();

            if (size != -1) {
                byte[] data = new byte[size];

                InputStreamUtil.readFully(in, data, 0, size);

                return data;
            }

            // unknown size
            byte[] data = new byte[1024];
            ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
            int r;
            while ((r = in.read(data)) != -1) {
                os.write(data, 0, r);
            }

            data = os.toByteArray();
            return data;
        } catch (SecurityException se) {
            throw handleException(se, className);
        }
    }

    /**
     * Validate the security certificates (signers) for the class data.
     */
    private Certificate[] getSigners(String className, JarEntry je) throws IOException {

        try {
            Certificate[] list = je.getCertificates();
            if ((list == null) || (list.length == 0)) {
                return null;
            }

            for (int i = 0; i < list.length; i++) {
                if (!(list[i] instanceof X509Certificate)) {
                    String msg = MessageService.getTextMessage(
                            MessageId.CM_UNKNOWN_CERTIFICATE, className,
                            getJarName());

                    throw new SecurityException(msg);
                }

                X509Certificate cert = (X509Certificate) list[i];

                cert.checkValidity();
            }

            return list;

        } catch (GeneralSecurityException gse) {
            // convert this into an unchecked security
            // exception. Unchecked as eventually it has
            // to pass through a method that's only throwing
            // ClassNotFoundException
            throw handleException(gse, className);
        }
        
    }

    /**
     * Provide a SecurityManager with information about the class name
     * and the jar file.
     */
    private SecurityException handleException(Exception e, String className) {
        String msg = MessageService.getTextMessage(
                MessageId.CM_SECURITY_EXCEPTION, className, getJarName(), e
                        .getLocalizedMessage());
        return new SecurityException(msg);
    }
    
    /**
     * Return the jar name if toString() is called
     * on this class loader.
     */
    @Override
    public String toString()
    {
        return getJarName() + ":" + super.toString();
    }

  // GemStone changes BEGIN
  // Provider interface methods
  public DependableFinder getDependableFinder() {
    return new DDdependableFinder(StoredFormatIds.JAR_DEPENDENTS_FINDER_V01_ID);
  }

  public String getObjectName() {
    // TODO Auto-generated method stub
    return null;
  }

  public UUID getObjectID() {
    return this.oid;
  }

  public boolean isDescriptorPersistent() {
    return true;
  }

  public String getClassType() {
    // TODO Auto-generated method stub
    return null;
  }

  private int invalidAction;

  private List deps;

  public void invalidateDependents(int action) {
    DependencyManager dd = Misc.getMemStore().getDatabase().getDataDictionary()
        .getDependencyManager();
    try {
      if (action == DependencyManager.DROP_JAR) {
        List dependencies = ((BasicDependencyManager)dd).getDependents(this);
        for (Object o : dependencies) {
          Dependent d = ((Dependency)o).getDependent();
          if (d instanceof GfxdAsyncEventListenerDescriptor) {
            throw StandardException.newException(
                SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, "drop jar",
                getJarNameAsString(),
                ((GfxdAsyncEventListenerDescriptor)d).getSenderId(), "");
          }
        }
      }
      LanguageConnectionContext lcc = Misc.getLanguageConnectionContext();
      this.invalidAction = action;
      this.deps = dd.invalidateFor(this, action, lcc);
    } catch (StandardException e) {
      throw new GemFireXDRuntimeException(e);
    }
  }

  public void refreshDependents() throws StandardException {
    if (this.invalidAction != DependencyManager.REPLACE_JAR ) {
      return;
    }
    if (this.deps != null) {
      for (Object o : this.deps) {
        Dependent d = ((Dependency)o).getDependent();
        d.makeInvalid(this.invalidAction, null);
      }
    }
  }
  // GemStone changes END
}
