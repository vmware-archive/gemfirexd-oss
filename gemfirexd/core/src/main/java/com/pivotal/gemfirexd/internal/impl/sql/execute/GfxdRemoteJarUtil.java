/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.JarUtil

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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.FileInfoDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.FileResource;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil;
import com.pivotal.gemfirexd.internal.impl.services.reflect.JarLoader;

public class GfxdRemoteJarUtil {

  // State passed in by the caller
  private final LanguageConnectionContext lcc;

  private final String schemaName;

  private final String sqlName;

  // Derived state

  private final FileResource fr;

  private final DataDictionary dd;

  private final DataDescriptorGenerator ddg;

  private long newId;

  private long oldId;

  //
  // State derived from the caller's context
  private GfxdRemoteJarUtil(LanguageConnectionContext lcc, String schemaName,
      String sqlName) throws StandardException {
    this.schemaName = schemaName;
    this.sqlName = sqlName;

    this.lcc = lcc;
    fr = lcc.getTransactionExecute().getFileHandler();
    dd = lcc.getDataDictionary();
    ddg = dd.getDataDescriptorGenerator();
  }

  /**
   * install a jar file to the current connection's database.
   * 
   * @param schemaName
   *          the name for the schema that holds the jar file.
   * @param sqlName
   *          the sql name for the jar file.
   * @param externalPath
   *          the path for the jar file to add.
   * @return The generationId for the jar file we add coming from remote node.
   * @exception StandardException
   *              Opps
   */
  public static long install(LanguageConnectionContext lcc, String schemaName,
      String sqlName, long id) throws StandardException {
    if (GemFireXDUtils.TraceApplicationJars) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
          "GfxdRemoteJarUtil: install called for " + schemaName + ":" + sqlName
              + " id is " + id, new Exception());
    }
    GfxdRemoteJarUtil jutil = new GfxdRemoteJarUtil(lcc, schemaName, sqlName);
    jutil.setId(id);
    return jutil.add();
  }

  private void setId(long id) {
    this.newId = id;
  }

  private void setOldId(long id) {
    this.oldId = id;
  }

  /**
   * Add a jar file to the current connection's database.
   * 
   * <P>
   * The reason for adding the jar file in this private instance method is that
   * it allows us to share set up logic with drop and replace.
   * 
   * @exception StandardException
   *              Opps
   */
  private long add() throws StandardException {
    //
    // Like create table we say we are writing before we read the dd
    dd.startWriting(lcc);
    FileInfoDescriptor fid = getInfo();
    if (fid != null)
      throw StandardException.newException(
          SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT, fid
              .getDescriptorType(), sqlName, fid.getSchemaDescriptor()
              .getDescriptorType(), schemaName);

    SchemaDescriptor sd = getSchemaDescriptor();
    try {
      if (GemFireXDUtils.TraceApplicationJars) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
            "GfxdRemoteJarUtil: add notifying classpath change for "
                + schemaName + ":" + sqlName);
      }
      notifyClassPathChange(this.schemaName + "." + this.sqlName,
          ClassFactory.JAR_ADD_OP_TYPE);
      notifyLoader(false);
      dd.invalidateAllSPSPlans();
      String fullName = schemaName + "." + sqlName;
      setJar(fullName, this.newId, true, 0L);
      fid = ddg.newFileInfoDescriptor(/*DJD*/null, sd, sqlName, this.newId);
      dd.addDescriptor(fid, sd, DataDictionary.SYSFILES_CATALOG_NUM, false,
          lcc.getTransactionExecute());
      return this.newId;
    } finally {
      notifyLoader(true);
    }
  }

  /**
   * Drop a jar file from the current connection's database.
   * 
   * @param schemaName
   *          the name for the schema that holds the jar file.
   * @param sqlName
   *          the sql name for the jar file.
   * 
   * @exception StandardException
   *              Opps
   */
  public static void drop(LanguageConnectionContext lcc, String schemaName,
      String sqlName, long id) throws StandardException {
    if (GemFireXDUtils.TraceApplicationJars) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
          "GfxdRemoteJarUtil: drop called for " + schemaName + ":" + sqlName
              + " with id " + id);
    }
    GfxdRemoteJarUtil jutil = new GfxdRemoteJarUtil(lcc, schemaName, sqlName);
    jutil.setOldId(id);
    jutil.drop();
  }

  /**
   * Drop a jar file from the current connection's database.
   * 
   * <P>
   * The reason for dropping the jar file in this private instance method is
   * that it allows us to share set up logic with add and replace.
   * 
   * @exception StandardException
   *              Opps
   */
  private void drop() throws StandardException {
    //
    // Like create table we say we are writing before we read the dd
    dd.startWriting(lcc);
    FileInfoDescriptor fid = getInfo();
    if (fid == null)
      throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST,
          sqlName, schemaName);

    String dbcp_s = PropertyUtil.getServiceProperty(
        lcc.getTransactionExecute(), Property.DATABASE_CLASSPATH);
    if (dbcp_s != null) {
      String[][] dbcp = IdUtil.parseDbClassPath(dbcp_s);
      boolean found = false;
      //
      // Look for the jar we are dropping on our database classpath.
      // We don't concern ourselves with 3 part names since they may
      // refer to a jar file in another database and may not occur in
      // a database classpath that is stored in the propert congomerate.
      for (int ix = 0; ix < dbcp.length; ix++)
        if (dbcp.length == 2 && dbcp[ix][0].equals(schemaName)
            && dbcp[ix][1].equals(sqlName))
          found = true;
      if (found)
        throw StandardException.newException(
            SQLState.LANG_CANT_DROP_JAR_ON_DB_CLASS_PATH_DURING_EXECUTION,
            IdUtil.mkQualifiedName(schemaName, sqlName), dbcp_s);
    }

    try {

      if (GemFireXDUtils.TraceApplicationJars) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
            "GfxdRemoteJarUtil: drop notifying classpath change for "
                + schemaName + ":" + sqlName);
      }
      // force refresh of JarLoader
      closeOldJarLoader(this.schemaName, this.sqlName);
      notifyClassPathChange(this.schemaName + "." + this.sqlName,
          ClassFactory.JAR_REMOVE_OP_TYPE);
      notifyLoader(false);
      dd.invalidateAllSPSPlans();
      DependencyManager dm = dd.getDependencyManager();
      dm.invalidateFor(fid, DependencyManager.DROP_JAR, lcc);

      dd.dropFileInfoDescriptor(fid);

      // pass the fully qualified name else the lookup in the table
      // will fail (#44043)
      fr.remove(this.schemaName + "." + this.sqlName, this.oldId, true);
    } finally {
      notifyLoader(true);
    }
  }

  /**
   * Replace a jar file from the current connection's database with the content
   * of an external file.
   * 
   * @param schemaName
   *          the name for the schema that holds the jar file.
   * @param sqlName
   *          the sql name for the jar file.
   * @param externalPath
   *          the path for the jar file to add.
   * @return The new generationId for the jar file we replace.
   * @exception StandardException
   *              Opps
   */
  public static long replace(LanguageConnectionContext lcc, String schemaName,
      String sqlName, long newId, long oldId) throws StandardException {
    if (GemFireXDUtils.TraceApplicationJars) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
          "GfxdRemoteJarUtil: replace called for " + schemaName + ":" + sqlName
              + " with new id " + newId + " old id " + oldId);
    }
    GfxdRemoteJarUtil jutil = new GfxdRemoteJarUtil(lcc, schemaName, sqlName);
    jutil.setId(newId);
    jutil.setOldId(oldId);
    jutil.replace();
    return newId;
  }

  /**
   * Replace a jar file in the current connection's database with the content of
   * an external file.
   * 
   * <P>
   * The reason for adding the jar file in this private instance method is that
   * it allows us to share set up logic with add and drop.
   * 
   * @param is
   *          An input stream for reading the new content of the jar file.
   * @exception StandardException
   *              Opps
   */
  private long replace() throws StandardException {
    //
    // Like create table we say we are writing before we read the dd
    dd.startWriting(lcc);

    //
    // Temporarily drop the FileInfoDescriptor from the data dictionary.
    FileInfoDescriptor fid = getInfo();
    if (fid == null)
      throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST,
          sqlName, schemaName);

    JarLoader oldLoader = null;
    boolean loaderNotified = false;
    try {
      // force refresh of JarLoader
      oldLoader = closeOldJarLoader(this.schemaName, this.sqlName);
      // disable loads from this jar
      notifyLoader(false);
      dd.invalidateAllSPSPlans();
      dd.dropFileInfoDescriptor(fid);
      String fullName = schemaName + "." + sqlName;
      setJar(fullName, this.newId, false, this.oldId);
      // Gemstone changes BEGIN
      notifyClassPathChange(this.schemaName + "." + this.sqlName, ClassFactory.JAR_REPLACE_OP_TYPE);
      // Gemstone changes END
      FileInfoDescriptor fid2 = ddg.newFileInfoDescriptor(fid.getUUID(),
          fid.getSchemaDescriptor(), sqlName, this.newId);
      dd.addDescriptor(fid2, fid.getSchemaDescriptor(),
          DataDictionary.SYSFILES_CATALOG_NUM, false,
          lcc.getTransactionExecute());
      // reenable class loading from this jar
      notifyLoader(true);
      loaderNotified = true;
      oldLoader.refreshDependents();
      return this.newId;

    } finally {
      // reenable class loading from this jar
      if (!loaderNotified) {
        notifyLoader(true);
      }
    }
  }

  /**
   * Get the FileInfoDescriptor for the Jar file or null if it does not exist.
   * 
   * @exception StandardException
   *              Ooops
   */
  private FileInfoDescriptor getInfo() throws StandardException {
    return dd.getFileInfoDescriptor(getSchemaDescriptor(), sqlName);
  }

  private SchemaDescriptor getSchemaDescriptor() throws StandardException {
    // create schema descriptor implicitly in replay since now we push
    // the install_jar messages to the top at which point the schema may
    // not have been created
    GemFireStore memStore = Misc.getMemStoreBooting();
    SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null,
        !memStore.initialDDLReplayInProgress());
    if (sd == null || sd.getUUID() == null) {
      UUID tmpSchemaId = dd.getUUIDFactory().createUUID();
      TransactionController tc = lcc.getTransactionExecute();
      sd = ddg.newSchemaDescriptor(schemaName, lcc.getAuthorizationId(),
          tmpSchemaId);
      dd.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false,
          tc);
      // Create schema in GemFireStore
      memStore.createSchemaRegion(this.schemaName, tc);
    }
    return sd;
  }

  private void notifyLoader(boolean reload) throws StandardException {
    ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
    cf.notifyModifyJar(reload);
  }

  private void notifyClassPathChange(String name, int jar_opType) {
    ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
    cf.notifyModifyClasspath(name, jar_opType);
  }

  private JarLoader closeOldJarLoader(String schemaName, String sqlName) {
    ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
    return cf.closeJarLoader(schemaName, sqlName);
  }

  /**
   * Copy the jar from the externally obtained input stream into the database
   * 
   * @param jarExternalName
   *          Name of jar with database structure.
   * @param contents
   *          Contents of jar file.
   * @param add
   *          true to add, false to replace
   * @param currentGenerationId
   *          generation id of existing version, ignored when adding.
   */
  private long setJar(final String jarExternalName, final long newId,
      final boolean add, final long oldId) throws StandardException {
    try {
      if (GemFireXDUtils.TraceApplicationJars) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_APP_JARS,
            "GfxdRemoteJarUtil: set jar called for " + jarExternalName
                + " with new id " + newId + " add is " + add + " old id is "
                + oldId);
      }
      return AccessController.doPrivileged(
          new PrivilegedExceptionAction<Long>() {
            @Override
            public Long run() throws StandardException {
              if (add) {
                fr.add(jarExternalName, null, newId);
              }
              else {
                fr.replace(jarExternalName, oldId, newId, null);
              }
              return Long.valueOf(oldId);
            }
          }).longValue();
    } catch (PrivilegedActionException e) {
      throw (StandardException)e.getException();
    }
  }
}
