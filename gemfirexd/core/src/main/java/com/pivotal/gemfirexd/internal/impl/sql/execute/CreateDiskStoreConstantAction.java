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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.operations.DiskStoreCreateOperation;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.NumericConstantNode;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public class CreateDiskStoreConstantAction extends DDLConstantAction {

  final private String diskStoreName;

  final private List<String> dirPaths;

  final private List<Integer> dirSizes;

  final private Map otherAttribs;

  public static final String REGION_PREFIX_FOR_CONFLATION =
      "__GFXD_INTERNAL_DISKSTORE_";

  CreateDiskStoreConstantAction(String diskStoreName, List<String> dirPaths,
      List<Integer> dirSizes, Map otherAttribs) {
    this.diskStoreName = diskStoreName;
    this.dirPaths = dirPaths;
    this.dirSizes = dirSizes;
    this.otherAttribs = otherAttribs;
  }

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP DISKSTORE statements.
  @Override
  public final String getSchemaName() {
    // Disk stores have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  @Override
  public final String getTableName() {
    return REGION_PREFIX_FOR_CONFLATION + diskStoreName;
  }

  // OBJECT METHODS

  @Override
  public String toString() {
    return constructToString("CREATE DISKSTORE ", diskStoreName);
  }
  
  // Is this filename valid?
  public static boolean isFilenameValid(String file) {
   // Illegal characters are
   //  asterisk
   //  question mark
   //  greater-than/less-than
   //  pipe character
   //  semicolon
   // Some are legal on Linux, but trying to create DISKSTORE "*" crashes anyway
   // So make this more restrictive and same as Windows restrictions
   Pattern illegalCharsPattern = Pattern.compile("[*?<>|;]");
   Matcher matcher = illegalCharsPattern.matcher(file);
   if (matcher.find())
   {
     return false;
   }
   return true;
  }


  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {
    if (!ServerGroupUtils.isDataStore()) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "Skipping create diskstore for " + diskStoreName + " on JVM of kind "
              + GemFireXDUtils.getMyVMKind());
      return;
    }
    DiskStoreFactory dsf = Misc.getGemFireCache().createDiskStoreFactory();
    GemFireStore store = Misc.getMemStore();
    int numDirs = dirPaths.size();
    StringBuilder dirPathsAndSizes = new StringBuilder();
    if (numDirs > 0) {
      File[] dirs = new File[numDirs];
      int sizes[] = new int[numDirs];
      boolean dirCreated[] = new boolean[numDirs];
      Arrays.fill(dirCreated, false);
      Hashtable<String, String> ht = new Hashtable<String, String>();
      String canonicalPath = null;
      boolean foundExplicitSize = false;
      for (int i = 0; i < numDirs; ++i) {
        String fileStr = dirPaths.get(i);
        fileStr = store.generatePersistentDirName(fileStr);
        // Verify that this directory name is legal
        if (!isFilenameValid(fileStr))
        {
          throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
              "Directory name " + dirPaths.get(i) + " was not valid");        
        }

        dirs[i] = new File(fileStr).getAbsoluteFile();
        dirPathsAndSizes.append(dirs[i].getAbsolutePath());
        dirCreated[i] = dirs[i].mkdir();

        /* Detect whether the user has entered duplicate directory names.
        * If yes, error out. For example, for CREATE DISKSTORE X ('DIR1', 
        * 'DIR1')
        * Note: Ideally we should error out even before the directories are 
        * created. However, it may not work in the case of symbolic links.
        * For example,  CREATE DISKSTORE X ('DIR1', 'SYM_LINK_TO_DIR1').
        * In this case, if DIR1 does not exist, getCannonicalPath()
        * will return different paths for DIR1 and the sym link and we 
        * will not detect duplicate paths. Once DIR1 is created, it 
        * returns the same canonical path for sym link and DIR1.
        */
        try {
          canonicalPath = dirs[i].getCanonicalPath();
        } catch (IOException ie) {
          throw GemFireXDRuntimeException.newRuntimeException(
              "Unexpected exception while accessing the directory "
                  + dirs[i].toString(), ie);
        }
        if (ht.put(canonicalPath, canonicalPath) != null) {
          // Duplicate directory found. Remove only the
          // directories that we have created and error out
          for (int k = 0; k <= i; ++k) {
            if (dirCreated[k]) {
              dirs[k].delete();
            }
          }
          throw StandardException.newException(
              SQLState.DISKSTORE_DUPLICATE_DIR_FOUND, canonicalPath);
        }

        int temp = dirSizes.get(i).intValue();
        // assign only if temp > 0;
        if (temp > 0) {
          sizes[i] = temp;
          dirPathsAndSizes.append("(");
          dirPathsAndSizes.append(temp);
          dirPathsAndSizes.append(")");
          foundExplicitSize = true;
        }
        dirPathsAndSizes.append(",");
      }
      dirPathsAndSizes.deleteCharAt(dirPathsAndSizes.length() - 1);
      if (foundExplicitSize) {
        dsf.setDiskDirsAndSizes(dirs, sizes);
      }
      else {
        dsf.setDiskDirs(dirs);
      }
    }
    else {
      String defaultDir = store.generatePersistentDirName(null);
      File dirs[] = new File[] { new File(defaultDir).getAbsoluteFile() };
      dirPathsAndSizes.append(dirs[0].getAbsolutePath());
      String fileNameToCheck = defaultDir + "\\" + diskStoreName;
      // Verify that this directory name is legal
      if (!isFilenameValid(fileNameToCheck))
      {
        throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
            "Disk Store name " + diskStoreName + " was not valid");        
      }
      dirs[0].mkdir();
      dsf.setDiskDirs(dirs);
    }
    Iterator<Map.Entry> entryItr = otherAttribs.entrySet().iterator();
    while (entryItr.hasNext()) {
      Map.Entry entry = entryItr.next();
      String key = (String)entry.getKey();
      Object vn = entry.getValue();
      try {
        if (key.equalsIgnoreCase("maxlogsize")) {
          NumericConstantNode ncn = (NumericConstantNode)vn;
          dsf.setMaxOplogSize(ncn.getValue().getLong());
        }
        else if (key.equalsIgnoreCase("compactionthreshold")) {
          NumericConstantNode ncn = (NumericConstantNode)vn;
          dsf.setCompactionThreshold(ncn.getValue().getInt());
        }
        else if (key.equalsIgnoreCase("timeinterval")) {
          NumericConstantNode ncn = (NumericConstantNode)vn;
          dsf.setTimeInterval(ncn.getValue().getInt());
        }
        else if (key.equalsIgnoreCase("writebuffersize")) {
          NumericConstantNode ncn = (NumericConstantNode)vn;
          dsf.setWriteBufferSize(ncn.getValue().getInt());
        }
        else if (key.equalsIgnoreCase("queuesize")) {
          NumericConstantNode ncn = (NumericConstantNode)vn;
          dsf.setQueueSize(ncn.getValue().getInt());
        }
        else if (key.equalsIgnoreCase("autocompact")) {
          String val = (String)vn;
          if (val.equalsIgnoreCase("true")) {
            dsf.setAutoCompact(Boolean.TRUE.booleanValue());
          }
          else if (val.equalsIgnoreCase("false")) {
            dsf.setAutoCompact(Boolean.FALSE.booleanValue());
          }
          else {
            // throw exception
          }
        }
        else if (key.equalsIgnoreCase("allowforcecompaction")) {
          String val = (String)vn;
          if (val.equalsIgnoreCase("true")) {
            dsf.setAllowForceCompaction(Boolean.TRUE.booleanValue());
          }
          else if (val.equalsIgnoreCase("false")) {
            dsf.setAllowForceCompaction(Boolean.FALSE.booleanValue());
          }
          else {
            // throw exception
          }
        }
      } catch (IllegalArgumentException e) {
        // If some value was invalid, wrap that IllegalArgumentException
        // in a SQL Exception for consumption
        throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
            "Value for " + key + " was not valid");
      }
    }
    DiskStoreCreateOperation startOp = new DiskStoreCreateOperation(dsf,
        diskStoreName, dirPathsAndSizes.toString());
    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    GemFireTransaction gft = (GemFireTransaction)lcc.getTransactionExecute();
    gft.logAndDo(startOp);
  }

}
