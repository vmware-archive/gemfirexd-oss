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
package com.pivotal.gemfirexd.internal.impl.sql.compile;

import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * 
 * @author jianxiachen
 *
 */

public final class CreateHDFSStoreNode extends DDLStatementNode {

  private String hdfsStoreName;
  
  private String nameNode;
  
  private HDFSStoreFactory hsf;
  
  private Map<Object, Object> otherAttribs;
  
  public static final String MAXQUEUEMEMORY = "maxqueuememory";
  
  public static final String BATCHSIZE = "batchsize";
  
  public static final String QUEUEPERSISTENT = "queuepersistent";
  
  public static final String DISKSTORENAME = "diskstorename";
  
  public static final String BATCHTIMEINTERVAL = "batchtimeinterval";    
    
  public static final String DISKSYNCHRONOUS = "disksynchronous";
  
  public static final String MINORCOMPACT = "minorcompact";
  
  public static final String MAJORCOMPACT = "majorcompact";

  public static final String MAXINPUTFILESIZE = "maxinputfilesize";

  public static final String MININPUTFILECOUNT = "mininputfilecount";
  
  public static final String MAXINPUTFILECOUNT = "maxinputfilecount";
  
  public static final String MINORCOMPACTIONTHREADS = "minorcompactionthreads"; 
    
  public static final String MAJORCOMPACTIONINTERVAL = "majorcompactioninterval";
  
  public static final String MAJORCOMPACTIONTHREADS = "majorcompactionthreads";
  
  public static final String HOMEDIR = "homedir";
   
  public static final String CLIENTCONFIGFILE = "clientconfigfile";
    
  public static final String BLOCKCACHESIZE = "blockcachesize";

  public static final String MAXWRITEONLYFILESIZE = "maxwriteonlyfilesize";
  
  public static final String WRITEONLYFILEROLLOVERINTERVAL = "writeonlyfilerolloverinterval";
  
  public static final String PURGEINTERVAL = "purgeinterval";
  
  public static final String DISPATCHERTHREADS = "dispatcherthreads";
  
  public CreateHDFSStoreNode() {
  }

  @Override
  public void init(Object arg1, Object arg2, Object arg3)
      throws StandardException {
    // only create HDFS stores on data store nodes
    hdfsStoreName = SharedUtils.SQLToUpperCase((String) arg1);
    nameNode = (String) arg2;
    otherAttribs = (Map<Object, Object>) arg3;
    
    hsf = Misc.getGemFireCache().createHDFSStoreFactory();
    hsf.setName(hdfsStoreName);
    hsf.setNameNodeURL(nameNode);

    HDFSEventQueueAttributesFactory hqf= new HDFSEventQueueAttributesFactory();
    HDFSStoreFactory.HDFSCompactionConfigFactory hcf = hsf.createCompactionConfigFactory(null);
    
    final Iterator<Map.Entry<Object, Object>> entryItr = otherAttribs.entrySet()
        .iterator();
    String diskStoreName = null;
    String homeDir = null; 
    boolean isPersistent = false;
    while (entryItr.hasNext()) {
      Map.Entry<Object, Object> entry = entryItr.next();
      String key = (String)entry.getKey();
      Object vn = entry.getValue();
      if (key.equalsIgnoreCase(MAXQUEUEMEMORY)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;        
        hqf.setMaximumQueueMemory(ncn.getValue().getInt());
      }
      else if (key.equalsIgnoreCase(BATCHSIZE)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hqf.setBatchSizeMB(ncn.getValue().getInt());        
      }
      else if (key.equalsIgnoreCase(BATCHTIMEINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hqf.setBatchTimeInterval(ncn.getValue().getInt());
      }else if (key.equalsIgnoreCase(DISPATCHERTHREADS)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hqf.setDispatcherThreads(ncn.getValue().getInt());
      }      
      else if (key.equalsIgnoreCase(QUEUEPERSISTENT)) {
        String val = (String) vn;
        if (val.equalsIgnoreCase("true")) {
          isPersistent = true;
          hqf.setPersistent(true);
        }
        else if (val.equalsIgnoreCase("false")) {
          hqf.setPersistent(false);
        }
        else {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(QUEUEPERSISTENT), val);
        }
      }
      else if (key.equalsIgnoreCase(DISKSTORENAME)) {
        // remove local persistence for HDFS loner mode
        if (!Misc.getMemStoreBooting().isHadoopGfxdLonerMode()) {
          diskStoreName = (String)vn;
          hqf.setDiskStoreName(diskStoreName);
        }
      }
      else if (key.equalsIgnoreCase(DISKSYNCHRONOUS)) {
        String val = (String) vn;
        if (val.equalsIgnoreCase("true")) {
          hqf.setDiskSynchronous(Boolean.TRUE.booleanValue());
        }
        else if (val.equalsIgnoreCase("false")) {
          hqf.setDiskSynchronous(Boolean.FALSE.booleanValue());
        }
        else {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(DISKSYNCHRONOUS), val);
        }
      }
      else if (key.equalsIgnoreCase(MINORCOMPACT)) {
        String val = (String) vn;
        if (val.equalsIgnoreCase("true")) {
          hcf.setAutoCompaction(Boolean.TRUE.booleanValue());
        }
        else if (val.equalsIgnoreCase("false")) {
          hcf.setAutoCompaction(Boolean.FALSE.booleanValue());
        }
        else {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MINORCOMPACT), val);
        }
      }
      else if (key.equalsIgnoreCase(MAJORCOMPACT)) {
        String val = (String) vn;
        if (val.equalsIgnoreCase("true")) {
          hcf.setAutoMajorCompaction(Boolean.TRUE.booleanValue());
        }
        else if (val.equalsIgnoreCase("false")) {
          hcf.setAutoMajorCompaction(Boolean.FALSE.booleanValue());
        }
        else {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MAJORCOMPACT), val);
        }
      }
      else if (key.equalsIgnoreCase(MAXINPUTFILESIZE)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hcf.setMaxInputFileSizeMB(ncn.getValue().getInt());
      }
      else if (key.equalsIgnoreCase(MININPUTFILECOUNT)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hcf.setMinInputFileCount(ncn.getValue().getInt());
      }
      else if (key.equalsIgnoreCase(MAXINPUTFILECOUNT)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hcf.setMaxInputFileCount(ncn.getValue().getInt());
      }
      else if (key.equalsIgnoreCase(MINORCOMPACTIONTHREADS)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        int concurrency = ncn.getValue().getInt();
        if (concurrency <= 0) {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MINORCOMPACTIONTHREADS), concurrency);
        }
        hcf.setMaxThreads(concurrency);
      }
      else if (key.equalsIgnoreCase(MAJORCOMPACTIONINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hcf.setMajorCompactionIntervalMins(ncn.getValue().getInt());
      }
      else if (key.equalsIgnoreCase(MAJORCOMPACTIONTHREADS)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        int concurrency = ncn.getValue().getInt();
        if (concurrency <= 0) {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MAJORCOMPACTIONTHREADS), concurrency);
        }
        hcf.setMajorCompactionMaxThreads(concurrency);
      }
      else if (key.equalsIgnoreCase(HOMEDIR)) {
        homeDir = (String) vn;
      }
      else if (key.equalsIgnoreCase(CLIENTCONFIGFILE)) {
        hsf.setHDFSClientConfigFile((String) vn);
      }
      else if (key.equalsIgnoreCase(BLOCKCACHESIZE)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hsf.setBlockCacheSize(ncn.getValue().getFloat());
      }
      else if (key.equalsIgnoreCase(MAXWRITEONLYFILESIZE)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hsf.setMaxFileSize(ncn.getValue().getInt());
      }
      else if (key.equalsIgnoreCase(WRITEONLYFILEROLLOVERINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hsf.setFileRolloverInterval(ncn.getValue().getInt());
      }
      else if (key.equalsIgnoreCase(PURGEINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        hcf.setOldFilesCleanupIntervalMins(ncn.getValue().getInt());
      }
      
    }
    // set default disk store if not specified
    if (isPersistent && diskStoreName == null
        && !Misc.getMemStoreBooting().isHadoopGfxdLonerMode()) {
      hqf.setDiskStoreName(GfxdConstants.GFXD_DEFAULT_DISKSTORE_NAME);
    }
    if (Misc.getMemStoreBooting().isHadoopGfxdLonerMode()){
      hqf.setPersistent(false);
      hqf.setDiskStoreName(null);
    }
    hsf.setHDFSEventQueueAttributes(hqf.create());
    hsf.setHDFSCompactionConfig(hcf.create());
    
    if (homeDir == null) {
      homeDir = hdfsStoreName;
    }
    hsf.setHomeDir(Misc.getMemStore().generateHdfsStoreDirName(homeDir));
  }

  @Override
  public String statementToString() {
    return "CREATE HDFSSTORE";
  }

  @Override
  public ConstantAction makeConstantAction() {
                return  getGenericConstantActionFactory().getCreateHDFSStoreConstantAction(
                                hdfsStoreName, hsf);
  }
}
