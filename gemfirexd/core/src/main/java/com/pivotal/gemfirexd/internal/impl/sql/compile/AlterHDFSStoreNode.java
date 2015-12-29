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

import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSCompactionConfigMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSEventQueueAttributesMutator;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreMutatorImpl;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * @author Ashvin
 */

public final class AlterHDFSStoreNode extends DDLStatementNode {

  private String hdfsStoreName;
  
  private HDFSStoreMutator mutator;
  
  private Map<Object, Object> otherAttribs;
  
  public static final String BATCHSIZE = "batchsize";
  public static final String BATCHTIMEINTERVAL = "batchtimeinterval";    
  public static final String MINORCOMPACT = "minorcompact";
  public static final String MAJORCOMPACT = "majorcompact";
  public static final String MAXINPUTFILESIZE = "maxinputfilesize";
  public static final String MININPUTFILECOUNT = "mininputfilecount";
  public static final String MAXINPUTFILECOUNT = "maxinputfilecount";
  public static final String MINORCOMPACTIONTHREADS = "minorcompactionthreads"; 
  public static final String MAJORCOMPACTIONINTERVAL = "majorcompactioninterval";
  public static final String MAJORCOMPACTIONTHREADS = "majorcompactionthreads";
  public static final String MAXWRITEONLYFILESIZE = "maxwriteonlyfilesize";
  public static final String WRITEONLYFILEROLLOVERINTERVAL = "writeonlyfilerolloverinterval";
  public static final String PURGEINTERVAL = "purgeinterval";

  
  public AlterHDFSStoreNode() {
  }

  @Override
  public void init(Object arg1, Object arg2)
      throws StandardException {
    // only alter HDFS stores on data store nodes
    hdfsStoreName = SharedUtils.SQLToUpperCase((String) arg1);
    otherAttribs = (Map<Object, Object>) arg2;
    
    mutator = new HDFSStoreMutatorImpl();
    HDFSCompactionConfigMutator compactionMutator = mutator.getCompactionConfigMutator();
    HDFSEventQueueAttributesMutator qMutator = mutator.getHDFSEventQueueAttributesMutator();

    final Iterator<Map.Entry<Object, Object>> entryItr = otherAttribs.entrySet().iterator();
    
    while (entryItr.hasNext()) {
      Map.Entry<Object, Object> entry = entryItr.next();
      String key = (String)entry.getKey();
      Object vn = entry.getValue();
      if (key.equalsIgnoreCase(BATCHSIZE)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        qMutator.setBatchSizeMB(ncn.getValue().getInt());        
      } else if (key.equalsIgnoreCase(BATCHTIMEINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        qMutator.setBatchTimeInterval(ncn.getValue().getInt());
      } else if (key.equalsIgnoreCase(MINORCOMPACT)) {
        String val = (String) vn;
        if (val.equalsIgnoreCase("true")) {
          compactionMutator.setAutoCompaction(Boolean.TRUE.booleanValue());
        } else if (val.equalsIgnoreCase("false")) {
          compactionMutator.setAutoCompaction(Boolean.FALSE.booleanValue());
        } else {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MINORCOMPACT), val);
        }
      } else if (key.equalsIgnoreCase(MAJORCOMPACT)) {
        String val = (String) vn;
        if (val.equalsIgnoreCase("true")) {
          compactionMutator.setAutoMajorCompaction(Boolean.TRUE.booleanValue());
        } else if (val.equalsIgnoreCase("false")) {
          compactionMutator.setAutoMajorCompaction(Boolean.FALSE.booleanValue());
        } else {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MAJORCOMPACT), val);
        }
      }
      else if (key.equalsIgnoreCase(MAXINPUTFILESIZE)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        compactionMutator.setMaxInputFileSizeMB(ncn.getValue().getInt());
      } else if (key.equalsIgnoreCase(MININPUTFILECOUNT)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        compactionMutator.setMinInputFileCount(ncn.getValue().getInt());
      } else if (key.equalsIgnoreCase(MAXINPUTFILECOUNT)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        compactionMutator.setMaxInputFileCount(ncn.getValue().getInt());
      } else if (key.equalsIgnoreCase(MINORCOMPACTIONTHREADS)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        int concurrency = ncn.getValue().getInt();
        if (concurrency <= 0) {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MINORCOMPACTIONTHREADS), concurrency);
        }
        compactionMutator.setMaxThreads(concurrency);
      } else if (key.equalsIgnoreCase(MAJORCOMPACTIONINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        compactionMutator.setMajorCompactionIntervalMins(ncn.getValue().getInt());
      } else if (key.equalsIgnoreCase(MAJORCOMPACTIONTHREADS)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        int concurrency = ncn.getValue().getInt();
        if (concurrency <= 0) {
          throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,
              StringUtil.SQLToUpperCase(MAJORCOMPACTIONTHREADS), concurrency);
        }
        compactionMutator.setMajorCompactionMaxThreads(concurrency);
      } else if (key.equalsIgnoreCase(MAXWRITEONLYFILESIZE)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        mutator.setMaxFileSize(ncn.getValue().getInt());
      } else if (key.equalsIgnoreCase(WRITEONLYFILEROLLOVERINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        mutator.setFileRolloverInterval(ncn.getValue().getInt());
      } else if (key.equalsIgnoreCase(PURGEINTERVAL)) {
        NumericConstantNode ncn = (NumericConstantNode) vn;
        compactionMutator.setOldFilesCleanupIntervalMins(ncn.getValue().getInt());
      }
    }
  }

  @Override
  public String statementToString() {
    return "ALTER HDFSSTORE";
  }

  @Override
  public ConstantAction makeConstantAction() {
    return getGenericConstantActionFactory().getAlterHDFSStoreConstantAction(
        hdfsStoreName, mutator);
  }
}
