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
/**
 * 
 */
package com.pivotal.pxf.plugins.gemfirexd;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.gemfirexd.util.GemFireXDManager;
import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.Row;

/**
 * This is very similar to HdfsDataFragmenter except the use of GemFireXDManager
 * utility.
 */
public class GemFireXDFragmenter  extends Fragmenter {

  private JobConf jobConf;

  private GemFireXDManager gfxdManager;

  /**
   * @param inputData
   * @throws IOException
   */
  public GemFireXDFragmenter(InputData inputData) throws Exception {
    this(inputData, new GemFireXDManager(inputData, true));
  }

  /**
   * @param inputData
   * @param mgr
   * @throws Exception
   */
  public GemFireXDFragmenter(InputData inputData, GemFireXDManager mgr)
      throws Exception {
    super(inputData);
    this.jobConf = new JobConf(new Configuration(), GemFireXDFragmenter.class);

    this.gfxdManager = mgr;
    this.gfxdManager.configureJob(this.jobConf, this.gfxdManager.getHomeDir());

    String msg = this.gfxdManager.verifyUserAttributes();

    if (msg != null && !msg.equals("")) {
      throw new IllegalArgumentException(msg);
    }
  }

  @Override
  public List<Fragment> getFragments() throws IOException {
    InputSplit[] splits;
//    try {
      splits = getSplits();
//    } finally {
//      this.gfxdManager.resetLonerSystemInUse();
//    }

    for (InputSplit split : splits) {
      CombineFileSplit cSplit = (CombineFileSplit)split;
      
      if (cSplit.getLength() > 0L) {
        String filepath = cSplit.getPath(0).toUri().getPath();
        filepath = filepath.substring(1);
        if (this.gfxdManager.getLogger().isDebugEnabled()) {
          this.gfxdManager.getLogger().debug("fragment-filepath " + filepath);
        }
        byte[] data = this.gfxdManager.populateUserData(cSplit);
        this.fragments.add(new Fragment(filepath, cSplit.getLocations(), data));
      }
    }
    return this.fragments;
  }

  private InputSplit[] getSplits() throws IOException {
    InputFormat<Key, Row> inputFormat = this.gfxdManager.getInputFormat();
    try {
      return inputFormat.getSplits(this.jobConf, 1);
    } catch (FileNotFoundException fnfe) {
      throw new FileNotFoundException(
          "Table "
              + this.gfxdManager.getTable()
              + " not found. "
              + "The LOCATION string may contain incorrect value for one or more of the following:"
              + "1. Path to HDFSSTORE (homeDir), 2. Schema name or 3. Table name. "
              + GemFireXDManager.LOCATION_FORMAT);
    }
  }

}
