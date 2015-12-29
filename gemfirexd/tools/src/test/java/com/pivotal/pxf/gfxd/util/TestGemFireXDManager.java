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
package com.pivotal.pxf.gfxd.util;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.gfxd.TestInputFormat;
import com.pivotal.pxf.plugins.gemfirexd.util.GemFireXDManager;
import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.Row;

public class TestGemFireXDManager extends GemFireXDManager {

  private InputData inputData = null;

  public TestGemFireXDManager(InputData data) {
    super(null);
    this.inputData = data;
  }

  @Override
  public boolean verifyTableSchema() {
    return true;
  }

  @Override
  public String verifyUserAttributes() {
    return "";
  }

  @Override
  public boolean shutdown(Properties props) {
    return true;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public InputFormat<Key, Row> getInputFormat() {
    return new TestInputFormat(
        GemFireXDManager.deconstructPath(this.inputData.getDataSource())[2]);
  }

  @Override
  public void configureJob(JobConf jobConf, String homeDirs) {
  }

  @Override
  public byte[] populateUserData(CombineFileSplit fileSplit) throws IOException {
    return null;
  }

  @Override
  public void readUserData() throws IOException {
  }

  @Override
  public void resetLonerSystemInUse() {
  }

  /**
   * Unused
   */
  public static class TestInputData {
    private String path = null;
    
    public TestInputData(String path) {
      this.path = path;
    }

    public byte[] getFragmentUserData() {
      return null;
    }
    
    public String path() {
      return this.path;
    }
  }

}


