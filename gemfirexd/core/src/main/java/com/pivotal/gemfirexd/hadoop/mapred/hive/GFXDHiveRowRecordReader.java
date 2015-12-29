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
package com.pivotal.gemfirexd.hadoop.mapred.hive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import com.pivotal.gemfirexd.hadoop.mapred.MapRedRowRecordReader;


/**
 * GFXDHiveRowRecordReader is used in conjunction with {@link GFXDHiveInputFormat}. 
 * 
 * @author hemantb
 *
 */
public class GFXDHiveRowRecordReader extends MapRedRowRecordReader {

  @Override
  protected Path[] getSplitPaths(InputSplit split){
    GFXDHiveSplit fileSplit = (GFXDHiveSplit) split;
    return fileSplit.getPaths();
  }
  
  @Override
  protected long[] getStartOffsets(InputSplit split){
    GFXDHiveSplit fileSplit = (GFXDHiveSplit) split;
    return fileSplit.getStartOffsets();
  }
  
  @Override
  protected long[] getLengths(InputSplit split){
    GFXDHiveSplit fileSplit = (GFXDHiveSplit) split;
    return fileSplit.getLengths();
  }
}
