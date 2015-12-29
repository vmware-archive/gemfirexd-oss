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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * RowInputFormat returns InputSplit objects of type CombineFileSplit. Since 
 * our single split can have multiple files, CombineFileSplit doesn't have a 
 * single path. When hive sees that our split type is CombineFileSplit which 
 * has no path associated with it, it executes some code which is not tested 
 * and throws an exception (JIRA HIVE-5925). 
 * 
 * To overcome the issue in our side, we create a new type of InputFormat - 
 * GFXDHiveInputFormat that returns a split of type GFXDHiveSplit. This split 
 * masquerades a CombineFileSplit as FileSplit to Hive framework to overcome 
 * the Hive bug that is seen for any split other than FileSplit. 
 * i.e. GFXDHiveSplit derives from FileSplit but encapsulates a CombineFileSplit. 
 * Any call to GFXDHiveSplit is passed to the encapsulated CombineFileSplit object. 
 * The functions of the FileSplit that are implemented by the new split are dummy 
 * functions (getPath and getStart) to fool Hive framework. The return values 
 * of these functions are going to be used only for book keeping purposes by 
 * Hive and are not used anywhere.
 * 
 * @author hemantb
 *
 */
public class GFXDHiveSplit extends FileSplit {
  
  private CombineFileSplit combineFileSplit;
  public GFXDHiveSplit() {

  }
  public GFXDHiveSplit(CombineFileSplit fileSplit) {
    this.combineFileSplit = fileSplit;
  }

  public long  getLength()  {
    return this.combineFileSplit.getLength();
  }

  public long getLength(int i) {
    return this.combineFileSplit.getLength(i);
  }

  public long[] getLengths() {
    return this.combineFileSplit.getLengths();
  }

  public String[] getLocations() throws IOException {
    return this.combineFileSplit.getLocations();
  }

  public int  getNumPaths() {
    return this.combineFileSplit.getNumPaths();
  }

  public long getOffset(int i) {
      return this.combineFileSplit.getOffset(i);
  }

  public Path getPath(int i) {
    return this.combineFileSplit.getPath(i);
  }
  @Override
  public Path getPath(){
    // This is a function overriden from FileSplit. 
    // Return a dummy value. This value is only used
    // for book keeping purpses. 
    if (this.combineFileSplit.getNumPaths() > 0)
      return this.combineFileSplit.getPath(0);
    return null;
  }

  @Override
  public long getStart() {
    // This is a function overriden from FileSplit. 
    // Return a dummy value. This value is only used
    // for book keeping purpses. 
    if (this.combineFileSplit.getNumPaths() > 0)
      return this.combineFileSplit.getStartOffsets()[0];
    return 0;
  }


  public Path[] getPaths(){
    return this.combineFileSplit.getPaths();
  }

  public long[] getStartOffsets() {
    return this.combineFileSplit.getStartOffsets();
  }

  public void readFields(DataInput in) throws IOException {
    this.combineFileSplit  = new CombineFileSplit();
    this.combineFileSplit.readFields(in);
  }

  public String toString() {
      return this.combineFileSplit.toString();
  }
  
  public void write(DataOutput out) throws IOException {
      this.combineFileSplit.write(out);
  }

}
