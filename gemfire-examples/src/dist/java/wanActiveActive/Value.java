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
package wanActiveActive;

import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.pdx.WritablePdxInstance;

import java.util.ArrayList;
import java.util.List;

/**
 * Value is the class of objects stored in the cache by this example application.
 * It contains an integer field which the example increments on every update,
 * a string field that shows the origin of the update and a history list of
 * the changes that have been made to it.<p>
 * 
 * Value implements the GemFire PdxSerializable interface for highest serialization
 * performance.  PdxSerializable also allows cache servers to view and manipulate
 * the object without having reference its class in their classpath.
 */
public class Value implements PdxSerializable {
  public static String MODIFICATION_COUNT_FIELD = "modificationCount";
  public static String MODIFICATION_FIELD = "modification";
  public static String HISTORY_FIELD = "history";
  public static String MERGED_FIELD = "merged";
  
  /**
   * no-arg constructor required for serialization
   */
  public Value() { }
  

  /** the number of times the object has been changed, not counting merges */
  private int modificationCount;

  /** a description of the last change to this object */
  private String modification;

  /** a history of the changes to this object */
  private List<String> history;
  
  /** merged is set to true by the conflict resolver if it merged a conflicting value into the cache */
  private boolean merged;
  
  
  /**
   * Construct a Value object
   */
  public Value(int intValue, String string) {
    this.modificationCount = intValue;
    this.modification = string;
    this.history = new ArrayList();
  }
  
  public int getModificationCount() {
    return this.modificationCount;
  }
  
  public void incrementModificationCount() {
    this.modificationCount++;
  }
  
  /**
   * The "merged" conflict resolution option from the hub cache.xml will cause
   * this flag to be set if the WANConflictResolver performs a merge.
   */
  public boolean mergedByConflictResolver() {
    return this.merged;
  }
  
  public void clearMergedFlag() {
    this.merged = false;
  }
  
  /**
   * Set the "modification" field of this object.
   * 
   * @param str the value to set as the new "modification" of this object
   */
  public void setModification(String str) {
    this.modification = str;
  }

  /**
   * @return the "modification" field of this object
   */
  public String getModification() {
    return this.modification;
  }

  
  /**
   * Add a string to the history list of this object
   * 
   * @param str the value to add to the history list
   */
  public void addHistory(String str) {
    this.history.add(0, str);
    if (this.history.size() > 10) {
      this.history = new ArrayList(this.history.subList(0, 10));
    }
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  @Override
  public void toData(PdxWriter writer) {
    writer.writeInt(MODIFICATION_COUNT_FIELD, this.modificationCount);
    writer.writeString(MODIFICATION_FIELD, this.modification);
    writer.writeObject(HISTORY_FIELD, this.history);
    writer.writeBoolean(MERGED_FIELD, this.merged);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  @Override
  public void fromData(PdxReader reader) {
    this.modificationCount = reader.readInt(MODIFICATION_COUNT_FIELD);
    this.modification = reader.readString(MODIFICATION_FIELD);
    this.history = (List)reader.readObject(HISTORY_FIELD);
    this.merged = reader.readBoolean(MERGED_FIELD);
  }


  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ id=").append(this.modificationCount)
      .append(", str='").append(this.modification).append('\'')
      .append(", merged=").append(this.merged)
      .append(", history=");
    appendHistory(sb);
    sb.append(" }");
    return sb.toString();
  }

  /**
   * Appends the trailing part of the modification history to the given
   * StringBuilder
   */
  private void appendHistory(StringBuilder sb) {
    int length = this.history.size();
    int end = length;
    if (end > 4) end = 4;
    int start = 0;
    sb.append('[');
    for (int index = start; index < end; index++) {
      if (start > 0) {
        sb.append(", ");
      }
      sb.append('\'').append(this.history.get(index)).append('\'');
    }
    if (end < length) {
      sb.append(", ...");
    }
    sb.append(']');
  }
  
  
  ////// Methods for accessing fields of serialized Value objects
  
  static int getModificationCount(PdxInstance serializedValue) {
    return ((Integer)serializedValue.getField(MODIFICATION_COUNT_FIELD)).intValue();
  }
  
  static List<String> getHistory(PdxInstance serializedValue) {
    return (List<String>)serializedValue.getField(HISTORY_FIELD);
  }
  
  static String getModification(PdxInstance serializedValue) {
    return (String)serializedValue.getField(MODIFICATION_FIELD);
  }
  
  static PdxInstance setHistory(PdxInstance serializedValue, List<String> newHistory) {
    WritablePdxInstance writer = serializedValue.createWriter();
    writer.setField(Value.HISTORY_FIELD, newHistory);
    writer.setField(Value.MERGED_FIELD, Boolean.TRUE);
    return writer;
  }
  
  
  
}