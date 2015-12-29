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
package delta;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Sample implementation of Delta
 * 
 * @author GemStone Systems, Inc.
 * @since 6.1
 */
public class DeltaCollection implements Delta, DataSerializable {
  //one boolean to manage changed status
  private transient boolean fieldChanged = false; 
  
  private static final String KEY_STRING = "_key";
  
  // Field containing actual data.
  private HashMap<String, SimpleDelta> map = new HashMap<String, SimpleDelta>();                         

  // items removed from map
  private transient Set<String> entriesRemoved = new HashSet<String>();
  // items added or updated in the map
  private transient HashMap<String, SimpleDelta> entriesAdded = new HashMap<String, SimpleDelta>();

  public DeltaCollection(){}
  
  public DeltaCollection( int iNumEntries ){
    // push entries to map
    for (int i = 0; i < iNumEntries; i++) {
      this.map.put(KEY_STRING + i, new SimpleDelta((25 + i), (25 + i)));
    }
  }
 
  public void addToMap(String k, SimpleDelta v) {
    this.entriesAdded.put(k, v);
    this.map.put(k,v);
    if( this.entriesRemoved.contains(k)) {
      this.entriesRemoved.remove(k);
    }
    this.fieldChanged = true;
  }
  
  public void removeFromMap(String k) {
    this.entriesRemoved.add(k);
    this.map.remove(k);
    this.fieldChanged = true;
  }
  
  @Override
  public String toString() {
    return "DeltaCollection [ hasDelta = " + this.hasDelta() + ", map = " + this.map + " ]";
  }
    
  public boolean hasDelta() {
    return this.fieldChanged;
  }

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    System.out.println("Applying delta to " + this.toString());
    if (in.readBoolean()) {
      // -- get count of number of entries removed, followed by the count of new/updated entries
      int numRemovedEntries = DataSerializer.readPrimitiveInt(in);
      int numUpdatedEntries = DataSerializer.readPrimitiveInt(in);

      int idx = 0;
      String key="";
      SimpleDelta val= null;
      
      // -- read in the removed entries 
      for( idx = 0; idx < numRemovedEntries; idx++ ) {
        key = DataSerializer.readString(in);
        this.map.remove(key);
      }

      // -- read in the added/updates entries
      for (int i = 0; i < numUpdatedEntries; i++) {
        key = DataSerializer.readString(in);
        
        
        // Has only the delta of the changes to the value 
        if (DataSerializer.readBoolean(in)) {
          val = this.map.get(key);
          val.fromDelta(in);
        }
        else {
          // Has the full value and not a delta
          try {
            val = DataSerializer.readObject(in);
          }
          catch (ClassNotFoundException cnfe) {
            throw new InvalidDeltaException(cnfe);
          }
        }

        this.map.put(key, val);
      }

      System.out.println(" Applied delta on DeltaCollection's field 'map' = "
          + this.map);
    }
  }

  public void toDelta(DataOutput out) throws IOException {
    System.out.println("Extracting delta from " + this.toString());

    out.writeBoolean(fieldChanged);
    if (fieldChanged) {
      //    -- write the count of entries removed followed by the count of 
      //    entries added/updated
      DataSerializer.writePrimitiveInt(this.entriesRemoved.size(), out);
      
      DataSerializer.writePrimitiveInt(this.entriesAdded.size(), out);

      //    -- write the keys of items removed to the stream
      for( String sRemoved : this.entriesRemoved ) {
        DataSerializer.writeString(sRemoved, out);
      }
      
      //    -- write the entries added/updates
      for( String sUpdated : this.entriesAdded.keySet() ) {
        SimpleDelta val = this.entriesAdded.get(sUpdated);
        DataSerializer.writeString(sUpdated, out);
        if (val.hasDelta()) {
          DataSerializer.writeBoolean(true, out);
          val.toDelta(out);
        }
        else {
          DataSerializer.writeBoolean(false, out);
          DataSerializer.writeObject(val, out);
        }
      }
      // Delta has been generated; no need to keep track of these changes
      this.entriesAdded.clear();
      this.entriesRemoved.clear();
      fieldChanged = false;
    } 
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.map = DataSerializer.readHashMap(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashMap(this.map, out);
  }
  
}
