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
package delta;

import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Sample implementation of Delta
 * 
 * @author GemStone Systems, Inc.
 * @since 6.1
 */
public class NestedDelta implements Delta, Serializable {

  //one boolean per field to manage changed status
  private transient boolean intFldChd = false;
  private transient boolean nestedFldChd = false;

  // Fields containing actual data.
  private int intVal;

  private NestedType nestedDelta = new NestedType();

  public NestedDelta() {
  }

  public NestedDelta(int iVal) {
    this.intVal = iVal;
  }
  
  public NestedDelta(int val, NestedType delta) {
    this.intVal = val;
    this.nestedDelta = delta;
  }

  /*
   * DeltaFeederClient uses this to update the nested field 'nestedDelta'
   */
  @SuppressWarnings("synthetic-access")
  public void setNestedDelta(NestedType delta) {
    if (!this.nestedDelta.equals(delta)) {
      this.nestedFldChd = true;
      if (!this.nestedDelta.fldSwitch == delta.fldSwitch) {
        this.nestedDelta.setSwitch(delta.fldSwitch);
      }
      if (this.nestedDelta.fldIdent != delta.fldIdent) {
        this.nestedDelta.setIdent(delta.fldIdent);
      }
    }
  }

 
  /*
   * DeltaFeederClient uses this to update the field 'intVal'
   */
  public void setIntVal(int intVal) {
    this.intFldChd = true;
    this.intVal = intVal;
  }

  public boolean hasDelta() {
    return this.intFldChd || this.nestedFldChd;
  }

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    System.out.println("Applying delta to " + this.toString());
    
    // Check if the field 'intVal' has changed
    if (in.readBoolean()) {
      this.intVal = in.readInt();
      System.out.println(" Applied delta to field 'intVal' = " + this.intVal);
    }

    // Check if the field 'nestedDelta' has changed
    if (in.readBoolean()) {
      if (in.readBoolean()) {
        // If 'delta' change for the field
        nestedDelta.fromDelta(in);
      }
      else {
        // Else create a new object and set it to the field
        nestedDelta = new NestedType(in.readBoolean(), in.readInt());
      }
    }
  }

  public void toDelta(DataOutput out) throws IOException, InvalidDeltaException {
    System.out.println("Extracting delta from " + this.toString());
    
    // Write boolean to indicate whether the field 'intVal' has changed.  
    out.writeBoolean(intFldChd);
    if (intFldChd) {
      // Write field value to DataOutput
      out.writeInt(this.intVal);
      this.intFldChd = false;
      System.out.println(" Wrote modified field 'intVal' = " + this.intVal + " to DataOutput");
    }
    
    // Write boolean to indicate whether the nestedDelta' has changed.  
    out.writeBoolean(nestedFldChd);
    if (nestedFldChd) {
      // Write the 'delta' changes if present
      if (this.nestedDelta.hasDelta()) {
        out.writeBoolean(Boolean.TRUE.booleanValue());
        this.nestedDelta.toDelta(out);
      }
      else {
        // else the whole object
        out.writeBoolean(Boolean.FALSE.booleanValue());
        out.writeBoolean(this.nestedDelta.getSwitch());
        out.writeInt(this.nestedDelta.getIdent());
      }
      
      nestedFldChd = false;
      System.out.println(" Wrote modified field 'nestedDelta' = "
          + this.nestedDelta + " to DataOutput");
    }
  }

  @Override
  public String toString() {
    return "NestedDelta [ hasDelta = " + this.hasDelta() + ", intVal = " + intVal
        + ", nestedDelta = " + this.nestedDelta + " ]";
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof NestedDelta)) {
      return false;
    }
    NestedDelta oCmp = (NestedDelta)other;
    if (this.intVal == oCmp.intVal
        && this.nestedDelta.equals(oCmp.nestedDelta)) {
      return true;
    }
    return false;
  }

  /*
   * Definition of Nested type
   */

  public static class NestedType implements Delta, Serializable {
    private boolean fldSwitch; 
    private int fldIdent = -1; 

    // one boolean per field to manage changed status
    private transient boolean boolFldChd = false;
    private transient boolean intFldChd = false;

    public NestedType() {
    }
    
    public NestedType(boolean bool, int id) {
      this.fldSwitch = bool;
      this.fldIdent = id;
    }

    public boolean getSwitch() {
      return fldSwitch;
    }

    public void setSwitch(boolean bool) {
      this.fldSwitch = bool;
      this.boolFldChd = true;
    }

    public int getIdent() {
      return fldIdent;
    }

    public void setIdent(int id) {
      this.fldIdent = id;
      this.intFldChd = true;
    }

    public boolean hasDelta() {
      return this.boolFldChd || this.intFldChd;
    }

    public void fromDelta(DataInput in) throws IOException,
        InvalidDeltaException {
      
      System.out.println("Applying delta to " + this.toString());
      
      if (in.readBoolean()) {
        this.fldSwitch = in.readBoolean();
        System.out.println(" Applied delta to field 'fldSwitch' = " + this.fldSwitch);
      }

      if (in.readBoolean()) {
        this.fldIdent = in.readInt();
        System.out.println(" Applied delta to field 'fldIdent' = " + this.fldIdent);
      }
    }

    public void toDelta(DataOutput out) throws IOException,
        InvalidDeltaException {
      
      System.out.println("Extracting delta to " + this.toString());

      out.writeBoolean(boolFldChd);
      if (boolFldChd) {
        out.writeBoolean(this.fldSwitch);
        this.boolFldChd = false;
        System.out.println(" Wrote modified field 'fldSwitch' = " + this.fldSwitch + " to DataOutput");
      }

      out.writeBoolean(intFldChd);
      if (intFldChd) {
        out.writeInt(this.fldIdent);
        this.intFldChd = false;
        System.out.println(" Wrote modified field 'fldIdent' = " + this.fldIdent + " to DataOutput");
      }
    }

    @Override
    public String toString() {
      return "NestedType [ hasDelta = " + this.hasDelta() + ", fldSwitch = "
          + this.fldSwitch + ", fldIdent = " + this.fldIdent + " ]";
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof NestedType)) {
        return false;
      }
      
      NestedType oCmp = (NestedType)other;
      if (this.fldSwitch == oCmp.fldSwitch && this.fldIdent == oCmp.fldIdent) {
        return true;
      }
      
      return false;
    }

  }
}
