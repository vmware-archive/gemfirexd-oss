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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import com.gemstone.gemfire.Delta;
import util.RandomValues;
import util.TestException;
import util.TestHelper;

public class DeltaObject extends util.QueryObject implements Delta {

public static boolean cloningEnabled; // used for validation
public transient boolean aPrimitiveLongChanged = false;
public transient boolean aPrimitiveIntChanged = false;
public transient boolean aPrimitiveShortChanged = false;
public transient boolean aPrimitiveFloatChanged = false;
public transient boolean aPrimitiveDoubleChanged = false;
public transient boolean aPrimitiveByteChanged = false;
public transient boolean aPrimitiveCharChanged = false;
public transient boolean aPrimitiveBooleanChanged = false;
public transient boolean aByteArrayChanged = false;
public transient boolean aStringChanged = false;

private final String END_SIGNAL = "delta END signal";
/** 
 *  All the delta fields are initialized using the given base value
 *  and valueGeneration to determine the values of each field.
 *
 *  @param base The base value for all fields. Fields are all set
 *         to a value using base as appropriate. Unused if 
 *         valueGeneration is RANDOM_VALUES.
 *  @param valueGeneration How to generate the values of the
 *         fields. Can be one of
 *            QueryObject.EQUAL_VALUES
 *            QueryObject.SEQUENTIAL_VALUES
 *            QueryObject.RANDOM_VALUES
 *  @param byteArraySize The size of byte[]. If < 0 then there is
 *         no byteArray (it is null).
 *  @param levels The number of levels of QueryObjects. 1 means
 *         the field aQueryObject is null, 2 means aQueryObject
 *         contains an instance of QueryObject with fields
 *         initialized with base+1, the next level is initialized
 *         with base+2 etc.
 */
public DeltaObject(long base, int valueGeneration, int byteArraySize, int levels) {
   super(base, valueGeneration, byteArraySize, levels);
}

// implement the delta interface

/** Write out changed fields
 */
public synchronized void toDelta(DataOutput out) throws IOException {
   try {
      _toDelta(out);
      reset();
   } catch (TestException e) {
      String aStr = "In vm id " + RemoteTestModule.getMyVmid() + ", " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(aStr);
      DeltaPropagationBB.getBB().getSharedMap().replace(DeltaPropagationBB.ErrorKey, null, aStr);
      throw e;
   } catch (Exception e) {
      String aStr = "In vm id " + RemoteTestModule.getMyVmid() + ", " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(aStr);
      DeltaPropagationBB.getBB().getSharedMap().replace(DeltaPropagationBB.ErrorKey, null, aStr);
      throw new TestException(aStr);
   }
}

protected void _toDelta(DataOutput out) throws IOException {
   Log.getLogWriter().info("In DeltaObject.toDelta, for " + this + "\n" + "with DataOutput " + out);
   DeltaObserver.addToDeltaKey(extra);

   // for validation later, encode with a toDelta id number
   long toDeltaIdNumber = DeltaPropagationBB.getBB().getSharedCounters().incrementAndRead(DeltaPropagationBB.toDeltaIdNumber);
   out.writeLong(toDeltaIdNumber);
   Log.getLogWriter().info("Wrote toDeltaIdNumber " + toDeltaIdNumber);

   out.writeBoolean(aPrimitiveLongChanged);
   if (aPrimitiveLongChanged) {
      out.writeLong(aPrimitiveLong);
      Log.getLogWriter().info("Wrote changed field aPrimitiveLong " + aPrimitiveLong);
   }
   out.writeBoolean(aPrimitiveIntChanged);
   if (aPrimitiveIntChanged) {
      out.writeInt(aPrimitiveInt);
      Log.getLogWriter().info("Wrote changed field aPrimitiveInt " + aPrimitiveInt);
   }
   out.writeBoolean(aPrimitiveShortChanged);
   if (aPrimitiveShortChanged) {
      out.writeShort(aPrimitiveShort);
      Log.getLogWriter().info("Wrote changed field aPrimitiveShort " + aPrimitiveShort);
   }
   out.writeBoolean(aPrimitiveFloatChanged);
   if (aPrimitiveFloatChanged) {
      out.writeFloat(aPrimitiveFloat);
      Log.getLogWriter().info("Wrote changed field aPrimitiveFloat " + aPrimitiveFloat);
   }
   out.writeBoolean(aPrimitiveDoubleChanged);
   if (aPrimitiveDoubleChanged) {
      out.writeDouble(aPrimitiveDouble);
      Log.getLogWriter().info("Wrote changed field aPrimitiveDouble " + aPrimitiveDouble);
   }
   out.writeBoolean(aPrimitiveByteChanged);
   if (aPrimitiveByteChanged) {
      out.writeByte(aPrimitiveByte);
      Log.getLogWriter().info("Wrote changed field aPrimitiveByte " + aPrimitiveByte);
   }
   out.writeBoolean(aPrimitiveCharChanged);
   if (aPrimitiveCharChanged) {
      out.writeChar(aPrimitiveChar);
      Log.getLogWriter().info("Wrote changed field aPrimitiveChar " + aPrimitiveChar);
   }
   out.writeBoolean(aPrimitiveBooleanChanged);
   if (aPrimitiveBooleanChanged) {
      out.writeBoolean(aPrimitiveBoolean);
      Log.getLogWriter().info("Wrote changed field aPrimitiveBoolean " + aPrimitiveBoolean);
   }
   out.writeBoolean(aByteArrayChanged);
   if (aByteArrayChanged) {
      out.writeInt(aByteArray.length);
      out.write(aByteArray);
      Log.getLogWriter().info("Wrote changed field aByteArray of length " + aByteArray.length);
   }
   out.writeBoolean(aStringChanged);
   if (aStringChanged) {
      out.writeInt(aString.length());
      out.writeBytes(aString);
      Log.getLogWriter().info("Wrote changed field aString " + aString);
   }
   out.writeInt(END_SIGNAL.length());
   out.writeBytes(END_SIGNAL);
   Log.getLogWriter().info("Wrote end signal: " + END_SIGNAL);
   Log.getLogWriter().info("Done writing in DeltaObject.toDelta for " + this);
}

/** Read in and set the changed fields
 */
public synchronized void fromDelta(DataInput in) throws IOException {
   try {
      _fromDelta(in);
      verifyCloning();
   } catch (TestException e) {
      String aStr = "In vm id " + RemoteTestModule.getMyVmid() + ", " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(aStr);
      DeltaPropagationBB.getBB().getSharedMap().replace(DeltaPropagationBB.ErrorKey, null, aStr);
      throw e;
   } catch (Exception e) {
      String aStr = "In vm id " + RemoteTestModule.getMyVmid() + ", " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(aStr);
      DeltaPropagationBB.getBB().getSharedMap().replace(DeltaPropagationBB.ErrorKey, null, aStr);
      throw new TestException(aStr);
   }
}

/**
 * Verify that cloning occurred as expected.
 */
private void verifyCloning() {
  if (!TestConfig.tab().booleanAt(DeltaPropagationPrms.enableCloningValidation, true)) {
     Log.getLogWriter().info("cloning validation is disabled");
     return;
  }
  Log.getLogWriter().info("In verifyCloning for " + this.toStringFull());
  Map regionReferences = DeltaObserver.getReferences();
  if (!(regionReferences.containsKey(this.extra))) {
    // key does not exist in regionReferences, likely because of the race
    // for bug 40836 (we were not able to put the current reference in 
    // the map when DeltaTestListener.afterCreate was invoked
    Log.getLogWriter().info("Unable to verify cloning because previous reference does not exist for " +
         this.toStringFull());
    return;
  }
  
  Object[] anArr = (Object[])(regionReferences.get(this.extra));
  if (anArr == null) { 
    throw new TestException("Error in test, unexpected null in region references for " + this.toStringFull());
  }
  int idHashCode = ((Integer)anArr[0]).intValue();
  String refString = ((String)anArr[1]);
  long deltaObjId = ((Long)anArr[2]);
  int thisIdHashCode = System.identityHashCode(this);
  Log.getLogWriter().info("For key " + this.extra + " fromDelta is being called on object with identityHashCode " +
      thisIdHashCode + ", previous object in region had identityHashCode " + idHashCode + " and DeltaObject id " + deltaObjId);
  if (cloningEnabled) {
    if (idHashCode == thisIdHashCode) { // the identity hash codes are the same
      // it is possible that a hashcode can be the same for two different objects
      // so if these are the same, check the id number which would be different if we
      // called clone
      if (deltaObjId == this.id) {
        String aStr = "In vm id " + RemoteTestModule.getMyVmid() + 
        ", expected fromDelta to be called on a clone, but it is being called on object with identityHashCode " +
        thisIdHashCode + ", " + this.toStringFull() + ", the object previously in the region has identityHashCode " +
        idHashCode + ", " + refString;
        Log.getLogWriter().info(aStr);
        DeltaPropagationBB.getBB().getSharedMap().replace(DeltaPropagationBB.ErrorKey, null, aStr);
        throw new TestException(aStr);
      }
    }
  } else if ((!cloningEnabled) && (idHashCode != thisIdHashCode)) {
    String aStr = "In vm id " + RemoteTestModule.getMyVmid() + 
    ", expected fromDelta to be called on object existing in region (no clone), but it is being called on object with identityHashCode " +
    thisIdHashCode + ", " + this.toStringFull() + ", the object previously in the region has identityHashCode " +
    idHashCode + ", " + refString;
    Log.getLogWriter().info(aStr);
    DeltaPropagationBB.getBB().getSharedMap().replace(DeltaPropagationBB.ErrorKey, null, aStr);
    throw new TestException(aStr);
  }
}

protected void _fromDelta(DataInput in) throws IOException {
   Log.getLogWriter().info("In DeltaObject.fromDelta for " + this + "\nwith dataInput " + in);
   DeltaObserver.addFromDeltaKey(extra);
   long toDeltaIdNumber = in.readLong();
   Log.getLogWriter().info("Read toDeltaIdNumber " + toDeltaIdNumber);
   Object prevKeyForThisIdNumber = DeltaObserver.addToDeltaIdNumber(toDeltaIdNumber, extra);
   if (prevKeyForThisIdNumber != null) {
      throw new TestException("In DeltaObject.fromDelta, fromDelta is being called on " + this.toStringFull() +
            " with fromDelta stream containing toDeltaIdNumber " + toDeltaIdNumber + 
            ", but this id number was previously processed in vm id " + RemoteTestModule.getMyVmid() + 
            " with key " + prevKeyForThisIdNumber);
   }
   boolean change = in.readBoolean();
   if (change) {
      aPrimitiveLong = in.readLong();
      Log.getLogWriter().info("Read and set field aPrimitiveLong to " + aPrimitiveLong);
   }
   change = in.readBoolean();
   if (change) {
      aPrimitiveInt = in.readInt();
      Log.getLogWriter().info("Read and set changed field aPrimitiveInt to " + aPrimitiveInt);
   }
   change = in.readBoolean();
   if (change) {
      aPrimitiveShort = in.readShort();
      Log.getLogWriter().info("Read and set changed field aPrimitiveShort to " + aPrimitiveShort);
   }
   change = in.readBoolean();
   if (change) {
      aPrimitiveFloat = in.readFloat();
      Log.getLogWriter().info("Read and set changed field aPrimitiveFloat to " + aPrimitiveFloat);
   }
   change = in.readBoolean();
   if (change) {
      aPrimitiveDouble = in.readDouble();
      Log.getLogWriter().info("Read and set changed field aPrimitiveDouble to " + aPrimitiveDouble);
   }
   change = in.readBoolean();
   if (change) {
      aPrimitiveByte = (byte)(in.readUnsignedByte());
      Log.getLogWriter().info("Read and set changed field aPrimitiveByte to " + aPrimitiveByte);
   }
   change = in.readBoolean();
   if (change) {
      aPrimitiveChar = in.readChar();
      Log.getLogWriter().info("Read and set changed field aPrimitiveChar to " + (int)aPrimitiveChar);
   }
   change = in.readBoolean();
   if (change) {
      aPrimitiveBoolean = in.readBoolean();
      Log.getLogWriter().info("Read and set changed field aPrimitiveBoolean to " + aPrimitiveBoolean);
   }
   change = in.readBoolean();
   if (change) {
      int length = in.readInt();
      Log.getLogWriter().info("Constructing byte[] of length " + length);
      aByteArray = new byte[length];
      in.readFully(aByteArray);
      Log.getLogWriter().info("Read and set changed field aByteArray of length to " + aByteArray.length);
   }
   change = in.readBoolean();
   if (change) {
      int length = in.readInt();
      Log.getLogWriter().info("Constructing String of length " + length);
      byte[] theBytes = new byte[length];
      in.readFully(theBytes);
      aString = new String(theBytes);
      Log.getLogWriter().info("Read and set changed field aString to " + aString);
   }
   int endSignalLength = in.readInt();
   byte[] theBytes = new byte[endSignalLength];
   in.readFully(theBytes);
   String endSignal = new String(theBytes);
   Log.getLogWriter().info("Read end signal: " + endSignal);
   if ((endSignal == null) || (!endSignal.equals(END_SIGNAL))) {
      String aStr = "In DeltaObject.fromDelta, expected to find endSignal: " + END_SIGNAL + ", but found " + 
                    endSignal;
      Log.getLogWriter().info(aStr);
      DeltaPropagationBB.getBB().getSharedMap().put(DeltaPropagationBB.ErrorKey, aStr);
      throw new TestException(aStr);
   }
   Log.getLogWriter().info("Done reading in DeltaObject.fromDelta for " + this.toStringFull());
}
  
/** Return true if this object has changed a field, false otherwise
 */
public boolean hasDelta() {
   boolean hasDelta = aPrimitiveLongChanged || aPrimitiveIntChanged || aPrimitiveShortChanged ||
      aPrimitiveFloatChanged || aPrimitiveDoubleChanged || aPrimitiveByteChanged ||
      aPrimitiveCharChanged || aPrimitiveBooleanChanged || aByteArrayChanged || aStringChanged;
   Log.getLogWriter().info("In DeltaObject.hasDelta, returning " + hasDelta + " for " + this);
   DeltaObserver.addHasDeltaKey(extra);
   return hasDelta; 
}

/** Reset the delta object to indicate no changes
 */
public void reset() {
   aPrimitiveLongChanged = false;
   aPrimitiveIntChanged = false;
   aPrimitiveShortChanged = false;
   aPrimitiveFloatChanged = false;
   aPrimitiveDoubleChanged = false;
   aPrimitiveByteChanged = false;
   aPrimitiveCharChanged = false;
   aPrimitiveBooleanChanged = false;
   aByteArrayChanged = false;
   aStringChanged = false;
}

/** Make a copy of the DeltaObject.
 */
public Object clone() throws CloneNotSupportedException {
   DeltaObject clonedObj = (DeltaObject)(super.clone());
   Log.getLogWriter().info("In DeltaObject clone, cloning " + System.identityHashCode(this) +
       ":" + this.toStringFull() + ", clone is " + System.identityHashCode(clonedObj) + ":" +
       clonedObj.toStringFull()); 
   return clonedObj;
}

/** Make a copy of the DeltaObject.
 *  This is so the test can make a copy of a DeltaObject without calling
 *  DeltaObject's clone() method, which does bookkeeping for validation 
 *  so the test can verify when clone() is called by the product.
 *  This copy is used in the regionsnapshot to record what the field values
 *  should be for validation; since the delta feature writes fields
 *  directly, it's a bit safer to put our own copy of the values into
 *  the regionsnapshot.
 */
public DeltaObject copy() {
   try {
      DeltaObject copy = (DeltaObject)(super.clone()); // copy the fields in the superclass without invoking DeltaObject.clone()
      copy.id = this.id; // make this an identical copy; the superclass clone method assigns a new id so
                         // as to make it easier to identify separate instances in logs which helps debugging,
                         // but here we really want an identical instance
      return copy;
   } catch (CloneNotSupportedException e) {
      throw new TestException(TestHelper.getStackTrace(e));   
   }
}

//================================================================================
// Override from superclass

/** Modify the DeltaObject according to changeValueGeneration and delta.
 *
 *  @param changeValueGeneration How to determine the new values 
 *         Can be one of:
 *         QueryObject.INCREMENT
 *         QueryObject.NEGATE 
 *         QueryObject.NULL_NONPRIM_FIELDS 
 *         If any fields are null and this is INCREMENT or NEGATE then
 *         the field is filled in with a random value.
 *  @param delta The number to increment or decrement; unused if
 *         changeValueGeneration is QueryObject.NEGATE.
 *  @param log If true, then log what is being modified, if false don't
 *         log since output can be large.
 */
public void modify(int changeValueGeneration, int delta, boolean log) {
   if (log) {
      Log.getLogWriter().info("Modifying " + this.toStringFull() + " with " + constantToString(changeValueGeneration) + " delta " + delta);
   }
   RandomValues rv = new RandomValues();
   if (changeValueGeneration == INCREMENT) {
      aPrimitiveLong = aPrimitiveLong + delta;
      aPrimitiveLongChanged = true;

      aPrimitiveInt = aPrimitiveInt + delta;
      aPrimitiveIntChanged = true;

      aPrimitiveShort = (short)(aPrimitiveShort + delta);
      aPrimitiveShortChanged = true;

      aPrimitiveFloat = aPrimitiveFloat + delta;
      aPrimitiveFloatChanged = true;

      aPrimitiveDouble = aPrimitiveDouble + delta;
      aPrimitiveDoubleChanged = true;

      aPrimitiveByte = (byte)(aPrimitiveByte + delta);
      aPrimitiveByteChanged = true;

      aPrimitiveChar = (char)(((byte)aPrimitiveChar) + delta);
      aPrimitiveCharChanged = true;

      aPrimitiveBoolean = !aPrimitiveBoolean;
      aPrimitiveBooleanChanged = true;

      try {
         aString = "" + (Long.valueOf(aString).longValue() + delta);
      } catch (NumberFormatException e) { // must be a random string
         aString = rv.getRandom_String();
      }
      aStringChanged = true;
   } else if (changeValueGeneration == NEGATE) {
      aPrimitiveLong = -aPrimitiveLong;
      aPrimitiveLongChanged = true;

      aPrimitiveInt = -aPrimitiveInt;
      aPrimitiveIntChanged = true;

      aPrimitiveShort = (short)(-aPrimitiveShort);
      aPrimitiveShortChanged = true;

      aPrimitiveFloat = -aPrimitiveFloat;
      aPrimitiveFloatChanged = true;

      aPrimitiveDouble = -aPrimitiveDouble;
      aPrimitiveDoubleChanged = true;

      aPrimitiveByte = (byte)(-aPrimitiveByte);
      aPrimitiveByteChanged = true;

      aPrimitiveChar = (char)(-((byte)aPrimitiveChar));
      aPrimitiveCharChanged = true;

      aPrimitiveBoolean = !aPrimitiveBoolean;
      aPrimitiveBooleanChanged = true;

      try {
         aString = new String("" + -(Long.valueOf(aString).longValue()));
      } catch (NumberFormatException e) { // must be a random string
         aString = rv.getRandom_String();
      }
      aStringChanged = true;
   } else if (changeValueGeneration == NULL_NONPRIM_FIELDS) {
      throw new TestException("nulling non-prim fields unsupported for delta test");
   } else {
      throw new TestException("unknown changeValueGeneration " + changeValueGeneration);
   }
   if (log) {
      Log.getLogWriter().info("Done modifying " + this.toStringFull() + " with " + constantToString(changeValueGeneration) + " delta is " + delta);
   }
}

}
