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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import hydra.Log;
import util.TestException;
import com.gemstone.gemfire.InvalidDeltaException;

/** Class that intentionally causes errors in a users's implementation
 *  of the delta interface to make sure the product throws reasonable
 *  errors
 */
public class BadDeltaObject extends delta.DeltaObject {

// test cases involving bad delta implementations
public static final int hasDeltaThrowsException    = 1;
public static final int toDeltaThrowsException     = 2;
public static final int fromDeltaThrowsException   = 3;
public static final int hasDeltaThrowsError        = 4;
public static final int toDeltaThrowsError         = 5;
public static final int fromDeltaThrowsError       = 6;
public static final int fromDeltaReadTooMuch       = 7;
public static final int toDeltaWriteNothing        = 8;
public static final int fromDeltaThrowsInvDelExc   = 9;
public static final int toDeltaThrowsIOException   = 10;
public static final int fromDeltaThrowsIOException = 11;
public static final int numberOfBadDeltaTestCases  = 11;

// blackboard key
public static String testCaseKey = "testCaseKey";

/** Constructor
 */
public BadDeltaObject(long base, int valueGeneration, int byteArraySize, int levels) {
   super(base, valueGeneration, byteArraySize, levels);
}

// implement the delta interface

/** Write out changed fields
 */
public synchronized void toDelta(DataOutput out) throws IOException {
   int testCase = ((Integer)(DeltaPropagationBB.getBB().getSharedMap().get(testCaseKey))).intValue();
   if (testCase == toDeltaThrowsException) {
      Log.getLogWriter().info("In BadDeltaObject.toDelta, toDelta is throwing Exception...");
      int[] anArr = new int[1];
      int anInt = anArr[10]; // throws Exception
   } else if (testCase == toDeltaThrowsError) {
      Log.getLogWriter().info("In BadDeltaObject.toDelta, toDelta is throwing Error...");
      throw new AssertionError("Causing an error in toDelta");
   } else if (testCase == toDeltaWriteNothing) { 
      Log.getLogWriter().info("In BadDeltaObject.toDelta, toDelta is not writing to DataOutput " + out + "...");
      // do nothing with the DataOutput
   } else if (testCase == toDeltaThrowsIOException) {
      Log.getLogWriter().info("In BadDeltaObject.toDelta, toDelta is throwing IOException...");
      throw new IOException("Causing an IOException in toDelta");
   } else { // normal
      super.toDelta(out);
   }
}

/** Read in and set the changed fields
 */
public synchronized void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
   int testCase = ((Integer)(DeltaPropagationBB.getBB().getSharedMap().get(testCaseKey))).intValue();
   if (testCase == fromDeltaThrowsException) {
      Log.getLogWriter().info("In BadDeltaObject.fromDelta, fromDelta is throwing Exception...");
      int[] anArr = new int[1];
      int anInt = anArr[10]; // throws Exception
   } else if (testCase == fromDeltaThrowsError) {
      Log.getLogWriter().info("In BadDeltaObject.fromDelta, fromDelta is throwing Error...");
      throw new AssertionError("Causing an error in fromDelta");
   } else if (testCase == fromDeltaReadTooMuch) { 
      Log.getLogWriter().info("In BadDeltaObject.fromDelta, fromDelta is reading more than toDelta wrote...");
      // read more than was written
      byte[] aByteArray = new byte[1000];
      in.readFully(aByteArray);
   } else if (testCase == fromDeltaThrowsInvDelExc) { 
      Log.getLogWriter().info("In BadDeltaObject.fromDelta, fromDelta is throwing InvalidDeltaException...");
      throw new InvalidDeltaException("Throwing InvalidDeltaException in fromDelta");
   } else if (testCase == fromDeltaThrowsIOException) {
      Log.getLogWriter().info("In BadDeltaObject.fromDelta, fromDelta is throwing IOException...");
      throw new IOException("Causing an IOException in fromDelta");
   } else if (testCase == toDeltaWriteNothing) {
      Log.getLogWriter().info("In BadDeltaObject.fromDelta, toDelta wrote nothing, but trying to read anyway...");
      byte[] aByteArray = new byte[1000];
      in.readFully(aByteArray);
   } else { // normal
      super.fromDelta(in);
   }
}

/** Return true if this object has changed a field, false otherwise
 */
public boolean hasDelta() {
   int testCase = ((Integer)(DeltaPropagationBB.getBB().getSharedMap().get(testCaseKey))).intValue();
   if (testCase == hasDeltaThrowsException) {
      Log.getLogWriter().info("In BadDeltaObject.hasDelta, hasDelta is throwing Exception...");
      int[] anArr = new int[1];
      int anInt = anArr[10]; // throws Exception
      return true; // won't get here because exception thrown on above line, but this makes compiler happy
   } else if (testCase == hasDeltaThrowsError) {
      Log.getLogWriter().info("In BadDeltaObject.hasDelta, hasDelta is throwing Error...");
      throw new AssertionError("Causing an error in hasDelta");
   } else { // normal
      return super.hasDelta();
   }
}

/** Return a string description of the given test case
 */
public static String testCaseToString(int deltaTestCase) {
   if (deltaTestCase == hasDeltaThrowsException) {
      return "hasDelta throws Exception";
   } else if (deltaTestCase == toDeltaThrowsException) {
      return "toDelta throws Exception";
   } else if (deltaTestCase == fromDeltaThrowsException) {
      return "fromDelta throws Exception";
   } else if (deltaTestCase == hasDeltaThrowsError) {
      return "hasDelta throws Error";
   } else if (deltaTestCase == toDeltaThrowsError) {
      return "toDelta throws Error";
   } else if (deltaTestCase == fromDeltaThrowsError) {
      return "fromDelta throws Error";
   } else if (deltaTestCase == fromDeltaReadTooMuch) {
      return "fromDelta reads more data than was written";
   } else if (deltaTestCase == toDeltaWriteNothing) {
      return "toDelta writes nothing";
   } else if (deltaTestCase == fromDeltaThrowsInvDelExc) {
      return "fromDelta throws InvalidDeltaException";
   } else if (deltaTestCase == toDeltaThrowsIOException) {
      return "toDelta throws IOException";
   } else if (deltaTestCase == fromDeltaThrowsIOException) {
      return "fromDelta throws IOException";
   } else {
      throw new TestException("Unknown deltaTestCase " + deltaTestCase);
   }
}

}
