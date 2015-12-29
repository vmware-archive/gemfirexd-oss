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
package util;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;
import hydra.TestConfig;

import java.io.IOException;
import java.math.BigInteger;

/**
 * @author lynn
 *
 */
public class PdxVersionedValueHolder extends VersionedValueHolder implements PdxSerializable {
  static {
    Log.getLogWriter().info("In static initializer for testsVersions/version2/util.PdxVersionedValueHolder");
  }

  /** No arg constructor
   * 
   */
  public PdxVersionedValueHolder() {
    myVersion = "version2: util.PdxVersionedValueHolder";
  }

  /** Create a new instance of PdxVersionedValueHolder. The new instance will have
   *  a {@link Long} for myValue, using the nameFactoryName to extract the long.
   *  If ValueHolderPrms.useExtraObject is set, the extraObject field
   *  will be filled in using randomValues.
   * 
   *  @param nameFactoryName - A name from {@link NameFactory}, used to extract
   *         a value for the PdxVersionedValueHolder myValue.
   *  @param randomValues - Used to generate an extraObject, if needed.
   *         If useExtraObject is false, this can be null
   *
   * @see NameFactory#getCounterForName
   */
  public PdxVersionedValueHolder(String nameFactoryName, RandomValues randomValues) {
    super(nameFactoryName, randomValues);
    myVersion = "version2: util.PdxVersionedValueHolder";
  }
  
/** 
 *  Create a new instance of PdxVersionedValueHolder. The new PdxVersionedValueHolder will
 *  have <code>anObj</code> placed as its <code>myValue</code> field
 *  and, if desired, extraObject will be filled in using
 *  <code>randomValues</code> if {@link
 *  ValueHolderPrms#useExtraObject} is set to true.
 * 
 *  @param anObj - Used for myValue.
 *  @param randomValues - Used to generate extraObject, if needed.
 */
  public PdxVersionedValueHolder(Object anObj, RandomValues randomValues) {
    super(anObj, randomValues);
    myVersion = "version2: util.PdxVersionedValueHolder";
  }
  
  /** Using the current values in this PdxVersionedValueHolder, return a new
   *  PdxVersionedValueHolder that has the same "value" in <code>myValue</code> and
   *  a new <code>extraObject</code> if extraObject is used. For
   *  example, if <code>this.myValue</code> contains a Long of value 34, the new
   *  PdxVersionedValueHolder will also have a myValue of 34, but it may be a
   *  String, an Integer, a BigInteger, etc.
   *
   *  @return A new instance of PdxVersionedValueHolder
   *
   *  @throws TestException
   *          If we cannot find an alternative value for this value in
   *          this <code>PdxVersionedValueHolder</code>.
   */
  @Override
  public BaseValueHolder getAlternateValueHolder(RandomValues randomValues) {
     BaseValueHolder vh = new PdxVersionedValueHolder(this.myValue, randomValues);
     long longValue = new Long(myValue.toString());
     if (myValue instanceof Long) {
        vh.myValue = myValue.toString();
     } else if (myValue instanceof String) {
        vh.myValue = new Integer((String)myValue);
     } else if (myValue instanceof Integer) {
        vh.myValue = new BigInteger(myValue.toString());
     } else if (myValue instanceof BigInteger) {
        vh.myValue = new Long(myValue.toString());
     } else {
        throw new TestException("Cannot get replace value for myValue in " + TestHelper.toString(vh));
     }
     return vh;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void toData(PdxWriter out) {
    Log.getLogWriter().info("In testsVersions/version2/util.PdxVersionedValueHolder.toData, " + 
        " calling MyToData...");
    myToData(out);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void fromData(PdxReader in) {
    Log.getLogWriter().info("In testsVersions/version2/util.PdxVersionedValueHolder.fromData, " +
        " calling myFromData...");
    myFromData(in);
  }

}
