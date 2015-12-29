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

import hydra.Log;
import hydra.TestConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.BaseValueHolder;
import util.ValueHolder;
import util.ValueHolderPrms;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;

/**
 *
 */
public class DeltaValueHolder extends ValueHolder implements Delta {
  
  private boolean hasDelta = false;

  public static boolean logCalls = false;
  
  /**
   * 
   */
  public DeltaValueHolder() {
    // TODO Auto-generated constructor stub
    myVersion = "delta.DeltaValueHolder";
  }

  /**
   * @param anObj
   * @param randomValues
   */
  public DeltaValueHolder(Object anObj, RandomValues randomValues) {
     super(anObj, randomValues);
    myVersion = "delta.DeltaValueHolder";
    // TODO Auto-generated constructor stub
  }

  /**
   * @param randomValues
   */
  public DeltaValueHolder(RandomValues randomValues) {
    super(randomValues);
    myVersion = "delta.DeltaValueHolder";
    // TODO Auto-generated constructor stub
  }

  /**
   * @param nameFactoryName
   * @param randomValues
   */
  public DeltaValueHolder(String nameFactoryName, RandomValues randomValues) {
    super(nameFactoryName, randomValues);
    myVersion = "delta.DeltaValueHolder";
    // TODO Auto-generated constructor stub
  }

  /**
   * @param anObj
   * @param randomValues
   * @param initModVal
   */
  public DeltaValueHolder(Object anObj, RandomValues randomValues,
      Integer initModVal) {
    super(anObj, randomValues, initModVal);
    myVersion = "delta.DeltaValueHolder";
    // TODO Auto-generated constructor stub
  }

  /**
   * @param nameFactoryName
   * @param randomValues
   * @param initModVal
   */
  public DeltaValueHolder(String nameFactoryName, RandomValues randomValues,
      Integer initModVal) {
    super(nameFactoryName, randomValues, initModVal);
    myVersion = "delta.DeltaValueHolder";
    // TODO Auto-generated constructor stub
  }

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    // TODO Auto-generated method stub
    try {
      if (logCalls) {
        Log.getLogWriter().info("In DeltaValueHolder fromDelta " + TestHelper.toString(this));
      }
      this.myValue = (Object)DataSerializer.readObject(in);
      this.extraObject = (Object)DataSerializer.readObject(in);
      //Log.getLogWriter().info("In DeltaValueHolder fromDelta, object changed to " + TestHelper.toString(this));
      //this.modVal = DataSerializer.readInteger(in);
    }
    catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public boolean hasDelta() {
    // TODO Auto-generated method stub
    if (logCalls) {
      Log.getLogWriter().info("In DeltaValueHolder hasDelta, returning " + hasDelta);
    }
    return hasDelta;
  }

  public void toDelta(DataOutput out) throws IOException {
    // TODO Auto-generated method stub    
    if (logCalls) {
      Log.getLogWriter().info("In DeltaValueHolder toDelta " + TestHelper.toString(this));
    }
    DataSerializer.writeObject(this.myValue, out);
    DataSerializer.writeObject(this.extraObject, out);
    //DataSerializer.writeInteger(this.modVal, out);
    hasDelta=false;
  }

  public BaseValueHolder getAlternateValueHolder(RandomValues randomValues) {
    DeltaValueHolder vh = new DeltaValueHolder();
    vh.hasDelta =true;
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
    if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject))
       vh.extraObject = randomValues.getRandomObjectGraph();
    return vh;
 }

}
