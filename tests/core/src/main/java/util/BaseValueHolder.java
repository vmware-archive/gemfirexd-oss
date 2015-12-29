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
package util; 

import hydra.Log;
import hydra.TestConfig;

import java.math.BigInteger;

/**
 * A class whose instances have a value and may also refer to another
 * object.  Instances of this class are used in a variety of different
 * tests.
 *
 * @see ValueHolderPrms
 * @see RandomValues
 *
 * @author Lynn Gallinat
 * @since GemFire 3.0
 */

/** DO NOT make this class implement java.io.Serializable!!! This is important
 *  for testing functions and partition resolvers which changed their API
 *  from using java.io.Serializable to Object in 6.6.2 (product changes by Darrel).
 */
public abstract class BaseValueHolder implements ValueHolderIF {
  
  public String myVersion;

  /** The value held by this <code>ValueHolder</code> */
public Object myValue = null;

  /** The other object referred to by this <code>ValueHolder</code> */
public Object extraObject = null;

  /** An object that can be manipulated (leaving myValue to hold a 
   *  value which allows validation against the key)
   */
public Integer modVal = null;

/** No-arg constructor */
public BaseValueHolder() {
}

/** 
 *  Create a new instance of ValueHolder. The new ValueHolder will
 *  have <code>anObj</code> placed as its <code>myValue</code> field
 *  and, if desired, extraObject will be filled in using
 *  <code>randomValues</code> if {@link
 *  ValueHolderPrms#useExtraObject} is set to true.
 * 
 *  @param anObj - Used for myValue.
 *  @param randomValues - Used to generate extraObject, if needed.
 */
public BaseValueHolder(Object anObj, RandomValues randomValues) {
   this.myValue = anObj;
   if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      this.extraObject = randomValues.getRandomObjectGraph();
   }
}

/** Create a new instance of ValueHolder. The new ValueHolder will have
 *  random values for myValue and, if desired, extraObject. If 
 *  ValueHolderPrms.useExtraObject is set, the extraObject field
 *  will be filled in.
 * 
 *  @param randomValues - Used to generate myValue and extraObject, if needed.
 */
public BaseValueHolder(RandomValues randomValues) {
   this.myValue = randomValues.getRandomObjectGraph();
   if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      this.extraObject = randomValues.getRandomObjectGraph();
   }
}

/** Create a new instance of ValueHolder. The new ValueHolder will have
 *  a {@link Long} for myValue, using the nameFactoryName to extract the long.
 *  If ValueHolderPrms.useExtraObject is set, the extraObject field
 *  will be filled in using randomValues.
 * 
 *  @param nameFactoryName - A name from {@link NameFactory}, used to extract
 *         a value for the ValueHolder myValue.
 *  @param randomValues - Used to generate an extraObject, if needed.
 *         If useExtraObject is false, this can be null
 *
 * @see NameFactory#getCounterForName
 */
public BaseValueHolder(String nameFactoryName, RandomValues randomValues) {
   this.myValue = new Long(NameFactory.getCounterForName(nameFactoryName));
   if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      this.extraObject = randomValues.getRandomObjectGraph();
   }
}

/** 
 *  Create a new instance of ValueHolder. The new ValueHolder will
 *  have <code>anObj</code> placed as its <code>myValue</code> field
 *  and, if desired, extraObject will be filled in using
 *  <code>randomValues</code> if {@link
 *  ValueHolderPrms#useExtraObject} is set to true.  In addition,
 *  modVal will be initialized with <code>initModVal</code>.
 * 
 *  @param anObj - Used for myValue.
 *  @param randomValues - Used to generate extraObject, if needed.
 *  @param initModVal - Use for initialization of modVal
 */
public BaseValueHolder(Object anObj, RandomValues randomValues, Integer initModVal) {
   this.myValue = anObj;
   this.modVal = initModVal;
   if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      this.extraObject = randomValues.getRandomObjectGraph();
   }
}

/** Create a new instance of ValueHolder. The new ValueHolder will have
 *  a {@link Long} for myValue, using the nameFactoryName to extract the long.
 *  If ValueHolderPrms.useExtraObject is set, the extraObject field
 *  will be filled in using randomValues.
 *  initModVal will be used to initialize modVal
 * 
 *  @param nameFactoryName - A name from {@link NameFactory}, used to extract
 *         a value for the ValueHolder myValue.
 *  @param randomValues - Used to generate an extraObject, if needed.
 *         If useExtraObject is false, this can be null
 *  @param initModVal - initializer for modVal
 *
 * @see NameFactory#getCounterForName
 */
public BaseValueHolder(String nameFactoryName, RandomValues randomValues, Integer initModVal) {
   this.myValue = new Long(NameFactory.getCounterForName(nameFactoryName));
   this.modVal = initModVal;
   if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      this.extraObject = randomValues.getRandomObjectGraph();
   }
}


  public Object getMyValue() {
    return this.myValue;
  }

  public Object getExtraObject() {
    return this.extraObject;
  }

  public Integer getModVal() {
    return this.modVal;
  }

  public void setModVal(Integer val) {
    this.modVal = val;
  }

  public void incrementModVal(Integer val) {
    this.modVal = new Integer(modVal.intValue() + 1);
  }

/* (non-Javadoc)
 * @see java.lang.Object#toString()
 */
@Override
public String toString() {
  return this.getClass().getName() + " [myVersion=" + this.myVersion + 
    ", myValue=" + TestHelper.toString(this.myValue) + 
    ", extraObject=" + TestHelper.toString(this.extraObject) + 
    ", modVal=" + this.modVal + "]";
}

/** Using the current values in this ValueHolder, return a new
 *  ValueHolder that has the same "value" in <code>myValue</code> and
 *  a new <code>extraObject</code> if extraObject is used. For
 *  example, if <code>this.myValue</code> contains a Long of value 34, the new
 *  ValueHolder will also have a myValue of 34, but it may be a
 *  String, an Integer, a BigInteger, etc.
 *
 *  @return A new instance of ValueHolder
 *
 *  @throws TestException
 *          If we cannot find an alternative value for this value in
 *          this <code>ValueHolder</code>.
 */
public BaseValueHolder getAlternateValueHolder(RandomValues randomValues) {
   BaseValueHolder vh = new ValueHolder();
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

/** Verify the fields of this instance
 * 
 * @param base The expected base value used to give the fields values. 
 */
public void verifyMyFields(long base) {
  // this is used by pdx tests, and should be reimplemented in subclasses for
  // pdx tests; for non-pdx tests, this is a noop
}

/**
 * @param string
 */
public void verifyMyFields(String string, long base) {
  // this is used by pdx tests, and should be reimplemented in subclasses for
  // pdx tests; for non-pdx tests, this is a noop
}

@Override
public int hashCode() {
  final int prime = 31;
  int result = 1;
  result = prime * result + ((this.myValue == null) ? 0 : this.myValue.toString().hashCode());
  return result;
}

@Override
public boolean equals(Object anObj) {
   if (anObj == null) {
      return false;
   }
   if (anObj.getClass().getName().equals(this.getClass().getName())) {
      BaseValueHolder vh = (BaseValueHolder)anObj;
      if (vh.myValue == null) {
         if (this.myValue != null) {
//            Log.getLogWriter().info("not equals(1):, vh.myValue is " + vh.myValue + ", this.myValue is " + this.myValue);
            return false;
         }
      }
      if (!(vh.myValue.equals(this.myValue))) {
//         Log.getLogWriter().info("not equals(2):, vh.myValue is " + vh.myValue + ", this.myValue is " + this.myValue);
         return false;
      }
      if (vh.extraObject == null) {
         if (this.extraObject != null) {
//            Log.getLogWriter().info("not equals(3):, vh.extraObject is " + vh.extraObject + ", this.extraObject is " + this.extraObject);
            return false;
         }
      }
      if (vh.modVal == null) {
         if (this.modVal != null) {
//            Log.getLogWriter().info("not equals(4):, vh.modVal is " + vh.modVal + ", this.modVal is " + this.modVal);
            return false;
         }
      } else {
        if (!(vh.modVal.equals(this.modVal))) {
//           Log.getLogWriter().info("not equals(5):, vh.modVal is " + vh.modVal + ", this.modVal is " + this.modVal);
           return false;
        }
      }
   } else {
//      Log.getLogWriter().info("not equals(6):, anObj is " + TestHelper.toString(anObj));
      return false;
   }
   return true;
}



}
