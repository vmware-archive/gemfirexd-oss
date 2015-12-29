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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/** Implements Externalizable which in turn implements java.io.Serializable.
 * 
 *  This does not simply implement java.io.Serializable because that only serializes
 *  fields defined here, but not in the superclass because the superclass is not java.io.Serializable. 
 *  The superclass BaseValueHolder is not serializable intentionally so we can test pdx classes 
 *  which also inherit from BaseValueHolder; we want pdx classes to be only serializable via pdx 
 *  and no other way for more thorough test coverage. But we want the fields inherited from 
 *  BaseValueHolder to be serialized thus we implement Externalizable which requires us to also 
 *  implement readExternal and writeExternal so we can serialize inherited fields.
 * @author lynn
 *
 */
public class ValueHolder extends BaseValueHolder implements java.io.Externalizable {

  public ValueHolder() {
    myVersion = "util.ValueHolder";
  }

  public ValueHolder(Object key, RandomValues randomValues, Integer integer) {
    super(key, randomValues, integer);
    myVersion = "util.ValueHolder";
  }

  public ValueHolder(String key, RandomValues randomValues) {
    super(key, randomValues);
    myVersion = "util.ValueHolder";
  }

  public ValueHolder(Object key, RandomValues randomValues) {
    super(key, randomValues);
    myVersion = "util.ValueHolder";
  }

  public ValueHolder(RandomValues randomValues) {
    super(randomValues);
    myVersion = "util.ValueHolder";
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(myVersion);
    out.writeObject(myValue);
    out.writeObject(extraObject);
    out.writeObject(modVal);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    myVersion = (String) in.readObject();
    myValue = in.readObject();
    extraObject = in.readObject();
    modVal = (Integer) in.readObject();
  }

}
