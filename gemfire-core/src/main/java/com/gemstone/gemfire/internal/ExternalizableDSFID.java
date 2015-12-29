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
package com.gemstone.gemfire.internal;

import java.io.*;

/**
 * Abstract class for subclasses that want to be Externalizable in
 * addition to being DataSerializableFixedID.
 * <p> Note: subclasses must also provide a zero-arg constructor
 *
 * @author Darrel Schneider
 * @since 5.7 
 */
public abstract class ExternalizableDSFID
  implements DataSerializableFixedID, Externalizable
{
  public abstract int getDSFID();
  public abstract void toData(DataOutput out) throws IOException;
  public abstract void fromData(DataInput in) throws IOException, ClassNotFoundException;

  public final void writeExternal(ObjectOutput out) throws IOException {
    toData(out);
  }
  public final void readExternal(ObjectInput in)
    throws IOException, ClassNotFoundException {
    fromData(in);
  }
}
