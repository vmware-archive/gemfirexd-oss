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

package objects.size;

import hydra.*;
import java.io.*;
import java.lang.reflect.*;
import objects.*;

public class ObjectSizer {

  /**
   * Task to determine the size of the serialized object given in {@link
   * objects.size.ObjectSizerPrms#objectType} when encoded with the index given
   * in {@link objects.size.ObjectSizerPrms$objectIndex}.
   * <p>
   * The object must belong to the "objects" package.  Uses normal serialization
   * unless object implements DataSerializable, in which case it uses that
   * instead.
   * <p>
   * Result goes to "size.out".
   */
  public static void sizeTask() throws Exception {

    String objectType =
      TestConfig.tab().stringAt(ObjectSizerPrms.objectType, "objects.PSTObject");
    int objectIndex =
      TestConfig.tab().intAt(ObjectSizerPrms.objectIndex, 0);
    Object obj = ObjectHelper.createObject(objectType, objectIndex);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      // optimized
      Class[] prms = {DataOutput.class};
      Method m = MethExecutor.getMethod(obj.getClass(), "toData", prms);
      Object[] methodArgs = new Object[1];
      methodArgs[0] = (DataOutput)new DataOutputStream(baos);
      m.invoke(obj, methodArgs);
    } catch (NoSuchMethodException e) {
      // non-optimized
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.flush();
    }
    String result = objectType + " with index " + objectIndex
                  + " serialized to " + baos.toByteArray().length + " bytes\n";
    FileUtil.appendToFile("size.out", result);
  }
}
