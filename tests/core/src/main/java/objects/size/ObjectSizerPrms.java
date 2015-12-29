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

/**
 *  A class used to store keys for configuration settings.
 * 
 * @author Lise Storc
 * @since 5.0
 */

public class ObjectSizerPrms extends BasePrms {

  /**
   *  (String)
   *  Type of object.  Must be an object in the "objects" test directory.
   *  Default is "objects.PSTObject".
   */
  public static Long objectType;

  /**
   *  (int)
   *  Index encoded in object.  Default is 0.
   */
  public static Long objectIndex;

  static {
    setValues(ObjectSizerPrms.class);
  }
  public static void main(String args[]) {
    dumpKeys();
  }
}
