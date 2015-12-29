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

package objects;

import java.io.Serializable;

/**
 * An integer based on index for which an even numbered index produces
 * a positive integer and an odd numbered index produces a negative integer.
 * This allows testing negative keys.
 */
public class PosNegInteger implements Serializable {

  public static Integer init(int index) {
    return index % 2 == 0 ? Integer.valueOf(index)
                          : 0 - Integer.valueOf(index);
  }
}
