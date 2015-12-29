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

package dlock.lockable;

import java.io.Serializable;
//import dlock.*;

/**
 *  Helper class holding an incrementable int.
 */

public class Counter implements Serializable {

  private int count;

  public Counter() {
    this.count = 0;
  }
  public void increment() {
    ++this.count;
  }
  public int value() { 
    return this.count;
  }   
  public String toString() {
    return String.valueOf( this.count );
  }
}
