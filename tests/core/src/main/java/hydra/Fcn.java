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

package hydra;

import java.io.Serializable;
import java.util.*;

/**
*
* Represents an arbitrary function suitable for passing to beanshell as
* specified in a test configuration file.
*
*/
public class Fcn implements Serializable {

  private Vector values;

  public Fcn( Vector v ) {
     this.values = v;
  }
  public String toBeanshellString() {
     String str = "";
     for ( int i = 0; i < values.size(); i++ ) {
        str = str + values.elementAt( i ) + " ";
     }
     return str;
  }
  public String toString() {
     String str = " FCN ";
     for ( int i = 0; i < values.size(); i++ ) {
        str = str + values.elementAt( i ) + " ";
     }
     return str;
  }
}
