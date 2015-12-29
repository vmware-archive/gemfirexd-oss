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
*
* Represents a random choice from within a set of discrete values. 
* There are two flavors of each "next<val>" method.  One flavor works from 
* an internal random number generator.  The other works from a generator 
* passed as a parameter.  The latter lets us get consistent results based 
* on the seed of the supplied generator.
*
*
*/
public class OneOf implements Serializable {

  private Vector values;

  public OneOf( Vector v ) {
     this.values = v;
  }
  public Object next( GsRandom rng ) {
     int i = rng(rng).nextInt(0, values.size() - 1);
     return values.elementAt( i ); 
  }
  public String toString() {
     String str = "ONEOF ";
     for ( int i = 0; i < values.size(); i++ ) {
        str += values.elementAt(i) + " ";
     }
     return str + "FOENO";
  }
  private GsRandom rng(GsRandom rng) {
    if (RemoteTestModule.Master == null && 
        Boolean.getBoolean("hydra.useFixedRandomInMaster")) {
      return new GsRandom(TestConfig.getInstance().getRandomSeed());
    } else {
      return rng;
    }
  }
}
