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

/**
*
* Represents a random choice from within a range of values defined by the
* extremes "upper" and "lower".  Returns the next random choice as
* an int, double, or long.  The random number generator is passed as a
* parameter to allow for consistent results based on the seed of the
* supplied generator.
*
*/
public class Range implements Serializable {
  private double lower;
  private double upper;

  public Range( double lo, double hi ) {
    lower = lo;
    upper = hi;
  }
  public double nextDouble( GsRandom rng ) {
     if ( lower == upper ) return lower;
     return rng(rng).nextDouble(lower, upper);
  }
  public int nextInt( GsRandom rng ) {
     if ( lower == upper ) return (int)lower;
     return rng(rng).nextInt((int) lower, (int) upper);
  }
  public long nextLong( GsRandom rng ) {
     if ( lower == upper ) return (long)lower;
     return rng(rng).nextLong((long) lower, (long) upper);
  }
  public String toString() {
     return "RANGE " + lower + " " + upper + " EGNAR";
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
