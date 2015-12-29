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

import java.lang.reflect.*;
import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import hydra.Log;

public class Bounds extends java.lang.Object implements java.io.Serializable {

// the possible trends
public static final int UPWARD = 0;
public static final int DOWNWARD = 1;

private static final String TREND_KEY = "CurrentTrend";  // key for the blackboard
private int targetLowPoint;          // the target low point 
private int targetHighPoint;         // the target high point
private boolean bounce;    // true if point between the low and high should gradually bounce from the
                                    //    target high point to the target low point, false if the point
                                    //    should randomly increase or decrease without necessarily
                                    //    hitting the end points

// Information to construct a blackboard to used in this class; the blackboard stores the TREND_KEY
private String bbClassName;
private String bbName;
private String bbType;
   
/** Creates new Bounds */
public Bounds() {
}

/** Creates new Bounds. The blackboard is only used when bounce is set to
 *  true, so bb can be null if bounce is false.
 *
 *  @param low The desired low bound.
 *  @param high The desired high bound.
 *  @param shouldBounce True if the point between the bounds should bounce between the low and
 *         high, false if the point between the bounds should stay within the low and high
 *         bounds without necessarily reaching either low or high points.
 *  @param bbClass The blackboard class to use if bounce is true, or null.
 *  @param bbName The blackboard name to use if bounce is true, or null.
 *  @param bbType The blackboard type to use if bounce is true, or null.
 */
public Bounds(int low, int high, boolean shouldBounce, Class bbClass, String bbName, String bbType) {
   targetLowPoint = low;
   targetHighPoint = high;
   bounce = shouldBounce; 
   this.bbClassName = bbClass.getName();
   this.bbName = bbName;
   this.bbType = bbType;
   if (bounce) {
      if ((bbClass == null) || (bbName == null) || (bbType == null))
         throw new TestException("bounce is true, but the blackboard is not complete; must have a blackboard" +
                   "; bbClass is " + bbClass + ", bbName is " + bbName + ", bbType is " + bbType);
      getBB().getSharedMap().put(Bounds.TREND_KEY, new Integer(UPWARD));
   } 
   Log.getLogWriter().info("Created Bounds with low " + targetLowPoint + ", high " +
       targetHighPoint + ", bounce " + bounce + ", blackboard " + bbClass.getName());
}

/** Return whether the point between the bounds should go upward or downward.
  * If this instance is configured to bounce, then the trend upward will
  * return UPWARD about 80% of the time, and DOWNWARD the remaining 20%
  * of the time; the trend downward will return DOWNWARD about 80% of the
  * time and UPWARD the remaining 20% of the time.
  * 
  * @param currentPoint The current point between the low and high bounds.
  *
  * @return DOWNWARD if the trend shows the object should shrink, UPWARD
  *         if the trend shows the object should grow.
  */  
public int getDirection(int currentPoint) {
   if (bounce) { // bouncing between high and low target points
//      int returnValue = 0;
      hydra.blackboard.SharedMap bbMap = getBB().getSharedMap();
      if (currentPoint >= targetHighPoint) { // time to bounce downward
         bbMap.put(TREND_KEY, new Integer(DOWNWARD));
         return DOWNWARD;
      }
      if (currentPoint <= targetLowPoint) { // time to bounce upward
         bbMap.put(TREND_KEY, new Integer(UPWARD));
         return UPWARD;
      }
      // not at high/low points
      int trend = ((Integer)bbMap.get(TREND_KEY)).intValue();
      if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 80) {
         // about 80% of the time, follow the current trend
         return trend;
      } else { // the other 20% of the time, return the opposite of the trend
         if (trend == UPWARD)
            return DOWNWARD;
         else // trend is downward
            return UPWARD;
      }
   } else {
      if (currentPoint >= targetHighPoint) { 
         return DOWNWARD;
      }
      if (currentPoint <= targetLowPoint) { 
         return UPWARD;
      }
      if (TestConfig.tab().getRandGen().nextBoolean())
         return UPWARD;
      else
         return DOWNWARD;
   }
}
     
/** Return the current instance as a string.
 *
 */
public String toString() {
   return super.toString() + ": targetLowPoint = " + targetLowPoint + ", targetHighPoint = " + targetHighPoint +
               ", bounce = " + bounce;
}

/** Return the current trend as a string. 
 *
 *  @return "UPWARD", or "DOWNWARD"
 */
public static String trendToString(int trend) {
   if (trend == UPWARD)
      return "UPWARD";
   else if (trend == DOWNWARD)
      return "DOWNWARD";
   else
      throw new TestException("Unknown trend " + trend);
}

/** Getter for property bounce.
 *
 * @return Value of property bounce.
 */
public boolean getBounce() {
   return bounce;
}

/** Getter for property targetHighPoint.
 *
 * @return Value of property targetHighPoint.
 */
public int getTargetHighPoint() {
   return targetHighPoint;
}

/** Getter for property targetLowPoint.
 * @return Value of property targetLowPoint.
 */
public int getTargetLowPoint() {
   return targetLowPoint;
}

/** Construct a blackboard for use by this class.
 *
 *  @return A hydra blackboard, used for storing the TREND_KEY.
 */
private Blackboard getBB() {
   try {
      Class bbClass = Class.forName(bbClassName);
      Constructor con = bbClass.getConstructor(new Class[] {String.class, String.class});
      Blackboard bb = (Blackboard)(con.newInstance(new Object[] {bbName, bbType}));
      return bb;
   } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (InvocationTargetException e) {
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
   }
}

}
