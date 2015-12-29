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
/*
 * BoundsPrms.java
 *
 * Created on August 5, 2002, 3:36 PM
 */

package util;

import hydra.BasePrms;

public class BoundsPrms extends BasePrms {

/** (Integer) The targeted low point. It is not guaranteed that a point will 
 *   never go below this. */
public static Long targetLowPoint;

/** (Integer) The targeted high point. It is not guaranteed that a point will
 *  never go above this. */
public static Long targetHighPoint;

/** (Boolean) True if the point should bounce between the targetLowPoint and targetHighPoint
 *  values. */
public static Long bounce;
   
// ================================================================================
static {
   BasePrms.setValues(BoundsPrms.class);
}

}
