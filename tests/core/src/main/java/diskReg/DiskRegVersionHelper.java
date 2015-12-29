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
package diskReg;

import util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;

public class DiskRegVersionHelper {

/** Get the value for the given key as it appears in the VM. 
 *
 *  @param aRegion The region that contains the key.
 *  @param key The key in the region.
 *
 *  @returns The value for key as it appears in the VM. Returns null
 *           if the value is not in the VM. 
 */
public static Object getValueInVM(Region aRegion, Object key) {
   try {
      Object value = ((LocalRegion)aRegion).getValueInVM(key);
      if (value instanceof CachedDeserializable) 
         return ((CachedDeserializable)value).getDeserializedForReading();
      else
         return value;
   } catch (EntryNotFoundException e) {
      return null; // if it is not there, the value in the VM is null
   }
}

public static long compareValues(Region aRegion, Object key) {
	Object vmObject;
	Object diskObject;
	try {
		Object value = ((LocalRegion)aRegion).getValueInVM(key);
		if (value instanceof CachedDeserializable) 
			vmObject = ((CachedDeserializable)value).getDeserializedForReading();
		else
			vmObject = value;
		diskObject = ((LocalRegion)aRegion).getValueOnDiskOrBuffer(key);
		if (vmObject.equals(diskObject))
			return 0;
		return 1;
	} catch (IllegalStateException e) {  // region does not write to disk
		return -1;
	} catch (EntryNotFoundException e) {
		return -1; 
	}
}

}
