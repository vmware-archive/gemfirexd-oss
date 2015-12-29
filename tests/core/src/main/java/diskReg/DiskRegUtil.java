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
//import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;

public class DiskRegUtil {

/** Get the number of entries in the region whose value resides in the VM
 *
 *  @returns int The number of entries in this region whose value resides in the VM
 */
public static long getNumEntriesInVM(Region aRegion) {
   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
   if (diskReg == null) // no disk is being used
      return aRegion.keys().size();
   if (diskReg.getStats() == null) throw new TestException("stats is NULL");
   // Added this check in response to BUG 41725 (as requested by darrel)
   long diskRegionCount = diskReg.getNumEntriesInVM();
   long statsCount = diskReg.getStats().getNumEntriesInVM();
   if (diskRegionCount != statsCount) {
      throw new TestException("BUG 41725 detected: DiskRegion.getEntriesInVM(" + diskRegionCount + ") != diskReg.getStats().getNumEntriesInVM(" + statsCount + ")" + util.TestHelper.getStackTrace());
   }
   return diskReg.getStats().getNumEntriesInVM();
}


/** Get the number of entries that have been written to disk.  Note: this does 
 *  not take removes into consideration. It is the total number of entries that 
 *  have been written to disk thus far, not the current number of entries on disk.
 *
 *  @returns int The number of entries in this region that have been written to disk.
 */
public static long getNumEntriesWrittenToDisk(Region aRegion) {
   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
   if (diskReg == null) // no disk is being used
      return 0;
   return diskReg.getStats().getWrites();
}


/** Get the number of entries that have overflowed to disk.  
 *
 *  @returns int The number of entries in this region that have overflowed to disk.
 */
public static long getNumOverflowOnDisk(Region aRegion) {
   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
   if (diskReg == null) // no disk is being used
      return 0;
   return diskReg.getStats().getNumOverflowOnDisk();
}

/** Get the value for the given key as it appears in the VM. 
 *
 *  @param aRegion The region that contains the key.
 *  @param key The key in the region.
 *
 *  @returns The value for key as it appears in the VM. Returns null
 *           if the value is not in the VM. 
 */
public static Object getValueInVM(Region aRegion, Object key) {
   return DiskRegVersionHelper.getValueInVM(aRegion, key);
}

/** Get the value for the given key as it appears on disk.
 *
 *  @param aRegion The region that contains the key.
 *  @param key The key in the region.
 *
 *  @returns The value for key as it appears on disk. Returns null
 *           if the value is not on disk or the region does not
 *           write to disk.
 */
public static Object getValueOnDiskOrBuffer(Region aRegion, Object key) {
   try {
      return ((LocalRegion)aRegion).getValueOnDiskOrBuffer(key);
   } catch (IllegalStateException e) {  // region does not write to disk
      return null;
   } catch (EntryNotFoundException e) {
      return null; // if it is not there, the value on the disk is null
   }
}

/** Compare the values for the given key as it appears in the VM,
 * 	and as it appears in the disk region. 
 *
 *  @param region a region expected to contain the values.
 *  @param key an Object containing the key in the region.
 *
 *  @returns 0 if the values both exist and are equal, 
 * 	1 if they both exist but are not equal, -1 if one or both do not exist. 
 */
public static long compareValues(Region aRegion, Object key) {
   return DiskRegVersionHelper.compareValues(aRegion, key);
}

/** Get the number of entries recovered from disk.
*
*  @param aRegion The region.
*
*  @returns The number of entries recovered from disk. Returns 0
*           if the region does not write to disk.
*/
public static long getRecoveredEntryCount(Region aRegion) {
	   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
	   if (diskReg == null) // no disk is being used
	      return 0;
	   return diskReg.getRecoveredEntryCount();
}

/** True if disk region is a backup of the region.
*
*  @param aRegion The region.
*
*  @returns true if the disk region is a backup, false otherwise.
*/
public static boolean isBackup(Region aRegion) {
	   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
	   if (diskReg == null) // no disk is being used
	      return false;
	   return diskReg.isBackup();
}

/** Get the current number of bytes in the disk write buffer.
 *
 *  @returns long The current number of bytes in the disk write buffer.
 */
public static long getBufferSize(Region aRegion) {
   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
   if (diskReg == null) // no disk is being used
      return 0;
   return diskReg.getDiskStore().getStats().getQueueSize();
}

/** Get the total number of bytes that have been written to disk.
 *
 *  @returns long The total number of bytes that have been written to disk.
 */
public static long getBytesWritten(Region aRegion) {
   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
   if (diskReg == null) // no disk is being used
      return 0;
   return diskReg.getStats().getBytesWritten();
}

/** Get the total number of commit operations.
 *
 *  @returns long The total number of commit operations.
 */
public static long getCommits(Region aRegion) {
  return 0;
}

/** Get the number of entries that have been removed from disk.  
 *
 *  @returns long  The total number of region entries that have been removed from disk.
 */
public static long getRemoves(Region aRegion) {
   DiskRegion diskReg = ((LocalRegion)aRegion).getDiskRegion();
   if (diskReg == null) // no disk is being used
      return 0;
   return diskReg.getStats().getRemoves();
}

}
