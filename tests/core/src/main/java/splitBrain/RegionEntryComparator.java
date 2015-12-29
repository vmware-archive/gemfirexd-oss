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

package splitBrain;

import hydra.*;
import util.*;

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.admin.*;

/**
 * Implementation of Comparator for Array of Region.Entry with String keys.
 */

public class RegionEntryComparator implements Comparator {

   public int compare(Object o1, Object o2) {
      String key1 = (String)((Region.Entry)o1).getKey();
      long id1 = NameFactory.getCounterForName(key1);

      String key2 = (String)((Region.Entry)o2).getKey();
      long id2 = NameFactory.getCounterForName(key2);

      if (id1 > id2) {
         return(1);
      } else if (id1 < id2) {
         return(-1);
      } 
      return(0);
   }

   public boolean equals(Object o) {
      Object key1 = ((Region.Entry)this).getKey();
      Object key2 = ((Region.Entry)o).getKey();

      if (key1.equals(key2)) {
         return true;
      } 
      return false;
   }

}
