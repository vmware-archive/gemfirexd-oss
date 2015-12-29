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

import java.util.*;

public class VecSorter {
private static int doCompare(Object o1, Object o2) throws Exception {
    if ((o1 instanceof String) && (o2 instanceof String)) {
      String str1 = (String) o1;
      str1 = str1.toUpperCase();
      String str2 = (String) o2;
      str2 = str2.toUpperCase();
      return str1.compareTo(str2);
    }
    else 
      throw new Exception("VecSorter can only handle Strings");
}

/*  quick sort support */
private static void swap(Vector arr, int i, int j) {
  Object tmp;
  tmp = arr.elementAt(i);
  arr.setElementAt(arr.elementAt(j), i);
  arr.setElementAt(tmp, j);
}

public static void quickSort (Vector arr, int left, int right) {
  int i, last;
  if (left >= right) {   /* do nothing if array contains fewer than two */
    return;              /* two elements */
  }
    swap(arr, left, (left+right) / 2);
    last = left;
    for (i = left+1; i <= right; i++) {
     try {
        if (doCompare(arr.elementAt(i), arr.elementAt(left)) < 0) {
         swap(arr, ++last, i);
        }
     }
     catch (Exception ex) {
        ex.printStackTrace();
     }
    }
    swap(arr, left, last);
    quickSort(arr, left, last-1);
    quickSort(arr, last+1, right);
}
 public static void main(String[] args) {
    Vector v = new Vector();
    for (int i = 0; i < 10000; i++) {
       v.add(String.valueOf(i));
    }

    
     long begin = System.currentTimeMillis();
     VecSorter.quickSort (v, 0, (v.size() - 1));
     long end = System.currentTimeMillis();
     System.out.println("Time for sort: " + (end - begin));
     for (int i = 0; i < 30; i++) {
        System.out.println(v.elementAt(i));
     }
 } 

}
