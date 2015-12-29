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
package sql.wan;

import java.util.StringTokenizer;
import java.util.Vector;

public class WanTestConfigFcns {
  //used to find remote side locators
  public static String generateRemoteSiteDS(String prefix, int numberOfWanSites) {
    String v = "";
    int n= numberOfWanSites;
    for ( int i = 1; i <= n; i++ ) {

      for (int j = 1; j <= n; j++) { 
        if (i != j)
        v += prefix + j + " ";
      }
      v += ",";
    }
    v += "none"; //no need for accessors/datastores
    return v;
  }
  
  public static String generateLastDoubleSuffixedNames(String prefix, int n, int m, boolean useComma) {
    String v = "";
    int i = n; //use the last wan site
    for (int j = 1; j <= m; j++) {
      v += prefix + "_" + i + "_" + j;
      if (i*j < m*n) {
        if (useComma) v += ",";
        v += " ";
      }
    }

    return v;
  }
  
  /**
   * Duplicates a list repeatedly for n times, and returns it as a list.
   * <p>
   * Example: pool("a b c", 1, 5) = "a b c a b"
   * <p>
   * Example: pool("a b c d", 2, 5) = "a a b b c c d d a a"
   */
  public static String pool(String list, int repeat, int n) {
    Vector<String> tokens = new Vector<String>();
    StringTokenizer tokenizer = new StringTokenizer(list, " ", false);
    while (tokenizer.hasMoreTokens()) {
      tokens.add(tokenizer.nextToken());
    }
    
    int s = tokens.size();
    for (int i = s; i<n ; i++) {
      tokens.add(tokens.elementAt(i%s));
    }
    
    String v = "";
    
    for (int i= 0; i < n; i++) {
      for (int j = 0; j < repeat; j++) {
        v += tokens.elementAt(i) + " ";
      }     
    }
    return v.trim();
  }

}
