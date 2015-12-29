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

/**
 * Functions for use in test configuration files.  All functions must return
 * Strings suitable for further parsing by hydra.
 */
public class TestConfigFcns {

  private static GsRandom rng = new GsRandom();

//------------------------------------------------------------------------------
// GENERATED NAMES
//------------------------------------------------------------------------------

  /**
   * Generates a list of strings with the specified prefix and suffixed 1,...,n.
   */
  public static String generateNames(String prefix, int n) {
    return generateNames(prefix, n, false);
  }
  /**
   * Generates a list of strings with the specified prefix and suffixed 1,...,n.
   * Note that n is a String containing an int. 
   */
  public static String generateNames(String prefix, String n, boolean useComma) {
    Integer intValue = Integer.valueOf(n);
    return generateNames(prefix, intValue, useComma);
  }
  /**
   * Same as {@link #generateNames(String,int)} but comma-separates the names
   * if asked.
   */
  public static String generateNames(String prefix, int n, boolean useComma) {
    String v = "";
    for ( int i = 1; i <= n; i++ ) {
      v += prefix + i;
      if (i < n) {
        if (useComma) v += ",";
        v += " ";
      }
    }
    return v;
  }

  /**
   * Same as {@link #generateNames(String,int,boolean)} but puts each value in a
   * quoted string.
   */
  public static String generateNamesString(String prefix, int n, boolean useComma) {
    String v = "";
    for ( int i = 1; i <= n; i++ ) {
      v += "\"" + prefix + i + "\"";
      if (i < n) {
        if (useComma) v += ",";
        v += " ";
      }
    }
    return v;
  }

  /**
   * Generates a list of strings with the specified prefix and suffixed
   * m,...,m+n-1.
   */
  public static String generateNames( String prefix, int n, int m ) {
    return generateNames(prefix, n, m, false);
  }
  /**
   * Same as {@link #generateNames(String,int,int)} but comma-separates the
   * names if asked.
   */
  public static String generateNames(String prefix, int n, int m, boolean useComma) {
    String v = "";
    for ( int i = m; i < m+n; i++ ) {
      v += prefix + i;
      if (i < m+n-1) {
        if (useComma) v += ",";
        v += " ";
      }
    }
    return v;
  }

  /**
   * Generates a list of strings with the specified prefix and suffixed 1,...,n
   * with each suffix occurring m times.  If varyFirst is true, the suffixes
   * are increased then repeated, else the suffixes are repeated first.  For
   * example:
   * <p>
   *  generateNamesRepeatedly("frip", 3, 2, true) yields:
   *  frip1 frip2 frip3 frip1 frip2 frip3.
   * <p>
   *  generateNamesRepeatedly("frip", 3, 2, false) yields:
   *  frip1 frip1 frip2 frip2 frip3 frip3.
   */
  public static String generateNamesRepeatedly( String prefix, int n, int m, boolean varyFirst ) {
    String v = "";
    if (varyFirst) {
      for (int j = 1; j <= m; j++) {
        for (int i = 1; i <= n; i++) {
          v += prefix + i;
          if (i*j < m*n) {
            v += " ";
          }
        }
      }
    } else {
      for (int i = 1; i <= n; i++) {
        for (int j = 1; j <= m; j++) {
          v += prefix + i;
          if (i*j < m*n) {
            v += " ";
          }
        }
      }
    }
    return v;
  }

  /**
   * Generates a list of strings with the specified prefix and suffixed
   * 2,...,n, 1, with each suffix occurring m times.  If varyFirst is true,
   * the suffixes are increased then repeated, else the suffixes are repeated
   * first.  For example:
   * <p>
   *  generateNamesRepeatedlyShift("frip", 3, 2, true, false) yields:
   *  frip2 frip3 frip1 frip2 frip3 frip1.
   * <p>
   *  generateNamesRepeatedlyShift("frip", 3, 2, false, true) yields:
   *  frip2, frip2, frip3, frip3, frip1, frip1.
   */
  public static String generateNamesRepeatedlyShift(String prefix, int n, int m, boolean varyFirst, boolean useComma) {
    String v = "";
    if (varyFirst) {
      for (int j = 1; j <= m; j++) {
        for (int i = 1; i <= n; i++) {
          int suffix = i%n + 1;
          v += prefix + suffix;
          if (i*j < m*n) {
            if (useComma) v += ",";
            v += " ";
          }
        }
      }
    } else {
      for (int i = 1; i <= n; i++) {
        for (int j = 1; j <= m; j++) {
          int suffix = i%n + 1;
          v += prefix + suffix;
          if (i*j < m*n) {
            if (useComma) v += ",";
            v += " ";
          }
        }
      }
    }
    return v;
  }

  /**
   * Generates comma-separated lists of strings where each list occurs m times
   * and the i'th unique list contains n-1 entries formed by suffixing the
   * specified prefix from i+1 to i-1.  For example:
   * <p>
   *  generateNameListsRepeatedlyShift("frip", 3, 2) yields:
   *    frip2 frip3, frip2 frip3,
   *    frip3 frip1, frip3 frip1,
   *    frip1 frip2, frip1 frip2
   */
  public static String generateNameListsRepeatedlyShift(String prefix, int n, int m) {
    String v = "";
    for (int i = 1; i <= n; i++) {
      String list = "";
      for (int k = 0; k < n-1; k++) {
        int suffix = (i+k)%n + 1;
        list += prefix + suffix;
        if (k < n-2) list += " ";
      }
      for (int j = 1; j <= m; j++) {
        v += list;
        if (i*j < m*n) {
          v += ", ";
        }
      }
    }
    return v;
  }

  /**
   *  Generates a list of strings with the specified prefix and suffixed x_y,
   *  where x varies 1,...,n and y varies 1,...,m, and x varies first if
   *  varyFirst is true, else y.  For example:
   *  <p>
   *  generateDoubleSuffixedNames("frip", 3, 2, true) yields:
   *  frip_1_1 frip_2_1 frip_3_1 frip_1_2 frip_2_2 frip_3_2.
   *  <p>
   *  generateDoubleSuffixedNames("frip", 3, 2, false) yields:
   *  frip_1_1 frip_1_2 frip_2_1 frip_2_2 frip_3_1 frip_3_2.
   */
  public static String generateDoubleSuffixedNames(String prefix, int n, int m, boolean varyFirst) {
    return generateDoubleSuffixedNames(prefix, n, m, varyFirst, false);
  }
  /**
   * Same as {@link #generateDoubleSuffixedNames(String,int,int,boolean)}
   * but comma-separates the names if asked.
   */
  public static String generateDoubleSuffixedNames(String prefix, int n, int m, boolean varyFirst, boolean useComma) {
    String v = "";
    if (varyFirst) {
      for (int j = 1; j <= m; j++) {
        for (int i = 1; i <= n; i++) {
          v += prefix + "_" + i + "_" + j;
          if (i*j < m*n) {
            if (useComma) v += ",";
            v += " ";
          }
        }
      }
    } else {
      for (int i = 1; i <= n; i++) {
        for (int j = 1; j <= m; j++) {
          v += prefix + "_" + i + "_" + j;
          if (i*j < m*n) {
            if (useComma) v += ",";
            v += " ";
          }
        }
      }
    }
    return v;
  }
  /**
   * Same as {@link #generateDoubleSuffixedNames(String,int,int,boolean)}
   * but comma-separates each run of names.
   */
  public static String generateDoubleSuffixedRunsOfNames(String prefix, int n, int m, boolean varyFirst) {
    String v = "";
    if (varyFirst) {
      for (int j = 1; j <= m; j++) {
        for (int i = 1; i <= n; i++) {
          v += prefix + "_" + i + "_" + j;
          if (i < n) v += " ";
        }
        if (j < m) v += ", ";
      }
    } else {
      for (int i = 1; i <= n; i++) {
        for (int j = 1; j <= m; j++) {
          v += prefix + "_" + i + "_" + j;
          if (j < m) v += " ";
        }
        if (i < n) v += ", ";
      }
    }
    return v;
  }

  /**
   *  Generates a list of strings with the specified prefix and suffixed x_y,
   *  where x is fixed and y varies 1,...,m.  For example:
   *  <p>
   *  generateDoubleSuffixedNames("frip", 3, 2, true) yields:
   *  frip_3_1, frip_3_2.
   *  <p>
   *  generateDoubleSuffixedNames("frip", 3, 2, false) yields:
   *  frip_3_1 frip_3_2.
   */
  public static String generateDoubleSuffixedFixedNames(String prefix, int n, int m) {
    return generateDoubleSuffixedFixedNames(prefix, n, m, false);
  }
  /**
   * Same as {@link #generateDoubleSuffixedNames(String,int,int,boolean)}
   * but comma-separates the names if asked.
   */
  public static String generateDoubleSuffixedFixedNames(String prefix, int n, int m, boolean useComma) {
    String v = "";
    for (int j = 1; j <= m; j++) {
      v += prefix + "_" + n + "_" + j;
      if (n*j < m*n) {
        if (useComma) v += ",";
        v += " ";
      }
    }
    return v;
  }

//------------------------------------------------------------------------------
// DUPLICATED NAMES
//------------------------------------------------------------------------------

  /**
   *  Duplicates a value n times and returns it as a list.
   */
  public static String duplicate( String val, int n ) {
    return duplicate(val, n, false);
  }
  /**
   * Duplicates a value n times and returns it as a list, complaining if it
   * is not the enforced value.
   */
  public static String duplicateEnforced(String val, String enforced, int n) {
    if (val == null) {
      String s = "Provided value cannot be null";
      throw new HydraConfigException(s);
    }
    else if (enforced == null) {
      String s = "Enforced value cannot be null";
      throw new HydraConfigException(s);
    }
    else if (!val.equals(enforced)) {
      String s = "Value is enforced to be \"" + enforced + "\""
               + ", cannot be set to \"" + val + "\"";
      throw new HydraConfigException(s);
    }
    return duplicate(enforced, n, false);
  }
  /**
   * Same as {@link #duplicate(String,int) but comma-separates the values if
   * asked.
   */
  public static String duplicate(String val, int n, boolean useComma) {
    String v = "";
    for ( int i = 1; i <= n; i++ ) {
      v += val;
      if (i < n) {
        if (useComma) v += ",";
        v += " ";
      }
    }
    return v;
  }

  /**
   * Same as {@link #duplicate(String,int,boolean)} but puts each value in a
   * quoted string.
   */
  public static String duplicateString(String val, int n, boolean useComma) {
    String v = "";
    for ( int i = 1; i <= n; i++ ) {
      v += "\"" + val + "\"";
      if (i < n) {
        if (useComma) v += ",";
        v += " ";
      }
    }
    return v;
  }

//------------------------------------------------------------------------------
// POOLED NAMES
//------------------------------------------------------------------------------

  /**
   * Duplicates a list until n values are created, and returns it as a list.
   * <p>
   * Example: pool("a b c", 5) = "a b c a b"
   * <p>
   * Example: pool("a b c", 2) = "a b"
   */
  public static String pool(String list, int n) {
    Vector tokens = new Vector();
    StringTokenizer tokenizer = new StringTokenizer(list, " ", false);
    while (tokenizer.hasMoreTokens()) {
      tokens.add(tokenizer.nextToken());
    }
    String v = "";
    for (int i = 0; i < n; i++) {
      v += tokens.elementAt(i%tokens.size());
      if (i < n-1) {
        v += " ";
      }
    }
    return v;
  }

  /**
   * Duplicates a list of lists until n values are created from each list, and
   * returns it as a list.
   * <p>
   * Example: poolList("a b, c", 5) = "a b a b a c c c c c"
   * <p>
   * Example: poolList("a b, c", 2) = "a b c c"
   */
  public static String poolList(String listOfLists, int n) {
    String v = "";
    StringTokenizer listTokenizer =
                    new StringTokenizer(listOfLists, ",", false);
    while (listTokenizer.hasMoreTokens()) {
      String list = listTokenizer.nextToken();
      Vector tokens = new Vector();
      StringTokenizer tokenizer = new StringTokenizer(list, " ", false);
      while (tokenizer.hasMoreTokens()) {
        tokens.add(tokenizer.nextToken().trim());
      }
      for (int i = 0; i < n; i++) {
        v += tokens.elementAt(i%tokens.size());
        v += " ";
      }
    }
    return v.trim();
  }

  /**
   * Duplicates a list of lists until n values are created from each list,
   * for a total of m pools, and returns it as a list.
   * <p>
   * Example: poolList("a b, c", 3, 3) = "a b a c c c a b a"
   * <p>
   * Example: poolList("a b, c", 2, 5) = "a b c c a b c c a b"
   */
  public static String poolList(String listOfLists, int n, int m) {
    if (n <= 0 || m <= 0) {
      throw new IllegalArgumentException("integer arguments must be positive");
    }
    String v = "";
    int count = 0;
    while (true) {
      StringTokenizer listTokenizer =
                      new StringTokenizer(listOfLists, ",", false);
      while (listTokenizer.hasMoreTokens()) {
        String list = listTokenizer.nextToken();
        Vector tokens = new Vector();
        StringTokenizer tokenizer = new StringTokenizer(list, " ", false);
        while (tokenizer.hasMoreTokens()) {
          tokens.add(tokenizer.nextToken().trim());
        }
        for (int i = 0; i < n; i++) {
          v += tokens.elementAt(i%tokens.size());
          v += " ";
        }
        ++count;
        if (count == m) {
          return v.trim();
        }
      }
    }
  }

  /**
   * Duplicates a list of lists until n lists are created, and returns it as a
   * list of lists.
   *
   * Example:
   *    poolListOfLists("a1 a2, b1 b2 b3", 3) = "a1 a2, b1 b2 b3, a1 a2";
   */
  public static String poolListOfLists(String listOfLists, int n) {
    List lists = new ArrayList();
    StringTokenizer tokenizer = new StringTokenizer(listOfLists, ",", false);
    while (tokenizer.hasMoreTokens()) {
      String list = tokenizer.nextToken();
      lists.add(list.trim());
    }
    String s = "";
    for (int i = 0; i < n; i++) {
      s += (String)lists.get(i%lists.size());
      if (i != n-1) s += ",";
      s += " ";
    }
    return s;
  }

//------------------------------------------------------------------------------
// ONEOF
//------------------------------------------------------------------------------

  /**
   * Returns a random choice from the supplied arguments.  This is parsed
   * once by master so allows multiple logical description objects to share
   * the same randomly chosen value.  Note that the random seed is NOT
   * controlled by hydra.Prms-randomSeed.
   */
  public static String oneof(String arg0, String arg1) {
    switch (rng.nextInt(0, 1)) {
      case 0: return arg0;
      case 1: return arg1;
      default: throw new HydraInternalException("Should not happen");
    }
  }

  /**
   * Returns a random choice from the supplied arguments.  This is parsed
   * once by master so allows multiple logical description objects to share
   * the same randomly chosen value.  Note that the random seed is NOT
   * controlled by hydra.Prms-randomSeed.
   */
  public static String oneof(String arg0, String arg1, String arg2) {
    switch (rng.nextInt(0, 2)) {
      case 0: return arg0;
      case 1: return arg1;
      case 2: return arg2;
      default: throw new HydraInternalException("Should not happen");
    }
  }
}
