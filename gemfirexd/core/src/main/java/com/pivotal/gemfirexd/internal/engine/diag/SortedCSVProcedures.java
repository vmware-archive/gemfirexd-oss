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

package com.pivotal.gemfirexd.internal.engine.diag;

/**
 * Encapsulates set operations like intersection, union etc on comma-separated
 * strings that are in sorted order.
 * 
 * @author swale
 */
public final class SortedCSVProcedures {

  // no instances allowed
  private SortedCSVProcedures() {
  }

  // to enable this class to be included in gemfirexd.jar
  public static void dummy() {
  }

  /**
   * Get the intersection between two comma-separated strings assuming both are
   * sorted, or empty string if no intersection.
   * 
   * @param csv1
   *          the first comma-separated string; should be non-null
   * @param csv2
   *          the second comma-separated string; should be non-null
   */
  public static String intersection(String csv1, String csv2) {
    final int csv1Len = csv1.length();
    final int csv2Len = csv2.length();

    final StringBuilder intersect = new StringBuilder();
    // store the start and current position of the current string in the CSVs
    int csv1Start = 0, csv1Cur = 0;
    int csv2Start = 0, csv2Cur = 0;
    char c1, c2;

    // loop till one of the strings does not end
    while (checkEndForIntersect(intersect, csv1, csv1Cur, csv1Len, csv1Start,
        csv2, csv2Cur, csv2Len) == EndStatus.MIDDLE
        && checkEndForIntersect(intersect, csv2, csv2Cur, csv2Len, csv2Start,
            csv1, csv1Cur, csv1Len) == EndStatus.MIDDLE) {
      // check if current strings are equal
      if ((c1 = csv1.charAt(csv1Cur)) != (c2 = csv2.charAt(csv2Cur))) {
        // if the current strings are not equal then go to next string in either
        // csv1 or csv2 depending on whether csv1 was less than or greater than
        // csv2 respectively
        if (c1 < c2) {
          // move to the end of current string in first CSV; if we reach the end
          // of CSV then break from the loop
          if ((csv1Cur = moveToStringEnd(csv1, csv1Cur, csv1Len)) == -1) {
            break;
          }
          // set the start position of first CSV to next string, and rewind to
          // the current string for second CSV
          csv1Start = csv1Cur;
          csv2Cur = csv2Start;
        }
        else {
          // move to the end of current string in second CSV; if we reach the
          // end of CSV then break from the loop
          if ((csv2Cur = moveToStringEnd(csv2, csv2Cur, csv2Len)) == -1) {
            break;
          }
          // set the start position of second CSV to next string, and rewind to
          // the current string for first CSV
          csv2Start = csv2Cur;
          csv1Cur = csv1Start;
        }
      }
      else if (c1 != ',') {
        // if end of current string is not reached then increment the current
        // positions and continue
        ++csv1Cur;
        ++csv2Cur;
      }
      else {
        // found a match; add the string to result and set the start positions
        // to the beginning of next string
        addStringToBuilder(intersect, csv1.substring(csv1Start, csv1Cur));
        csv1Start = ++csv1Cur;
        csv2Start = ++csv2Cur;
      }
    }
    return intersect.toString();
  }

  /**
   * Get the intersection between two comma-separated server group lists
   * assuming both are sorted, or empty string if no intersection. If one of the
   * groups is empty then it is taken to be all members as per convention so
   * intersection will return all groups, so in this way this method is
   * different from {@link #intersection(String, String)} method.
   * 
   * @param csv1
   *          the first comma-separated string; should be non-null
   * @param csv2
   *          the second comma-separated string; should be non-null
   */
  public static String groupsIntersection(String csv1, String csv2) {
    if (csv1.length() == 0 || csv2.length() == 0) {
      return "";
    }
    return intersection(csv1, csv2);
  }

  /**
   * Return true if there is an intersection between two comma-separated strings
   * assuming both are sorted. If only checking for intersection is required
   * then this method will be faster than {@link #intersection(String, String)}
   * since it will terminate as soon as the first intersection is found.
   * 
   * @param csv1
   *          the first comma-separated string; should be non-null
   * @param csv2
   *          the second comma-separated string; should be non-null
   */
  public static Boolean intersect(String csv1, String csv2) {
    final int csv1Len = csv1.length();
    final int csv2Len = csv2.length();

    // store the start and current position of the current string in the CSVs
    int csv1Start = 0, csv1Cur = 0;
    int csv2Start = 0, csv2Cur = 0;
    char c1, c2;

    EndStatus status;
    // loop till one of the strings does not end
    while ((status = checkEndForIntersect(null, csv1, csv1Cur, csv1Len,
        csv1Start, csv2, csv2Cur, csv2Len)) == EndStatus.MIDDLE
        && (status = checkEndForIntersect(null, csv2, csv2Cur, csv2Len,
            csv2Start, csv1, csv1Cur, csv1Len)) == EndStatus.MIDDLE) {
      // check if current strings are equal
      if ((c1 = csv1.charAt(csv1Cur)) != (c2 = csv2.charAt(csv2Cur))) {
        // if the current strings are not equal then go to next string in either
        // csv1 or csv2 depending on whether csv1 was less than or greater than
        // csv2 respectively
        if (c1 < c2) {
          // move to the end of current string in first CSV; if we reach the end
          // of CSV then no match was found
          if ((csv1Cur = moveToStringEnd(csv1, csv1Cur, csv1Len)) == -1) {
            return Boolean.FALSE;
          }
          // set the start position of first CSV to next string, and rewind to
          // the current string for second CSV
          csv1Start = csv1Cur;
          csv2Cur = csv2Start;
        }
        else {
          // move to the end of current string in second CSV; if we reach the
          // end of CSV then no match was found
          if ((csv2Cur = moveToStringEnd(csv2, csv2Cur, csv2Len)) == -1) {
            return Boolean.FALSE;
          }
          // set the start position of second CSV to next string, and rewind to
          // the current string for first CSV
          csv2Start = csv2Cur;
          csv1Cur = csv1Start;
        }
      }
      else if (c1 != ',') {
        // if end of current string is not reached then increment the current
        // positions and continue
        ++csv1Cur;
        ++csv2Cur;
      }
      else {
        // found a match
        return Boolean.TRUE;
      }
    }
    return Boolean.valueOf(status == EndStatus.END_WITH_INTERSECTION);
  }

  /**
   * Return true if there is an intersection between two comma-separated server
   * group lists assuming both are sorted. If one of the groups is empty then it
   * is taken to be all members as per convention so intersection will return
   * true, so in this way this method is different from
   * {@link #intersect(String, String)} method.
   * 
   * @param csv1
   *          the first comma-separated string; should be non-null
   * @param csv2
   *          the second comma-separated string; should be non-null
   */
  public static Boolean groupsIntersect(String csv1, String csv2) {
    if (csv1.length() == 0 && csv2.length() == 0) {
      return Boolean.TRUE;
    }
    return intersect(csv1, csv2);
  }

  /**
   * Get the union between two comma-separated strings assuming both are sorted.
   * 
   * @param csv1
   *          the first comma-separated string; should be non-null
   * @param csv2
   *          the second comma-separated string; should be non-null
   */
  public static String union(String csv1, String csv2) {
    final int csv1Len = csv1.length();
    final int csv2Len = csv2.length();

    final StringBuilder union = new StringBuilder();
    // store the start and current position of the current string in the CSVs
    int csv1Start = 0, csv1Cur = 0;
    int csv2Start = 0, csv2Cur = 0;
    char c1, c2;

    // loop till one of the strings does not end
    while (!checkEndForUnion(union, csv1, csv1Cur, csv1Len, csv1Start, csv2,
        csv2Cur, csv2Len, csv2Start)
        && !checkEndForUnion(union, csv2, csv2Cur, csv2Len, csv2Start, csv1,
            csv1Cur, csv1Len, csv1Start)) {
      // check if current strings are equal
      if ((c1 = csv1.charAt(csv1Cur)) != (c2 = csv2.charAt(csv2Cur))) {
        // if the current strings are not equal then go to next string in either
        // csv1 or csv2 depending on whether csv1 was less than or greater than
        // csv2 respectively
        if (c1 < c2) {
          // move to the end of current string in first CSV; if we reach the end
          // of CSV then add remaining strings and break from the loop
          if ((csv1Cur = moveToStringEnd(csv1, csv1Cur, csv1Len)) == -1) {
            // first add the last string in csv1
            addStringToBuilder(union, csv1.substring(csv1Start));
            // add all the remaining strings in csv2
            addStringToBuilder(union, csv2.substring(csv2Start));
            break;
          }
          // add the current string in csv1 to union
          addStringToBuilder(union, csv1.substring(csv1Start, csv1Cur - 1));
          // set the start position of first CSV to next string, and rewind to
          // the current string for second CSV
          csv1Start = csv1Cur;
          csv2Cur = csv2Start;
        }
        else {
          // move to the end of current string in second CSV; if we reach the
          // end of CSV then add remaining strings and break from the loop
          if ((csv2Cur = moveToStringEnd(csv2, csv2Cur, csv2Len)) == -1) {
            // first add the last string in csv2
            addStringToBuilder(union, csv2.substring(csv2Start));
            // add all the remaining strings in csv1
            addStringToBuilder(union, csv1.substring(csv1Start));
            break;
          }
          // add the current string in csv2 to union
          addStringToBuilder(union, csv2.substring(csv2Start, csv2Cur - 1));
          // set the start position of second CSV to next string, and rewind to
          // the current string for first CSV
          csv2Start = csv2Cur;
          csv1Cur = csv1Start;
        }
      }
      else if (c1 != ',') {
        // if end of current string is not reached then increment the current
        // positions and continue
        ++csv1Cur;
        ++csv2Cur;
      }
      else {
        // found a match; add the string once to result and set the start
        // positions for both CSVs to the beginning of next strings
        addStringToBuilder(union, csv1.substring(csv1Start, csv1Cur));
        csv1Start = ++csv1Cur;
        csv2Start = ++csv2Cur;
      }
    }
    return union.toString();
  }

  /**
   * Get the union between two comma-separated server group lists assuming both
   * are sorted. If one of the groups is empty then it is taken to be all
   * members as per convention so union will return all groups i.e. empty CSV,
   * so in this way this method is different from {@link #union(String, String)}
   * method.
   * 
   * @param csv1
   *          the first comma-separated string; should be non-null
   * @param csv2
   *          the second comma-separated string; should be non-null
   */
  public static String groupsUnion(String csv1, String csv2) {
    if (csv1.length() == 0 || csv2.length() == 0) {
      return "";
    }
    return union(csv1, csv2);
  }

  /**
   * Check if first CSV string has reached the end and also add any intersection
   * to the end of the given StringBuilder.
   * 
   * @param intersect
   *          the StringBuilder that holds the interection result
   * @param csv1
   *          the first CSV to be checked
   * @param pos1
   *          the position in the first CSV
   * @param len1
   *          length of the first CSV
   * @param start1
   *          the start position of the current string in the first CSV
   * @param csv2
   *          the second CSV to be checked
   * @param pos2
   *          the position in the second CSV
   * @param len2
   *          length of the second CSV
   * 
   * @return {@link EndStatus#END} if end of "csv1" has been reached,
   *         {@link EndStatus#END_WITH_INTERSECTION} if end of "csv1" has been
   *         reached and the last string intersects with "csv2" and
   *         {@link EndStatus#MIDDLE} otherwise; if the end of "csv1" has been
   *         reached and there is an intersection with "csv2" then add the
   *         intersection to "intersect"
   */
  private static EndStatus checkEndForIntersect(StringBuilder intersect,
      String csv1, int pos1, int len1, int start1, String csv2, int pos2,
      int len2) {
    // check if first CSV is not exhausted
    if (pos1 < len1) {
      return EndStatus.MIDDLE;
    }
    // first CSV is exhausted; check if second CSV has not been exhausted nor
    // has reached end of current string in which case there is no intersection
    // for last string of "csv1"
    if ((pos2 < len2 && csv2.charAt(pos2) != ',') || (start1 == len1)) {
      return EndStatus.END;
    }
    // match found at the end of "csv1"; add intersection to end of the given
    // StringBuilder
    if (intersect != null) {
      addStringToBuilder(intersect, csv1.substring(start1));
    }
    return EndStatus.END_WITH_INTERSECTION;
  }

  /**
   * Move to the end of current string in the given CSV and return the starting
   * position of the next string, or return -1 if there are no more strings
   * left.
   */
  private static int moveToStringEnd(String csv, int pos, int len) {
    while (csv.charAt(pos++) != ',') {
      if (pos >= len) {
        return -1;
      }
    }
    return pos;
  }

  /**
   * Check if first CSV string has reached the end and also add any remaining
   * strings to the end of the given StringBuilder.
   * 
   * @param union
   *          the StringBuilder that holds the union result
   * @param csv1
   *          the first CSV to be checked
   * @param pos1
   *          the position in the first CSV
   * @param len1
   *          length of the first CSV
   * @param start1
   *          the start position of the current string in the first CSV
   * @param csv2
   *          the second CSV to be checked
   * @param pos2
   *          the position in the second CSV
   * @param len2
   *          length of the second CSV
   * @param start2
   *          the start position of the current string in the second CSV
   * 
   * @return true if end of first CSV has been reached and false otherwise; if
   *         the result is true and there is something remaining in "csv2" then
   *         add it to "union"; also add the last string of "csv1" if it does
   *         not match with any in "csv2"
   */
  private static boolean checkEndForUnion(StringBuilder union, String csv1,
      int pos1, int len1, int start1, String csv2, int pos2, int len2,
      int start2) {
    // check if first CSV is not exhausted
    if (pos1 < len1) {
      return false;
    }
    // first CSV is exhausted; check if second CSV has not been exhausted nor
    // has reached end of current string in which case there is no match for
    // last string of "csv1" so add it to union
    if (pos2 < len2 && csv2.charAt(pos2) != ',') {
      // add the last string of "csv1"
      addStringToBuilder(union, csv1.substring(start1));
    }
    // add remaining string of "csv2" to "union"; this will add the
    // intersection, if any, only once since "csv1"'s last string is not added
    // above for the case of intersection
    addStringToBuilder(union, csv2.substring(start2));
    return true;
  }

  /**
   * Append the given string to the end of given {@link StringBuilder}
   * separating by a comma from the previous string, if any.
   */
  private static void addStringToBuilder(StringBuilder sb, String s) {
    if (sb.length() > 0) {
      sb.append(',');
    }
    sb.append(s);
  }

  /**
   * Status for the result of {@link SortedCSVProcedures#checkEndForIntersect}.
   */
  private static enum EndStatus {
    /** end of current string in the CSV has not been reached */
    MIDDLE,
    /** end of current string in the CSV has been reached */
    END,
    /**
     * end of current string in the CSV has been reached and there is an
     * intersection for the last string
     */
    END_WITH_INTERSECTION,
  }
}
