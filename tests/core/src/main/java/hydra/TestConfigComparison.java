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

import java.io.*;
import java.util.*;

import util.*;

/**
 *
 *  Used to compare test configuration files for test runs.
 *
 */
public class TestConfigComparison {

  /**
   *  Returns the result of comparing latest.conf files as an array of sorted
   *  difference lists: (dir1diffset,dir2diffset,...dir3diffset).
   *
   * @see FileUtil#getTextAsSet
   */
  public static List[] getLatestConfDiffs( List dirs ) {

    if ( dirs == null ) {
      throw new HydraRuntimeException( "No test directories provided" );
    }
    List[] diffs = new List[ dirs.size() ];
    if ( dirs.size() == 1 ) {
      return diffs;
    }

    // read the files into sorted sets
    Vector sets = new Vector();
    for ( Iterator i = dirs.iterator(); i.hasNext(); ) {
      String dir = (String) i.next();
      try {
        SortedSet set = FileUtil.getTextAsSet( dir + "/latest.conf" );
        sets.add( set );
      } catch( FileNotFoundException e ) {
        Log.getLogWriter().warning( "...skipping " + dir + " for lack of a latest.conf file" );
        sets.add( null );
      } catch( IOException e ) {
        throw new HydraRuntimeException( "Problem reading file", e );
      }
    }

    // capture the differences in sorted sets
    for ( int i = 0; i < sets.size(); i++ ) {
      SortedSet set = (SortedSet) sets.elementAt(i);
      if ( set == null ) {
        diffs[i] = null;
      } else {
        diffs[i] = new ArrayList();
        for ( Iterator it = set.iterator(); it.hasNext(); ) {
          String entry = (String) it.next();
          for ( int j = 0; j < sets.size(); j++ ) {
            if ( j != i ) {
              SortedSet other = (SortedSet) sets.elementAt(j);
              if ( other != null && ! other.contains( entry ) ) {
                diffs[i].add( entry );
                break;
              }
            }
          }
        }
      }
    }
    return diffs;
  }

  public static void main( String args[] ) {
    if ( args.length < 1 ) {
      System.out.println( "Usage: TestConfigComparison <testDirs>" );
      System.exit(1);
    }
    List fns = new ArrayList();
    for ( int i = 0; i < args.length; i++ ) {
      fns.add( args[i] );
    }
    try {
      List[] diffs = getLatestConfDiffs( fns );
      for ( int i = 0; i < diffs.length; i++ ) {
        System.out.println( args[i] + "\n" + diffs[i] + "\n" );
      }
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      System.out.println( TestHelper.getStackTrace( t ) );
      System.exit(1);
    }
  }
}
