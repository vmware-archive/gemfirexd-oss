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

package perffmwk;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;

import hydra.*;

//import java.io.*;
import java.util.*;
//import java.util.regex.*;

/** 
 *
 *  Holds an individual statistics specification generated at runtime.
 *
 */

public class RuntimeStatSpec extends StatSpec {

  /** Cache of matching archives for this spec */
  protected List matchingArchiveCache;

  //////////////////////////////////////////////////////////////////////////// 
  ////    CONSTRUCTORS                                                    ////
  //////////////////////////////////////////////////////////////////////////// 

  protected RuntimeStatSpec() {
    super( "runtime" );
  }
  public RuntimeStatSpec( PerformanceStatistics statInst, StatisticDescriptor statDesc ) {
    super( statInst, statDesc );
    this.name = "runtime";
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    NAMING                                                          ////
  //////////////////////////////////////////////////////////////////////////// 

  /**
   *  For internal use only.  Generates a name for statistics specifications
   *  generated at runtime.
   */
  protected void autogenerateName() {
    synchronized( StatSpec.class ) {
      Assert.assertTrue( this.name == null || this.name.equals( "runtime" ) );
      this.name = StatSpecTokens.STATSPEC + (++INDEX);
    }
  }
  private static int INDEX = -1;

  //////////////////////////////////////////////////////////////////////////// 
  ////    OVERRIDDEN METHODS                                              ////
  //////////////////////////////////////////////////////////////////////////// 

  protected List getMatchingArchives() {
    if ( this.matchingArchiveCache == null ) {
      if (!TestConfig.tab().booleanAt(Prms.useNFS)) {
        String s = "Cannot read statistic archives properly when "
                 + BasePrms.nameForKey(Prms.useNFS) + " is set false";
        throw new HydraConfigException(s);
      }
      String masterUserDir = TestConfig.getInstance().getMasterDescription()
                                       .getVmDescription()
                                       .getHostDescription()
                                       .getUserDir();
      SortedMap allArchives = TestFileUtil.getStatisticArchives(masterUserDir);
      this.matchingArchiveCache = findMatchingArchives( allArchives, getId() );
      if ( Log.getLogWriter().fineEnabled() ) {
        Log.getLogWriter().fine( "Found matching archives: " + this.matchingArchiveCache );
      }
    }
    if ( Log.getLogWriter().fineEnabled() ) {
      Log.getLogWriter().fine( "Matching archives: " + this.matchingArchiveCache );
    }
    return this.matchingArchiveCache;
  }
}
