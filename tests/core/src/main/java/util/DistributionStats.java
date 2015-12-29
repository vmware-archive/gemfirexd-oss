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

package util;

import hydra.*;
import java.util.List;
import perffmwk.*;

/**
 *  Accesses GemFire distribution statistics.
 */

public class DistributionStats {

  /**
   *  reportDistributionStats ENDTASK: Prints distribution statistics
   *  for the system this VM is associated with.
   */
  public static void reportDistributionStats() {
    printDistributionStats();
  }

  /**
   *  Prints distribution statistics.
   */
  public static void printDistributionStats() {

    StringBuffer buf = new StringBuffer( 200 );
    buf.append( "DISTRIBUTION STATS..............\n" );

    buf.append( "TotalSentMessages: " );
    int sent = getStat( "sentMessages", StatSpecTokens.MAX );
    buf.append( sent ).append( "\n" );

    buf.append( "MeanSentMessagesTime: " );
    if ( sent != 0 ) {
      int sentTime = getStat( "sentMessagesTime", StatSpecTokens.MEAN );
      buf.append( sentTime ).append( "\n" );
    } else {
      buf.append( "none" ).append( "\n" );
    }

    buf.append( "TotalReceivedMessages: " );
    int received = getStat( "receivedMessages", StatSpecTokens.MAX );
    buf.append( received ).append( "\n" );

    // processed messages
    buf.append( "TotalProcessedMessages: " );
    int processed = getStat( "processedMessages", StatSpecTokens.MAX );
    buf.append( processed ).append( "\n" );

    // processed messages time
    buf.append( "MeanProcessedMessagesTime: " );
    if ( processed != 0 ) {
      int processedTime = getStat( "processedMessagesTime", StatSpecTokens.MEAN );
      buf.append( processedTime ).append( "\n" );
    } else {
      buf.append( "none" ).append( "\n" );
    }

    Log.getLogWriter().info( buf.toString() );
  }

  private static int getStat( String statName, String opType ) {
    String spec = "* "
                + "DistributionStats "
                + "* "
                + statName + " "
                + StatSpecTokens.FILTER_TYPE + "="
                                + StatSpecTokens.FILTER_NONE + " "
                + StatSpecTokens.COMBINE_TYPE + "="
                                + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                + StatSpecTokens.OP_TYPES + "=" + opType;
    List psvs = PerfStatMgr.getInstance().readStatistics( spec );
    if (psvs == null) {
      Log.getLogWriter().warning( "Could not fetch stat " + statName );
      return 0;
    }
    PerfStatValue psv = (PerfStatValue) psvs.get(0);
    if ( opType.equals( StatSpecTokens.MAX ) ) {
      return (int) psv.getMax();
    } else if ( opType.equals( StatSpecTokens.MEAN ) ) {
      return (int) psv.getMean();
    } else {
      String s = opType + " not implemented yet";
      throw new UnsupportedOperationException( s );
    }
  }
}
