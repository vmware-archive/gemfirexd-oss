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
package management.operations;

/**
 * 
 * TODO : Add more methods for particular configurations
 * 
 * @author tushark
 * 
 */
public class RegionDefSpecCreator {

  public static String NEW_LINE = System.getProperty("line.separator") + "\t\t";
  public static String REGION_SPEC_SEPARATOR = ":";
  public static String REGION_PREFIX = "OperationsRegion";

  /* Simple method for creating large number of regions */

  public static String createReplicatedRegions(int numRegions) {

    StringBuilder sb = new StringBuilder();
    sb.append("\"");
    for (int i = 1; i <= numRegions; i++) {
      StringBuilder regionSpec = new StringBuilder();
      regionSpec.append(" specName = ").append(REGION_PREFIX).append(i).append(REGION_SPEC_SEPARATOR).append(NEW_LINE)
          .append("regionName = ").append("OperationsRegion").append(i).append(REGION_SPEC_SEPARATOR).append(NEW_LINE)
          .append(" scope = ack").append(REGION_SPEC_SEPARATOR).append(NEW_LINE).append(" dataPolicy = replicate")
          .append(REGION_SPEC_SEPARATOR).append(NEW_LINE).append(" entryTTLSec = 0").append(REGION_SPEC_SEPARATOR)
          .append(NEW_LINE).append(" entryTTLAction = destroy").append(REGION_SPEC_SEPARATOR).append(NEW_LINE);
      sb.append(regionSpec.toString());
    }
    sb.append("\"");
    /*
     * specName = OperationsRegion: regionName = OperationsRegion: scope = ack,
     * noack, global: dataPolicy = replicate: entryTTLSec = 0 20: entryTTLAction
     * = destroy invalidate: entryIdleTimeoutSec = 0 20: entryIdleTimeoutAction
     * = destroy invalidate: statisticsEnabled = true:
     */
    return sb.toString();
  }

  public static String createReplicatedRegionsList(int numRegions) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numRegions; i++)
      sb.append(REGION_PREFIX).append(i).append(" ");
    return sb.toString();
  }

  public static void main(String[] args) {

    String sb = createReplicatedRegions(5);

    System.out.println("Generated String \n " + sb);

  }

}
