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

import sql.SQLPrms;
import hydra.BasePrms;

public class SQLWanPrms extends BasePrms {
  static {
    setValues( SQLWanPrms.class );
  }
  
  /**(int) number of peers in each site. 
   */
  public static Long numOfPeersPerSite;
  
  /**(int) number of data store nodes in each site. 
   */
  public static Long numOfDataStoresPerSite;
  
  /**(int) total number of locators. 
   */
  public static Long numOfLocators;
  
  /**(int) total number of accessors. 
   */
  public static Long numOfAccessors;
  
  /**(int) total number of wan sites. 
   */
  public static Long numOfWanSites;
  
  /**(int) total number of threads per vm. 
   */
  public static Long numOfThreadsPerVM;
  
  /**(int) total number of threads per data store vm. 
   */
  public static Long  numOfThreadsPerStoreVM;
  
  /**(int) total number of threads per data Accessor vm. 
   */
  public static Long  numOfThreadsPerAccessorVM;
    
  /**(int) total number of threads per site. 
   */
  public static Long numOfThreadsPerSite;
  
  /**(int) total number of accessor threads per site. 
   */
  public static Long numOfAccessorThreadsPerSite;
  
  /**(boolean) enable wan gateway queue conflation. 
   */
  public static Long enableQueueConflation;
  
  /**(boolean) enable wan gateway queue persistence. 
   */
  public static Long enableQueuePersistence;
  
 
  
  /**(boolean) is this a wan test. 
   */
  public static Long isWanTest;
  
  /**(boolean) is this a wan test using Accessors/DataStore configuration.. 
   */
  public static Long isWanAccessorsConfig;

  /**(boolean) whether all wan sites using same partition strategies for all tables. 
   */
  public static Long useSamePartitionAllWanSites;
  
  /**(boolean) whether only one wan site publishes. 
   */
  public static Long isSingleSitePublisher;
  
  /**(String Vector) each wan site's remote site id. 
   */
  public static Long mineToRemoteId;
  
  
  /**(int) total number of thin client threads per site. 
   */
  public static Long numOfClientThreadsPerSite;
  
  /**(int) total number of ClientNodes. 
   */
  public static Long numOfClientNodes;
  
  /**(int) number of server nodes in each site. 
   */
  public static Long numOfServersPerSite;
  
  /**(int) whether each wan has its own set of keys. 
   */
  public static Long testWanUniqueKeys;
  
  /**(boolean) whether each wan use default setting for sender configuration. 
   */
  public static Long useDefaultSenderSetting;
}
