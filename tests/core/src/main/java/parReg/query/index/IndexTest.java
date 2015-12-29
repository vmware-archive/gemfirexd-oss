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

package parReg.query.index;

import java.util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;
import distcache.gemfire.GemFireCachePrms;
import query.index.IndexPrms;
import util.*;

public class IndexTest {
   static protected IndexTest indexTest;
    
   public static synchronized void HydraTask_CreateIndex() {
      if(indexTest == null) {
          indexTest = new IndexTest();
      }
      indexTest.createIndex(GemFireCachePrms.getRegionName());
   }
    
   public void createIndex(String RegionName) {
      Region region = RegionHelper.getRegion(RegionName);
      if(region == null) {
         Log.getLogWriter().info("The region is not created properly");
      } else {
         if(!region.isDestroyed()) {
           Log.getLogWriter().info("Obtained the region with name :" + RegionName);
           QueryService qs = CacheHelper.getCache().getQueryService();
           if(qs != null) {
              try{
                 long numOfIndexes = 1;
                 numOfIndexes = TestConfig.tab().longAt(IndexPrms.numOfIndexes, 1); 
                 qs.createIndex("nameIndex", IndexType.PRIMARY_KEY, "name","/" + RegionName);
                 Log.getLogWriter().info("Index nameIndex Created successfully");
                 if(numOfIndexes >= 2) {
                   qs.createIndex("idIndex", IndexType.FUNCTIONAL, "id","/" + RegionName);
                   Log.getLogWriter().info("Index idIndex Created successfully");
                 }
                 if (numOfIndexes >= 3) {
                   qs.createIndex("mktValueIndex", IndexType.FUNCTIONAL, "pVal.mktValue", "/" + RegionName 
                       + " pf, pf.positions.values pVal TYPE Position", "import parReg.\"query\".Position;" );
                   Log.getLogWriter().info("Index mktValueIndex Created successfully");
                 }
                 if (numOfIndexes >= 4){
                   qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status","/" + RegionName);
                   Log.getLogWriter().info("Index statusIndex Created successfully");
                 } //developer needs this to debug
               
              } catch (IndexNameConflictException e) {
                  Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
              }  
              catch (IndexExistsException e) {
                  Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
              } catch(QueryException e) {
                 throw new HydraRuntimeException("Could not create Index " + TestHelper.getStackTrace(e));
              }  
            } else {
               Log.getLogWriter().info("Hey !!!!! Could not obtain QueryService for the cache ");
            }   
         } else {
            Log.getLogWriter().info("Hey !!!!! Region.isDestroyed() returned true for region : " + RegionName); 
         }
      }
   }
   
   public void createIndex1(String RegionName) {
     Region region = RegionHelper.getRegion(RegionName);
     if(region == null) {
        Log.getLogWriter().info("The region is not created properly");
     } else {
        if(!region.isDestroyed()) {
          Log.getLogWriter().info("Obtained the region with name :" + RegionName);
          QueryService qs = CacheHelper.getCache().getQueryService();
          if(qs != null) {
             try{
                qs.createIndex("nameIndex", IndexType.PRIMARY_KEY, "name","/" + RegionName);
                Log.getLogWriter().info("Index nameIndex Created successfully");             
             } catch (IndexNameConflictException e) {
                 Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
             } catch (IndexExistsException e) {
                 Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
             } catch(QueryException e) {
                throw new HydraRuntimeException("Could not create Index " + TestHelper.getStackTrace(e));
             }  
           } else {
              Log.getLogWriter().info("Hey !!!!! Could not obtain QueryService for the cache ");
           }   
        } else {
           Log.getLogWriter().info("Hey !!!!! Region.isDestroyed() returned true for region : " + RegionName); 
        }
     }
  }
   
   public void createIndex2(String RegionName) {
     Region region = RegionHelper.getRegion(RegionName);
     if(region == null) {
        Log.getLogWriter().info("The region is not created properly");
     } else {
        if(!region.isDestroyed()) {
          Log.getLogWriter().info("Obtained the region with name :" + RegionName);
          QueryService qs = CacheHelper.getCache().getQueryService();
          if(qs != null) {
             try{
                qs.createIndex("idIndex", IndexType.FUNCTIONAL, "id","/" + RegionName);
                Log.getLogWriter().info("Index idIndex Created successfully");              
             } catch (IndexNameConflictException e) {
                 Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
             } catch (IndexExistsException e) {
                 Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
             } catch(QueryException e) {
                throw new HydraRuntimeException("Could not create Index " + TestHelper.getStackTrace(e));
             }  
           } else {
              Log.getLogWriter().info("Hey !!!!! Could not obtain QueryService for the cache ");
           }   
        } else {
           Log.getLogWriter().info("Hey !!!!! Region.isDestroyed() returned true for region : " + RegionName); 
        }
     }
  }
  
   public void createIndex3(String RegionName) {
     Region region = RegionHelper.getRegion(RegionName);
     if(region == null) {
        Log.getLogWriter().info("The region is not created properly");
     } else {
        if(!region.isDestroyed()) {
          Log.getLogWriter().info("Obtained the region with name :" + RegionName);
          QueryService qs = CacheHelper.getCache().getQueryService();
          if(qs != null) {
             try{
                qs.createIndex("mktValueIndex", IndexType.FUNCTIONAL, "pVal.mktValue", "/" + RegionName 
                    + " pf, pf.positions.values pVal TYPE Position", "import parReg.\"query\".Position;" );
                Log.getLogWriter().info("Index mktValueIndex Created successfully");              
             } catch (IndexNameConflictException e) {
                 Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
             } catch (IndexExistsException e) {
                 Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
             } catch(QueryException e) {
                throw new HydraRuntimeException("Could not create Index " + TestHelper.getStackTrace(e));
             }  
           } else {
              Log.getLogWriter().info("Hey !!!!! Could not obtain QueryService for the cache ");
           }   
        } else {
           Log.getLogWriter().info("Hey !!!!! Region.isDestroyed() returned true for region : " + RegionName); 
        }
     }
  }
   
   public static synchronized void HydraTask_RemoveIndex() {
      if(indexTest == null) {
          indexTest = new IndexTest();
      }
      indexTest.removeIndex(GemFireCachePrms.getRegionName());
   }
   
   //removes the first available index from the region
   public void removeIndex(String RegionName) {
      Region region = RegionHelper.getRegion(RegionName);
      if(region == null) {
         Log.getLogWriter().info("The region is not created properly");
      } else {
         Log.getLogWriter().info("Obtained the region with name :" + RegionName);
         QueryService qs = CacheHelper.getCache().getQueryService();
         if(qs != null) {
            try{
               Collection indexes = qs.getIndexes(region);
               if(indexes == null) {
                 Log.getLogWriter().info("No index manager defined");
                   return; //no IndexManager defined
               }
               if (indexes.size() == 0) {
                   return; //no indexes defined
               }
               Log.getLogWriter().info("The size of the index to be removed is " + indexes.size());
               Iterator iter = indexes.iterator();
               if (iter.hasNext()) {
                  Index idx = (Index)(iter.next());
                  String name = idx.getName();
                  qs.removeIndex(idx);
                  Log.getLogWriter().info("Index " + name + " removed successfully");
               }
            } catch(Exception e) {
               throw new HydraRuntimeException("Could not remove Index " + TestHelper.getStackTrace(e)); 
            }
         } else {
            Log.getLogWriter().info("Hey !!!!! Could not obtain QueryService for the cache ");
         }   
        
      }
   }
}
