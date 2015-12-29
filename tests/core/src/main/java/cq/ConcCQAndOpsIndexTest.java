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
package cq;

import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryException;

import util.TestException;
import mapregion.MapPrms;
import hydra.Log;
import hydra.TestConfig;

public class ConcCQAndOpsIndexTest extends ConcCQAndOpsTest {
  protected static ConcCQAndOpsIndexTest concCQAndOpsIndexTest;
  protected boolean createQtyIndex;
  protected boolean createMktValueIndex;
  
  public synchronized static void HydraTask_initialize() {
    if (concCQAndOpsIndexTest == null) {
      concCQAndOpsIndexTest = new ConcCQAndOpsIndexTest();
      concCQAndOpsIndexTest.initialize();
    }
  }
  
  protected void createIndex() {
    createQtyIndex = TestConfig.tab().booleanAt(CQUtilPrms.createQtyIndex, false);
    createMktValueIndex = TestConfig.tab().booleanAt(CQUtilPrms.createMktValueIndex, false);
    
    IndexTest indexTest = new IndexTest();
    String[] regionNames = MapPrms.getRegionNames();
    
    if (createQtyIndex) {
      for (int i =0; i<regionNames.length; i++) {
        try {
          indexTest.createQtyIndex(regionNames[i]);
        } catch (IndexNameConflictException e) {
          Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
        } catch (IndexExistsException e) {
            Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
        } catch(QueryException e) {
           throw new TestException("Could not create Index " + e.getStackTrace());
        }  
      }
    } // end if createQtyIndex
    
    if (createMktValueIndex) {
      for (int i =0; i<regionNames.length; i++) {
        try {
          indexTest.createMktValueIndex(regionNames[i]);
        } catch (IndexNameConflictException e) {
          Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
        } catch (IndexExistsException e) {
            Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
        } catch(QueryException e) {
           throw new TestException("Could not create Index " + e.getStackTrace());
        }  
      }
    } //end if createMktValueIndex
  }

}
