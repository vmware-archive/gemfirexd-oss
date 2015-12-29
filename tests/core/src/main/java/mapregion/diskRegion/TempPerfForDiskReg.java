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
package mapregion.diskRegion;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import objects.ObjectHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestTask;
import util.TestException;
import util.TestHelper;
import cacheperf.CachePerfPrms;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;

import mapregion.*;

public class TempPerfForDiskReg
{

  static TempPerfForDiskReg testInstance;

  static DistributedSystem ds;

  static private Cache cache;

  static  Region diskRegion;
  
  static String regionName;

  static RegionAttributes attr;

  static int totalThreads = 0;

  public TempPerfForDiskReg(){}//end of constructor

  //////////////hydra init task methods//////////////
  public synchronized static void HydraTask_initialize()
  {
    if (testInstance == null) {
      testInstance = new TempPerfForDiskReg();
      testInstance.initialize();
    }
    testInstance.initHydraThreadLocals();

  }//end of HydraTask_initialize


  protected void initialize()
  {
    try {
      ////initialize cache
      initCache();
      attr = RegionHelper.getRegionAttributes(ConfigPrms.getRegionConfig());
      regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();

      TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
      totalThreads = task.getTotalThreads();

      ///create region...
      if (TempPerfForDiskReg.diskRegion == null) {
        diskRegion = RegionHelper.createRegion(regionName, attr);
      }
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }

  }//end of initialize

  private static HydraThreadLocal localkeycount = new HydraThreadLocal();

  protected void initHydraThreadLocals() {
    int startPoint = getStartPoint();
    setKeyCount(startPoint);
  }

  /**
   *  Sets the key count for a thread's workload.
   */
  protected void setKeyCount( int n ) {
    localkeycount.set( new Integer( n ) );
  }

  /**
   *  Gets the key count for a thread's workload.
   */
  protected int getKeyCount() {
    Integer n = (Integer) localkeycount.get();
    if ( n == null ) {
      n = new Integer(0);
      localkeycount.set( n );
    }
    return n.intValue();
  }


  protected synchronized  int getStartPoint() {
    startPoint++;
    return startPoint;
  }

  /**
   * Connects to DistributedSystem and creates cache
   */

  private synchronized void initCache()
  {
        try {
            if (cache == null || cache.isClosed()) {            	
          	cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
            }
          }
          catch (Exception ex) {
            throw new TestException(TestHelper.getStackTrace(ex));
          }
  }//end of initCache

  public static void HydraTask_PerformPuts()
  {
    testInstance.performPutOperations();
  }//end of HydraTask_PerformPuts


  private static volatile long totalPuts=0;

  static Object totalLock = "total";
  static int maxKeys = CachePerfPrms.getMaxKeys();

  static volatile long totalTime=0;
  static volatile int startPoint = 0;

  static ArrayList al = new ArrayList();

  protected void performPutOperations ()
  {
   try
   {
     Object key=null, val=null;
     String objectType = MapPrms.getObjectType();
     long start, end;
     long puts = 0;
     int putKeyInt= 0;

     int startPoint = getKeyCount();
     startPoint = startPoint * maxKeys;

     start = System.currentTimeMillis();
     do{
       putKeyInt++;
       key = ObjectHelper.createName(startPoint + putKeyInt);
       val = ObjectHelper.createObject(objectType, (startPoint + putKeyInt));
       diskRegion.put(key, val);
       puts++;
     } while (putKeyInt < maxKeys);

     end = System.currentTimeMillis();


     synchronized (totalLock) {
       al.add( new Long((long)(end - start)));
       totalPuts = (totalPuts+puts);
       totalTime =totalTime + (end - start);
    }

   }
   catch(Exception ex){
     throw new TestException(TestHelper.getStackTrace(ex));
   }
  }//end of performPutOperations

  public synchronized static void HydraTask_CloseTask(){
    testInstance.printValues();
    testInstance.closeCache();
  }//end of closeTask

  private static FileOutputStream file;
  private static BufferedWriter wr;
  
  protected void printValues() {
	  try{
	  Date date = new Date(System.currentTimeMillis());
  	  file = new FileOutputStream("./diskRegPerf-"+(date.toGMTString().substring(0, date.toGMTString().indexOf("200")+4).replace(' ', '-'))+".txt");
      wr = new BufferedWriter(new OutputStreamWriter(file));
      
      double avgTime = totalTime/totalThreads;
      
      wr.write("====================DISK REGION PERFORMANCE REPORT===============================");                      
      wr.newLine();      
      wr.flush();            
      wr.write(" TOTAL NUMBER OF THREADS ARE: "+totalThreads);
      wr.flush();      
      wr.newLine();
      wr.write(" TOTAL NUMBER OF PUTS IS: "+totalPuts);
      wr.flush(); 
      wr.newLine();
      wr.write(" TOTAL TIME TAKEN IS (total time per thread): "+avgTime);
      wr.flush(); 
      wr.newLine();      
      wr.write(" AVERAGE NUMBER OF PUTS PER SECOND IS: "+((totalPuts*1000)/(avgTime)));
      wr.flush(); 
      
      //write in vm logs too!
      Log.getLogWriter().info(" TOTAL NUMBER OF THREADS ARE: "+totalThreads);
      Log.getLogWriter().info(" key count for this thread is: "+getKeyCount());
      Log.getLogWriter().info(" TOTAL NUMBER OF PUTS IS: "+totalPuts);
      Log.getLogWriter().info(" TOTAL TIME TAKEN IS (total time per thread): "+avgTime);
      Log.getLogWriter().info(" AVERAGE NUMBER OF PUTS PER SECOND IS: "+((totalPuts*1000)/(avgTime)));
	  }
	  catch (Exception ex) {
	        throw new TestException(TestHelper.getStackTrace(ex));
	  }

   Iterator itr = al.iterator();
   while(itr.hasNext()){
     System.out.println( ( (Long)itr.next() ).longValue() );
   }
   System.out.println( "-------------------------------------------------------" );

  }

  public void closeCache()
  {
    try {
      if (cache != null || !cache.isClosed()) {
        CacheHelper.closeCache();
      }      
    }
    catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }//end of closeCache

}//end of TempPerfForDiskReg