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

package query.index;

/**
 *
 * @author  prafulla
 *
 *This test to test the functionality of index creation or index removal concurrently happening on the region
 *along with region or entry operations.
 *
 *sh build.sh execute-battery -Dbt.file=/export/shared/users/prafulla/svn/gemfire/trunk/tests/query/index/IndexTest.bt 
 *
 */
import java.util.*;
import java.io.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.data.*;
import hydra.*;
import util.*;

public class IndexWithRegionOperationsTest {
    
    static IndexWithRegionOperationsTest indexTest = new IndexWithRegionOperationsTest();
    
    static final int REGION_OPERATIONS = 30;
    static final int INDEX_OPERATIONS = 4;
    static volatile int counter1 = 0;
    static volatile int counter2 = 0;
    
    /** Creates a new instance of IndexWithRegionOperationsTest */
    public IndexWithRegionOperationsTest() {
    }////end of constructor
    
    ////////////// hydra init task methods//////////////
    public synchronized static void HydraTask_initialize() {
        if(indexTest == null){
            indexTest = new IndexWithRegionOperationsTest();
        }
        indexTest.initialize();
    }//end of HydraTask_initialize
    
    protected void initialize(){
        if(nonMirroredRegion == null){
            createNonMirroredRegion();
        }
        if (mirroredRegion == null){
            createMirroredRegion();
        }
        if (globalRegion == null){
            createGlobalRegion();
        }
        if (pbtRegion == null){
            createPersistBackUpRegion();
        }
    }//end of initialize
    
    /*create region methods for regions used in tests*/
    
    ///////variables required...
    static DistributedSystem ds = null;
    static Properties props = new Properties();
    static Cache cache;
    static Region nonMirroredRegion;
    static Region mirroredRegion;
    static Region globalRegion;
    static Region pbtRegion;//a region which has persist back up true settings
    
    public static void createCache(){
        try{
            ds = DistributedSystem.connect(props);
            cache = CacheFactory.create(ds);
        }catch(Exception e){
            Log.getLogWriter().info("Got expected Exception: caught while creating cache");
            e.printStackTrace();
        }
    }//end of createCache
    
    public void createNonMirroredRegion(){
        try{
            if(cache == null || cache.isClosed()){
                createCache();
            }
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setIndexMaintenanceSynchronous(true);
            factory.setValueConstraint(Country.class);
            nonMirroredRegion = cache.createVMRegion("CountriesNM", factory.createRegionAttributes());
            Log.getLogWriter().info("non mirrored region created");
            
        }catch(Exception e){
            Log.getLogWriter().info("Got expected Exception: caught while creating non-mirrored region");
            e.printStackTrace();
        }
    }//end of createNonMirroredRegion
    
    
    public void createMirroredRegion(){
        try{
            if(cache == null || cache.isClosed()){
                createCache();
            }
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setMirrorType(MirrorType.KEYS_VALUES);
            factory.setIndexMaintenanceSynchronous(true);
            factory.setValueConstraint(Country.class);
            mirroredRegion = cache.createVMRegion("CountriesM", factory.createRegionAttributes());
            Log.getLogWriter().info("mirrored region created");
            
        }catch(Exception e){
            Log.getLogWriter().info("Got expected Exception: caught while creating mirrored region");
            e.printStackTrace();
        }
    }//end of createMirroredRegion
    
    public void createGlobalRegion(){
        try{
            if(cache == null || cache.isClosed()){
                createCache();
            }
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.GLOBAL);
            factory.setIndexMaintenanceSynchronous(true);
            factory.setValueConstraint(Country.class);
            globalRegion = cache.createVMRegion("CountriesG", factory.createRegionAttributes());
            Log.getLogWriter().info("global region created");
            
        }catch(Exception e){
            Log.getLogWriter().info("Got expected Exception: caught while creating global region");
            e.printStackTrace();
        }
    }//end of createGlobalRegion
    
    protected void createPersistBackUpRegion(){
        try{
            if(cache == null || cache.isClosed()){
                createCache();
            }
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
            long l = ds.getId();
            String str = Long.toString(l);
            File []f = new File[1];            
            f[0] = new File("persist"+str);
            f[0].mkdir();
            factory.setDiskStoreName(cache.createDiskStoreFactory()
                                     .setDiskDirs(f)
                                     .create("CountriesPBT")
                                     .getName());
            factory.setDiskSynchronous(false);
            pbtRegion = cache.createVMRegion("CountriesPBT", factory.createRegionAttributes());
            Log.getLogWriter().info("CountriesPBT region created");
        } catch(Exception ex){
            Log.getLogWriter().info("Got expected Exception: caught while creating persist back up true type region");
            ex.printStackTrace();
        }
    }//createPersistBackUpRegion
    
    //////////////////////// hydra test tasks
    public static void HydraTask_PerformIndex() {
        if(indexTest == null) {
            indexTest = new IndexWithRegionOperationsTest();
        }
        indexTest.performIndex();
    }//end of HydraTask_PerformIndex
    
    
    public static void HydraTask_PerformRegionOperations() {
        if(indexTest == null) {
            indexTest = new IndexWithRegionOperationsTest();
        }
        indexTest.performRegionOperation();
    }//end of HydraTask_PerformRegionOperations
    
    public static void HydraTask_PerformExecuteQuery() {
        if(indexTest == null) {
            indexTest = new IndexWithRegionOperationsTest();
        }
        indexTest.performExecuteQuery();
    }//end of HydraTask_PerformExecuteQuery
    
    //////////////////////////supplementary methods
    
    protected void performIndex(){
        counter1 ++;
        int j = (counter1 % 4);
        try{
            //for(int j = 0; j<3; j++){
                Region region = getRegion(j);
                if(region == null){
                    throw new TestException("Null Region");
                }
                
                for(int i = 1; i< 3; i++){
                    switch(i % 2){
                        case 1:
                            Log.getLogWriter().info("creating indexes on region: "+region.getName());
                            createIndex(region);
                            break;
                        case 0:
                            Log.getLogWriter().info("removing indexes from region: "+region.getName());
                            removeIndex(region);
                            break;
                        default:
                            Log.getLogWriter().info("No mathing operations found for: " + (i % 2) + "" +
                            "Trying again...");
                            break;
                    }//end of swith block
                }
            //}
        }catch(IndexNameConflictException ince){
            Log.getLogWriter().info("Index with the same name already exists while index creation");
        }
        catch(Exception e){
            Log.getLogWriter().info("Got expected Exception: caught while performning index operations on region"+e);
            e.printStackTrace();
        }
    }//end of performIndex
    
    protected void createIndex(Region region) throws Exception {
        QueryService qs;
        qs = cache.getQueryService();
        try{
        if(region.getName().equalsIgnoreCase("CountriesNM")){
            qs.createIndex("villageName-nm", IndexType.FUNCTIONAL, "v.name", "/CountriesNM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("cityName-nm", IndexType.FUNCTIONAL, "ct.name", "/CountriesNM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("districtName-nm", IndexType.FUNCTIONAL, "d.name", "/CountriesNM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("stateName-nm", IndexType.FUNCTIONAL, "s.name", "/CountriesNM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName1-nm", IndexType.FUNCTIONAL, "c.name", "/CountriesNM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName2-nm", IndexType.FUNCTIONAL, "c.name", "/CountriesNM c");
            Log.getLogWriter().info("Index creation is done on region "+region.getName());
            
        }else if(region.getName().equalsIgnoreCase("CountriesM")){
            
            qs.createIndex("villageName-m", IndexType.FUNCTIONAL, "v.name", "/CountriesM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("cityName-m", IndexType.FUNCTIONAL, "ct.name", "/CountriesM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("districtName-m", IndexType.FUNCTIONAL, "d.name", "/CountriesM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("stateName-m", IndexType.FUNCTIONAL, "s.name", "/CountriesM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName1-m", IndexType.FUNCTIONAL, "c.name", "/CountriesM c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName2-m", IndexType.FUNCTIONAL, "c.name", "/CountriesM c");
            Log.getLogWriter().info("Index creation is done on region "+region.getName());
            
        }else if(region.getName().equalsIgnoreCase("CountriesG")){
            
            qs.createIndex("villageName-g", IndexType.FUNCTIONAL, "v.name", "/CountriesG c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("cityName-g", IndexType.FUNCTIONAL, "ct.name", "/CountriesG c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("districtName-g", IndexType.FUNCTIONAL, "d.name", "/CountriesG c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("stateName-g", IndexType.FUNCTIONAL, "s.name", "/CountriesG c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName1-g", IndexType.FUNCTIONAL, "c.name", "/CountriesG c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName2-g", IndexType.FUNCTIONAL, "c.name", "/CountriesG c");
            Log.getLogWriter().info("Index creation is done on region "+region.getName());
            
        }else if(region.getName().equalsIgnoreCase("CountriesPBT")){
            
            qs.createIndex("villageName-pbt", IndexType.FUNCTIONAL, "v.name", "/CountriesPBT c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("cityName-pbt", IndexType.FUNCTIONAL, "ct.name", "/CountriesPBT c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("districtName-pbt", IndexType.FUNCTIONAL, "d.name", "/CountriesPBT c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("stateName-pbt", IndexType.FUNCTIONAL, "s.name", "/CountriesPBT c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName1-pbt", IndexType.FUNCTIONAL, "c.name", "/CountriesPBT c, c.states s, s.districts d, d.cities ct, d.villages v");
            qs.createIndex("countryName2-pbt", IndexType.FUNCTIONAL, "c.name", "/CountriesPBT c");
            Log.getLogWriter().info("Index creation is done on region "+region.getName());
        }
        
        }catch(Exception e){
            Log.getLogWriter().info("Got expected Exception: caught while creating index on region "+e);
        }
        
    }//end of createIndex
    
    protected void removeIndex(Region region) throws Exception {
        QueryService qs;
        Index idx=null;
        qs = cache.getQueryService();
        if(qs != null) {
            try{
                Collection indexes = qs.getIndexes(region);
                if(indexes == null) {
                    Log.getLogWriter().info("No indexes are found on this region: "+region.getName()+" Collection of indexes is found null");
                    return; //no IndexManager defined
                }
                if (indexes.size() == 0) {
                    Log.getLogWriter().info("No indexes are found on this region: "+region.getName() + "Size of collection of indexes is found zero");
                    return; //no indexes defined
                }
                Log.getLogWriter().info("-------- Region "+region.getName()+" has "+indexes.size()+" indexes");
                Iterator iter = indexes.iterator();
                //Iterator iter = (qs.getIndexes(region)).iterator();
                while (iter.hasNext()) {
                    idx = (Index)(iter.next());
                    String name = idx.getName();
                    qs.removeIndex(idx);
                    Log.getLogWriter().info("Index " + name + " removed successfully");
                }
            } catch(Exception e) {
                //e.printStackTrace();
                Log.getLogWriter().info("Got expected Exception: caught while removing index from region: "+region.getName()+"/n Index Name is: "+idx.getName());
                e.printStackTrace();
                //throw new HydraRuntimeException("Could not remove Index " + e);
            }
        }else{
            Log.getLogWriter().info("No query service is found for this cache while index removal");
        }
        
    }//end of removeIndex
    
    protected void performRegionOperation() {
        counter2 ++;
        int j = (counter2 % 4);
        try{
            //for (int j=0; j<3; j++){
                Region region = getRegion(j);
                if(region == null){
                    throw new TestException("Null Region");
                }
                
                for (int i = 1; i < REGION_OPERATIONS + 1; i++){
                    switch(i % 7){
                        case 0:
                            doClear(region);
                            break;
                        case 1:
                            doPut(region);
                            break;
                        case 2:
                            doUpdate(region);
                            break;
                        case 3:
                            doGet(region);
                            break;
                        case 4:
                            doDestroy(region);
                            break;
                        case 5:
                            doInvalidate(region);
                            break;
                        case 6:
                            doRemove(region);
                            break;
                        default:
                            Log.getLogWriter().info("No mathing operations found for: " + (i % 7) + "" +
                            "Trying again...");
                            break;
                            
                    }//end of switch
                }
            //}
        }catch(EntryNotFoundException enfe){
            Log.getLogWriter().info("Entry Not Found while performing operation on entry");
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
    }//end of performRegionOperation
    
    static int PUT = 200;
    static int GET = 10;
    static int UPDATE = 10;
    static int DESTROY = 20;
    static int INVALIDATE = 30;
    static int REMOVE = 40;
    
    public static void doPut(Region region) throws Exception {
        int temp = 0;
        for(int i=(PUT-50); i<PUT; i++){
            region.put(new Integer(i), new Country(i, 2, 2, 2, 2));
            temp++;
        }
        PUT = temp;
        Log.getLogWriter().info("Performed put on region: "+region.getName());
    }//end of doPut
    
    public static void doUpdate(Region region) throws Exception {
        int temp=0;
        for(int i=(UPDATE-10); i<UPDATE; i++){
            region.put(new Integer(i), new Country(i+10, 2, 2, 2, 2));
            temp++;
        }
        GET = 50+temp;
        Log.getLogWriter().info("Performed update on region: "+region.getName());
    }//end of doUpdate
    
    public static void doGet(Region region) throws Exception {
        int temp=0;
        for(int i=(GET-10); i<GET; i++){
            region.get(new Integer(i));
            temp++;
        }
        GET = 50+temp;
        Log.getLogWriter().info("Performed get on region: "+region.getName());
    }//end of doGet
    
    public static void doDestroy(Region region) throws Exception {
        int temp=0;
        for(int i=(DESTROY-10); i<DESTROY-5; i++){
            region.destroy(new Integer(i));
            temp++;
        }
        DESTROY = 50+temp;
        Log.getLogWriter().info("Performed destroy on region: "+region.getName());
    }//end of doDestroy
    
    public static void doInvalidate(Region region) throws Exception {
        int temp=0;
        for(int i=(INVALIDATE-10); i<INVALIDATE; i++){
            region.invalidate(new Integer(i));
            temp++;
        }
        INVALIDATE = 50+temp;
        Log.getLogWriter().info("Performed invalidate on region: "+region.getName());
    }//end of doInvalidate
    
    public static void doRemove(Region region) throws Exception {
        int temp=0;
        for(int i=(REMOVE-10); i<REMOVE; i++){
            region.remove(new Integer(i));
            temp++;
        }
        REMOVE = 50+temp;
        Log.getLogWriter().info("Performed put remove region: "+region.getName());
    }//end of doRemove
    
    public static void doClear(Region region) throws Exception {
        region.clear();
        Log.getLogWriter().info("Performed clear on region: "+region.getName());
    }//end of doClear
    
    ////////////// hydra close task methods//////////////
    public synchronized static void HydraTask_closetask() {        
        indexTest.closeTask();        
    }//end of HydraTask_closetask
    
    protected void closeTask(){
        closeCache();
    }// end of closeTask
    
    ////////////// close cache and disconnect from distributed system //////////////
    
    protected void closeCache(){
        try {
            if(!cache.isClosed()){
                cache.close();
            }
            if(ds.isConnected()){
                ds.disconnect();
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }//end of closeCache
    
    
    /////////// getRegion method gives the region on which operations should be performed
    
    public static Region getRegion(int i){
        Region region = null;
        switch (i){
            case 0:
                region =  nonMirroredRegion;
                break;
            case 1:
                region =  mirroredRegion;
                break;
            case 2:
                region =  globalRegion;
                break;
            case 3:
                region =  pbtRegion;
                break;
            default:
                Log.getLogWriter().info("No mathing region found for: " + i + "" +
                " Trying again...");
                break;
        }//end of switch
        
        return region;
        
    }//end of getRegion
    
    ///////////execute query task
    
    static String [] queries = {
    	"SELECT DISTINCT * FROM /CountriesNM cnm, cnm.states snm, snm.districts dnm, dnm.villages vnm, dnm.cities ctnm, /CountriesM cm, cm.states sm, sm.districts dm, dm.villages vm, dm.cities ctm WHERE vnm.name = vm.name ",
    	"SELECT DISTINCT * FROM /CountriesNM cnm, cnm.states snm, snm.districts dnm, dnm.villages vnm, dnm.cities ctnm, /CountriesPBT cpbt, cpbt.states spbt, spbt.districts dpbt, dpbt.villages vpbt, dpbt.cities ctpbt WHERE ctnm.name = ctpbt.name ",
    	"SELECT DISTINCT * FROM /CountriesG cg, cg.states sg, sg.districts dg, dg.villages vg, dg.cities ctg, /CountriesPBT cpbt, cpbt.states spbt, spbt.districts dpbt, dpbt.villages vpbt, dpbt.cities ctpbt WHERE cg.name = cpbt.name ",
    	"SELECT DISTINCT * FROM /CountriesG cg, cg.states sg, sg.districts dg, dg.villages vg, dg.cities ctg, /CountriesM cm, cm.states sm, sm.districts dm, dm.villages vm, dm.cities ctm WHERE sg.name = sm.name ",    	
    	"SELECT DISTINCT * FROM /CountriesG cg, cg.states sg, /CountriesM cm, cm.states sm, /CountriesPBT cpbt, cpbt.states spbt WHERE sg.name = sm.name AND sg.name = sg.name",
    	
    	
    	"SELECT DISTINCT * FROM /CountriesNM cnm, cnm.states snm, snm.districts dnm, dnm.villages vnm, dnm.cities ctnm, /CountriesM cm, cm.states sm, sm.districts dm, dm.villages vm, dm.cities ctm WHERE vnm.name = 'MAHARASHTRA_VILLAGE1' AND ctm.name = 'MUMBAI' ",
    	"SELECT DISTINCT * FROM /CountriesNM cnm, cnm.states snm, snm.districts dnm, dnm.villages vnm, dnm.cities ctnm, /CountriesPBT cpbt, cpbt.states spbt, spbt.districts dpbt, dpbt.villages vpbt, dpbt.cities ctpbt WHERE ctnm.name = 'MUMBAI' AND ctpbt.name = 'PUNE' ",
    	"SELECT DISTINCT * FROM /CountriesG cg, cg.states sg, sg.districts dg, dg.villages vg, dg.cities ctg, /CountriesPBT cpbt, cpbt.states spbt, spbt.districts dpbt, dpbt.villages vpbt, dpbt.cities ctpbt WHERE cg.name = 'INDIA' AND  cpbt.name = 'INDIA'",
    	"SELECT DISTINCT * FROM /CountriesG cg, cg.states sg, /CountriesM cm, cm.states sm, /CountriesPBT cpbt, cpbt.states spbt WHERE sg.name = 'MAHARASHTRA' AND sm.name = 'MAHARASHTRA' AND spbt.name = 'MAHARASHTRA'",
    	
    	"SELECT DISTINCT * FROM /CountriesNM cnm, cnm.states snm, snm.districts dnm, dnm.villages vnm, dnm.cities ctnm, /CountriesM cm, cm.states sm, sm.districts dm, dm.villages vm, dm.cities ctm WHERE vnm.name = vm.name AND vnm.name = 'MAHARASHTRA_VILLAGE1'",
    	"SELECT DISTINCT * FROM /CountriesNM cnm, cnm.states snm, snm.districts dnm, dnm.villages vnm, dnm.cities ctnm, /CountriesPBT cpbt, cpbt.states spbt, spbt.districts dpbt, dpbt.villages vpbt, dpbt.cities ctpbt WHERE ctnm.name = ctpbt.name AND ctpbt.name = 'PUNE'",
    	"SELECT DISTINCT * FROM /CountriesG cg, cg.states sg, sg.districts dg, dg.villages vg, dg.cities ctg, /CountriesPBT cpbt, cpbt.states spbt, spbt.districts dpbt, dpbt.villages vpbt, dpbt.cities ctpbt WHERE cg.name = cpbt.name AND cg.name = 'INDIA'",
    	"SELECT DISTINCT * FROM /CountriesG cg, cg.states sg, sg.districts dg, dg.villages vg, dg.cities ctg, /CountriesM cm, cm.states sm, sm.districts dm, dm.villages vm, dm.cities ctm WHERE sg.name = sm.name AND sg.name = 'MAHARASHTRA' AND sm.name = 'MAHARASHTRA'"   	
    	    	
    	};
    
    static final int QUERY_EXECUTED = 2;
    
    static volatile int queryCntr = 0;
    static Object queryCntrLock = "queryCntrLock";
    
    protected void performExecuteQuery(){
    	Query q=null;	
    	try{
    		String sqlStr;
            long startTime, endTime, totalTime = 0;
            SelectResults rs=null;
            
            
    		//for (int x = 0; x<queries.length; x++){ 
    			System.out.println("Query No: "+ queryCntr + "...execution");
    			sqlStr = queries[queryCntr];
    			QueryService qs = cache.getQueryService();
    			q = qs.newQuery(sqlStr);
    			totalTime = 0;
             
    			for (int i=0; i<QUERY_EXECUTED; i++){
    				startTime = System.currentTimeMillis();
    				rs = (SelectResults) q.execute();
    				endTime = System.currentTimeMillis();
    				totalTime = totalTime + (endTime - startTime);
    			}
             
    			long avgExecutionTime = totalTime/ QUERY_EXECUTED;
    			
    			synchronized(queryCntrLock){
    			  queryCntr++;
    			  if(queryCntr == (queries.length - 1) ){
    			    queryCntr = 0;
    			    }
    			}
    			
    			Log.getLogWriter().info("Average query execution time for query execution is: "+avgExecutionTime+" . Result set size is: "+rs.size()+" . Query string is: "+q.getQueryString()+" .");
         //}
    	}
    	catch(Exception ex){
    		Log.getLogWriter().warning("Got expected Exception: caught while executing query: "+q.toString());
    		ex.printStackTrace();
    	}
    }//end of performExecuteQuery
    
}//end of IndexWithRegionOperationsTest
