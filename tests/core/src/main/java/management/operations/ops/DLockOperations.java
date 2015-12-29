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
package management.operations.ops;

import hydra.GsRandom;
import hydra.TestConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import management.operations.OperationPrms;
import management.operations.OperationsBlackboard;
import management.operations.events.DLockOperationEvents;
import util.TestException;

import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedSystem;

public class DLockOperations {
  
  
  public static final int CREATE_DLOCK = 81;
  public static final int DESTROY_DLOCK = 82;
  public static final int LOCKUNLCOK_DLOCK = 83;
  public static final int BECOMEGRANTOR_DLCOK = 84;
  private static long waitTimeMillis = 0;
  private static long leaseTimeMillis = 0;
  private static long delay  =0;
  private static Set<String> dLcokNames = new HashSet<String>();
  private static GsRandom randGen = TestConfig.tab().getRandGen();
  
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Lock readLock = lock.readLock();
  private Lock writeLock = lock.writeLock();
  private DLockOperationEvents operationRecorder = null;
  private DistributedSystem ds= null;
  private String DLOCK_PREFIX = "DLOCK";
  
  public DLockOperations(DistributedSystem ds, DLockOperationEvents events){
    this.ds = ds;
    waitTimeMillis = TestConfig.tab().longAt(OperationPrms.dlockWaitTimeMillis);
    leaseTimeMillis = TestConfig.tab().longAt(OperationPrms.dlockLeaseTimeMillis);
    delay= TestConfig.tab().longAt(OperationPrms.dlockDelay);
    this.operationRecorder = events;
  }  
  
  public DLockOperationEvents getOperationRecorder() {
    return operationRecorder;
  }

  public void setOperationRecorder(DLockOperationEvents operationRecorder) {
    this.operationRecorder = operationRecorder;
  }

  public String createDLock(){
    writeLock.lock();
    try{
      String name = DLOCK_PREFIX + OperationsBlackboard.getBB().getNextDLockCounter();
      DistributedLockService service = DistributedLockService.create(name, ds);
      if(service ==null)
        throw new TestException("Create Operation for LockService named " + name + " failed, returned null");
      dLcokNames.add(name);
      operationRecorder.dlockCreated(name, service);
      return name;
    }finally{
      writeLock.unlock();
    }
  }
  
  public void destroyLock(String name){
    writeLock.lock();
    try{
      if(dLcokNames.contains(name)){
        DistributedLockService service = DistributedLockService.getServiceNamed(name);
        if(service ==null)
          throw new TestException("LockService named " + name + " not found");
        else {
          DistributedLockService.destroy(name);
          operationRecorder.dlockDestroyed(name, service);
        }
      }else{
        throw new TestException("DLock named " + name + " was not created by DLockOperations");
      }
    }finally{
      writeLock.unlock();
    }
  }
  
  public Set<String> listOfAvailableLocks(){
    readLock.lock();
    try{
      return Collections.unmodifiableSet(dLcokNames);
    }finally{
      readLock.unlock();
    }
  }
  
  public void doLockUnLock(String name, long delay){
    readLock.lock();
    try{
      DistributedLockService service = DistributedLockService.getServiceNamed(name);
      if(service ==null)
        throw new TestException("LockService named " + name + " not found");
      else {
        long l1 = System.currentTimeMillis();
        int randomNumber = randGen.nextInt(10);
        //long randomDelay = randGen.nextLong(delay);
        String lockName = name+"_LOCK_" + randomNumber;
        service.lock(lockName, waitTimeMillis, leaseTimeMillis);
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
        service.unlock(lockName);
        long l2 = System.currentTimeMillis();
        operationRecorder.dlockLockUnLock(name, service, l1, l2, lockName);
      }
    }finally{
      readLock.unlock();
    }
  }
  
  public void becomeGrantor(String name){
    readLock.lock();
    try{
      DistributedLockService.becomeLockGrantor(name);
      DistributedLockService service = DistributedLockService.getServiceNamed(name);
      operationRecorder.dlockBecomeGrantor(name, service);
    }finally{
      readLock.unlock();
    }
  }


}
