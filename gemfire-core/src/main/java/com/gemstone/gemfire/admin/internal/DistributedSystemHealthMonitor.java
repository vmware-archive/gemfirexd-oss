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
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import java.util.*;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * A thread that monitors the health of the distributed system.  It is
 * kind of like a {@link
 * com.gemstone.gemfire.distributed.internal.HealthMonitorImpl}.  In
 * order to get it to place nice with the rest of the health
 * monitoring APIs, this class pretends that it is a
 * <code>GemFireVM</code>.  Kind of hokey, but it beats a bunch of
 * special-case code.
 *
 * @author David Whitlock
 *
 * @since 3.5
 * */
class DistributedSystemHealthMonitor implements Runnable, GemFireVM {

  /** Evaluates the health of the distributed system */
  private DistributedSystemHealthEvaluator eval;

  /** Notified when the health of the distributed system changes */
  private GemFireHealthImpl healthImpl;

  /** The number of seconds between health checks */
  private int interval;

  /** Used for logging */
  private LogWriterI18n logger;

  /** The thread in which the monitoring occurs */
  private Thread thread;

  /** Has this monitor been asked to stop? */
  private volatile boolean stopRequested = false;

  /** The health of the distributed system the last time we checked. */
  private GemFireHealth.Health prevHealth = GemFireHealth.GOOD_HEALTH;

  /** The most recent <code>OKAY_HEALTH</code> diagnoses of the
   * GemFire system */
  private List okayDiagnoses;

  /** The most recent <code>POOR_HEALTH</code> diagnoses of the
   * GemFire system */
  private List poorDiagnoses;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthMonitor</code> that
   * evaluates the health of the distributed system against the given
   * thresholds once every <code>interval</code> seconds.
   *
   * @param eval
   *        Used to evaluate the health of the distributed system
   * @param healthImpl
   *        Receives callbacks when the health of the distributed
   *        system changes
   * @param interval
   *        How often the health is checked
   * @param logger
   *        Used for logging problems
   */
  DistributedSystemHealthMonitor(DistributedSystemHealthEvaluator eval,
                                 GemFireHealthImpl healthImpl,
                                 int interval,
                                 LogWriterI18n logger) {
    this.eval = eval;
    this.healthImpl = healthImpl;
    this.interval = interval;
    this.logger = logger;
    this.okayDiagnoses = new ArrayList();
    this.poorDiagnoses = new ArrayList();

    ThreadGroup group =
      LogWriterImpl.createThreadGroup(LocalizedStrings.DistributedSystemHealthMonitor_HEALTH_MONITORS.toLocalizedString(), logger);
    String name = LocalizedStrings.DistributedSystemHealthMonitor_HEALTH_MONITOR_FOR_0.toLocalizedString(eval.getDescription());
    this.thread = new Thread(group, this, name);
    this.thread.setDaemon(true);
  }

  /**
   * Does the work of monitoring the health of the distributed
   * system. 
   */
  public void run() {
    logger.fine("Monitoring health of " + this.eval.getDescription() +
                " every " + interval + " seconds");

    while (!this.stopRequested) {
      SystemFailure.checkFailure();
      try {
        Thread.sleep(interval * 1000);
        List status = new ArrayList();
        eval.evaluate(status);

        GemFireHealth.Health overallHealth = GemFireHealth.GOOD_HEALTH;
        this.okayDiagnoses.clear();
        this.poorDiagnoses.clear();

        for (Iterator iter = status.iterator(); iter.hasNext(); ) {
          AbstractHealthEvaluator.HealthStatus health =
            (AbstractHealthEvaluator.HealthStatus) iter.next();
          if (overallHealth == GemFireHealth.GOOD_HEALTH) {
            if ((health.getHealthCode() != GemFireHealth.GOOD_HEALTH)) {
              overallHealth = health.getHealthCode();
            }

          } else if (overallHealth == GemFireHealth.OKAY_HEALTH) {
            if (health.getHealthCode() == GemFireHealth.POOR_HEALTH) {
              overallHealth = GemFireHealth.POOR_HEALTH;
            }
          }

          GemFireHealth.Health healthCode = health.getHealthCode();
          if (healthCode == GemFireHealth.OKAY_HEALTH) {
            this.okayDiagnoses.add(health.getDiagnosis());

          } else if (healthCode == GemFireHealth.POOR_HEALTH) {
            this.poorDiagnoses.add(health.getDiagnosis());
            break;
          }
        }
        
        if (overallHealth != prevHealth) {
          healthImpl.healthChanged(this, overallHealth);
          this.prevHealth = overallHealth;
        }
        
      } catch (InterruptedException ex) {
        // We're all done
        // No need to reset the interrupted flag, since we're going to exit.
        break;
      }
    }

    eval.close();
    logger.fine("Stopped checking for distributed system health");
  }

  /**
   * Starts this <code>DistributedSystemHealthMonitor</code>
   */
  void start(){
    this.thread.start();
  }

  /**
   * Stops this <code>DistributedSystemHealthMonitor</code>
   */
  void stop() {
    if (this.thread.isAlive()) {
      this.stopRequested = true;
      this.thread.interrupt();
      this.healthImpl.nodeLeft(null, this);

      try {
        this.thread.join();
      } 
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        this.logger.warning(LocalizedStrings.DistributedSystemHealthMonitor_INTERRUPTED_WHILE_STOPPING_HEALTH_MONITOR_THREAD, ex);
      }
    }
  }

  //////////////////////  GemFireVM Methods  //////////////////////

  public java.net.InetAddress getHost() {
    try {
      return SocketCreator.getLocalHost();

    } catch (Exception ex) {
      throw new com.gemstone.gemfire.InternalGemFireException(LocalizedStrings.DistributedSystemHealthMonitor_COULD_NOT_GET_LOCALHOST.toLocalizedString());
    }
  }
  
  public String getName() {
//    return getId().toString();
    throw new UnsupportedOperationException("Not a real GemFireVM");
  }

  public java.io.File getWorkingDirectory() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public java.io.File getGemFireDir() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }
  
  public java.util.Date getBirthDate() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public Properties getLicenseInfo(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public GemFireMemberStatus getSnapshot() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public RegionSubRegionSnapshot getRegionSnapshot() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public StatResource[] getStats(String statisticsTypeName){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public StatResource[] getAllStats(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }
   
  public DLockInfo[] getDistributedLockInfo(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public void addStatListener(StatListener observer,
                              StatResource observedResource,
                              Stat observedStat){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }
  
  public void removeStatListener(StatListener observer){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }  

  public void addHealthListener(HealthListener observer,
                                GemFireHealthConfig cfg){

  }
  
  public void removeHealthListener(){

  }

  public void resetHealthStatus(){
    this.prevHealth = GemFireHealth.GOOD_HEALTH;
  }

  public String[] getHealthDiagnosis(GemFireHealth.Health healthCode){
    if (healthCode == GemFireHealth.GOOD_HEALTH) {
      return new String[0];

    } else if (healthCode == GemFireHealth.OKAY_HEALTH) {
      String[] array = new String[this.okayDiagnoses.size()];
      this.okayDiagnoses.toArray(array);
      return array;

    } else {
      Assert.assertTrue(healthCode == GemFireHealth.POOR_HEALTH);
      String[] array = new String[this.poorDiagnoses.size()];
      this.poorDiagnoses.toArray(array);
      return array;
    }
  }

  public Config getConfig(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public void setConfig(Config cfg){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public GfManagerAgent getManagerAgent(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }
  
  public String[] getSystemLogs(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public void setInspectionClasspath(String classpath){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }
  
  public String getInspectionClasspath(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }
  
  public Region[] getRootRegions(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public Region getRegion(CacheInfo c, String path) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public Region createVMRootRegion(CacheInfo c, String name,
                                   RegionAttributes attrs) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public Region createSubregion(CacheInfo c, String parentPath,
                                String name, RegionAttributes attrs) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public void setCacheInspectionMode(int mode) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public int getCacheInspectionMode(){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public void takeRegionSnapshot(String regionName, int snapshotId){
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public InternalDistributedMember getId() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public CacheInfo getCacheInfo() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public String getVersionInfo() {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public CacheInfo setCacheLockTimeout(CacheInfo c, int v) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public CacheInfo setCacheLockLease(CacheInfo c, int v) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public CacheInfo setCacheSearchTimeout(CacheInfo c, int v) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public AdminBridgeServer addBridgeServer(CacheInfo cache)
    throws AdminException {

    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public AdminBridgeServer getBridgeInfo(CacheInfo cache, 
                                         int id)
    throws AdminException {

    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public AdminBridgeServer startBridgeServer(CacheInfo cache,
                                             AdminBridgeServer bridge)
    throws AdminException {

    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  public AdminBridgeServer stopBridgeServer(CacheInfo cache,
                                            AdminBridgeServer bridge)
    throws AdminException {

    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  /**
   * This operation is not supported for this object. Will throw 
   * UnsupportedOperationException if invoked.
   */
  public void setAlertsManager(StatAlertDefinition[] alertDefs, 
      long refreshInterval, boolean setRemotely) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  /**
   * This operation is not supported for this object. Will throw 
   * UnsupportedOperationException if invoked.
   */
  public void setRefreshInterval(long refreshInterval) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }

  /**
   * This operation is not supported for this object. Will throw 
   * UnsupportedOperationException if invoked.
   */
  public void updateAlertDefinitions(StatAlertDefinition[] alertDefs,
      int actionCode) {
    throw new UnsupportedOperationException(LocalizedStrings.DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM.toLocalizedString());
  }
}
