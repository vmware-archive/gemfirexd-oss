/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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
package gfxdperf.ycsb.core.workloads;

import gfxdperf.ycsb.core.DB;
import gfxdperf.ycsb.core.ByteIterator;
import gfxdperf.ycsb.core.LongByteIterator;
import gfxdperf.ycsb.core.RandomByteIterator;
import gfxdperf.ycsb.core.StringByteIterator;
import gfxdperf.ycsb.core.Utils;
import gfxdperf.ycsb.core.Workload;
import gfxdperf.ycsb.core.WorkloadException;
import gfxdperf.ycsb.core.generator.ConstantIntegerGenerator;
import gfxdperf.ycsb.core.generator.CounterGenerator;
import gfxdperf.ycsb.core.generator.DiscreteGenerator;
import gfxdperf.ycsb.core.generator.ExponentialGenerator;
import gfxdperf.ycsb.core.generator.Generator;
import gfxdperf.ycsb.core.generator.HistogramGenerator;
import gfxdperf.ycsb.core.generator.HotspotIntegerGenerator;
import gfxdperf.ycsb.core.generator.IntegerGenerator;
import gfxdperf.ycsb.core.generator.ScrambledZipfianGenerator;
import gfxdperf.ycsb.core.generator.SkewedLatestGenerator;
import gfxdperf.ycsb.core.generator.UniformIntegerGenerator;
import gfxdperf.ycsb.core.generator.ZipfianGenerator;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms.FieldLengthDistribution;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms.InsertOrder;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms.RequestDistribution;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms.ScanLengthDistribution;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The relative
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 *
 * Properties to control the client are in {@link gfxdperf.ycsb.core.workloads.CoreWorkloadPrms}.
 */
public class CoreWorkload extends Workload
{
  public static String table;
  int fieldcount;
  int fieldstart;
  boolean readallfields;
  boolean writeallfields;
  IntegerGenerator keysequence;
  DiscreteGenerator operationchooser;
  IntegerGenerator keychooser;
  Generator fieldchooser;
  IntegerGenerator fieldlengthgenerator;
  CounterGenerator transactioninsertkeysequence;
  IntegerGenerator scanlength;
  boolean orderedinserts;
  int recordcount;
  int myThreadId;
  int numThreads;

  protected static IntegerGenerator getFieldLengthGenerator() throws WorkloadException{
    IntegerGenerator fieldlengthgenerator;
    FieldLengthDistribution fieldlengthdistribution = CoreWorkloadPrms.getFieldLengthDistribution();
    int fieldlength = CoreWorkloadPrms.getFieldLength();
    String fieldlengthhistogram = CoreWorkloadPrms.getFieldLengthHistogramFile();
    if(fieldlengthdistribution == FieldLengthDistribution.constant) {
      fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
    } else if(fieldlengthdistribution == FieldLengthDistribution.uniform) {
      fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
    } else if(fieldlengthdistribution == FieldLengthDistribution.zipfian) {
      fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
    } else if(fieldlengthdistribution == FieldLengthDistribution.histogram) {
      try {
        fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch(IOException e) {
        throw new WorkloadException("Couldn't read field length histogram file: "+fieldlengthhistogram, e);
      }
    } else {
      throw new WorkloadException("Should not happen");
    }
    return fieldlengthgenerator;
  }

  /**
   * Initialize the scenario. Called once before any operations are started.
   */
  public void init(Properties p, int threadId, int threads) throws WorkloadException {
    table = CoreWorkloadPrms.getTableName();
    fieldcount = CoreWorkloadPrms.getFieldCount();
    fieldstart = CoreWorkloadPrms.getFieldStart();
    fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator();

    double insertproportion = CoreWorkloadPrms.getInsertProportion();
    double readmodifywriteproportion = CoreWorkloadPrms.getReadModifyWriteProportion();
    double readproportion = CoreWorkloadPrms.getReadProportion();
    double scanproportion = CoreWorkloadPrms.getScanProportion();
    double updateproportion = CoreWorkloadPrms.getUpdateProportion();

    recordcount = CoreWorkloadPrms.getRecordCount();
    RequestDistribution requestdistrib = CoreWorkloadPrms.getRequestDistribution();
    int maxscanlength = CoreWorkloadPrms.getMaxScanLength();
    ScanLengthDistribution scanlengthdistrib = CoreWorkloadPrms.getScanLengthDistribution();

    readallfields = CoreWorkloadPrms.getReadAllFields();
    writeallfields = CoreWorkloadPrms.getWriteAllFields();

    // each thread does its own chunk of the keys
    myThreadId = threadId;
    numThreads = threads;
    int recordsPerThread = recordcount/numThreads;
    int remainder = recordcount - numThreads * recordsPerThread;
    if (remainder > 0) {
      String s = "Number of threads: " + numThreads
               + " does not evenly divide recordcount: " + recordcount;
      throw new WorkloadException(s);
    }
    int insertstart = 0;
    for (int i = 0; i < myThreadId; i++) {
      insertstart += recordsPerThread;
    }

    if (CoreWorkloadPrms.getInsertOrder() == InsertOrder.hashed)
    {
      orderedinserts=false;
    }
    else if (requestdistrib == RequestDistribution.exponential)
    {
      keychooser = new ExponentialGenerator(recordcount);
    }
    else
    {
      orderedinserts=true;
    }

    keysequence=new CounterGenerator(insertstart);
    operationchooser=new DiscreteGenerator();

    if (readproportion + updateproportion + insertproportion + scanproportion + readmodifywriteproportion != 1.00) {
      String s = "Proportions do not add up to 1.00";
      throw new WorkloadException(s);
    }
    if (readproportion>0)
    {
      operationchooser.addValue(readproportion,"READ");
    }
    if (updateproportion>0)
    {
      operationchooser.addValue(updateproportion,"UPDATE");
    }
    if (insertproportion>0)
    {
      operationchooser.addValue(insertproportion,"INSERT");
    }
    if (scanproportion>0)
    {
      operationchooser.addValue(scanproportion,"SCAN");
    }
    if (readmodifywriteproportion>0)
    {
      operationchooser.addValue(readmodifywriteproportion,"READMODIFYWRITE");
    }

    //MAKE THIS WORK FOR MULTIPLE CLIENTS SO THEY DON'T INSERT THE SAME RECORDS
    transactioninsertkeysequence=new CounterGenerator(recordcount);

    if (requestdistrib == RequestDistribution.uniform)
    {
      keychooser=new UniformIntegerGenerator(0,recordcount-1);
    }
    else if (requestdistrib == RequestDistribution.zipfian)
    {
      // this does not attempt to include new values inserted after initial load
      // since the workload is typically time-based, not iteration-based,
      // so do not use zipfian distribution with workloads that do inserts
      // to avoid changing which keys are popular by shifting the modulus
      if (insertproportion > 0.0) {
        throw new WorkloadException("Workloads with inserts are not recommended with zipfian distribution");
      }
      keychooser=new ScrambledZipfianGenerator(recordcount);
    }
    else if (requestdistrib == RequestDistribution.latest)
    {
      keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
    }
    else if (requestdistrib == RequestDistribution.hotspot)
    {
      keychooser = new HotspotIntegerGenerator(0, recordcount - 1);
    }
    else
    {
      throw new WorkloadException("Unknown request distribution \""+requestdistrib+"\"");
    }

    fieldchooser=new UniformIntegerGenerator(fieldstart,fieldcount-1);

    if (scanlengthdistrib == ScanLengthDistribution.uniform)
    {
      scanlength=new UniformIntegerGenerator(1,maxscanlength);
    }
    else if (scanlengthdistrib == ScanLengthDistribution.zipfian)
    {
      scanlength=new ZipfianGenerator(1,maxscanlength);
    }
    else
    {
      throw new WorkloadException("Distribution \""+scanlengthdistrib+"\" not allowed for scan length");
    }
  }

  @Override
  public Object initThread(Properties p) throws WorkloadException {
    return null;
  }

  public String buildKeyName(long keynum) {
    if (!orderedinserts)
    {
      keynum=Utils.hash(keynum);
    }
    return "user"+keynum;
  }

  HashMap<String, ByteIterator> buildValues(String fk) {
     HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

     for (int i=0; i<fieldcount; i++)
     {
       String fieldkey="field"+i;
       ByteIterator data= new RandomByteIterator(fieldlengthgenerator.nextInt());
       values.put(fieldkey,data);
     }
    return values;
  }

  HashMap<String, ByteIterator> buildUpdate() {
    //update a random field
    HashMap<String, ByteIterator> values=new HashMap<String,ByteIterator>();
    String fieldname="field"+fieldchooser.nextString();
    ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextInt());
    values.put(fieldname,data);
    return values;
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads, this
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations.
   */
  public void doInsert(DB db, Object threadstate)
  {
    int keynum=keysequence.nextInt();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(null);
    int result = db.insert(table,dbkey,values);
    // but no results are captured below!!!
    // note result in statistics
    // throw an error if it fails
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations.
   */
  public void doTransaction(DB db, Object threadstate)
  {
    String op=operationchooser.nextString();

    if (op.compareTo("READ")==0)
    {
      doTransactionRead(db);
    }
    else if (op.compareTo("UPDATE")==0)
    {
      doTransactionUpdate(db);
    }
    else if (op.compareTo("INSERT")==0)
    {
      doTransactionInsert(db);
    }
    else if (op.compareTo("SCAN")==0)
    {
      doTransactionScan(db);
    }
    else if (op.compareTo("READMODIFYWRITE")==0)
    {
      doTransactionReadModifyWrite(db);
    }
  }

  int nextKeynum() {
    int keynum;
    if(keychooser instanceof ExponentialGenerator) {
      do
      {
        keynum=transactioninsertkeysequence.lastInt() - keychooser.nextInt();
      }
      while(keynum < 0);
    } else {
      do
      {
        keynum=keychooser.nextInt();
      }
      while (keynum > transactioninsertkeysequence.lastInt());
    }
    return keynum;
  }

  public void doTransactionRead(DB db)
  {
    //choose a random key
    int keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields=null;

    if (!readallfields)
    {
      //read a random field
      String fieldname="field"+fieldchooser.nextString();

      fields=new HashSet<String>();
      fields.add(fieldname);
    }

    db.read(table,keyname,fields,new HashMap<String,ByteIterator>());
  }

  public void doTransactionReadModifyWrite(DB db)
  {
    //choose a random key
    int keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields=null;

    if (!readallfields)
    {
      //read a random field
      String fieldname="field"+fieldchooser.nextString();

      fields=new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String,ByteIterator> values;

    if (writeallfields)
    {
       //new data for all the fields
       values = buildValues(null);
    }
    else
    {
       //update a random field
       values = buildUpdate();
    }

    //do the transaction

    db.read(table,keyname,fields,new HashMap<String,ByteIterator>());

    db.update(table,keyname,values);
  }

  public void doTransactionScan(DB db)
  {
    //choose a random key
    int keynum = nextKeynum();

    String startkeyname = buildKeyName(keynum);

    //choose a random scan length
    int len=scanlength.nextInt();

    HashSet<String> fields=null;

    if (!readallfields)
    {
      //read a random field
      String fieldname="field"+fieldchooser.nextString();

      fields=new HashSet<String>();
      fields.add(fieldname);
    }

    db.scan(table,startkeyname,len,fields,new Vector<HashMap<String,ByteIterator>>());
  }

  public void doTransactionUpdate(DB db)
  {
    //choose a random key
    int keynum = nextKeynum();

    String keyname=buildKeyName(keynum);

    HashMap<String,ByteIterator> values;

    if (writeallfields)
    {
       //new data for all the fields
       values = buildValues(null);
    }
    else
    {
       //update a random field
       values = buildUpdate();
    }

    db.update(table,keyname,values);
  }

  public void doTransactionInsert(DB db)
  {
    //choose the next key
    int keynum=transactioninsertkeysequence.nextInt();

    String dbkey = buildKeyName(keynum);

    HashMap<String, ByteIterator> values = buildValues(null);
    db.insert(table,dbkey,values);
  }
}
