/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.reference.Module

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.pivotal.gemfirexd.internal.iapi.reference;

public interface Module {

	String CacheFactory = "com.pivotal.gemfirexd.internal.iapi.services.cache.CacheFactory";
	String CipherFactoryBuilder = "com.pivotal.gemfirexd.internal.iapi.services.crypto.CipherFactoryBuilder";
	String ClassFactory = "com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory";
	String DaemonFactory = "com.pivotal.gemfirexd.internal.iapi.services.daemon.DaemonFactory";
	String JavaFactory ="com.pivotal.gemfirexd.internal.iapi.services.compiler.JavaFactory";
	String LockFactory = "com.pivotal.gemfirexd.internal.iapi.services.locks.LockFactory";
	String PropertyFactory = "com.pivotal.gemfirexd.internal.iapi.services.property.PropertyFactory";
	String ResourceAdapter = "com.pivotal.gemfirexd.internal.iapi.jdbc.ResourceAdapter";
	String SparkServiceModule = "com.pivotal.gemfirexd.internal.impl.services.spark.GfxdSparkServiceImpl";  
  String JMX = "com.pivotal.gemfirexd.internal.iapi.services.jmx.ManagementService";

}
