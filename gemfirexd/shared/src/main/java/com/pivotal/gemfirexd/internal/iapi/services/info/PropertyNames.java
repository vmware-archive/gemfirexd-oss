/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.services.info.PropertyNames

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

package com.pivotal.gemfirexd.internal.iapi.services.info;

/**
  This class defines the names of the properties to use when
  you extract the parts of a product version from a properties
  file.
  */
public abstract class PropertyNames
{
	public final static String
	PRODUCT_VENDOR_NAME   = "gemfirexd.product.vendor",
	PRODUCT_TECHNOLOGY_NAME = "gemfirexd.product.technology.name",
    PRODUCT_EXTERNAL_NAME = "gemfirexd.product.external.name",
    PRODUCT_EXTERNAL_VERSION = "gemfirexd.product.external.version",
	PRODUCT_MAJOR_VERSION = "gemfirexd.version.major",
	PRODUCT_MINOR_VERSION = "gemfirexd.version.minor",
	PRODUCT_MAINT_VERSION = "gemfirexd.version.maint",
	PRODUCT_DRDA_MAINT_VERSION = "gemfirexd.version.drdamaint",
    PRODUCT_BETA_VERSION  = "gemfirexd.version.beta",
	PRODUCT_BUILD_NUMBER  = "gemfirexd.build.number",
    PRODUCT_WHICH_ZIP_FILE = "gemfirexd.product.file";
}

