/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.services.info.ProductGenusNames

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

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.iapi.services.info;

/**
  Holder class for Derby genus names.

  <P>
  A product genus defines a product's category (tools, DBMS etc). 
  Currently, Derby only ships one jar file per genus. The info file
  defined in this file is used by sysinfo to determine version information.

  <P>
  A correct run time environment should include at most one Derby
  jar file of a given genus. This helps avoid situations in which the
  environment loads classes from more than one version. 

  <P>
  Please note that the list provided here serves to document product
  genus names and to facilitate consistent naming in code. Because the
  list of supported Derby genus names may change with time, the
  code in this package does *NOT* restrict users to the product genus
  names listed here.
  */
public interface ProductGenusNames
{

	/**Genus name for dbms products.*/
	public static String DBMS = "DBMS";
	public static String DBMS_INFO = "/com/pivotal/gemfirexd/internal/info/DBMS.properties";

	/**Genus name for tools products.*/
	public static String TOOLS = "tools";
	public static String TOOLS_INFO = "/com/pivotal/gemfirexd/internal/info/tools.properties";

	/**Genus name for net products.*/
	public static String NET = "net";
	public static String NET_INFO = "/com/pivotal/gemfirexd/internal/info/net.properties";

	/**Genus name for network client */
	public static String DNC = "dnc";
	public static String DNC_INFO = "/com/pivotal/gemfirexd/internal/info/dnc.properties";

// GemStone changes BEGIN
	/** Genus name for thrift client */
	public static final String THRIFT = "thrift";
	public static final String THRIFT_INFO_FILE = "thrift.properties";
	public static final String THRIFT_INFO =
	  "/com/pivotal/gemfirexd/internal/info/" + THRIFT_INFO_FILE;
	public static final String THRIFT_INFO_PACKAGE =
	    "com.pivotal.gemfirexd.internal.info";
// GemStone changes END
}


