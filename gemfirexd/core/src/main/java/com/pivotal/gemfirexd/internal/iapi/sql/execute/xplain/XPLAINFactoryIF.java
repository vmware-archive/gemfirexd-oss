/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.execute.xplain.XPLAINFactoryIF

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.pivotal.gemfirexd.internal.iapi.sql.execute.xplain;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;
/**
 * This is the factory interface of the XPLAINFactory facility. It extends the 
 * possibilities and provides a convenient protocol to explain queries 
 * on basis of the query execution plan. This plan manfifests in Derby in the 
 * different ResultSets and their associated statistics. The introduction of 
 * this factory interface makes it possible to switch to another implementation 
 * or to easily extend the API.
 *  
 */
public interface XPLAINFactoryIF {

    /**
    Module name for the monitor's module locating system.
    */
    String MODULE = "com.pivotal.gemfirexd.internal.iapi.sql.execute.xplain.XPLAINFactoryIF";
    
    /**
     * This method returns an appropriate visitor to traverse the 
     * ResultSetStatistics. Depending on the current configuration, 
     * the perfect visitor will be chosen, created and cached by this factory
     * method. 
     * @param lcc Language Connection Context 
     * @param statsEnabled statistics enabled 
     * @param explainConnection explain connection
     * @return a XPLAINVisitor to traverse the ResultSetStatistics
     * @throws StandardException 
     * @see XPLAINVisitor
     */
    public ResultSetStatisticsVisitor getXPLAINVisitor(LanguageConnectionContext lcc, boolean statsEnabled, boolean explainConnection) throws StandardException;
    
    /**
     * This method gets called when the user switches off the explain facility.
     * The factory destroys for example the cached visitor implementation(s) or 
     * releases resources to save memory.
     */
    public void freeResources();
    
    //GemStone changes BEGIN
    public void applyXPLAINMode(String key, Object value) throws StandardException;
    //GemStone changes END
}
