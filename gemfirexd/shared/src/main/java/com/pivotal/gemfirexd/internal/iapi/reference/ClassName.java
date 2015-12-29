/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.reference.ClassName

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


package com.pivotal.gemfirexd.internal.iapi.reference;


/**
	List of strings representing class names, which are typically found
    for classes with implement the Formatable interface.
    These strings are removed from the code to separate them from the
    strings which need to be internationalized. It also reduces footprint.
    <P>
	This class has no methods, all it contains are String's which by default
	are public, static and final since they are declared in an interface.
*/

public interface ClassName
{

	String STORE_CONGLOMDIR =
		"com.pivotal.gemfirexd.internal.impl.store.access.ConglomerateDirectory";

	String STORE_PCXENA =
		"com.pivotal.gemfirexd.internal.impl.store.access.PC_XenaVersion";


	String DataValueFactory = "com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory";
	String DataValueDescriptor = "com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor";

	String BooleanDataValue = "com.pivotal.gemfirexd.internal.iapi.types.BooleanDataValue";

 	String BitDataValue = "com.pivotal.gemfirexd.internal.iapi.types.BitDataValue";
	String StringDataValue = "com.pivotal.gemfirexd.internal.iapi.types.StringDataValue";
	String DateTimeDataValue = "com.pivotal.gemfirexd.internal.iapi.types.DateTimeDataValue";
	String NumberDataValue = "com.pivotal.gemfirexd.internal.iapi.types.NumberDataValue";
	String RefDataValue = "com.pivotal.gemfirexd.internal.iapi.types.RefDataValue";
	String UserDataValue = "com.pivotal.gemfirexd.internal.iapi.types.UserDataValue";
	String ConcatableDataValue  = "com.pivotal.gemfirexd.internal.iapi.types.ConcatableDataValue";
	String XMLDataValue  = "com.pivotal.gemfirexd.internal.iapi.types.XMLDataValue";

	String FormatableBitSet = "com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet";

	String BaseActivation = "com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation";
	String BaseExpressionActivation = "com.pivotal.gemfirexd.internal.impl.sql.execute.BaseExpressionActivation";

	String CursorActivation = "com.pivotal.gemfirexd.internal.impl.sql.execute.CursorActivation";

	String Row = "com.pivotal.gemfirexd.internal.iapi.sql.Row";
	String Qualifier = "com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier";

	String RunTimeStatistics = "com.pivotal.gemfirexd.internal.iapi.sql.execute.RunTimeStatistics";

	String Storable = "com.pivotal.gemfirexd.internal.iapi.services.io.Storable";
	String StandardException = "com.pivotal.gemfirexd.internal.iapi.error.StandardException";

	String LanguageConnectionContext = "com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext";
	String ConstantAction = "com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction";
	String DataDictionary = "com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary";

	String CursorResultSet = "com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet";

	String ExecIndexRow = "com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow";

	String ExecPreparedStatement = "com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement";

	String ExecRow = "com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow";
	String Activation = "com.pivotal.gemfirexd.internal.iapi.sql.Activation";

	String ResultSet = "com.pivotal.gemfirexd.internal.iapi.sql.ResultSet";

	String FileMonitor = "com.pivotal.gemfirexd.internal.impl.services.monitor.FileMonitor";

	String GeneratedClass = "com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedClass";
	String GeneratedMethod = "com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod";
	String GeneratedByteCode = "com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedByteCode";

	String Context = "com.pivotal.gemfirexd.internal.iapi.services.context.Context";

	String NoPutResultSet = "com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet";

	String ResultSetFactory = "com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetFactory";
	String RowFactory = "com.pivotal.gemfirexd.internal.iapi.sql.execute.RowFactory";

	String RowLocation = "com.pivotal.gemfirexd.internal.iapi.types.RowLocation";

	String VariableSizeDataValue = "com.pivotal.gemfirexd.internal.iapi.types.VariableSizeDataValue";
	String ParameterValueSet = "com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet";


	String CurrentDatetime = "com.pivotal.gemfirexd.internal.impl.sql.execute.CurrentDatetime";

	String MaxMinAggregator = "com.pivotal.gemfirexd.internal.impl.sql.execute.MaxMinAggregator";
	String SumAggregator = "com.pivotal.gemfirexd.internal.impl.sql.execute.SumAggregator";
	String CountAggregator = "com.pivotal.gemfirexd.internal.impl.sql.execute.CountAggregator";
	String AvgAggregator = "com.pivotal.gemfirexd.internal.impl.sql.execute.AvgAggregator";

	String ExecutionFactory = "com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory";
	String LanguageFactory ="com.pivotal.gemfirexd.internal.iapi.sql.LanguageFactory";
	String ParameterValueSetFactory ="com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSetFactory";

	String TriggerNewTransitionRows = "com.pivotal.gemfirexd.internal.catalog.TriggerNewTransitionRows";
	String TriggerOldTransitionRows = "com.pivotal.gemfirexd.internal.catalog.TriggerOldTransitionRows";
	String VTICosting = "com.pivotal.gemfirexd.internal.vti.VTICosting";

	String Authorizer = "com.pivotal.gemfirexd.internal.iapi.sql.conn.Authorizer";
// GemStone changes BEGIN
	String TableScanResultSet = "com.pivotal.gemfirexd.internal.impl.sql.execute.TableScanResultSet";
	String ProcedureResultProcessor="com.pivotal.gemfirexd.procedure.ProcedureResultProcessor";
	String ProcedureProxy= "com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProcedureProxy";
	String ProcedureExecutionContext="com.pivotal.gemfirexd.procedure.ProcedureExecutionContext";
	String DVDSet="com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet";
//	String ProcedureSender="com.pivotal.gemfirexd.internal.engine.procedure.cohort.ProcedureSender";
// GemStone changes END	
}
