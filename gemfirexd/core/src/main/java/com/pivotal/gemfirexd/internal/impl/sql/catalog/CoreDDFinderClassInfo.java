/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.CoreDDFinderClassInfo

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

package	com.pivotal.gemfirexd.internal.impl.sql.catalog;

import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableInstanceGetter;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

/**
 * FormatableInstanceGetter to load stored instances
 * of DependableFinder. Class is registered in RegisteredFormatIds
 * 
 * @see com.pivotal.gemfirexd.internal.catalog.DependableFinder
 * @see com.pivotal.gemfirexd.internal.iapi.services.io.RegisteredFormatIds
 *
 */
public class CoreDDFinderClassInfo extends FormatableInstanceGetter {

	public Object getNewInstance() 
	{
		switch (fmtId) 
		{
			/* DependableFinders */
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID: 
			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
			case StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID:
			case StoredFormatIds.ROUTINE_PERMISSION_FINDER_V01_ID:
			case StoredFormatIds.COLUMNS_PERMISSION_FINDER_V01_ID:
				return new DDdependableFinder(fmtId);
			case StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID:
				return new DDColumnDependableFinder(fmtId);
			default:
				return null;
		}

	}
}
