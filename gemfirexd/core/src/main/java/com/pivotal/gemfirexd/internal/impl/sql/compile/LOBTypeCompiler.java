/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.LOBTypeCompiler

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

package com.pivotal.gemfirexd.internal.impl.sql.compile;









import java.sql.Types;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.reference.JDBC20Translation;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.types.BitDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

/**
 * This class implements TypeCompiler for the SQL LOB types.
 *
 */

public class LOBTypeCompiler extends BaseTypeCompiler
{
        /**
         * Tell whether this type (LOB) can be converted to the given type.
         *
		 * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, 
								   boolean forDataTypeFunction)
        {

// GemStone changes BEGIN
            final int otherJDBCTypeId = otherType.getJDBCTypeId();
            return otherJDBCTypeId == Types.BLOB ||
                otherJDBCTypeId == Types.BINARY ||
                otherJDBCTypeId == Types.VARBINARY;
            /* (original code)
            return  (otherType.isBlobTypeId());
            */
// GemStone changes END
        }

        /**
         * Tell whether this type (LOB) is compatible with the given type.
         *
         * @param otherType     The TypeId of the other type.
         */
		public boolean compatible(TypeId otherType)
		{
				return convertible(otherType,false);
		}

        /**
         * Tell whether this type (LOB) can be stored into from the given type.
         *
         * @param otherType     The TypeId of the other type.
         * @param cf            A ClassFactory
         */

        public boolean storable(TypeId otherType, ClassFactory cf)
        {
// GemStone changes BEGIN
			final int otherJDBCTypeId = otherType.getJDBCTypeId();
			return otherJDBCTypeId == Types.BLOB ||
			    otherJDBCTypeId == Types.BINARY ||
			    otherJDBCTypeId == Types.VARBINARY;
			/* (original code)
            // no automatic conversions at store time

			return  (otherType.isBlobTypeId());
			*/
// GemStone changes END
        }

        /** @see TypeCompiler#interfaceName */
        public String interfaceName()
        {
            return ClassName.BitDataValue;
        }

        /**
         * @see TypeCompiler#getCorrespondingPrimitiveTypeName
         */

        public String getCorrespondingPrimitiveTypeName() {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.BLOB_TYPE_ID:  return "java.sql.Blob";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in getCorrespondingPrimitiveTypeName() - " + formatId);
                    return null;
            }
        }

        /**
         * @see TypeCompiler#getCastToCharWidth
         */
        public int getCastToCharWidth(DataTypeDescriptor dts)
        {
                return dts.getMaximumWidth();
        }

        String nullMethodName() {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.BLOB_TYPE_ID:  return "getNullBlob";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in nullMethodName() - " + formatId);
                    return null;
            }
        }

        String dataValueMethodName()
        {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.BLOB_TYPE_ID:  return "getBlobDataValue";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in dataValueMethodName() - " + formatId);
                    return null;
                }
        }
}
