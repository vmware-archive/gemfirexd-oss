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

package com.pivotal.gemfirexd.internal.impl.sql.compile;

import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.StringDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

/**
 * This class implements TypeCompiler for the JSON types.
 *
 */

public class JSONTypeCompiler extends BaseTypeCompiler
{
        /**
         * Tell whether this type (LOB) can be converted to the given type.
         *
         * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, 
                                   boolean forDataTypeFunction)
        {
            // allow casting to any string
            return (otherType.isStringTypeId()) ;

        }

        /**
         * Tell whether this type (CLOB) is compatible with the given type.
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
            // no automatic conversions at store time--but string
            // literals (or values of type CHAR/VARCHAR) are STORABLE
            // as clobs, even if the two types can't be COMPARED.
            return (otherType.isStringTypeId()) ;
        }

        /** @see TypeCompiler#interfaceName */
        public String interfaceName()
        {
            return ClassName.StringDataValue;
        }

        /**
         * @see TypeCompiler#getCorrespondingPrimitiveTypeName
         */

        public String getCorrespondingPrimitiveTypeName() {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.JSON_TYPE_ID:  return "com.pivotal.gemfirexd.internal.iapi.types.JSON"; 
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
                case StoredFormatIds.JSON_TYPE_ID:  return "getNullJSON";
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
                case StoredFormatIds.JSON_TYPE_ID:  return "getJSONDataValue";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in dataValueMethodName() - " + formatId);
                    return null;
                }
        }
        
        /**
         * Push the collation type if it is not COLLATION_TYPE_UCS_BASIC.
         * 
         * @param collationType Collation type of character values.
         * @return true collationType will be pushed, false collationType will be ignored.
         */
        boolean pushCollationForDataValue(int collationType)
        {
            return collationType != StringDataValue.COLLATION_TYPE_UCS_BASIC;
        }
}
