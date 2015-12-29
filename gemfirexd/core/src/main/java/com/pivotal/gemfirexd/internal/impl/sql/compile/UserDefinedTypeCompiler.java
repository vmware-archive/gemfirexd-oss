/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.UserDefinedTypeCompiler

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






import com.pivotal.gemfirexd.internal.catalog.types.UserDefinedTypeIdImpl;
import com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.LocalField;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;

public class UserDefinedTypeCompiler extends BaseTypeCompiler
{
	/* TypeCompiler methods */

	/**
	 * Right now, casting is not allowed from one user defined type
     * to another.
	 *
	 * @param otherType 
	 * @param forDataTypeFunction
	 * @return true if otherType is convertible to this type, else false.
	 * 
	 *@see TypeCompiler#convertible
	 */
	public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
	{
        if ( getTypeId().getBaseTypeId().isAnsiUDT() )
        {
            if ( !otherType.getBaseTypeId().isAnsiUDT() ) { return false; }
            
            UserDefinedTypeIdImpl thisTypeID = (UserDefinedTypeIdImpl) getTypeId().getBaseTypeId();
            UserDefinedTypeIdImpl thatTypeID = (UserDefinedTypeIdImpl) otherType.getBaseTypeId();
            
            return thisTypeID.getSQLTypeName().equals( thatTypeID.getSQLTypeName() );
        }
        
		/*
		** We are a non-ANSI user defined type, we are
		** going to have to let the client find out
		** the hard way.
		*/
		return true;
	}

	 /** @see TypeCompiler#compatible */
	public boolean compatible(TypeId otherType)
	{
		return convertible(otherType, false);
	}

	/**
     * ANSI UDTs can only be stored into values of exactly their own
     * type. This restriction can be lifted when we implement the
     * ANSI subclassing clauses.
     *
	 * Old-style User types are storable into other user types that they
	 * are assignable to. The other type must be a subclass of
	 * this type, or implement this type as one of its interfaces.
	 *
	 * @param otherType the type of the instance to store into this type.
	 * @param cf		A ClassFactory
	 * @return true if otherType is storable into this type, else false.
	 */
	public boolean storable(TypeId otherType, ClassFactory cf)
	{
        if ( !otherType.isUserDefinedTypeId() ) { return false; }

        UserDefinedTypeIdImpl thisTypeID = (UserDefinedTypeIdImpl) getTypeId().getBaseTypeId();
        UserDefinedTypeIdImpl thatTypeID = (UserDefinedTypeIdImpl) otherType.getBaseTypeId();

        if ( thisTypeID.isAnsiUDT() != thatTypeID.isAnsiUDT() ) { return false; }

        if ( thisTypeID.isAnsiUDT() )
        {
            return thisTypeID.getSQLTypeName().equals( thatTypeID.getSQLTypeName() );
        }
        
		return cf.getClassInspector().assignableTo(
			   otherType.getCorrespondingJavaTypeName(),
			   getTypeId().getCorrespondingJavaTypeName());
	}

	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.UserDataValue;
	}
			
	/**
	 * @see TypeCompiler#getCorrespondingPrimitiveTypeName
	 */

	public String getCorrespondingPrimitiveTypeName()
	{
		return getTypeId().getCorrespondingJavaTypeName();
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		// This is the maximum maximum width for user types
		return -1;
	}

	String nullMethodName()
	{
            if(DVDSet.class.getName().equals(getCorrespondingPrimitiveTypeName()) )
            {
                //getNullDVDSet
                return "getNull" + DVDSet.class.getSimpleName();
            }
		return "getNullObject";
	}

	public void generateDataValue(MethodBuilder mb, int collationType,
			LocalField field)
	{
		// cast the value to an object for method resolution
		mb.upCast("java.lang.Object");

		super.generateDataValue(mb, collationType, field);
	}

// GemStone changes BEGIN
  @Override
  public DataTypeDescriptor resolveArithmeticOperation(
      DataTypeDescriptor leftType, DataTypeDescriptor rightType, String operator)
      throws StandardException {

    String leftJavaTypeName = leftType.getTypeId()
        .getCorrespondingJavaTypeName();
    String rightJavaTypeName = rightType.getTypeId()
        .getCorrespondingJavaTypeName();

    if (DVDSet.class.getName().equals(leftJavaTypeName)
        && DVDSet.class.getName().equals(rightJavaTypeName)) {
      TypeId cti = null;
      if (leftType.getTypeId().typePrecedence() > rightType.getTypeId()
          .typePrecedence()) {
        cti = TypeId.getUserDefinedTypeId(
            com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet.class
                .getName(), ((UserDefinedTypeIdImpl)leftType.getTypeId()
                .getBaseTypeId()).getClassParam(), false);
      }
      else {
        cti = TypeId.getUserDefinedTypeId(
            com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet.class
                .getName(), ((UserDefinedTypeIdImpl)rightType.getTypeId()
                .getBaseTypeId()).getClassParam(), false);
      }
      return new DataTypeDescriptor(cti, false);
    }
    else {
      NumericTypeCompiler numeric = new NumericTypeCompiler();
      return numeric.resolveArithmeticOperation(rightType, leftType, operator);
    }
    // return super.resolveArithmeticOperation(leftType, rightType, operator);
  }
// GemStone changes END
}
