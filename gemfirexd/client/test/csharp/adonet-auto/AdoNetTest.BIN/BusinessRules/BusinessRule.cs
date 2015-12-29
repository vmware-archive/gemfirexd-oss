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
 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AdoNetTest.BIN.BusinessRules
{
    /// <summary>
    /// Base class for implementing and enforcing validation of business object properties
    /// </summary>
    public abstract class BusinessRule
    {
        public String PropertyName { get; set; }
        public String ErrorMessage { get; set; }

        public BusinessRule()
        {
        }

        public BusinessRule(String propName)
        {
            PropertyName = propName;
            ErrorMessage = propName + " is invalid";
        }

        public BusinessRule(String propName, String errorMsg)
            : this(propName)
        {
            ErrorMessage = errorMsg;
        }

        public BusinessRule(BusinessObjects.BusinessObject otherObject, 
            ValidationOperator @operator, String errorMessage)
        {
        }

        public abstract bool Validate(BusinessObjects.BusinessObject businessObject);

        protected object GetPropertyValue(BusinessObjects.BusinessObject businessObject)
        {
            return businessObject.GetType().GetProperty(
                PropertyName).GetValue(businessObject, null);
        }
    }

    public enum ValidationDataType
    {
        String,
        Integer,
        Double,
        Decimal,
        Date
    }

    public enum ValidationOperator
    {
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanEqual,
        LessThan,
        LessThanEqual
    }
}
