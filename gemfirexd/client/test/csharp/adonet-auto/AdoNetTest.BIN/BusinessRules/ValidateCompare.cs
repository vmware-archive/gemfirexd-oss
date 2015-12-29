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
using AdoNetTest.BIN.BusinessObjects;

namespace AdoNetTest.BIN.BusinessRules
{
    /// <summary>
    /// Specific implementation of business rule for comparing business objects
    /// </summary>
    class ValidateCompare : BusinessRule
    {
        private BusinessObject OtherObject { get; set; }
        private ValidationOperator Operator { get; set; }

        public ValidateCompare(BusinessObjects.BusinessObject otherObject,
            ValidationOperator @operator, String errorMessage)
        {
            OtherObject = otherObject;
            Operator = @operator;
            ErrorMessage = errorMessage;
        }

        public override bool Validate(BusinessObject thisObject)
        {
            try
            {
                switch (Operator)
                {
                    case ValidationOperator.Equal:
                        return thisObject.Equals(OtherObject);
                    case ValidationOperator.NotEqual:
                        return !thisObject.Equals(OtherObject);
                }
            }
            catch
            {
                ;
            }

            return false;
        }
    }
}
