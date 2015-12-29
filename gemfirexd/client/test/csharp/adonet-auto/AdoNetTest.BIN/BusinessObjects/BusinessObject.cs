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
using AdoNetTest.BIN.BusinessRules;

namespace AdoNetTest.BIN.BusinessObjects
{
    /// <summary>
    /// Base class for all business objects that are individually associated with
    /// a table entity in the simple commerce (default) database
    /// </summary>
    public abstract class BusinessObject
    {
        private IList<BusinessRule> businessRules = new List<BusinessRule>();

        private IList<String> validationErrors = new List<String>();
        public IList<String> ValidationErrors
        {
            get { return validationErrors; }
        }

        public void AddRule(BusinessRule rule)
        {
            businessRules.Add(rule);
        }

        public bool Validate()
        {
            bool isValid = true;

            validationErrors.Clear();

            foreach (BusinessRule rule in businessRules)
            {
                if (!rule.Validate(this))
                {
                    isValid = false;
                    validationErrors.Add(rule.ErrorMessage);
                }
            }

            return isValid;
        }

        public bool Validate(BusinessObject otherObject)
        {
            String errorMsg = String.Format("{0} validation failed",
                otherObject.GetType().Name);

            this.AddRule(new ValidateCompare(otherObject, ValidationOperator.Equal, errorMsg));

            return Validate();
        }
    }
}
