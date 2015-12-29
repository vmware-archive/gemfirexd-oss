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
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace ado_net_gemfirexd_client.ViewModels
{
    public abstract class ObservableObject<T> : INotifyPropertyChanged
    {
        #region Implementation of INotifyPropertyChanged

        /// <summary>
        /// Occurs when a property value changes.
        /// </summary>
        public event PropertyChangedEventHandler PropertyChanged;

        #endregion

        /// <summary>
        /// Called when [property changed].
        /// </summary>
        /// <param name="property">The property.</param>
        public void OnPropertyChanged(Expression<Func<T, object>> property)
        {
            var propertyName = GetPropertyName(property);
            if (PropertyChanged != null)
            {
                var handler = PropertyChanged;
                handler(this, new PropertyChangedEventArgs(propertyName));
            }
        }

        /// <summary>
        /// Gets the name of the property.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        private string GetPropertyName(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("propertyExpresssion");
            }

            var memberExpression = expression.Body as MemberExpression;
            if (memberExpression == null)
            {
                throw new ArgumentException("The expression is not a member access expression.", "expression");
            }
            var property = memberExpression.Member as PropertyInfo;
            if (property == null)
            {
                throw new ArgumentException("The member access expression does not access a property.", "expression");
            }
            var getMethod = property.GetGetMethod(true);
            if (getMethod.IsStatic)
            {
                throw new ArgumentException("The referenced property is a static property.", "expression");
            }
            return memberExpression.Member.Name;
        }
    }
}
