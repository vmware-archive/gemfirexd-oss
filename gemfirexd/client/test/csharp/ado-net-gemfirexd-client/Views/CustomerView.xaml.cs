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
using System.Collections;
using System.Windows.Controls;
using System.Windows.Data;
using ado_net_gemfirexd_client;
using System.Collections.Generic;

namespace ado_net_gemfirexd_client.Views
{
    /// <summary>
    /// Interaction logic for CustomerView.xaml
    /// </summary>
    public partial class CustomerView : UserControl
    {
        public CustomerView()
        {
            InitializeComponent();
        }
    }

    #region ConcatNamesConverter

    public class ConcatNamesConverter : IMultiValueConverter
    {
        public object Convert(object[] values, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            try
            {
                string lastName = (string)values[0];
                string firstName = (string)values[1];
                string title = (string)values[2];

                string displayValue = lastName.ToUpper();
                if (firstName != string.Empty)
                {
                    displayValue += ", ";
                    displayValue += firstName;
                }
                if (title != string.Empty)
                {
                    displayValue += " (";
                    displayValue += title;
                    displayValue += ")";
                }

                return displayValue;
            }
            catch (Exception)
            {

            }
            return string.Empty;
        }
        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, System.Globalization.CultureInfo culture)
        {
            // Not implemented
            return null;
        }
    }
    #endregion
}
