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
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace GemFireXDDBI
{
    /// <summary>
    /// User interface for openning datasource
    /// </summary>
    public partial class OpenDB : Form
    {
        public String SelectedDBConn { get { return this.comboBoxDBConn.SelectedValue.ToString(); } }

        public OpenDB()
        {
            InitializeComponent();

            GetDBConnStrings();
        }

        private void GetDBConnStrings()
        {
            comboBoxDBConn.DataSource = Configuration.Configurator.GetDBConnStrings();
            comboBoxDBConn.DisplayMember = "name";
            comboBoxDBConn.ValueMember = "name";
        }

        private void buttonOK_Click(object sender, EventArgs e)
        {
            this.DialogResult = DialogResult.OK;
            this.Close();
        }

        private void buttonCancel_Click(object sender, EventArgs e)
        {
            this.DialogResult = DialogResult.Cancel;
            this.Close();
        }
    }
}
