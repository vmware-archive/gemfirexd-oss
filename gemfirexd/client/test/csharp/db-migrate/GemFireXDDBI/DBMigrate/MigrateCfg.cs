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
using System.Configuration;

namespace GemFireXDDBI.DBMigrate
{
    /// <summary>
    /// User interface for specifying migration options
    /// </summary>
    public partial class MigrateCfg : Form
    {
        public String SelectedSourceDBConn { get { return this.comboBoxSourceDB.SelectedValue.ToString(); } }
        public String SelectedDestDBConn { get { return this.comboBoxDestDB.SelectedValue.ToString(); } }

        private static readonly String sourceProviderName = "System.Data.SqlClient";
        private static readonly String destProviderName = "GemFireXD.java.jdbc";

        public MigrateCfg()
        {
            InitializeComponent();

            GetDBConnStrings();
        }

        private void GetDBConnStrings()
        {
            comboBoxSourceDB.DataSource = Configuration.Configurator.GetDBConnStrings(sourceProviderName);
            comboBoxSourceDB.DisplayMember = "name";
            comboBoxSourceDB.ValueMember = "name";
            comboBoxDestDB.DataSource = Configuration.Configurator.GetDBConnStrings(destProviderName);
            comboBoxDestDB.DisplayMember = "name";
            comboBoxDestDB.ValueMember = "name";
        }

        private void buttonOK_Click(object sender, EventArgs e)
        {
            Migrator.SourceDBConnName = comboBoxSourceDB.SelectedValue.ToString().Trim();
            Migrator.DestDBConnName = "GoofGemFireXD";// comboBoxDestDB.SelectedValue.ToString().Trim();

            Migrator.MigrateTables = checkBoxTables.Checked;
            Migrator.MigrateViews = checkBoxViews.Checked;
            Migrator.MigrateIndexes = checkBoxIndexes.Checked;
            Migrator.MigrateProcedures = checkBoxStoredProcedures.Checked;
            Migrator.MigrateFunctions = checkBoxFunctions.Checked;
            Migrator.MigrateTriggers = checkBoxTriggers.Checked;
            

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
