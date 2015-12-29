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
using System.Threading;

namespace GemFireXDDBI.DBMigrate
{
    /// <summary>
    /// Migration progress and status
    /// </summary>
    public partial class Progressor : Form
    {
        private Thread migrator;
        private Thread progressor;

        public Progressor()
        {
            InitializeComponent();

            labelProgress.Text = "Start migrating database...";
            Migrator.MigrateEvent += new Migrator.MigrateEventHandler(Migrator_MigrateEvent);
        }

        void Migrator_MigrateEvent(MigrateEventArgs e)
        {
            labelProgress.Text = e.Message;
        }

        private void buttonCancel_Click(object sender, EventArgs e)
        {                          
            labelProgress.Text = "Canceling operation... please wait.";    
            migrator.Abort();
            this.DialogResult = DialogResult.Abort;
        }

        public void Run()
        {
            if (!VerifyDbConnections())
                return;

            try
            {
                //progressor = new Thread(new ThreadStart(ShowProgress));
                //progressor.Start();                

                migrator = new Thread(new ThreadStart(Migrator.MigrateDB));
                migrator.Start();
                migrator.Join();

                if (Migrator.Result == Util.Result.Completed)
                    this.DialogResult = DialogResult.OK;
                else if (Migrator.Result == Util.Result.Aborted)
                    this.DialogResult = DialogResult.Abort;

                progressor.Abort();
            }
            catch (Exception e)
            {
                Util.Helper.Log(e);
            }
        }

        private void ShowProgress()
        {
            try
            {
                if (this.ShowDialog() == DialogResult.Abort)
                {
                    this.Close();
                }
            }
            catch (Exception e)
            {
                Util.Helper.Log(e);
            }
        }

        private bool VerifyDbConnections()
        {
            if (TestConnection(Migrator.SourceDBConnName) != 1 
                || TestConnection(Migrator.DestDBConnName) != 1)
                return false;
            
            return true;
        }

        private int TestConnection(String connName)
        {
            int connState = 0;

            try
            {
                 connState = DBI.SQLFactory.GetSqlDBI(connName).TestConnection();
            }
            catch (Exception e)
            {
                MessageBox.Show(String.Format("Failed to open {0} connection", connName),
                    "DB Migrate", MessageBoxButtons.OK, MessageBoxIcon.Error);

                Util.Helper.Log(e);

                return 0;
            }

            return connState;
        }
    }
}
