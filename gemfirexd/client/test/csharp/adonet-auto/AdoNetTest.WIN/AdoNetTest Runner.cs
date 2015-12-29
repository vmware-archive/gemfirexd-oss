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
using System.Threading;
using System.Windows.Forms;
using AdoNetTest.BIN;

namespace AdoNetTest.WIN
{
    public partial class formRunner : Form
    {
        private SQLFTestRunner runner;
        private String configXML;

        public formRunner()
        {
            InitializeComponent();
            buttonStart.Enabled = true;
        }

        private void toolStripConfigFile_Click(object sender, EventArgs e)
        {
            OpenFileDialog openDlg = new OpenFileDialog();
            openDlg.InitialDirectory = Environment.CurrentDirectory;
            openDlg.Filter = "config files (*.xml)|*.*";
            openDlg.FilterIndex = 2;
            openDlg.RestoreDirectory = true;

            if (openDlg.ShowDialog() == DialogResult.OK)
                configXML = openDlg.FileName;
        }

        private void radioButtonRunOnce_CheckedChanged(object sender, EventArgs e)
        {
            if (radioButtonRunOnce.Checked)
                radioButtonRunContinuous.Checked = false;
        }

        private void radioButtonRunContinuous_CheckedChanged(object sender, EventArgs e)
        {
            if (radioButtonRunContinuous.Checked)
                radioButtonRunOnce.Checked = false;
        }

        private void buttonStart_Click(object sender, EventArgs e)
        {
            SQLFTestRunner.TestEvent += new SQLFTestRunner.TestEventHandler(SQLFTestRunner_TestEvent);

            runner = new SQLFTestRunner(radioButtonRunContinuous.Checked);
            runner.SetConfigFile(configXML);

            Thread t = new Thread(new ThreadStart(runner.Run));
            t.Start();

            buttonStart.Enabled = false;
            buttonStop.Enabled = true;
            button1.Enabled = false;
            comboBox1.Enabled = false;
        }

        void SQLFTestRunner_TestEvent(TestEventArgs e)
        {
            if (e.ToString() == "Done")
            {
                SQLFTestRunner.TestEvent -= new SQLFTestRunner.TestEventHandler(SQLFTestRunner_TestEvent);
                buttonStart.Enabled = true;
                buttonStop.Enabled = false;
                button1.Enabled = true;
                comboBox1.Enabled = true;
                runner = null;
            }
            listBoxMsg.Items.Add(e.ToString());
        }

        private void buttonStop_Click(object sender, EventArgs e)
        {
            runner.Interrupt();
            buttonStop.Enabled = false;
        }

        private void button1_Click(object sender, EventArgs e)
        {
          SQLFTestRunner.TestEvent += new SQLFTestRunner.TestEventHandler(SQLFTestRunner_TestEvent);

          string testname = (string)comboBox1.SelectedItem;

          runner = new SQLFTestRunner(testname);
          runner.SetConfigFile(configXML);

          Thread t = new Thread(new ThreadStart(runner.RunOne));
          t.Start();

          buttonStart.Enabled = false;
          buttonStop.Enabled = true;
          button1.Enabled = false;
          comboBox1.Enabled = false;
        }

        private void formRunner_Load(object sender, EventArgs e)
        {
          List<string> tests = SQLFTestRunner.GetTestList();
          tests.Sort();

          foreach (string test in tests)
          {
            comboBox1.Items.Add(test);
          }

          comboBox1.Refresh();
        }

        private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
        {
          // no op
        }
    }
}
