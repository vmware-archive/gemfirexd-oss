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

namespace AdoNetTest.BIN
{    
    /// <summary>
    /// 
    /// </summary>
    public class TestEventArgs
    {
        /// <summary>
        /// 
        /// </summary>
        private String testName;
        public String TestName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        private String testState;
        public String TestState { get; set; }

        /// <summary>
        /// 
        /// </summary>
        private String testResult;
        public String TestResult { get; set; }

        /// <summary>
        /// 
        /// </summary>
        private String message;
        public String Message { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public TestEventArgs()
            : this(string.Empty, string.Empty, string.Empty, string.Empty)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        public TestEventArgs(String message)
            : this(string.Empty, string.Empty, string.Empty, message)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="testName"></param>
        /// <param name="testState"></param>
        /// <param name="testResult"></param>
        /// <param name="message"></param>
        public TestEventArgs(String testName, String testState, String testResult, String message)
        {
            this.testName = testName;
            this.testState = testState;
            this.testResult = testResult;
            this.message = message;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if(testName == string.Empty)
                return message;
            else
                return string.Format("{0} {1} {2}", testName, testState, testResult);
        }
    }
}
