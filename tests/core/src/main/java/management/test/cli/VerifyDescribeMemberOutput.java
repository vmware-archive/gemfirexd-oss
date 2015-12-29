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
package management.test.cli;

import hydra.Log;

/**
 * The VerifyDescribeMemberOutput class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 7.x
 */
public class VerifyDescribeMemberOutput {
  final String LIT_NAME        = "Name        :";
  final String LIT_ID          = "Id          :";
  final String LIT_HOST        = "Host        :";
  final String LIT_PID         = "PID         :";
  final String LIT_GROUP       = "Groups      :";
  final String LIT_USED_HEAP   = "Used Heap   :";
  final String LIT_MAX_HEAP    = "Max Heap    :";
  final String LIT_WORKING_DIR = "Working Dir :";
  final String LIT_LOG_FILE    = "Log file    :";
  final String LIT_LOCATORS    = "Locators    :";

  private String memberInfo;
  private String outputText;
  private String expHostName;
  private String expMemberName;
  private String expPID;
  private String expThing1;
  private String expThing2;
  private String expID;

  protected VerifyDescribeMemberOutput(String memberInfo, String outputText) {
    Log.getLogWriter().info("VerifyDescribeMemberOutput: memberInfo=" + memberInfo);
    Log.getLogWriter().info("VerifyDescribeMemberOutput: outputText=" + outputText);
    // remove the unneeded start of the member info string
    String trimmedMemberInfo = memberInfo.substring("GemFire:type=Member,member=".length());
    Log.getLogWriter().info("VerifyDescribeMemberOutput: trimmedMemberInfo=" + trimmedMemberInfo);
    this.memberInfo = trimmedMemberInfo;
    this.outputText = outputText;
    parseMemberInfo();
  }

  private void parseMemberInfo() {
    Log.getLogWriter().info("parseMemberInfo: memberInfo=" + memberInfo);

    //Parse-out the Host Name
    int expStart = 0;
    int expEnd = memberInfo.indexOf("(", expStart);
    expHostName = memberInfo.substring(expStart, expEnd) + ".gemstone.com";
    Log.getLogWriter().info("parseMemberInfo: expHostName=|" + expHostName + "|");

    //Parse-out the Member Name (might contain dashes)
    expStart = expEnd + 1;
    String temp = memberInfo.substring(expStart, memberInfo.indexOf(")", expStart));
    Log.getLogWriter().info("parseMemberInfo: temp=|" + temp + "|");
    expEnd = temp.lastIndexOf("-");
    expMemberName = temp.substring(0, expEnd);
    Log.getLogWriter().info("parseMemberInfo: expMemberName=|" + expMemberName + "|");

    //Parse-out the PID
    expStart = memberInfo.indexOf(expMemberName) + expMemberName.length() + 1;
    expEnd = memberInfo.indexOf(")", expStart);
    expPID = memberInfo.substring(expStart, expEnd);
    Log.getLogWriter().info("parseMemberInfo: expPID=|" + expPID + "|");

    expStart = memberInfo.indexOf("<", expEnd);
    expEnd = memberInfo.indexOf(">", expStart) + 1;
    expThing1 = memberInfo.substring(expStart, expEnd);
    Log.getLogWriter().info("parseMemberInfo: expThing1=|" + expThing1 + "|");

    expStart = memberInfo.indexOf("-", expEnd) + 1;
    expThing2 = memberInfo.substring(expStart);
    Log.getLogWriter().info("parseMemberInfo: expThing2=|" + expThing2 + "|");

    expID =
      new StringBuffer(expHostName).append("(").append(expMemberName).append(":").append(expPID).append(")").append(expThing1).append(":")
                               .append(expThing2).toString();
    Log.getLogWriter().info("parseMemberInfo: expID=|" + expID + "|");
  }

  protected boolean validate() throws StringIndexOutOfBoundsException {
    Log.getLogWriter().info("validate: memberInfo=" + memberInfo);
    Log.getLogWriter().info("validate: outputText=" + outputText);

    boolean validHost = validateHost();
    Log.getLogWriter().info("validate: validHost=" + validHost);

    boolean validName = validateName();
    Log.getLogWriter().info("validate: validName=" + validName);

    return validHost && validName;
  }

  private boolean validateHost() {
    int start = outputText.indexOf(LIT_HOST) + 1;
    Log.getLogWriter().info("validateHost: start=" + start);

    int end = outputText.indexOf("\n", start);
    Log.getLogWriter().info("validateHost: end=" + end);

    String actHost = outputText.substring(start + LIT_HOST.length(), end);
    Log.getLogWriter().info("validateHost: actHost=|" + actHost + "|");

    return this.expHostName.equals(actHost);
  }

  private boolean validateName() {
    int start = outputText.indexOf(LIT_NAME) + 1;
    Log.getLogWriter().info("validateName: start=" + start);

    int end = outputText.indexOf("\n", start);
    Log.getLogWriter().info("validateName: end=" + end);

    String actName = outputText.substring(start + LIT_NAME.length(), end);
    Log.getLogWriter().info("validateName: actName=|" + actName + "|");

    return this.expMemberName.equals(actName);
  }
}
