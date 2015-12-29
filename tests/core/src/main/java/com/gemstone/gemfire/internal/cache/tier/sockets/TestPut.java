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
 package com.gemstone.gemfire.internal.cache.tier.sockets;
 
import java.io.IOException;

import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;

/**
 * @author Pallavi
 * 
 * TestPut is a dummy command to verify Command handling in BackwardCompatibilityComamndDUnitTest.
 */

 public class TestPut implements Command {

   public TestPut() {	
   }
			  
   final public void execute(Message msg, ServerConnection servConn) {
	 // set flag true - to be checked in test
	 BackwardCompatibilityCommandDUnitTest.TEST_PUT_COMMAND_INVOKED = true;
	 
	 // write reply to clients 
	 servConn.setAsTrue(REQUIRES_RESPONSE);
	 writeReply(msg, servConn);
	 servConn.setAsTrue(RESPONDED);	 
   }

   private void writeReply(Message origMsg, ServerConnection servConn) {
     Message replyMsg = servConn.getReplyMessage();
     replyMsg.setMessageType(MessageType.REPLY);
     replyMsg.setNumberOfParts(1);
     replyMsg.setTransactionId(origMsg.getTransactionId());
     replyMsg.addBytesPart(BaseCommand.OK_BYTES);
     try {
       replyMsg.send();
     } 
     catch (IOException ioe){
       ioe.printStackTrace();
     }
   }
 }

