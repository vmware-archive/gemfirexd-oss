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
package  management.cli;

import java.util.List;

import jline.SimpleCompletor;

public class EventCompletor extends SimpleCompletor{

	public static ThreadLocal<List<String>> tabCompletorOutput = new ThreadLocal<List<String>>();
	
	public EventCompletor(String[] candidateStrings) {
		super(candidateStrings);		
	}
	
	@Override
	public int complete(String buffer, int cursor, List candidates){
		String b = buffer;
		int i = super.complete(buffer, cursor, candidates);
		Util.log("Completor called with buffer " + b);
		tabCompletorOutput.set(candidates);
		return i;
	}

}
