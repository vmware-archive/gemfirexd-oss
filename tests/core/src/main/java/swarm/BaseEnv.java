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
package swarm;

public class BaseEnv {

	private final String name;
	private final String checkoutPath;
	private final String outputPath;
	private final int id;
	
	public BaseEnv(String name,String checkoutPath, String outputPath,int id) {
		this.name = name;
		this.checkoutPath = checkoutPath;
		this.id = id;
		this.outputPath = outputPath;
	}

	public String getName() {
		return name;
	}

	public String getCheckoutPath() {
		return checkoutPath;
	}
	
	public int getId() {
		return id;
	}
	
	public String getOutputPath() {
		return outputPath;
	}
}
