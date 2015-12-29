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
package com.gemstone.gemfire.management.internal.cli.help.format;

import java.util.List;

/**
 * @author Nikhil Jadhav
 *
 */
public class DataNode {
  String data;
  List<DataNode> children;

  public DataNode(String data, List<DataNode> dataNode) {
    this.data = data;
    this.children = dataNode;
  }

  public String getData() {
    return data;
  }
  
  public List<DataNode> getChildren() {
    return children;
  }
  
  public boolean addChild(DataNode dataNode) {
    if (this.children != null) {
      this.children.add(dataNode);
      return true;
    } else {
      return false;
    }
  }

}
