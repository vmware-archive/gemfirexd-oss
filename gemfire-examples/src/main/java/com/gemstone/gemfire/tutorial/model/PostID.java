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
package com.gemstone.gemfire.tutorial.model;

import java.io.Serializable;

/**
 * A PostID is a A unique ID for a post from a specific user. The ID of a post
 * is the a combination of the target and a timestamp.
 * 
 * The PostID is used as the key for the posts region. Because of that, it 
 * must implement equals and hashCode. It must also be Serializable
 * so that it can be distributed other VMs.
 * 
 * @author GemStone Systems, Inc.
 */
public class PostID implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private final String author;
  private final long timestamp;
  
  public PostID(String target, long timestamp) {
    super();
    this.author = target;
    this.timestamp = timestamp;
  }

  public String getAuthor() {
    return author;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((author == null) ? 0 : author.hashCode());
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof PostID))
      return false;
    PostID other = (PostID) obj;
    if (author == null) {
      if (other.author != null)
        return false;
    } else if (!author.equals(other.author))
      return false;
    if (timestamp != other.timestamp)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "PostID [author=" + author + ", timestamp=" + timestamp + "]";
  }
  
}
