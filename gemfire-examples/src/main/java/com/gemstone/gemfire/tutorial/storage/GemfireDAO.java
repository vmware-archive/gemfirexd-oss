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
package com.gemstone.gemfire.tutorial.storage;

import static com.gemstone.gemfire.cache.RegionShortcut.PARTITION_REDUNDANT;
import static com.gemstone.gemfire.cache.RegionShortcut.REPLICATE;
import static com.gemstone.gemfire.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static com.gemstone.gemfire.cache.client.ClientRegionShortcut.PROXY;

import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.tutorial.model.PostID;
import com.gemstone.gemfire.tutorial.model.Profile;

/**
 * Data Access Object to store people and posts using GemFire. Calling the
 * {@link #initPeer()} method will connect this object to the GemFire
 * distributed system as a GemFire peer. Calling the {@link #initClient()} will
 * connect to the distributed system as a GemFire client.
 * 
 * Once one of the init method is called, the rest of the methods in this DAO
 * can be used to query and update the store.
 * 
 * @author GemStone Systems, Inc.
 */
public class GemfireDAO {
  
  private Region<String,Profile> people;
  private Region<PostID, String> posts;
  private boolean isClient;

  /**
   * Connect to the distributed system as a peer and declare
   * the people and posts regions.
   */
  public void initPeer() {
    //Connect to the distributed system. We're connecting to a known locator, 
    //which GemFire will use to discover other peers.
    Cache cache = new CacheFactory()
      .set("locators", "localhost[55221]")
      .set("mcast-port", "0")
      .set("log-level", "error")
      .create();
    
    //Create a listener that will print to standard out
    LoggingCacheListener listener = new LoggingCacheListener();
    
    //Declare the people region. After this call completes,
    //this member may have copied the contents of the region
    //from a peer
    people = cache.<String, Profile>createRegionFactory(REPLICATE)
      .addCacheListener(listener)
      .create("people");
    
    //Declare the posts region.
    posts =  cache.<PostID, String>createRegionFactory(PARTITION_REDUNDANT)
      .addCacheListener(listener)
      .create("posts");
  }
  
  /** Connect to the distributed system as a client and 
   * declare the people and posts regions
   */
  public void initClient() {
    //Connect to the system as a client. We're using a known locator
    //to discover servers in the distributed system.
    //
    //In addition, this client can subscribe to updates
    //from the servers.
    ClientCache cache = new ClientCacheFactory()
      .addPoolLocator("localhost", 55221)
      .setPoolSubscriptionEnabled(true)
      .setPoolSubscriptionRedundancy(1)
      .set("log-level", "error")
      .create();
    
    //Create a listener that will print to standard out
    LoggingCacheListener listener = new LoggingCacheListener();
    
    //Declare the people region.
    people = cache.<String, Profile>createClientRegionFactory(CACHING_PROXY)
      .addCacheListener(listener)
      .create("people");
    
    //Declare the posts region
    posts = cache.<PostID, String>createClientRegionFactory(PROXY)
      .create("posts");
    
    //Subscribe to updates for the people region. We're
    //subscribing to all updates to this region so we can
    //cache the people locally.
    people.registerInterestRegex(".*");
    
    isClient = true;
  }

  /**
   * Add a new person
   * @param name The name of the person to add.
   * @param profile The profile for this person
   */
  public void addPerson(String name, Profile profile) {
    people.put(name, profile);
  }

  /**
   * Add a new post from a particular user.
   * @param author the author of the post
   * @param text the text of the post
   */
  public void addPost(String author, String text) {
    PostID id = new PostID(author, System.currentTimeMillis());
    posts.put(id, text);
  }
  
  /**
   * Get this list of people in the system
   */
  public Set<String> getPeople() {
    return people.keySet();
  }

  /**
   * Get the profile of a specific person
   * 
   * @param person
   *          the persons name
   * @return the profile for the person, or null if the person is not in the
   *         system
   */
  public Profile getProfile(String person) {
    return people.get(person);
  }
  
  /**
   * Get this list of all posts in the system
   * 
   * @return A list of the IDs of all of the posts in the system
   */
  public Set<PostID> getPosts() {
    if(isClient) {
      return posts.keySetOnServer();
    } else {
      return posts.keySet();
    }
  }
  
  /**
   * Get a post, given the post ID.
   * 
   * @param post
   *          the post ID
   * @return the text of the post, or null if the post is not in found
   */
  public String getPost(PostID post) {
    return posts.get(post);
  }
}
