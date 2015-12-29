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

package hydra.gemfirexd;

import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.Constants;
import com.pivotal.gemfirexd.auth.callback.UserAuthenticator;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;

import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.HydraConfigException;
import hydra.HydraVector;
import hydra.TestConfig;

import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe fabric server security.
 */
public class FabricSecurityDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this fabric security description */
  private String name;

  /** Remaining parameters, in alphabetical order */

  private String authProvider;
  private String authzDefaultConnectionMode;
  private List authzFullAccessUsers;
  private String authzFullAccessUsersStr;
  private List authzReadOnlyAccessUsers;
  private String authzReadOnlyAccessUsersStr;
  private String password;
  private String serverAuthProvider;
  private Boolean sqlAuthorization;
  private String user;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public FabricSecurityDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in alphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this fabric security description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this fabric security description.
   */
  private void setName(String str) {
    this.name = str;
  }

  public String getAuthProvider() {
    return this.authProvider;
  }
  private void setAuthProvider(String str) {
    this.authProvider = str;
  }

  public String getAuthzDefaultConnectionMode() {
    return this.authzDefaultConnectionMode;
  }
  private void setAuthzDefaultConnectionMode(String str) {
    this.authzDefaultConnectionMode = str;
  }

  public List<String> getAuthzFullAccessUsers() {
    return this.authzFullAccessUsers;
  }
  private void setAuthzFullAccessUsers(List<String> list) {
    this.authzFullAccessUsers = list;
  }

  public String getAuthzFullAccessUsersStr() {
    return this.authzFullAccessUsersStr;
  }
  private void setAuthzFullAccessUsersStr(String str) {
    this.authzFullAccessUsersStr = str;
  }

  public List<String> getAuthzReadOnlyAccessUsers() {
    return this.authzReadOnlyAccessUsers;
  }
  private void setAuthzReadOnlyAccessUsers(List<String> list) {
    this.authzReadOnlyAccessUsers = list;
  }

  public String getAuthzReadOnlyAccessUsersStr() {
    return this.authzReadOnlyAccessUsersStr;
  }
  private void setAuthzReadOnlyAccessUsersStr(String str) {
    this.authzReadOnlyAccessUsersStr = str;
  }

  public String getPassword() {
    return this.password;
  }
  private void setPassword(String str) {
    this.password = str;
  }

  public String getServerAuthProvider() {
    return this.serverAuthProvider;
  }
  private void setServerAuthProvider(String str) {
    this.serverAuthProvider = str;
  }

  public Boolean getSqlAuthorization() {
    return this.sqlAuthorization;
  }
  private void setSqlAuthorization(Boolean bool) {
    this.sqlAuthorization = bool;
  }

  public String getUser() {
    return this.user;
  }
  private void setUser(String str) {
    this.user = str;
  }

//------------------------------------------------------------------------------
// Properties
//------------------------------------------------------------------------------

  /**
   * Returns the boot properties for this fabric security description.
   */
  protected Properties getBootProperties() {
    Properties p = new Properties();

    if (getAuthProvider() != null) {
      p.setProperty(Attribute.AUTH_PROVIDER,
                  getAuthProvider());
    }
    if (getAuthzDefaultConnectionMode() != null) {
      p.setProperty(com.pivotal.gemfirexd.Property.AUTHZ_DEFAULT_CONNECTION_MODE,
                  getAuthzDefaultConnectionMode());
    }
    if (getAuthzFullAccessUsers() != null) {
      p.setProperty(com.pivotal.gemfirexd.Property.AUTHZ_FULL_ACCESS_USERS,
                  getAuthzFullAccessUsersStr());
    }
    if (getAuthzReadOnlyAccessUsers() != null) {
      p.setProperty(com.pivotal.gemfirexd.Property.AUTHZ_READ_ONLY_ACCESS_USERS,
                  getAuthzReadOnlyAccessUsersStr());
    }
    if (getServerAuthProvider() != null) {
      p.setProperty(Attribute.SERVER_AUTH_PROVIDER,
                  getServerAuthProvider());
    }
    p.setProperty(com.pivotal.gemfirexd.Property.SQL_AUTHORIZATION,
                  getSqlAuthorization().toString());
    if (getUser() != null) {
      p.setProperty(Property.USER_PROPERTY_PREFIX +  getUser(),
          getPassword());
      p.setProperty(TestConfig.tab().getRandGen().nextBoolean() ?
          Attribute.USERNAME_ATTR : Attribute.USERNAME_ALT_ATTR,
          getUser());
      p.setProperty(Attribute.PASSWORD_ATTR, getPassword());
    }

    return p;
  }

  /**
   * Returns the shutdown properties for this fabric security description.
   */
  protected Properties getShutdownProperties() {
    Properties p = new Properties();

    if (getUser() != null) {
      p.setProperty(Property.USER_PROPERTY_PREFIX +  getUser(),
          getPassword());
      
      p.setProperty(Attribute.USERNAME_ATTR, getUser());
      p.setProperty(Attribute.PASSWORD_ATTR, getPassword());
    }

    return p;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap<String,Object> toSortedMap() {
    SortedMap<String,Object> map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "authProvider", getAuthProvider());
    map.put(header + "authzDefaultConnectionMode", getAuthzDefaultConnectionMode());
    map.put(header + "authzFullAccessUsers", getAuthzFullAccessUsersStr());
    map.put(header + "authzReadOnlyAccessUsers", getAuthzReadOnlyAccessUsersStr());
    map.put(header + "password", getPassword());
    map.put(header + "serverAuthProvider", getServerAuthProvider());
    map.put(header + "sqlAuthorization", getSqlAuthorization());
    map.put(header + "user", getUser());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates fabric descriptions from the test configuration parameters.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each fabric security name
    SortedMap<String,FabricSecurityDescription> fsds = new TreeMap();
    Vector names = tab.vecAt(FabricSecurityPrms.names, new HydraVector());
    for (int i = 0; i < names.size(); i++) {
      String name = (String)names.elementAt(i);
      if (fsds.containsKey(name)) {
        String s = BasePrms.nameForKey(FabricSecurityPrms.names)
                 + " contains duplicate entries: " + names;
        throw new HydraConfigException(s);
      }
      FabricSecurityDescription fsd = createFabricSecurityDescription(name,
                                          config, sconfig, i);
      fsds.put(name, fsd);
    }
    return fsds;
  }

  /**
   * Creates the fabric security description using test configuration parameters
   * and product defaults.
   */
  private static FabricSecurityDescription createFabricSecurityDescription(
                 String name, TestConfig config, GfxdTestConfig sconfig,
                 int index) {

    ConfigHashtable tab = config.getParameters();

    FabricSecurityDescription fsd = new FabricSecurityDescription();
    fsd.setName(name);

    // authProvider
    {
      Long key = FabricSecurityPrms.authProvider;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        fsd.setAuthProvider(getAuthProvider(str, key));
      }
    }
    // authzDefaultConnectionMode
    {
      Long key = FabricSecurityPrms.authzDefaultConnectionMode;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        fsd.setAuthzDefaultConnectionMode(getAuthzDefaultConnectionMode(str, key));
      }
    }
    // authzFullAccessUsers
    {
      Long key = FabricSecurityPrms.authzFullAccessUsers;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        List<String> list = getAccessUsers(strs, key);
        fsd.setAuthzFullAccessUsers(list);
        fsd.setAuthzFullAccessUsersStr(getAccessUsersString(list));
      }
    }
    // authzReadOnlyAccessUsers
    {
      Long key = FabricSecurityPrms.authzReadOnlyAccessUsers;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        List<String> list = getAccessUsers(strs, key);
        fsd.setAuthzReadOnlyAccessUsers(list);
        fsd.setAuthzReadOnlyAccessUsersStr(getAccessUsersString(list));
      }
    }
    // password
    {
      Long key = FabricSecurityPrms.password;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        fsd.setPassword(str);
      }
    }
    // serverAuthProvider
    {
      Long key = FabricSecurityPrms.serverAuthProvider;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        fsd.setServerAuthProvider(getAuthProvider(str, key));
      }
    }
    // sqlAuthorization
    {
      Long key = FabricSecurityPrms.sqlAuthorization;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.FALSE;
      }
      fsd.setSqlAuthorization(bool);
    }
    // user
    {
      Long key = FabricSecurityPrms.user;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        fsd.setUser(str);
      }
    }
    return fsd;
  }

//------------------------------------------------------------------------------
// authProvider and serverAuthProvider configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the authentication provider for the given string.
   *
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement UserAuthenticator.
   */
  private static String getAuthProvider(String str, Long key) {
    if (str.equalsIgnoreCase(Constants.AUTHENTICATION_PROVIDER_BUILTIN)) {
      return Constants.AUTHENTICATION_PROVIDER_BUILTIN;
    } else if (str.equalsIgnoreCase(Constants.AUTHENTICATION_PROVIDER_LDAP)) {
      return Constants.AUTHENTICATION_PROVIDER_LDAP;
    } else {
      try {
        UserAuthenticator obj = (UserAuthenticator)getInstance(key, str);
      } catch (ClassCastException e) {
        String s = BasePrms.nameForKey(key) + " does not implement "
                 + UserAuthenticator.class.getName() + ": " + str;
        throw new HydraConfigException(s);
      }
      return str;
    }
  }

//------------------------------------------------------------------------------
// authzDefaultConnectionMode configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the authz default connection mode for the given string.
   */
  private static String getAuthzDefaultConnectionMode(String str, Long key) {
    if (str.equalsIgnoreCase(Property.NO_ACCESS)) {
      return Property.NO_ACCESS;
    } else if (str.equalsIgnoreCase(Property.READ_ONLY_ACCESS)) {
      return Property.READ_ONLY_ACCESS;
    } else if (str.equalsIgnoreCase(Property.FULL_ACCESS)) {
      return Property.FULL_ACCESS;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// authzFullAccessUsers and authzReadOnlyAccessUsers configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the access users list for the given string.
   */
  private static List<String> getAccessUsers(Vector strs, Long key) {
    if (strs.size() == 0) {
      return null; // specified as "default"
    } else if (strs.size() == 1 && strs.get(0).equals(BasePrms.NONE)) {
      return null; // specified as "none"
    } else {
      return new ArrayList(strs);
    }
  }
  /**
   * Returns the access users string for the given access users list.
   */
  private static String getAccessUsersString(List<String> users) {
    if (users == null) {
      return null;
    } else {
      String str ="";
      for (String user : users) {
        str += str.length() == 0 ? user : ", " + user;
      }
      return str;
    }
  }
}
