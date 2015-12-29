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

package objects;

import java.io.Serializable;

/**
 * An SSO User Profile value object.
 */

public class UserProfile implements ConfigurableObject, Serializable {

  private static java.text.DecimalFormat formatter = new java.text.DecimalFormat("0000000000");

  private String UUID;
  private String secretQuestion;
  private String secretAnswer;
  private String firstName;
  private String lastName;
  private String street;
  private String city;
  private String state;
  private String postalCode;
  private String countryCode;
  private String emailAddress;
  private String phone;
  private String street2;
  private String fax;
  private String account;
  private String accountRelation;
  private java.util.Date lastModify;
  private String company;
  private java.util.Date lastLogin;
  private String languageCode;
  private java.util.Date registration;
  private String userID;
  private String password;
  private String initialName;
  private String marketingAnswer;
  private String emailAllowedFlag;
  private String deactivateCode;
  private String logonAppName;

  public UserProfile() {
  }
  public void init( int index ) {
    this.UUID = formatter.format(index);
    this.account = String.valueOf( index );
    this.company = "UseCase2";
    this.countryCode = "US";
    this.emailAddress = "Elvis@usecase2.com";
    this.firstName = "Elvis" + index;
    this.lastName = "Elvis";
    this.phone = "000-000-0000";
    this.city = "MEMPHIS";
    this.state = "TN";
    this.secretAnswer = new String( new char[ UserProfilePrms.getSecretAnswerSize() ] );
  }
  public int getIndex() {
    return ( new Integer( this.account ) ).intValue();
  }
  public void validate( int index ) {
    int encodedIndex = this.getIndex();
    if ( encodedIndex != index ) {
      throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex );
    }
  }

  //----------------------------------------------------------------------------
  // Accessors
  //----------------------------------------------------------------------------

  public String getUUID() {
    return UUID;
  }
  public void setUUID(String UUID) {
    this.UUID = UUID;
  }
  public String getSecretQuestion() {
    return secretQuestion;
  }
  public void setSecretQuestion(String secretQuestion) {
    this.secretQuestion = secretQuestion;
  }
  public String getSecretAnswer() {
    return secretAnswer;
  }
  public void setSecretAnswer(String secretAnswer) {
    this.secretAnswer = secretAnswer;
  }
  public String getFirstName() {
    return firstName;
  }
  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }
  public String getLastName() {
    return lastName;
  }
  public void setLastName(String lastName) {
    this.lastName = lastName;
  }
  public String getStreet() {
    return street;
  }
  public void setStreet(String street) {
    this.street = street;
  }
  public String getCity() {
    return city;
  }
  public void setCity(String city) {
    this.city = city;
  }
  public String getState() {
    return state;
  }
  public void setState(String state) {
    this.state = state;
  }
  public String getPostalCode() {
    return postalCode;
  }
  public void setPostalCode(String postalCode) {
    this.postalCode = postalCode;
  }
  public String getCountryCode() {
    return countryCode;
  }
  public void setCountryCode(String countryCode) {
    this.countryCode = countryCode;
  }
  public String getEmailAddress() {
    return emailAddress;
  }
  public void setEmailAddress(String emailAddress) {
    this.emailAddress = emailAddress;
  }
  public String getPhone() {
    return phone;
  }
  public void setPhone(String phone) {
    this.phone = phone;
  }
  public String getStreet2() {
    return street2;
  }
  public void setStreet2(String street2) {
    this.street2 = street2;
  }
  public String getFax() {
    return fax;
  }
  public void setFax(String fax) {
    this.fax = fax;
  }
  public String getAccount() {
    return account;
  }
  public void setAccount(String account) {
    this.account = account;
  }
  public String getAccountRelation() {
    return accountRelation;
  }
  public void setAccountRelation(String accountRelation) {
    this.accountRelation = accountRelation;
  }
  public java.util.Date getLastModify() {
    return lastModify;
  }
  public void setLastModify(java.util.Date lastModify) {
    this.lastModify = lastModify;
  }
  public String getCompany() {
    return company;
  }
  public void setCompany(String company) {
    this.company = company;
  }
  public java.util.Date getLastLogin() {
    return lastLogin;
  }
  public void setLastLogin(java.util.Date lastLogin) {
    this.lastLogin = lastLogin;
  }
  public String getLanguageCode() {
    return languageCode;
  }
  public void setLanguageCode(String languageCode) {
    this.languageCode = languageCode;
  }
  public java.util.Date getRegistration() {
    return registration;
  }
  public void setRegistration(java.util.Date registration) {
    this.registration = registration;
  }
  public String getUserID() {
    return userID;
  }
  public void setUserID(String userID) {
    this.userID = userID;
  }
  public String getPassword() {
    return password;
  }
  public void setPassword(String password) {
    this.password = password;
  }
  public String getInitialName() {
    return initialName;
  }
  public void setInitialName(String initialName) {
    this.initialName = initialName;
  }
  public String getMarketingAnswer() {
    return marketingAnswer;
  }
  public void setMarketingAnswer(String marketingAnswer) {
    this.marketingAnswer = marketingAnswer;
  }
  public String getEmailAllowedFlag() {
    return emailAllowedFlag;
  }
  public void setEmailAllowedFlag(String emailAllowedFlag) {
    this.emailAllowedFlag = emailAllowedFlag;
  }
  public String getDeactivateCode() {
    return deactivateCode;
  }
  public void setDeactivateCode(String deactivateCode) {
    this.deactivateCode = deactivateCode;
  }
  public String getLogonAppName() {
    return logonAppName;
  }
  public void setLogonAppName(String logonAppName) {
    this.logonAppName = logonAppName;
  }
  public String toString() {
    return "UserProfile(" + this.account + ")=" + this.secretAnswer.length();
  }
}
