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
package management.jmx.validation;

import javax.management.ObjectName;

import management.util.HydraUtil;

public class ValidationResult {

  private static final String NEW_LINE = System.getProperty("line.separator");
  public String url;
  public String attribute;
  boolean isError;
  public Throwable error;
  public String operationName;
  public Object value, expectedValue;
  public Object result, expectedResult;
  public JMXValidationError errorType;
  public String message;
  public ObjectName objectName;
  public String notification;

  public static ValidationResult missingNotification(ObjectName name, String url, String notification, String message) {
    ValidationResult result = new ValidationResult();
    result.objectName = name;
    result.url = url;
    result.errorType = JMXValidationError.NOTIFICATION_MISSING;
    result.message = "Error missing notification - " + message;
    result.notification = notification;
    return result;
  }

  public static ValidationResult mbeanNotFound(ObjectName name, String url) {
    ValidationResult result = new ValidationResult();
    result.objectName = name;
    result.url = url;
    result.errorType = JMXValidationError.MBEAN_NOT_FOUND;
    return result;
  }

  public static ValidationResult attributeDontMatch(ObjectName name, String url, String atr, Object value,
      Object expectedValue) {
    ValidationResult result = new ValidationResult();
    result.objectName = name;
    result.url = url;
    result.errorType = JMXValidationError.ATTRIBUTE_NOT_MATCHED;
    result.attribute = atr;
    result.value = value;
    result.expectedValue = expectedValue;
    return result;
  }

  public static ValidationResult operationResultedInError(ObjectName name, String url, String operation, Throwable error) {
    ValidationResult result = new ValidationResult();
    result.objectName = name;
    result.url = url;
    result.errorType = JMXValidationError.OPERATION_ERROR;
    result.operationName = operation;
    result.error = error;
    result.isError = true;
    return result;
  }

  public static ValidationResult operationResultUnExpected(ObjectName name, String url, String operation,
      Object opResult, Object expected) {
    ValidationResult result = new ValidationResult();
    result.objectName = name;
    result.url = url;
    result.errorType = JMXValidationError.OPERATION_RESULT_UNEXPECTED;
    result.operationName = operation;
    result.expectedResult = expected;
    result.result = opResult;
    return result;
  }

  public static ValidationResult jmxError(ObjectName name, String url, Throwable t) {
    ValidationResult result = new ValidationResult();
    result.objectName = name;
    result.url = url;
    result.isError = true;
    result.error = t;
    result.errorType = JMXValidationError.JMX_ERROR;
    return result;
  }

  public static ValidationResult error(ObjectName name, String url, String message) {
    ValidationResult result = new ValidationResult();
    result.objectName = name;
    result.url = url;
    result.errorType = JMXValidationError.USER_ERROR;
    result.message = message;
    return result;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (errorType.equals(JMXValidationError.ATTRIBUTE_NOT_MATCHED)) {
      sb.append(" REASON : ").append(errorType).append(" attribute : ").append(attribute).append(" value : ")
          .append(value).append(" expectedValue : ").append(expectedValue).append(" objectName : ").append(objectName)
          .append(" url : ").append(url).append(" ");
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.OPERATION_ERROR)) {
      sb.append(" REASON : ").append(errorType).append(" operation : ").append(operationName)
          .append(" errorString : ").append(error.getMessage()).append(" objectName : ").append(objectName)
          .append(" url : ").append(url).append(NEW_LINE).append(" StackTrace ")
          .append(HydraUtil.getStackTraceAsString(error)).append(NEW_LINE).append(" ");
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.MBEAN_NOT_FOUND)) {
      sb.append(" REASON : ").append(errorType).append(" objectName : ").append(objectName).append(" url : ")
          .append(url).append(" ");
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.OPERATION_RESULT_UNEXPECTED)) {
      sb.append(" REASON : ").append(errorType).append(" operation : ").append(operationName)
          .append(" expectedResult : ").append(expectedResult).append(" result : ").append(result)
          .append(" objectName : ").append(objectName).append(" url : ").append(url).append(" ");
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.NOTIFICATION_MISSING)) {
      sb.append(" REASON : ").append(errorType).append(" message : ").append(message).append(" notification : ")
          .append(notification).append(" objectName : ").append(" url : ").append(url);
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.NOTIFICATION_UNEXPECTED)) {
      sb.append(" REASON : ").append(errorType).append(" value : ").append(value).append(" expectedValue : ")
          .append(expectedValue).append(" attribute : ").append(attribute).append(" objectName : ").append(objectName)
          .append(" url : ").append(url).append(" ");
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.JMX_ERROR)) {
      sb.append(" REASON : ").append(errorType).append(" error : ").append(error.getMessage())
          .append(" objectName : ").append(objectName).append(" url : ").append(url).append(NEW_LINE)
          .append(" StackTrace ").append(HydraUtil.getStackTraceAsString(error)).append(NEW_LINE).append(" ");
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.JAVA_ERROR)) {
      sb.append(" REASON : ").append(errorType).append(" value : ").append(value).append(" expectedValue : ")
          .append(expectedValue).append(" attribute : ").append(attribute).append(" objectName : ").append(objectName)
          .append(" url : ").append(url).append(NEW_LINE).append(" StackTrace ")
          .append(HydraUtil.getStackTraceAsString(error)).append(NEW_LINE).append(" ");
      return sb.toString();
    } else if (errorType.equals(JMXValidationError.USER_ERROR)) {
      sb.append(" REASON : ").append(errorType).append(" message : ").append(message).append(" objectName : ")
          .append(objectName).append(" url : ").append(url).append(NEW_LINE).append(" StackTrace ")
          .append(HydraUtil.getStackTraceAsString(error)).append(NEW_LINE).append(" ");
      return sb.toString();
    }
    return null;
  }

  public void setObjectName(ObjectName objectName) {
    this.objectName = objectName;
  }

  public ValidationResult objectName(ObjectName objectName) {
    this.objectName = objectName;
    return this;
  }

  // TODO JAVA_ERROR

  // TODO toString Method

}
