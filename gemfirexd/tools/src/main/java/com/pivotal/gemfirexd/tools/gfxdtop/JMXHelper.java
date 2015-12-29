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
package com.pivotal.gemfirexd.tools.gfxdtop;


public class JMXHelper {
  
  private static GfxdLogger LOGGER = new GfxdLogger();
  
  
  public static Float getFloatAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Float.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Float.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Float.valueOf(0.0f);
      } else {
        return (Float) object;
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Float.valueOf(0.0f);
    }
  }

 
  public static Integer getIntegerAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Integer.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Integer.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Integer.valueOf(0);
      } else {
        return (Integer) object;
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Integer.valueOf(0);
    }
  }

  
  public static Long getLongAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Long.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Long.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Long.valueOf(0);
      } else {
        return (Long) object;
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Long.valueOf(0);
    }

  }

  
  public static String getStringAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(String.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + String.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return "";
      } else {
        return (String) object;
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return "";
    }
  }

  
  public static Boolean getBooleanAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Boolean.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Boolean.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Boolean.FALSE;
      } else {
        return (Boolean) object;
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Boolean.FALSE;
    }
  }

  
  public static Double getDoubleAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Double.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Double.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Double.valueOf(0);
      } else {
        return (Double) object;
      }
    } catch (Exception e) {
      e.printStackTrace();
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Double.valueOf(0);
    }
  }
  
  public static boolean isQuoted(String value) {
    final int len = value.length();
    if (len < 2 || value.charAt(0) != '"' || value.charAt(len - 1) != '"') {
      return false;
    } else {
      return true;
    }
  }

}
