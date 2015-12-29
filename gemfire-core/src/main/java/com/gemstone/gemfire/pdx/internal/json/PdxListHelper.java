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
package com.gemstone.gemfire.pdx.internal.json; 
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/*
 * This class is to convert JSON array into List.
 */
public class PdxListHelper {
  String m_name;
  PdxListHelper m_parent;
  List list = new LinkedList();
  LogWriterI18n logger = null;
  
  public PdxListHelper(PdxListHelper parent, String name)
  {
    GemFireCacheImpl gci = (GemFireCacheImpl)CacheFactory.getAnyInstance();
    logger = gci.getLoggerI18n();
    m_name = name;
    if(logger.finerEnabled()) {
      logger.finer("PdxListHelper name: " + name  );
    }
    m_parent = parent;
  }
  
  public PdxListHelper getParent()
  {
    return m_parent;
  }
  
  public void setListName(String fieldName)
  {
    if(logger.finerEnabled()) {
      logger.finer("setListName fieldName: " + fieldName  );
    }
    m_name = fieldName;
  }
  public void addStringField(String fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addStringField fieldValue: " + fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addByteField(byte fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addByteField fieldValue: " + fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addShortField(short fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addShortField fieldValue: " + fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addIntField(int fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addIntField fieldValue: " + fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addLongField(long fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addLongField fieldValue: " + fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addBigIntegerField(BigInteger fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addBigIntegerField fieldValue: " + fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addBooleanField(boolean fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addBooleanField fieldValue: " + fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addFloatField(float fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addFloatField fieldName: " + fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addDoubleField(double fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addDoubleField fieldName: " + fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addBigDecimalField(BigDecimal fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addBigDecimalField fieldName: " + fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addNullField(Object fieldValue)
  {
    if(logger.finerEnabled()) {
      logger.finer("addNULLField fieldName: " + fieldValue   );
    }
    list.add(fieldValue);
  }
  
  public PdxListHelper addListField()
  {
    if(logger.finerEnabled()) {
      logger.finer("addListField fieldName: " );
    }
    PdxListHelper tmp = new PdxListHelper(this, "no-name");
    list.add(tmp);
    return tmp;
  }
  
  public PdxListHelper endListField()
  {
    if(logger.finerEnabled()) {
      logger.finer("endListField fieldName: " );
    }
    return m_parent;
  }
  
  public void addObjectField(String fieldName, PdxInstanceHelper dpi)
  {
    if(fieldName != null)
      throw new IllegalStateException("addObjectField:list should have object no fieldname");
    if(logger.finerEnabled()) {
      logger.finer("addObjectField fieldName: " + fieldName  );
    }
    //dpi.setPdxFieldName(fieldName);
    list.add(dpi.getPdxInstance());
  }
  
  public void endObjectField(String fieldName)
  {
    if(logger.finerEnabled()) {
      logger.finer("endObjectField fieldName: " + fieldName  );
    }
  }
  
  public List getList()
  {
    return list;
  }
}
