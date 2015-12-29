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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.Map;
import java.util.Vector;
import java.util.HashMap;

/**
 * @author shaley
 *
 */
public class SerializerCreation {
  private final Vector<Class> serializerReg = new Vector<Class>();
  private final HashMap<Class, Integer> instantiatorReg = new HashMap<Class, Integer>();
 
  public static class InstantiatorImpl extends Instantiator{
    private Class m_class;
    private LogWriter m_logWriter;
    
    public InstantiatorImpl(Class<? extends DataSerializable> c, int classId) {
      super(c, classId);
      m_class = c;
      m_logWriter = null;
    }
    
    /**
     * @param c
     * @param classId
     */
    public InstantiatorImpl(Class<? extends DataSerializable> c, int classId, LogWriter logger) {
      super(c, classId);
      m_class = c;
      m_logWriter = logger;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.Instantiator#newInstance()
     */
    @Override
    public DataSerializable newInstance() {
      try {            
        return (DataSerializable) m_class.newInstance();
      }
      catch(Exception ex) {
        if(m_logWriter!=null) {
          String msg = LocalizedStrings.SerializerCreation_A_0_INSTANTIATION_FAILED
            .toLocalizedString(m_class.getName());
          m_logWriter.error(msg, ex);
        }
        return null;
      }              
    }    
  }
  
  public SerializerCreation() {
  }
    
  public void registerSerializer(Class c) {
    serializerReg.add(c);
  }
  
  public void registerInstantiator(Class c, Integer id) {
    instantiatorReg.put(c, id);
  }
  
  public void create(LogWriter logger){
    for(Class c : serializerReg ) {
      if(logger!=null) logger.fine("Registering serializer: "+c.getName());
      DataSerializer.register(c);
    }
    
    for(Map.Entry<Class, Integer> e : instantiatorReg.entrySet()) {
      final Class k = e.getKey();
      if(logger!=null) logger.fine("Registering instantiator: "+k.getName());
      Instantiator.register(new InstantiatorImpl(k, e.getValue(), logger));
    }
  }
  
  public Vector<Class> getSerializerRegistrations(){
    return serializerReg;
  }
  
  public HashMap<Class, Integer> getInstantiatorRegistrations() {
    return instantiatorReg;
  }
}
