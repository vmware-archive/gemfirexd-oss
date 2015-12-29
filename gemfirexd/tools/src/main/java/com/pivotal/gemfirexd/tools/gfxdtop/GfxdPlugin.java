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


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.SwingWorker;

/* TODO: Add jconsole.jar
import com.sun.tools.jconsole.JConsolePlugin;
import com.sun.tools.jconsole.JConsoleContext;
import com.sun.tools.jconsole.JConsoleContext.ConnectionState;
*/

/**
 * JTopPlugin is a subclass to com.sun.tools.jconsole.JConsolePlugin
 *
 * JTopPlugin is loaded and instantiated by JConsole.  One instance
 * is created for each window that JConsole creates. It listens to
 * the connected property change so that it will update JTop with
 * the valid MBeanServerConnection object.  JTop is a JPanel object
 * displaying the thread and its CPU usage information.
 */
public class GfxdPlugin /* extends  JConsolePlugin */ implements PropertyChangeListener
{
    private GfxdTop gfxdTop = null;
    private Map<String, JPanel> tabs = null;

    public GfxdPlugin() {
        // register itself as a listener
      
        /* TODO: Add jconsole.jar
       /*addContextPropertyChangeListener(this); */
    }

    /*
     * Returns a JTop tab to be added in JConsole.
     */
    public synchronized Map<String, JPanel> getTabs() {
      /* TODO : Add jconsole.jar
        if (tabs == null) {
            jtop = new GfxdTop();
            jtop.setMBeanServerConnection(
                getContext().getMBeanServerConnection());
            // use LinkedHashMap if you want a predictable order
            // of the tabs to be added in JConsole
            tabs = new LinkedHashMap<String, JPanel>();
            tabs.put("GfxdTop", jtop);
        }
        return tabs;
        */
      return null;
    }

    /*
     * Returns a SwingWorker which is responsible for updating the JTop tab.
     */
    public SwingWorker<?,?> newSwingWorker() {
        return gfxdTop.newSwingWorker();    
    }

    // You can implement the dispose() method if you need to release 
    // any resource when the plugin instance is disposed when the JConsole
    // window is closed.
    //
    // public void dispose() {
    // }
                                                                    
    /*
     * Property listener to reset the MBeanServerConnection
     * at reconnection time.
     */
    public void propertyChange(PropertyChangeEvent ev) {
        String prop = ev.getPropertyName();
        
        /* TODO : Add jconsole.jar
        
        if (prop == JConsoleContext.CONNECTION_STATE_PROPERTY) {
            ConnectionState oldState = (ConnectionState)ev.getOldValue();
            ConnectionState newState = (ConnectionState)ev.getNewValue();
            // JConsole supports disconnection and reconnection
            // The MBeanServerConnection will become invalid when
            // disconnected. Need to use the new MBeanServerConnection object
            // created at reconnection time.
            if (newState == ConnectionState.CONNECTED && jtop != null) {
                jtop.setMBeanServerConnection(
                    getContext().getMBeanServerConnection()); 
            }
        }
        
        */
    }
}
