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
package examples.jmxmonitor;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import examples.jmxmonitor.notifier.StatsNotifier;
import examples.jmxmonitor.jmxstats.GemFireXDStats;


/*
 * It is a regular class parsing an xml file for the JMX monitor configurations
 * and properties. Ideally it should be a private non-static inner class of the
 * JMXGemFireXDMonitor. Since its code is not related to demonstrated JMX APIs or
 * GemFireXD MBeans, it is put in a separate file on purpose.
 *
 */
class MonitorConfigHelper {
	private String agentHost;
	private String agentPort;
	private String runMode = "continuous";
	private long runDuration = 0; //in second
	private long pollInterval = 0; //in second
	
	private Map<String, StatsNotifier> statsNotifierMap = new HashMap<String, StatsNotifier>();	
	private Map<String, Class<GemFireXDStats>> memberStatsClassMap = new HashMap<String, Class<GemFireXDStats>>();
	private Map<String, Map<String, String>> memberStatsClassPropertiesMap = new HashMap<String, Map<String, String>>();
	//can extend to systemStats
	
	private String configFile = null;
	
	MonitorConfigHelper(String configFile) {
		this.configFile = configFile;
	}
	
	@SuppressWarnings("unchecked")
	void parseMonitorConfiguration() throws JMXGemFireXDMonitorException
	{
		InputStream inputStream = MonitorConfigHelper.class.getResourceAsStream(configFile);
		Document doc = null;
		//ToDo: xml parsing validation
	    try {
	        if (inputStream == null) {
	            throw new IllegalStateException("Could not find the " + configFile);
	        }
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();   
            //ToDo: xml validation
            DocumentBuilder db = dbf.newDocumentBuilder();
            doc = db.parse(inputStream);
	    } catch(ParserConfigurationException pce) {
            throw new JMXGemFireXDMonitorException("Could not parse the config file", pce);
        } catch(SAXException se) {
            throw new JMXGemFireXDMonitorException("Could not parse the config file", se);
        } catch(IOException ioe) {
            throw new JMXGemFireXDMonitorException("Could not parse the config file", ioe);
        }

        if(doc == null) {
            throw new JMXGemFireXDMonitorException("Could not parse the config file" + configFile);
        }
        
        NodeList configElements = doc.getElementsByTagName("Configuration");
        
        if(configElements != null && configElements.getLength() == 1) {
        	NodeList agentConnElementList = ((Element)configElements.item(0)).getElementsByTagName("AgentConnection");
        	if (agentConnElementList != null && agentConnElementList.getLength() == 1) {	
            	agentHost = ((Element)agentConnElementList.item(0)).getAttribute("agent-host");
            	if (agentHost == null) {
            		agentHost = "localhost";
            	}
            	agentPort = ((Element)agentConnElementList.item(0)).getAttribute("agent-port");
            	if (agentPort == null) {
            		agentPort = "1099";   
            	}	                	
    		} else {
    			throw new JMXGemFireXDMonitorException("The number of the element AgentConnection is not 1");
    		}
        	
        	NodeList statsNotifierElementList = ((Element)configElements.item(0)).getElementsByTagName("StatsNotifier");
			for (int j = 0; j < statsNotifierElementList.getLength(); j++) {
				Element notifierNode = (Element)statsNotifierElementList.item(j);    
        		String notifierId = notifierNode.getAttribute("id");
        		String className = notifierNode.getAttribute("java-class");
                try {
                    Class<?> notifierClass = Class.forName(className);
                    StatsNotifier notifier = (StatsNotifier)notifierClass.newInstance();
                    statsNotifierMap.put(notifierId, notifier);
                    //ToDo: to add any properties to this instance
                } catch(ClassNotFoundException cnfe) {
                	throw new JMXGemFireXDMonitorException("Could not find class " + className, cnfe);
                } catch(IllegalAccessException iae){
                	throw new JMXGemFireXDMonitorException("Could not access class " + className, iae);
                } catch(InstantiationException ie) {
                	throw new JMXGemFireXDMonitorException("Could not instantiate class " + className, ie);
                }                					
        	}
					
			NodeList runModeElementList = ((Element)configElements.item(0)).getElementsByTagName("RunMode");
			if (runModeElementList.getLength() != 0) {
				runMode = ((Element)runModeElementList.item(0)).getAttribute("mode");
				try {
					runDuration = Long.valueOf(((Element)runModeElementList.item(0)).getAttribute("duration").trim());
				} catch (NumberFormatException e) {
					//do nothing, runDuration is default to 0
				}	
				try {
					pollInterval = Long.valueOf(((Element)runModeElementList.item(0)).getAttribute("poll-interval").trim());
				} catch (NumberFormatException e) {
					//do nothing, runDuration is default to 0
				}					
			}			
        } else {
        	throw new JMXGemFireXDMonitorException("The number of the element Configuration is not 1");
        }
  
        NodeList jmxStatsElements = doc.getElementsByTagName("JMXStats");
        if(jmxStatsElements != null && jmxStatsElements.getLength() == 1) {   	
        	NodeList memberStatsElementList = ((Element)jmxStatsElements.item(0)).getElementsByTagName("MemberStats");
        	for (int i = 0; i < memberStatsElementList.getLength(); i++) {
        		Element memberStatsElement = (Element)memberStatsElementList.item(i);
        		String statsItemId = memberStatsElement.getAttribute("id");
        		String className = memberStatsElement.getAttribute("java-class");
    		
                try {
                    Class statsClass = Class.forName(className);
                    memberStatsClassMap.put(statsItemId, statsClass);
                    //ToDo: to add any properties to this instance
                } catch(ClassNotFoundException cnfe) {
                	throw new JMXGemFireXDMonitorException("Could not find class " + className, cnfe);
                } 
                
                Map<String, String> statsItemPropertyMap = new HashMap<String, String>();
                
                NodeList statsItemPropertyNodeList = memberStatsElement.getChildNodes();	                
                for (int j = 0; j < statsItemPropertyNodeList.getLength(); j++) {
                	Node statsItemPropertyNode = statsItemPropertyNodeList.item(j);
                	if (statsItemPropertyNode.getNodeType() != Node.ELEMENT_NODE) {
                		continue;
                	}
        		
                	if (statsItemPropertyNode.getNodeName().equalsIgnoreCase("StatsNotification")) {
                		statsItemPropertyMap.put("notifier-id", ((Element)statsItemPropertyNode).getAttribute("notifier-id"));
                		NodeList thresholdNodeList = ((Element)statsItemPropertyNode).getElementsByTagName("NotifcationThreshold");
                		for (int k = 0; k < thresholdNodeList.getLength(); k++) {
                			Element thresholdNode = (Element)thresholdNodeList.item(k);
                			statsItemPropertyMap.put(thresholdNode.getAttribute("name"), thresholdNode.getAttribute("value"));
                		}           			
                	} else {
                		statsItemPropertyMap.put(statsItemPropertyNode.getNodeName(), statsItemPropertyNode.getTextContent().trim());
                	}  
                }
                memberStatsClassPropertiesMap.put(statsItemId, statsItemPropertyMap);
    		}
        } else {
        	throw new JMXGemFireXDMonitorException("The number of the element JMXStats is not 1");
        }
	}
	
	String getAgentHost() {
		return agentHost;
	}
	
	String getAgentPort() {
		return agentPort;
	}

	long getPollInterval() {
		return pollInterval*1000; //convert second to millsecond
	}
	
	String getRunMode() {
		return runMode;
	}

	long getRunDuration() {
		return runDuration*1000; //convert second to millsecond
	}
	
	Map<String, StatsNotifier> getStatsNotifierMap() {
		return statsNotifierMap;
	}
	
	Map<String, Class<GemFireXDStats>> getMemberStatsClassMap() {
		return memberStatsClassMap;
	}
	
	Map<String, Map<String, String>> getMemberStatsClassPropertiesMap() {
		return memberStatsClassPropertiesMap;
	}	
}
