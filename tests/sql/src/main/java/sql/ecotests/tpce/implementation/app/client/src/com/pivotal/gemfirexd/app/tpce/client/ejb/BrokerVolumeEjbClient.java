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
package com.pivotal.gemfirexd.app.tpce.client.ejb;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.pivotal.gemfirexd.app.tpce.input.BrokerVolumeTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.ejb.EjbBrokerVolume;
import com.pivotal.gemfirexd.app.tpce.jpa.ejb.EjbBrokerVolumeRemote;
import com.pivotal.gemfirexd.app.tpce.output.BrokerVolumeTxnOutput;


public class BrokerVolumeEjbClient {
    public static final String APP_NAME = "";
    public static final String MODULE_NAME = "ejb-jpa";
    public static final String BEAN_NAME =EjbBrokerVolume.class.getSimpleName();
    public static final String VIEWCLASS_NAME = EjbBrokerVolumeRemote.class.getName();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
	    	List<String> brokerList = new ArrayList<String>();
	    	brokerList.add("Sylvia P. Stieff");
	    	brokerList.add("Mabel G. Clawson");
	    	brokerList.add("Patrick G. Coder");
	    	brokerList.add("Sylvia P. Stieff");
	    	BrokerVolumeTxnInput bvInput = new BrokerVolumeTxnInput("Technology", brokerList); 
	    	EjbBrokerVolumeRemote bv = lookupBrokerVolume();
	    	BrokerVolumeTxnOutput bvOutput = (BrokerVolumeTxnOutput)bv.runTxn(bvInput); 
	    	System.out.println(bvOutput.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    private static EjbBrokerVolumeRemote lookupBrokerVolume() throws NamingException {
        final Hashtable<Object, Object> jndiProperties = new Hashtable<Object, Object>();
        //https://docs.jboss.org/author/display/AS71/EJB+invocations+from+a+remote+client+using+JNDI
        //https://docs.jboss.org/author/display/AS71/Remote+EJB+invocations+via+JNDI+-+EJB+client+API+or+remote-naming+project
        //add-user.bat to add the tpceuser/tpcepwd to security realm
        //See following links for some problems in remoting client
        //https://community.jboss.org/thread/195830
        //https://community.jboss.org/message/724411 -- maybe some multithread issue
        
        /*
        jndiProperties.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
        jndiProperties.put(Context.PROVIDER_URL,"remote://localhost:4447");
        jndiProperties.put(Context.SECURITY_PRINCIPAL, "tpceuser");
        jndiProperties.put(Context.SECURITY_CREDENTIALS, "tpcepwd");
        jndiProperties.put("jboss.naming.client.ejb.context", true);
        */
      
        //Or use:
        //following properties are required in addition to jboss-ejb-client.properties to avoid the IllegalStateException
        /*jboss-ejb-client.properties in the classpath
			endpoint.name=client-endpoint
			remote.connectionprovider.create.options.org.xnio.Options.SSL_ENABLED=false
			remote.connections=default 
			remote.connection.default.host=localhost
			remote.connection.default.port = 4447
			remote.connection.default.connect.options.org.xnio.Options.SASL_POLICY_NOANONYMOUS=false 
			remote.connection.default.username=tpceuser
			remote.connection.default.password=tpcepwd
         */
                
        jndiProperties.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
        final Context context = new InitialContext(jndiProperties);

        //EJB 3.1 java:global... seems not work
        String jndiString = "ejb:" + APP_NAME + "/" + MODULE_NAME + "/" + BEAN_NAME + "!" + VIEWCLASS_NAME;
        return (EjbBrokerVolumeRemote) context.lookup(jndiString);
    }
}
