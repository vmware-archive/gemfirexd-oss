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
package swarm;

import java.net.BindException;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import com.gemstone.gemfire.internal.AvailablePort;


public class WebServer  {

  private final static int PORT = 9090; 
  
  private Server server = null;
  public WebServer() {
    int attempts = 0;
    int port = PORT;
    while(attempts<100) {
      if(AvailablePort.isPortAvailable(port,AvailablePort.SOCKET)) {
        try {
          server = new Server(port);
          //server.setHandler(new BatteryHandler());
          
          ContextHandlerCollection contexts = new ContextHandlerCollection();
          server.setHandler(contexts);

          
          System.getProperties().list(System.out);
          String osBuildDir = System.getProperty("osbuild.dir",".");
          
          WebAppContext jsp = new WebAppContext(contexts,osBuildDir+"/SwarmWebContent","/");
          //Context jee = new WebAppContext(contexts,"/export/shared_build/users/gregp/gftrunk/tests/swarm/jeexplorer-1.15.war","/jee");
          
          /*ResourceHandler resource_handler=new ResourceHandler();
          resource_handler.setResourceBase("/export/shared_build/users/gregp/gftrunk/tests/swarm/WebContent");
          jsp.addHandler(resource_handler);
          */
          
//          Context root = new Context(contexts,"/",Context.SESSIONS);
          jsp.addServlet(new ServletHolder(new SwarmServlet()), "/shh");
          jsp.addServlet(new ServletHolder(new SwarmServlet()), "/shh/*");
          //server.start();
          
          server.start();
          break;
        } catch(BindException e) {
          port = port+1;
          attempts++;
          continue;
        } catch(Exception e) {
          e.printStackTrace();
        }
      } else {
        port = port+1;
        attempts++;
      }
    }
  }
  
  public Server getServer() {
    return server;
  }
  
  public static void main(String[] args) throws Exception {
   WebServer boi = new WebServer();
   boi.getServer().join();
  }

}
