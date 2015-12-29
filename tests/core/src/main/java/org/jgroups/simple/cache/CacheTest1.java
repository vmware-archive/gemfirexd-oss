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
package org.jgroups.simple.cache;

import java.io.*;
import java.util.Enumeration;
//import com.jgroups.simple.cache.*;

public class CacheTest1 implements java.io.Serializable
{
    public void showHelp()
    {
        System.out.println("");
        System.out.println("Initially you can use 'set' to set attributes for the region.");
        System.out.println("When ready, use 'connect' to define the region.");
        System.out.println("");
        System.out.println("Once connected you can use these commands:");
        System.out.println("attr name        - fetch the attributes of an object");
        System.out.println("def name         - define an object");
        System.out.println("des name         - destroy an object");
        System.out.println("get name         - get the value of an object");
        System.out.println("inv name         - invalidate an object");
        System.out.println("list             - list all objects");
        System.out.println("own name         - get ownership of an object");
        System.out.println("put name value   - initial put of an object");
        System.out.println("rel name         - release ownership");
        System.out.println("repl name value  - update an object");
        System.out.println("reset            - reset attributes");
        System.out.println("set attr <value> - set an attribute");
        System.out.println("exit or quit");
        System.out.println("");
        System.out.println("For 'set', you can set idleTime, timeToLive, defaultTimeToLive, and");
        System.out.println("attribute flags in lower case.");
        System.out.println("Use 'no ' in front of an attribute flag to turn it off");
        System.out.println("Set a loader with 'set loader value'.  Loaders do netSearch first.");
        System.out.println("Set a listener with 'set listener tag'.  The tag is displayed in notifications.");
        System.out.println("");
    }


    public static void main(String args[]) throws Exception
    {
        new CacheTest1().go();
    }
    
    public String parseName(String command)
    {
        int space = command.indexOf(' ');
        if (space < 0) {
            System.err.println("You need to give a name argument for this command");
            return null;
        }
        else {
            int space2 = command.indexOf(' ', space+1);
            if (space2 < 0)
                return command.substring(space+1);
            else
                return command.substring(space+1, space2);
        }
    }
    
    public String parseValue(String command)
    {
        int space = command.indexOf(' ');
        if (space < 0) {
            System.err.println("You need to give a value for this command");
            return null;
        }
        space = command.indexOf(' ', space+1);
        if (space < 0) {
            System.err.println("You need to give a value for this command");
            return null;
        }
        else {
            int space2 = command.indexOf(' ', space+1);
            if (space2 < 0)
                return command.substring(space+1);
            else
                return command.substring(space+1, space2);
        }
    }
    
    public int parseInt(String value)
    {
        try {
            return Integer.parseInt(value);
        }
        catch (Exception e) {
            System.err.println("illegal number: " + value);
            return -1;
        }
    }

    public long processAttributeFlag(String flagName, long flags, boolean set)
    {
        if (flagName.startsWith("dis")) {
            if (!set)
                return flags & (~Attributes.DISTRIBUTE);
            else
                return flags | Attributes.DISTRIBUTE;
        }
        else if (flagName.startsWith("rep")) {
            if (!set)
                return flags & (~Attributes.REPLY);
            else
                return flags | Attributes.REPLY;
        }
        else if (flagName.startsWith("syn")) {
            if (!set)
                return flags & (~Attributes.SYNCHRONIZE);
            else
                return flags | Attributes.SYNCHRONIZE;
        }
        else if (flagName.startsWith("original")) {
            if (!set)
                return flags & (~Attributes.ORIGINAL);
            else
                return flags | Attributes.ORIGINAL;
        }
        else {
            System.err.println("Flag not understood: " + flagName);
            return flags;
        }
    }
    
    public void go() throws Exception
    {
        long attrFlags = 0;

        CacheAttributes cattr = new CacheAttributes();
        cattr.setDistribute(true);
        int maxObjects = Integer.getInteger("maxObjects", 0).intValue();
        if (maxObjects > 0)
            cattr.setMaxObjects(maxObjects);
        JCache.init(cattr);
        
        BufferedReader bin = new BufferedReader(new InputStreamReader(System.in));

        CacheAccess ca = null;
        Attributes attr = new Attributes();
        attr.setFlags(attrFlags);

        String command;

        System.out.println("Press Enter for help at the command prompt.");
        System.out.println("");

        while (true) {
            System.out.print("> ");
            System.out.flush();
            command = bin.readLine();

            if (command.startsWith("exit") || command.startsWith("quit")) {
                ca.close();
                JCache.close();
                break;
            }
            else if (command.startsWith("set")) {
                String name = parseName(command);
                if (name == null)
                    continue;
                if (name.equals("idleTime")) {
                    String value = parseValue(command);
                    if (value != null) {
                        int idleTime = parseInt(value);
                        attr.setIdleTime(idleTime);
                    }
                }
                else if (name.equals("timeToLive")) {
                    String value = parseValue(command);
                    if (value != null) {
                        int timeToLive = parseInt(value);
                        attr.setTimeToLive(timeToLive);
                    }
                }
                else if (name.equals("defaultTimeToLive")) {
                    String value = parseValue(command);
                    if (value != null) {
                        int timeToLive = parseInt(value);
                        attr.setDefaultTimeToLive(timeToLive);
                    }
                }
                else if (name.equals("listener")) {
                    final String value = parseValue(command);
                    if (value != null) {
                        CacheEventListener l = new CacheEventListener() {
                            public void handleEvent(CacheEvent event) throws JCacheException
                            {
                                if (event.getId() == CacheEvent.OBJECT_INVALIDATED)
                                    System.out.println("object '"+value+"' invalidated: " + event);
                                else
                                    System.out.println("object '"+value+"' updated: " + event);
                            }
                        };
                        attr.setListener(l);
                    }
                }
                else if (name.equals("loader")) {
                    final String value = parseValue(command);
                    if (value != null)  {
                        attr.setLoader(new CacheLoader() {
                            public Object load(Object handle, Object args) throws JCacheException
                            {
                                try { return netSearch(handle, 1000); }
                                catch (JCacheException e) { }
                                return value;
                            }
                        });
                    }
                }
                else if (name.equals("no")) {
                    String value = parseValue(command);
                    if (value != null)
                        attrFlags = processAttributeFlag(value, attrFlags, false);
                }
                else {
                    attrFlags = processAttributeFlag(name, attrFlags, true);
                }
                attr.setFlags(attrFlags);
            }
            else if (ca == null) {
                if (command.startsWith("con")) {
                    try {
                        CacheAccess.defineRegion("MyRegion", attr);
                        System.out.println("region MyRegion is now defined.  Attributes are now reset to defaults");
                    }
                    catch (ObjectExistsException e) { }
                
                    ca = CacheAccess.getAccess("MyRegion", null);
                    attr = new Attributes();
                    attrFlags = 0;
                }
                else {
                    System.err.println("You need to connect before using that command");
                    showHelp();
                }
            }
            else if (command.startsWith("repl")) {
                String name = parseName(command);
                if (name != null) {
                    String value = parseValue(command);
                    if (value != null) {
                        try {
                            ca.replace(name, value);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            else if (command.startsWith("put")) {
                String name = parseName(command);
                if (name != null) {
                    String value = parseValue(command);
                    if (value != null) {
                        try {
                            ca.put(name, attr, value);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            else if (command.startsWith("def")) {
                String name = parseName(command);
                if (name != null) {
                    try {
                        ca.defineObject(name, attr);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else if (command.startsWith("get")) {
                String name = parseName(command);
                if (name != null) {
                    try {
                        String value = (String)ca.get(name);
                        System.out.println("result: " + value);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else if (command.startsWith("attr")) {
                if (command.indexOf(' ') < 0) {
                    Attributes regionAttr = ca.getAttributes();
                    System.out.println("region attributes: " + regionAttr);
                }
                else {
                    String name = parseName(command);
                    if (name != null) {
                        try {
                            Attributes entryAttr = ca.getAttributes(name);
                            System.out.println(entryAttr.toString());
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            else if (command.startsWith("own")) {
                String name = parseName(command);
                if (name != null) {
                    try {
                        ca.getOwnership(name, 0);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else if (command.startsWith("rel")) {
                String name = parseName(command);
                if (name != null) {
                    try {
                        ca.releaseOwnership(name, 0);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else if (command.startsWith("reset")) {
                attr = new Attributes();
                attrFlags = 0;
                System.out.println("attributes have been reset to defaults");
            }
            else if (command.startsWith("inv")) {
                String name = parseName(command);
                if (name != null) {
                    try {
                        ca.invalidate(name);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else if (command.startsWith("des")) {
                String name = parseName(command);
                if (name != null) {
                    try {
                        ca.destroy(name);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else if (command.startsWith("list")) {
                // enumerate the cache
                Enumeration e = JCache.listCacheObjects("MyRegion");
                while (e.hasMoreElements()) {
                    CacheObjectInfo co = (CacheObjectInfo)e.nextElement();
                    System.out.println(co.name + "   refcount: " + co.refcount +
                    "  accesses: " + co.accesses + "  expire: " + co.expire);
                }
            }
            else {
                showHelp();
            }
        }
    }
}
