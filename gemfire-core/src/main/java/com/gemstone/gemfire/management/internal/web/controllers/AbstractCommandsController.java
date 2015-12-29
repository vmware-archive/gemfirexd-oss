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
package com.gemstone.gemfire.management.internal.web.controllers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.web.controllers.support.MemberMXBeanAdapter;
import com.gemstone.gemfire.management.internal.web.util.UriUtils;

import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * The AbstractCommandsController class is the abstract base class encapsulating common functionality across all
 * Management Controller classes that expose REST API service endpoints (URLs/URIs) for GemFire shell (Gfsh) commands.
 * <p/>
 * @author John Blum
 * @see org.springframework.stereotype.Controller
 * @since 7.5
 */
@SuppressWarnings("unused")
public class AbstractCommandsController {

  protected static final String DEFAULT_ENCODING = UriUtils.DEFAULT_ENCODING;
  protected static final String REST_API_VERSION = "/v1";

  protected final Logger logger = Logger.getLogger(getClass().getName());

  private MemberMXBean managingMemberMXBeanProxy;

  /**
   * Asserts the argument is valid, as determined by the caller based on the expression evaluated and the result passed
   * to this assertion.
   * <p/>
   * @param validArg a boolean value indicating the evaluation of the expression validating the argument.
   * @param message a String value used as the message when constructing an IllegalArgumentException.
   * @param args Object arguments used to populate placeholder's in the message.
   * @throws IllegalArgumentException if the argument is not valid.
   * @see java.lang.String#format(String, Object...)
   */
  protected static void assertArgument(final boolean validArg, final String message, final Object... args) {
    if (!validArg) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  /**
   * Asserts the Object reference is not null!
   * <p/>
   * @param obj the reference to the Object.
   * @param message a String value used as the message when constructing an NullPointerException.
   * @param args Object arguments used to populate placeholder's in the message.
   * @throws NullPointerException if the Object reference is null.
   * @see java.lang.String#format(String, Object...)
   */
  protected static void assertNotNull(final Object obj, final String message, final Object... args) {
    if (obj == null) {
      throw new NullPointerException(String.format(message, args));
    }
  }

  /**
   * Asserts whether the conditional state based on the expression evaluated and passed to this assertion is valid.
   * <p/>
   * @param validState a boolean value indicating the evaluation of the expression from which the conditional state
   * is based.  For example, a caller might use an expression of the form (initableObj.isInitialized()).
   * @param message a String values used as the message when constructing an IllegalStateException.
   * @param args Object arguments used to populate placeholder's in the message.
   * @throws IllegalStateException if the conditional state is not valid.
   * @see java.lang.String#format(String, Object...)
   */
  protected static void assertState(final boolean validState, final String message, final Object... args) {
    if (!validState) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  /**
   * Decodes the encoded String value using the default encoding UTF-8.  It is assumed the String value was encoded
   * with the URLEncoder using the UTF-8 encoding.  This method handles UnsupportedEncodingException by just returning
   * the encodedValue.
   * <p/>
   * @param encodedValue the encoded String value to decode.
   * @return the decoded value of the String or encodedValue if the UTF-8 encoding is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#decode(String)
   */
  protected static String decode(final String encodedValue) {
    return UriUtils.decode(encodedValue);
  }

  /**
   * Decodes the encoded String value using the specified encoding (such as UTF-8).  It is assumed the String value
   * was encoded with the URLEncoder using the specified encoding.  This method handles UnsupportedEncodingException
   * by just returning the encodedValue.
   * <p/>
   * @param encodedValue a String value encoded in the encoding.
   * @param encoding a String value specifying the encoding.
   * @return the decoded value of the String or encodedValue if the specified encoding is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#decode(String, String)
   */
  protected static String decode(final String encodedValue, final String encoding) {
    return UriUtils.decode(encodedValue, encoding);
  }

  /**
   * Gets the specified value if not null or empty, otherwise returns the default value.
   * <p/>
   * @param value the String value being evaluated for having value (not null and not empty).
   * @param defaultValue the default String value returned if 'value' has no value.
   * @return 'value' if not null or empty, otherwise returns the default value.
   * @see #hasValue(String)
   */
  protected static String defaultIfNoValue(final String value, final String defaultValue) {
    return (hasValue(value) ? value : defaultValue);
  }

  /**
   * Encodes the String value using the default encoding UTF-8.
   * <p/>
   * @param value the String value to encode.
   * @return an encoded value of the String using the default encoding UTF-8 or value if the UTF-8 encoding
   * is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#encode(String)
   */
  protected static String encode(final String value) {
    return UriUtils.encode(value);
  }

  /**
   * Encodes the String value using the specified encoding (such as UTF-8).
   * <p/>
   * @param value the String value to encode.
   * @param encoding a String value indicating the encoding.
   * @return an encoded value of the String using the specified encoding or value if the specified encoding
   * is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#encode(String, String)
   */
  protected static String encode(final String value, final String encoding) {
    return UriUtils.encode(value, encoding);
  }

  /**
   * Handles any Exception thrown by a REST API web service endpoint, HTTP request handler method during the invocation
   * and processing of a command.
   * <p/>
   * @param cause the Exception causing the error.
   * @return a ResponseEntity with an appropriate HTTP status code (500 - Internal Server Error) and HTTP response body
   * containing the stack trace of the Exception.
   * @see org.springframework.http.ResponseEntity
   * @see org.springframework.web.bind.annotation.ExceptionHandler
   * @see org.springframework.web.bind.annotation.ResponseBody
   */
  @ExceptionHandler(Exception.class)
  @ResponseBody
  public ResponseEntity<String> handleException(final Exception cause) {
    final String stackTrace = printStackTrace(cause);
    logger.severe(stackTrace);
    return new ResponseEntity<String>(stackTrace, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * Determines whether the specified Object has value, which is determined by a non-null Object reference.
   * <p/>
   * @param value the Object value being evaluated for value.
   * @return a boolean value indicating whether the specified Object has value.
   * @see java.lang.Object
   */
  protected static boolean hasValue(final Object value) {
    return (value != null);
  }

  /**
   * Determines whether the specified Object array has any value, which is determined by a non-null Object array
   * reference along with containing elements.
   * <p/>
   * @param array an Object array being evaluated for value.
   * @return a boolean indicating whether the specified Object array has any value.
   * @see java.lang.Object
   */
  protected static boolean hasValue(final Object[] array) {
    if (array != null && array.length > 0) {
      for (final Object element : array) {
        if (hasValue(element)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Determines whether the specified String has value, determined by whether the String is non-null, not empty
   * and not blank.
   * <p/>
   * @param value the String being evaluated for value.
   * @return a boolean indicating whether the specified String has value or not.
   * @see java.lang.String
   */
  protected static boolean hasValue(final String value) {
    return !StringUtils.isBlank(value);
  }

  /**
   * Initializes data bindings for various HTTP request handler method parameter Java class types.
   * <p/>
   * @param dataBinder the DataBinder implementation used for Web transactions.
   * @see org.springframework.web.bind.WebDataBinder
   * @see org.springframework.web.bind.annotation.InitBinder
   */
  @InitBinder
  public void initBinder(final WebDataBinder dataBinder) {
    dataBinder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(
      StringArrayPropertyEditor.DEFAULT_SEPARATOR, false));
  }

  /**
   * Writes the stack trace of the Throwable to a String.
   * <p/>
   * @param t a Throwable object who's stack trace will be written to a String.
   * @return a String containing the stack trace of the Throwable.
   * @see java.lang.Throwable#printStackTrace(java.io.PrintWriter)
   */
  protected String printStackTrace(final Throwable t) {
    final StringWriter stackTraceWriter = new StringWriter();
    t.printStackTrace(new PrintWriter(stackTraceWriter));
    return stackTraceWriter.toString();
  }

  /**
   * Converts the URI relative path to an absolute path based on the Servlet context information.
   * <p/>
   * @param path the URI relative path to append to the Servlet context path.
   * @return a URI constructed with all component path information.
   * @see java.net.URI
   * @see org.springframework.web.servlet.support.ServletUriComponentsBuilder#fromCurrentContextPath()
   */
  protected URI toUri(final String path) {
    return ServletUriComponentsBuilder.fromCurrentContextPath().path(REST_API_VERSION).path(path).build().toUri();
  }

  /**
   * Gets a reference to the platform MBeanServer running in this JVM process.  The MBeanServer instance constitutes
   * a connection to the MBeanServer.
   * <p/>
   * @return a reference to the platform MBeanServer for this JVM process.
   * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
   * @see javax.management.MBeanServer
   */
  protected MBeanServer getMBeanServer() {
    return ManagementFactory.getPlatformMBeanServer();
  }

  /**
   * Gets the MemberMXBean from the JVM Platform MBeanServer for the specified member, identified by name or ID,
   * in the GemFire cluster.
   * <p/>
   * @param memberNameId a String indicating the name or ID of the GemFire member.
   * @return a proxy to the GemFire member's MemberMXBean.
   * @throws IllegalStateException if no MemberMXBean could be found for GemFire member with ID or name.
   * @throws RuntimeException wrapping the MalformedObjectNameException if the ObjectName pattern is malformed.
   * @see #getMBeanServer()
   * @see javax.management.JMX
   * @see com.gemstone.gemfire.management.MemberMXBean
   * @see com.gemstone.gemfire.management.internal.ManagementConstants
   */
  protected MemberMXBean getMemberMXBean(final String memberNameId) {
    try {
      final MBeanServer connection = getMBeanServer();

      final String objectNamePattern = ManagementConstants.OBJECTNAME__PREFIX.concat("type=Member,*");

      // NOTE possibly throws a MalformedObjectNameException, but this should not happen
      final ObjectName objectName = ObjectName.getInstance(objectNamePattern);

      final QueryExp query = Query.or(
        Query.eq(Query.attr("Name"), Query.value(memberNameId)),
        Query.eq(Query.attr("Id"), Query.value(memberNameId))
      );

      final Set<ObjectName> objectNames = connection.queryNames(objectName, query);

      assertState(isMemberMXBeanFound(objectNames),
        "No MemberMXBean with ObjectName (%1$s) was found in the Platform MBeanServer for member (%2$s)!",
          objectName, memberNameId);

      return JMX.newMXBeanProxy(connection, objectNames.iterator().next(), MemberMXBean.class);
    }
    catch (MalformedObjectNameException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isMemberMXBeanFound(final Collection<ObjectName> objectNames) {
    return (objectNames != null && objectNames.size() == 1);
  }

  /**
   * Lookup operation for the MemberMXBean representing the Manager in the GemFire cluster.  This method gets
   * an instance fo the Platform MBeanServer for this JVM process and uses it to lookup the MemberMXBean for the
   * GemFire Manager based on the ObjectName declared in the DistributedSystemMXBean.getManagerObjectName() operation.
   * <p/>
   * @return a proxy instance to the MemberMXBean of the GemFire Manager.
   * @see #getMBeanServer()
   * @see #createMemberMXBeanForManagerUsingAdapter(javax.management.MBeanServer, javax.management.ObjectName)
   * @see #createMemberMXBeanForManagerUsingProxy(javax.management.MBeanServer, javax.management.ObjectName)
   * @see com.gemstone.gemfire.management.DistributedSystemMXBean
   * @see com.gemstone.gemfire.management.MemberMXBean
   */
  protected synchronized MemberMXBean getManagingMemberMXBean() {
    if (managingMemberMXBeanProxy == null) {
      final MBeanServer platformMBeanServer = getMBeanServer();

      final DistributedSystemMXBean distributedSystemMXBean = JMX.newMXBeanProxy(platformMBeanServer,
        MBeanJMXAdapter.getDistributedSystemName(), DistributedSystemMXBean.class);

      //managingMemberMXBeanProxy = createMemberMXBeanForManagerUsingAdapter(platformMBeanServer,
      //  distributedSystemMXBean.getMemberObjectName());

      managingMemberMXBeanProxy = createMemberMXBeanForManagerUsingProxy(platformMBeanServer,
        distributedSystemMXBean.getMemberObjectName());
    }

    return managingMemberMXBeanProxy;
  }

  private MemberMXBean createMemberMXBeanForManagerUsingAdapter(final MBeanServer server, final ObjectName managingMemberObjectName) {
    return new MemberMXBeanProxy(server, managingMemberObjectName);
  }

  private MemberMXBean createMemberMXBeanForManagerUsingProxy(final MBeanServer server, final ObjectName managingMemberObjectName) {
    return JMX.newMXBeanProxy(server, managingMemberObjectName, MemberMXBean.class);
  }

  /**
   * Executes the specified command String as if entered by the user from the GemFire Shell (Gfsh).  Note, that Gfsh
   * performs validation of the command during parsing before sending the command execution to the Manager
   * for processing.
   * <p/>
   * @param command a String value containing a valid command String as would be entered by the user in Gfsh.
   * @return a result of the command execution as a String value, typically marshalled in JSON to be serialized back
   * to Gfsh.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   * @see #processCommand(String, java.util.Map)
   */
  protected String processCommand(final String command) {
    return processCommand(command, Collections.<String, String>singletonMap(
      Gfsh.ENV_APP_NAME, Gfsh.GFSH_APP_NAME));
  }

  /**
   * Executes the specified command String as if entered by the user from the GemFire Shell (Gfsh).  Note, that Gfsh
   * performs validation of the command during parsing before sending the command execution to the Manager
   * for processing.
   * <p/>
   * @param command a String value containing a valid command String as would be entered by the user in Gfsh.
   * @param environment a Map containing any environment configuration settings to be used by the Manager during
   * command execution.  For example, when executing commands originating from Gfsh, the key/value pair (APP_NAME=gfsh)
   * is a specified mapping in the "environment".  Note, it is common for the REST API to act a bridge, or an adapter
   * between Gfsh and the Manager, and thus need to specify this key/value pair mapping.
   * @return a result of the command execution as a String value, typically marshalled in JSON to be serialized back
   * to Gfsh.
   * @see com.gemstone.gemfire.management.MemberMXBean#processCommand(String)
   */
  protected String processCommand(final String command, final Map<String, String> environment) {
    if (logger.isLoggable(Level.FINE)) {
      logger.fine(String.format("Processing Command (%1$s)...", command));
    }

    return getManagingMemberMXBean().processCommand(command, environment);
  }

  /**
   * The MemberMXBeanProxy class is a proxy for the MemberMXBean interface transforming an operation on the member
   * MBean into a invocation on the MBeanServer, invoke method.
   * <p/>
   * @see com.gemstone.gemfire.management.internal.web.controllers.support.MemberMXBeanAdapter
   */
  private static class MemberMXBeanProxy extends MemberMXBeanAdapter {

    private final MBeanServer server;

    private final ObjectName objectName;

    public MemberMXBeanProxy(final MBeanServer server, final ObjectName objectName) {
      assertNotNull(server, "The connection or reference to the Platform MBeanServer cannot be null!");
      assertNotNull(objectName, "The JMX ObjectName for the GemFire Manager MemberMXBean cannot be null!");
      this.server = server;
      this.objectName = objectName;
    }

    protected MBeanServer getMBeanServer() {
      return server;
    }

    protected ObjectName getObjectName() {
      return objectName;
    }

    @Override
    public String processCommand(final String commandString, final Map<String, String> env) {
      try {
        return String.valueOf(getMBeanServer().invoke(getObjectName(), "processCommand",
          new Object[] { commandString, env }, new String[] { String.class.getName(), Map.class.getName() }));
      }
      catch (Exception e) {
        throw new RuntimeException(String.format(
          "An error occurred while executing processCommand with command String (%1$s) on the MemberMXBean (%2$s) of the GemFire Manager using environment (%3$s)!",
            commandString, getObjectName(), env), e);
      }
    }
  }

}
