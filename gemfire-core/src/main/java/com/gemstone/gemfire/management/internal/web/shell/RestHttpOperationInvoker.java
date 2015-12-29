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
package com.gemstone.gemfire.management.internal.web.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.internal.lang.Filter;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.internal.cli.CommandRequest;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.management.internal.web.domain.LinkIndex;
import com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest;
import com.gemstone.gemfire.management.internal.web.util.ConvertUtils;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.util.UriTemplate;

/**
 * The RestHttpOperationInvoker class is an implementation of the OperationInvoker interface that translates (adapts) 
 * GemFire shell command invocations into HTTP requests to a corresponding REST API call hosted by the GemFire Manager's
 * HTTP service using the Spring RestTemplate.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
 * @see com.gemstone.gemfire.management.internal.cli.shell.OperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.AbstractHttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.SimpleHttpOperationInvoker
 * @since 7.5
 */
@SuppressWarnings("unused")
public class RestHttpOperationInvoker extends AbstractHttpOperationInvoker {

  protected static final String RESOURCES_REQUEST_PARAMETER = "resources";

  // the HttpOperationInvoker used when this RestHttpOperationInvoker is unable to resolve the correct REST API
  // web service endpoint (URI) for a command
  private final HttpOperationInvoker httpOperationInvoker;

  // the LinkIndex containing Links to all GemFire REST API web service endpoints
  private final LinkIndex linkIndex;

  /**
   * Constructs an instance of the RestHttpOperationInvoker class initialized with the given link index containing links
   * referencing all REST API web service endpoints.  This constructor should only be used for testing purposes.
   * <p/>
   * @param linkIndex the LinkIndex containing Links to all REST API web service endpoints in GemFire's REST interface.
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   */
  RestHttpOperationInvoker(final LinkIndex linkIndex) {
    super(REST_API_URL);
    assert linkIndex != null : "The Link Index resolving commands to REST API web service endpoints cannot be null!";
    this.linkIndex = linkIndex;
    this.httpOperationInvoker = new SimpleHttpOperationInvoker();
  }

  /**
   * Constructs an instance of the RestHttpOperationInvoker class initialized with the given link index containing links
   * referencing all REST API web service endpoints.  In addition, a reference to the instance of GemFire shell (Gfsh)
   * using this RestHttpOperationInvoker to send command invocations to the GemFire Manager's HTTP service via HTTP
   * for processing is required in order to interact with the shell and provide feedback to the user.
   * <p/>
   * @param linkIndex the LinkIndex containing Links to all REST API web service endpoints in GemFire' REST interface.
   * @param gfsh a reference to the instance of the GemFire shell using this OperationInvoker to process commands.
   * @see #RestHttpOperationInvoker(com.gemstone.gemfire.management.internal.web.domain.LinkIndex, com.gemstone.gemfire.management.internal.cli.shell.Gfsh, String)
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  public RestHttpOperationInvoker(final LinkIndex linkIndex, final Gfsh gfsh) {
    this(linkIndex, gfsh, CliStrings.CONNECT__DEFAULT_BASE_URL);
  }

  /**
   * Constructs an instance of the RestHttpOperationInvoker class initialized with the given link index containing links
   * referencing all REST API web service endpoints.  In addition, a reference to the instance of GemFire shell (Gfsh)
   * using this RestHttpOperationInvoker to send command invocations to the GemFire Manager's HTTP service via HTTP
   * for processing is required in order to interact with the shell and provide feedback to the user.  Finally, a URL
   * to the HTTP service running in the GemFire Manager is specified as the base location for all HTTP requests.
   * <p/>
   * @param linkIndex the LinkIndex containing Links to all REST API web service endpoints in GemFire's REST interface.
   * @param gfsh a reference to the instance of the GemFire shell using this OperationInvoker to process commands.
   * @param baseUrl the String specifying the base URL to the GemFire Manager's HTTP service, REST interface.
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  public RestHttpOperationInvoker(final LinkIndex linkIndex, final Gfsh gfsh, final String baseUrl) {
    super(gfsh, baseUrl);
    assert linkIndex != null : "The Link Index resolving commands to REST API web service endpoints cannot be null!";
    this.linkIndex = linkIndex;
    this.httpOperationInvoker = new SimpleHttpOperationInvoker(gfsh, baseUrl);
  }

  /**
   * Returns a reference to an implementation of HttpOperationInvoker used as the fallback by this
   * RestHttpOperationInvoker for processing commands via HTTP requests.
   * <p/>
   * @return an instance of HttpOperationInvoker used by this RestHttpOperationInvoker as a fallback to process commands
   * via HTTP requests.
   * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker
   */
  protected HttpOperationInvoker getHttpOperationInvoker() {
    return httpOperationInvoker;
  }

  /**
   * Returns the LinkIndex resolving Gfsh commands to GemFire REST API web service endpoints.  The corresponding
   * web service endpoint is a URI/URL uniquely identifying the resource on which the command was invoked.
   * <p/>
   * @return the LinkIndex containing Links for all GemFire REST API web service endpoints.
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   */
  protected LinkIndex getLinkIndex() {
    return linkIndex;
  }

  /**
   * Creates an HTTP request from the specified command invocation encapsulated by the CommandRequest object.
   * The CommandRequest identifies the resource targeted by the command invocation along with any parameters to be sent
   * as part of the HTTP request.
   * <p/>
   * @param command the CommandRequest object encapsulating details of the command invocation.
   * @return a client HTTP request detailing the operation to be performed on the remote resource targeted by the
   * command invocation.
   * @see AbstractHttpOperationInvoker#createHttpRequest(com.gemstone.gemfire.management.internal.web.domain.Link)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest
   * @see com.gemstone.gemfire.management.internal.web.util.ConvertUtils#convert(byte[][])
   */
  protected ClientHttpRequest createHttpRequest(final CommandRequest command) {
    final ClientHttpRequest request = createHttpRequest(findLink(command));

    //request.getParameters().setAll(new HashMap<String, Object>(CollectionUtils.removeKeys(
    //  new HashMap<String, String>(command.getParameters()), ExcludeNoValueFilter.INSTANCE)));

    final Map<String, String> commandParameters = command.getParameters();

    for (final Map.Entry<String, String> entry : commandParameters.entrySet()) {
      if (ExcludeNoValueFilter.INSTANCE.accept(entry)) {
        request.addRequestParameterValues(entry.getKey(), entry.getValue());
      }
    }

    if (command.getFileData() != null) {
      request.addRequestParameterValues(RESOURCES_REQUEST_PARAMETER, (Object[]) ConvertUtils.convert(command.getFileData()));
    }

    return request;
  }

  /**
   * Finds a Link from the Link Index containing the HTTP request URI to the web service endpoint for the relative
   * operation on the resource.
   * <p/>
   * @param relation a String describing the relative operation (state transition) on the resource.
   * @return an instance of Link containing the HTTP request URI used to perform the intended operation on the resource.
   * @see #getLinkIndex()
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex#find(String)
   */
  @Override
  protected Link findLink(final String relation) {
    return getLinkIndex().find(relation);
  }

  /**
   * Finds a Link from the Link Index corresponding to the command invocation.  The CommandRequest indicates the
   * intended function on the target resource so the proper Link based on it's relation (the state transition of the
   * corresponding function), along with it's method of operation and corresponding REST API web service endpoint (URI),
   * can be identified.
   * <p/>
   * @param command the CommandRequest object encapsulating the details of the command invocation.
   * @return a Link referencing the correct REST API web service endpoint (URI) and method for the command invocation.
   * @see #getLinkIndex()
   * @see #resolveLink(com.gemstone.gemfire.management.internal.cli.CommandRequest, java.util.List)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   */
  protected Link findLink(final CommandRequest command) {
    final List<Link> linksFound = new ArrayList<Link>(getLinkIndex().size());

    for (final Link link : getLinkIndex()) {
      if (command.getInput().startsWith(link.getRelation())) {
        linksFound.add(link);
      }
    }

    if (linksFound.isEmpty()) {
      throw new RestApiCallForCommandNotFoundException(String.format("No REST API call for command (%1$s) was found!",
        command.getInput()));
    }

    return (linksFound.size() > 1 ? resolveLink(command, linksFound) : linksFound.get(0));
  }

  /**
   * Resolves one Link from a Collection of Links based on the command invocation matching multiple relations from
   * the Link Index.
   * <p/>
   * @param command the CommandRequest object encapsulating details of the command invocation.
   * @param links a Collection of Links for the command matching the relation.
   * @return the resolved Link matching the command exactly as entered by the user.
   * @see #findLink(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see org.springframework.web.util.UriTemplate
   */
  // Find and use the Link with the greatest number of path variables that can be expanded!
  protected Link resolveLink(final CommandRequest command, final List<Link> links) {
    // NOTE, Gfsh's ParseResult contains a Map entry for all command options whether or not the user set the option
    // with a value on the command-line, argh!
    final Map<String, String> commandParametersCopy = CollectionUtils.removeKeys(
      new HashMap<String, String>(command.getParameters()), ExcludeNoValueFilter.INSTANCE);

    Link resolvedLink = null;

    int pathVariableCount = 0;

    for (final Link link : links) {
      final List<String> pathVariables = new UriTemplate(decode(link.getHref().toString())).getVariableNames();

      // first, all path variables in the URL/URI template must be resolvable/expandable for this Link
      // to even be considered...
      if (commandParametersCopy.keySet().containsAll(pathVariables)) {
        // then, either we have not found a Link for the command yet, or the number of resolvable/expandable
        // path variables in this Link has to be greater than the number of resolvable/expandable path variables
        // for the last Link
        if (resolvedLink == null || (pathVariables.size() > pathVariableCount)) {
          resolvedLink = link;
          pathVariableCount = pathVariables.size();
        }
      }
    }

    if (resolvedLink == null) {
      throw new RestApiCallForCommandNotFoundException(String.format("No REST API call for command (%1$s) was found!",
        command.getInput()));
    }

    return resolvedLink;
  }

  /**
   * Processes the requested command.  Sends the command to the GemFire Manager for remote processing (execution).
   * <p/>
   * @param command the command requested/entered by the user to be processed.
   * @return the result of the command execution.
   * @see #createHttpRequest(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see #handleResourceAccessException(org.springframework.web.client.ResourceAccessException)
   * @see #isConnected()
   * @see #send(com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest, Class, java.util.Map)
   * @see #simpleProcessCommand(com.gemstone.gemfire.management.internal.cli.CommandRequest, RestApiCallForCommandNotFoundException)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see org.springframework.http.ResponseEntity
   */
  @Override
  public String processCommand(final CommandRequest command) {
    assert isConnected() : "Gfsh must be connected to the GemFire Manager in order to process commands remotely!";

    try {
      final ResponseEntity<String> response = send(createHttpRequest(command), String.class, command.getParameters());

      return response.getBody();
    }
    catch (RestApiCallForCommandNotFoundException e) {
      return simpleProcessCommand(command, e);
    }
    catch (ResourceAccessException e) {
      return handleResourceAccessException(e);
    }
  }

  /**
   * A method to process the command by sending an HTTP request to the simple URL/URI web service endpoint, where all
   * details of the request and command invocation are encoded in the URL/URI.
   * <p/>
   * @param command the CommandRequest encapsulating the details of the command invocation.
   * @param e the RestApiCallForCommandNotFoundException indicating the standard REST API web service endpoint
   * could not be found.
   * @return the result of the command execution.
   * @see #getHttpOperationInvoker()
   * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker#processCommand(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   */
  protected String simpleProcessCommand(final CommandRequest command, final RestApiCallForCommandNotFoundException e) {
    if (getHttpOperationInvoker() != null) {
      printWarning("WARNING - No REST API web service endpoint (URI) exists for command (%1$s); using the non-RESTful, simple URI.",
        command.getName());

      return String.valueOf(getHttpOperationInvoker().processCommand(command));
    }

    throw e;
  }

  protected static final class ExcludeNoValueFilter implements Filter<Map.Entry<String, String>> {

    protected static final ExcludeNoValueFilter INSTANCE = new ExcludeNoValueFilter();

    @Override
    public boolean accept(final Map.Entry<String, String> entry) {
      return !StringUtils.isBlank(entry.getValue());
    }
  }

}
