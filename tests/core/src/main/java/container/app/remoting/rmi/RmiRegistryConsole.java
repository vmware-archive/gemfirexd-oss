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
package container.app.remoting.rmi;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import container.app.remoting.rmi.service.Pingable;
import container.app.util.ArrayUtils;
import container.app.util.Assert;
import container.app.util.StringUtils;
import container.app.util.SystemUtils;

public class RmiRegistryConsole {
  
  private static final StringBuilder menu = new StringBuilder("\nRMI Registry Console - Please enter one of the following commands:\n");
  
  static {
    menu.append("1. list - lists all available remote object services in the registry.\n");
    menu.append("2. ping <service-name> - determines if the specified remote object service is responding to remote calls.\n");
    menu.append("3. mirror <service-name> <argument> - reverses the characters in the specified argument.\n");
    menu.append("4. echo <service-name> <argument> - echos the given argument.\n");
    menu.append("5. help - print the menu.\n");
    menu.append("6. exit - exits from the RMI Registry Console.\n\n");
  }

  private static void locateRegistry(final ConsoleContext context) throws RemoteException {
    context.setRegistry(LocateRegistry.getRegistry(context.getHost(), context.getPort()));
  }

  private static ConsoleContext parseCommandLineArguments(final String... args) {
    String host = null;
    int port = -1;

    for (final String arg : args) {
      if ("-help".equalsIgnoreCase(arg.trim())) {
        SystemUtils.printToStandardOut("> java container.app.remoting.rmi.RmiRegistryConsole [host] [port]");
        System.exit(0);
      }
      else {
        try {
          port = Integer.parseInt(arg);
        } 
        catch (NumberFormatException e) {
          host = arg;
        }
      }
    }

    return new ConsoleContext(host, port);
  }

  private static String promptForUserInput(final Scanner in) {
    System.out.print("> ");
    System.out.flush();
    return in.nextLine();
  }

  /**
   * @param args
   */
  public static void main(final String[] args) throws Exception {
    final ConsoleContext context = parseCommandLineArguments(args);
    final Scanner in = new Scanner(System.in);
    String userInput = null;

    locateRegistry(context);
    System.out.println(menu.toString());

    do {
      userInput = promptForUserInput(in);
      AbstractCommand.parseCommandLine(userInput).execute(context);
    }
    while (true);
  }

  protected static class ConsoleContext {

    protected static final String DEFAULT_HOST = "localhost";
    protected static final int DEFAULT_PORT = 1099;

    private final int port;

    private Registry registry;

    private final String host;

    public ConsoleContext() {
      this(DEFAULT_HOST, DEFAULT_PORT);
    }
    
    public ConsoleContext(final String host, final int port) {
      this.host = StringUtils.defaultIfNoValue(host, DEFAULT_HOST);
      this.port = (port > 0 ? port : DEFAULT_PORT);
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public Registry getRegistry() {
      Assert.state(registry != null, "The RMI Registry service was not properly located!");
      return registry;
    }

    public void setRegistry(final Registry registry) {
      Assert.notNull(registry, "The reference to the RMI Registry cannot be null!");
      this.registry = registry;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getName());
      buffer.append("{host = ").append(getHost());
      buffer.append(", port = ").append(getPort());
      buffer.append("}");
      return buffer.toString();
    }
  }

  protected static enum CommandKeyword {
    ECHO,
    EXIT,
    HELP,
    LIST,
    MIRROR,
    PING,
    UNKNOWN;
    
    public static CommandKeyword getByName(final String commandName) {
      for (final CommandKeyword cmd : values()) {
        if (cmd.name().equalsIgnoreCase(StringUtils.trim(commandName))) {
          return cmd;
        }
      }

      return UNKNOWN;
    }
    
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  protected static interface Command {

    public void execute(ConsoleContext context);

    public void parseArguments(String[] arguments);

  }

  protected static abstract class AbstractCommand implements Command {

    private static final Map<CommandKeyword, Command> COMMAND_MAP = new HashMap<CommandKeyword, Command>();

    static {
      COMMAND_MAP.put(CommandKeyword.ECHO, new EchoCommand());
      COMMAND_MAP.put(CommandKeyword.EXIT, new ExitCommand());
      COMMAND_MAP.put(CommandKeyword.HELP, new HelpCommand());
      COMMAND_MAP.put(CommandKeyword.LIST, new ListCommand());
      COMMAND_MAP.put(CommandKeyword.MIRROR, new MirrorCommand());
      COMMAND_MAP.put(CommandKeyword.PING, new PingCommand());
      COMMAND_MAP.put(CommandKeyword.UNKNOWN, new UnknownCommand());
    }

    protected abstract CommandKeyword getKeyword();

    public void execute(final ConsoleContext context) {
      try {
        executeImpl(context);
      }
      catch (Exception e) {
        SystemUtils.printToStandardError("The execution of command ({0}) failed with the following error: {1}", 
            this, e.getMessage());
      }
    }

    protected abstract void executeImpl(ConsoleContext context) throws Exception;

    public static Command lookupCommand(final CommandKeyword cmdKey) {
      return COMMAND_MAP.get(cmdKey);
    }

    public static Command parseCommandLine(final String commandLine) {
      final Scanner scanner = new Scanner(commandLine);

      if (scanner.hasNext()) {
        final String commandName = scanner.next();
        
        final StringBuilder commandArguments = new StringBuilder(" ");
        
        while (scanner.hasNext()) {
          commandArguments.append(scanner.next());
          commandArguments.append(" ");
        }

        final Command command = lookupCommand(CommandKeyword.getByName(commandName));
        command.parseArguments(StringUtils.split(commandArguments.toString().trim()));

        return command;
      }

      return lookupCommand(CommandKeyword.UNKNOWN); 
    }

    @Override
    public String toString() {
      return getKeyword().toString();
    }
  }

  protected static abstract class AbstractRegistryCommand extends AbstractCommand {

    public void parseArguments(final String[] arguments) {
    }
  }

  protected static abstract class AbstractServiceCallCommand extends AbstractCommand {

    private String message;
    private String serviceName;

    protected String getMessage() {
      return message;
    }

    protected Pingable getPingableService(final ConsoleContext context) throws NotBoundException, RemoteException {
      final Registry registry = context.getRegistry();

      final Remote remoteObject = registry.lookup(getServiceName());
      Assert.notNull(remoteObject, "The remote object was null!");
      Assert.instanceOf(remoteObject, Pingable.class, "The remote object must be an instance of Pingable to invoke this command!");
      
      return (Pingable) remoteObject;

    }

    protected String getServiceName() {
      return serviceName;
    }

    public void parseArguments(final String[] arguments) {
      this.serviceName = ArrayUtils.elementAt(arguments, 0, null);
      Assert.notNull(this.serviceName, "The service name cannot be null!");
      this.message = ArrayUtils.elementAt(arguments, 1, "null");
    }
  }

  protected static class EchoCommand extends AbstractServiceCallCommand {

    @Override
    protected CommandKeyword getKeyword() {
      return CommandKeyword.ECHO;
    }

    @Override
    protected void executeImpl(final ConsoleContext context) throws Exception {
      SystemUtils.printToStandardOut(getPingableService(context).echo(getMessage()));
    }
  }

  protected static class ExitCommand extends AbstractRegistryCommand {

    @Override
    protected CommandKeyword getKeyword() {
      return CommandKeyword.EXIT;
    }

    @Override
    protected void executeImpl(final ConsoleContext context) throws Exception {
      SystemUtils.printToStandardOut("Exiting the RMI Registry Console...");
      System.exit(0);
    }
  }

  protected static class HelpCommand extends AbstractRegistryCommand {

    @Override
    protected CommandKeyword getKeyword() {
      return CommandKeyword.HELP;
    }

    @Override
    protected void executeImpl(final ConsoleContext context) throws Exception {
      SystemUtils.printToStandardOut(menu.toString());
    }
  }

  protected static class ListCommand extends AbstractRegistryCommand {

    @Override
    protected CommandKeyword getKeyword() {
      return CommandKeyword.LIST;
    }

    @Override
    public void executeImpl(final ConsoleContext context) throws Exception {
      SystemUtils.printToStandardOut("Available services for host ({0}) listening on port ({1}): {2}", 
          context.getHost(), context.getPort(), Arrays.asList(context.getRegistry().list()));
    }
  }

  protected static class MirrorCommand extends AbstractServiceCallCommand {

    @Override
    protected CommandKeyword getKeyword() {
      return CommandKeyword.MIRROR;
    }

    @Override
    protected void executeImpl(final ConsoleContext context) throws Exception {
      SystemUtils.printToStandardOut(getPingableService(context).mirror(getMessage()));
    }
  }

  protected static class PingCommand extends AbstractServiceCallCommand {

    @Override
    protected CommandKeyword getKeyword() {
      return CommandKeyword.PING;
    }

    @Override
    protected void executeImpl(final ConsoleContext context) throws Exception {
      try {
        Assert.state(getPingableService(context).isAlive(), "");
        SystemUtils.printToStandardOut("{0} is alive!", getServiceName());
      }
      catch (Exception e) {
        SystemUtils.printToStandardOut("{0} is unavailable!", getServiceName());
        e.printStackTrace(System.err);
        System.err.flush();
        Throwable t = e.getCause();
        while (t != null) {
          System.err.print("Caused by: ");
          t.printStackTrace(System.err);
          System.err.flush();
          t = t.getCause();
        }
      }
    }
  }

  protected static final class UnknownCommand extends AbstractRegistryCommand {

    @Override
    protected CommandKeyword getKeyword() {
      return CommandKeyword.UNKNOWN;
    }

    @Override
    public void executeImpl(final ConsoleContext context) {
    }
  }

}
