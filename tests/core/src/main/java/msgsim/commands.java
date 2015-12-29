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
package msgsim;

public interface commands {

  public static final int BASE_PORT = 15963; // servers use ports 15963...

  // these are messaging commands - they are read and executed by "reader" threads
  public static enum MessageProtocolCommand {
    INVALID((byte) -1),
    INIT((byte) 0),
    USE_THREAD_OWNED((byte) 1),
    USE_SHARED((byte) 2),
    HAND_OFF_LISTENERS((byte) 3),
    USE_THREAD_OWNED_UNORDERED((byte) 4),
    USE_POOLED_CONNECTIONS((byte) 5),
    NORMAL_MESSAGE((byte) 10),
    SET_PAYLOAD_SIZE((byte) 11),
    ACK_MESSAGE((byte) 12),
    PRINT_QUEUE_SIZES((byte) 13),
    EXIT((byte) 127);

    private final byte byteValue;

    MessageProtocolCommand(final byte byteValue) {
      this.byteValue = byteValue;
    }

    public static MessageProtocolCommand valueOf(final byte byteValue) {
      for (final MessageProtocolCommand command : values()) {
        if (command.toByte() == byteValue) {
          return command;
        }
      }

      return INVALID;
    }

    public boolean isInvalid() {
      return this.equals(INVALID);
    }

    public byte toByte() {
      return byteValue;
    }
  }

  // These are scenarios that go with the "scenario" command line argument.  The default is SERVER.
  public static enum MessagingScenario {
    UNKNOWN(-1, "Unknown"),
    SERVER(0, "Server"),
    SHARED_CONNECTIONS(1, "conserve-sockets=true"),
    SHARED_CONNECTIONS_WITH_DOMINO(2, "conserve-sockets=true, readers get owned ack sockets (conserve-sockets=false"),
    THREAD_OWNED_CONNECTIONS(3, "conserve-sockets=false"),
    THREAD_OWNED_CONNECTIONS_WITH_DOMINO(4, "conserve-sockets=false, readers get conserve-sockets=false"),
    POOLED_CONNECTIONS(5, "queued connection pool"),
    POOLED_CONNECTIONS_WITH_DOMINO(6, "queued connection pool with domino messages"),
    HANDOFF_LISTENERS(7, "conserve-sockets=true, listener-handoff"),
    HANDOFF_LISTENERS_WITH_DOMINO(8, "conserve-sockets=true, readers get owned ack sockets (conserve-sockets=false"),
    EXIT(999, "Shutdown");

    private final int id;
    private final String description;

    MessagingScenario(final int id, final String description) {
      this.id = id;
      this.description = description;
    }

    public static MessagingScenario getDefault() {
      return SERVER;
    }

    public static MessagingScenario valueOf(final int id) {
      for (final MessagingScenario scenario : values()) {
        if (scenario.getId() == id) {
          return scenario;
        }
      }

      return UNKNOWN;
    }

    public String getDescription() {
      return description;
    }

    public int getId() {
      return id;
    }

    public boolean isExit() {
      return this.equals(EXIT);
    }

    public boolean isServer() {
      return this.equals(SERVER);
    }

    @Override
    public String toString() {
      return getDescription();
    }
  }

}

