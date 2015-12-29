These tests exercise various kinds of mirroring along with events.
This describes what each test does.


Test                             Scope               Mirroring   Description
-------------------------------  ------------------- ----------- ----------------------------------------------
serialMirrorKeysDist.conf        any distributed     keys        Serial execution.
serialMirrorKeysValuesDist.conf  any distributed     keysValues  Serial execution.
mirrorKeysDist.conf              any distributed     keys        Concurrent execution.
mirrorKeysValuesDist.conf        any distributed     keysValues  Concurrent execution.
mirrorNoneDist.conf              any distributed     none        Concurrent execution.
mirrorNoneLocal.conf             local               none        Concurrent execution.
mirrorKeysLocal.conf             local               keys        Concurrent execution.
mirrorKeysValuesLocal.conf       local               keysValues  Concurrent execution.



This chart shows the behavior of adding new keys/objects using the "put" method on a
region. The tests attempt to verify the behavior of mirroring and events using this 
chart as an example of the expected behavior.

Across the top, the chart shows 2 connected Gemfire systems, each having 2 VMs attached 
to them. There are > 1 threads in each VM. VM1 connected to the first Gemfire
system has a thread which adds new keys and objects into a region.

All VMs have a VM root region defined. VM1 is the only VM to do puts of new keys. 
The other VMs receive keys and/or values based on the type of mirroring begin tested. VM1's 
mirroring attribute can be any of KEYS, KEYS_VALUES, or NONE (since it is putting, not receiving). 
The side of the chart shows the scope of all VMs, and the mirroring attributes of VM2, VM3 and VM4.

The chart assumes that each VM has an object listener installed in the root region.

The interior of the chart shows whether any thread in the VM can "see" the keys or "see"
the values which were added by VM1. It also shows which (if any) event should occur from
adding the key. If the chart shows "-->", it means that the same thing repeats across
the chart for the remaining VMs. So, "keys" in the interior of the chart means that a
thread in this VM can "see" the keys, likewise for objects.

Persistent backup is used to verify that end task VMs have correct mirroring.


                           Gemfire                       Gemfire                 
                           System 1                      System 2                
                              |                             |                       
                      +-------+-------+              +------+------+         
                      |               |              |             |         
               |     VM1      |      VM2     |      VM3            VM4      
---------------+--------------+--------------+--------------+--------------+
Local scope    |              |              |              |              |
               |              |              |              |              |
Mirror attr    | keys         |  none-->     |              |              |
None for VM2-4 | values       |  none-->     |              |              |
               | add event    |  none-->     |              |              |
               |              |              |              |              |
               | END TASK create region:     |              |              |
               | no keys-->   |              |              |              |
               | no values--> |              |              |              |
               | no events--> |              |              |              |
               | END TASK get:|              |              |              |
               | Cannot obtain with get-->   |              |              |
        -------+--------------+--------------+--------------+--------------+
               |                                                           |
Mirror attr    | N/A Cannot create a mirrored region with local scope-->   |
Keys for VM2-4 |                                                           |
               |                                                           |   
        -------+--------------+--------------+--------------+--------------+
               |                                                           |
Mirror attr    | N/A Cannot create a mirrored region with local scope-->   |
KeysValues     |                                                           |
for VM2-4      |                                                           |
---------------+--------------+--------------+--------------+--------------+
Any dist scope |              |              |              |              |
               |              |              |              |              |
Mirror attr    | keys         |  none-->     |              |              |
None for VM2-4 | values       |  none-->     |              |              |
               | add event    |  none-->     |              |              |
               |              |              |              |              |
               | END TASK create region:     |              |              |
               | no keys-->   |              |              |              |
               | no valuse--> |              |              |              |
               | no events--> |              |              |              |
               | END TASK get:|              |              |              |
               | add event--> |              |              |              |
        -------+--------------+--------------+--------------+--------------+
               |              |              |              |              |
Mirror attr    | keys-->      |              |              |              |
Keys for VM2-4 | values       |  maybe-->    |              |              |
               | add event--> |              |              |              |
               |              |              |              |              |
               | END TASK create region:     |              |              |
               | keys-->      |              |              |              |
               | values       | maybe-->     |              |              |
               | no events--> |              |              |              |
               | END TASK get:|              |              |              |
               | no events    | add event--> |              |              |
        -------+--------------+--------------+--------------+--------------+
               |              |              |              |              |
Mirror attr    | keys-->      |              |              |              |
KeysValues     | values -->   |              |              |              |
for VM2-4      | add event--> |              |              |              |
               |              |              |              |              |
               | END TASK create region:     |              |              |
               | keys-->      |              |              |              |
               | values -->   |              |              |              |
               | no events--> |              |              |              |
               | END TASK get:|              |              |              |
               | no events--> |              |              |              |
        -------+--------------+--------------+--------------+--------------+
