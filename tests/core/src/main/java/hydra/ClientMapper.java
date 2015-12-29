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

package hydra;

import com.gemstone.gemfire.LogWriter;

import java.util.*;

/**
 *
 * A ClientMapper knows how to map client threads to thread groups.
 *
 */
public class ClientMapper {

  /**
   * Creates a thread mapper for mapping client records to thread groups.
   * @param threadGroups The thread groups clients will map to.  Maps the name
   *                     of a thread group to its {@link HydraThreadGroup}
   *                     object.
   */
  public ClientMapper( Map threadGroups ) {
    this.threadGroups = threadGroups;
    this.threadSubgroups = subgroupsFromGroups( threadGroups );
  }

  /**
   *  Maps the client records to the thread groups for this mapper and sets
   *  each client's thread group name to its mapped thread group.
   *  @param clientRecords the client records to map.
   *  @throws HydraRuntimeException if it is impossible to map the clients.
   *  @throws IllegalArgumentException if clientRecords is null or empty.
   */
  public void mapClients( Map cvmrs ) {
    if ( log().finestEnabled() ) {
      log().finest( "Mapping clients: " + cvmrs );
      log().finest( "Using groups: " + this.threadGroups );
    }
    mapClientsToGroups( cvmrs );
    assignThreadGroupIds( cvmrs );
  }

  /**
   *  Assigns logical thread group ids to clients, numbering from 0 to N-1,
   *  where N is the number of threads in the thread group.  Also assigns the
   *  thread group name as a non-transient field of the client record.
   */
  private void assignThreadGroupIds( Map cvmrs ) {
    HashMap map = new HashMap();
    for ( Iterator i = cvmrs.values().iterator(); i.hasNext(); ) {
      ClientVmRecord cvmr = (ClientVmRecord) i.next();
      for ( Iterator j = cvmr.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord cr = (ClientRecord) j.next();
        String tgname = cr.getThreadGroup().getName();
        Integer index = (Integer) map.get( tgname );
        if ( index == null ) {
          index = new Integer(0);
          map.put( tgname, index );
        }
        int tgid = index.intValue();
        cr.setThreadGroupName( tgname );
        cr.setThreadGroupId( tgid );
        map.put( tgname, new Integer( tgid + 1 ) );
      }
    }
  }

  /**
   *  Prints the client mapping.
   */
  public String getMappingAsString( Map cvmrs ) {
    StringBuffer buf = new StringBuffer();
    buf.append( "\nCLIENT MAPPING:" );
    for ( Iterator i = cvmrs.values().iterator(); i.hasNext(); ) {
      ClientVmRecord cvmr = (ClientVmRecord) i.next();
      for ( Iterator j = cvmr.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord cr = (ClientRecord) j.next();
        buf.append( "\n    " + cr.getThreadGroupName() + "\t" + cr );
      }
    }
    return buf.toString();
  }

  /**
   * Returns a list of lists containing the client mapping.
   */
  public List<List<String>> getMappingAsList(Map cvmrs) {
    List<List<String>> list = new ArrayList();
    for (Iterator i = cvmrs.values().iterator(); i.hasNext();) {
      ClientVmRecord cvmr = (ClientVmRecord)i.next();
      for (ClientRecord cr : cvmr.getClients().values()) {
        List sublist = new ArrayList();
        sublist.add(cr.getThreadGroupName());
        sublist.add(cr.toString());
        list.add(sublist);
      }
    }
    return list;
  }

  /** 
   *  Maps clients to groups, trying all permutations.
   */
  private void mapClientsToGroups( Map cvmrs ) {
    // start with an order that has a reasonable chance of success
    Vector subgroups = sortSubgroups( this.threadSubgroups );
    boolean containsDefault = containsDefault( subgroups );

    PermutationGenerator pgen = null;
    if ( containsDefault ) {
      if ( subgroups.size() == 1 )
        pgen = new PermutationGenerator( subgroups.size() );
      else
        pgen = new PermutationGenerator( subgroups.size() - 1 );
    } else {
      pgen = new PermutationGenerator( subgroups.size() );
    }
    int[] indices;
    Vector permutation;
    while ( pgen.hasMore() ) {

      // sort the clients into buckets by vmid
      HashMap namemap = sortClientsByNameAndVmid( cvmrs );
      if ( log().finestEnabled() ) {
        log().finest( "Using buckets:\n" + nameMapToString(namemap) );
      }

      permutation = new Vector();
      indices = pgen.getNext();
      for ( int i = 0; i < indices.length; i++ )
        permutation.add( subgroups.elementAt( indices[i] ) );
      if ( containsDefault && subgroups.size() > 1 )
        permutation.add( subgroups.elementAt( subgroups.size() - 1 ) );

      if ( log().finestEnabled() ) {
        log().finest( "Trying subgroup permutation: " + permutation );
      }
      try {
        mapClientsToGroups( namemap, permutation );
        return; // success
      } catch( HydraConfigException e ) {
        if ( log().fineEnabled() ) {
          log().fine( "Unable to map clients:\n" + cvmrs +
                      "\nto thread groups:\n" + permutation +
                      "\nbecause: " + e.getMessage() );
        }
        // toss out unproductive work
        for ( Iterator i = cvmrs.values().iterator(); i.hasNext(); ) {
          ClientVmRecord cvmr = (ClientVmRecord) i.next();
          for ( Iterator j = cvmr.getClients().values().iterator(); j.hasNext(); ) {
            ( (ClientRecord) j.next() ).setThreadGroup( null );
          }
        }
      }
    }
    throw new HydraConfigException( "Failed to map clients:\n" + cvmrs +
                                    "\nto thread groups:\n" + this.threadGroups );
  }

  /**
   *  Returns whether or not the given list of sorted
   *  {@link HydraThreadSubgroup}s contains the
   *  {@linkplain HydraThreadGroup#DEFAULT_NAME default} group.
   */
  private boolean containsDefault( Vector subgroups ) {
    for ( int i = 0; i < subgroups.size(); i++ ) {
      HydraThreadSubgroup subgroup = (HydraThreadSubgroup) subgroups.elementAt(i);
      if ( subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        // make sure it's the last one in the list
        if ( i != subgroups.size() - 1 )
          throw new HydraInternalException( "Default thread subgroup is present but not last in list: " + subgroups );
        return true;
      }
    }
    return false;
  }

  /** 
   *  Sorts the subgroups in the order they should be mapped.
   */
  private Vector sortSubgroups( Vector subgroups ) {
    // note that there should be only one default subgroup
    Vector sorted = new Vector();

    // first add groups any that specify client names and num vms
    // (except default subgroup)
    for ( Iterator i = subgroups.iterator(); i.hasNext(); ) {
      HydraThreadSubgroup subgroup = (HydraThreadSubgroup) i.next();
      if ( subgroup.getClientNames() != null
           && subgroup.getTotalVMs() > 0
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        sorted.add( subgroup );
      }
    }

    // next add any that specify client names but not num vms (except
    // default subgroup)
    for ( Iterator i = subgroups.iterator(); i.hasNext(); ) {
      HydraThreadSubgroup subgroup = (HydraThreadSubgroup) i.next();
      if ( subgroup.getClientNames() != null
           && subgroup.getTotalVMs() == 0
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        sorted.add( subgroup );
      }
    }

    // next add any that specify num vms but not client names (except
    // default subgroup)
    for ( Iterator i = subgroups.iterator(); i.hasNext(); ) {
      HydraThreadSubgroup subgroup = (HydraThreadSubgroup) i.next();
      if ( subgroup.getClientNames() == null
           && subgroup.getTotalVMs() > 0
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        sorted.add( subgroup );
      }
    }

    // next add any that specify num threads (except default subgroup)
    for ( Iterator i = subgroups.iterator(); i.hasNext(); ) {
      HydraThreadSubgroup subgroup = (HydraThreadSubgroup) i.next();
      if ( subgroup.getClientNames() == null
           && subgroup.getTotalVMs() == 0
           && subgroup.getTotalThreads() > 0
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        sorted.add( subgroup );
      }
    }

    // next add the default subgroup, if needed
    HydraThreadGroup defaultGroup =
        (HydraThreadGroup) this.threadGroups.get( HydraThreadGroup.DEFAULT_NAME );
    if ( defaultGroup != null ) {
      HydraThreadSubgroup defaultSubgroup = defaultGroup.getSubgroup(0);
      sorted.add( defaultSubgroup );
    }

    if ( sorted.size() != subgroups.size() )
      throw new HydraInternalException( "Failed to sort all subgroups:\nsorted --> " + sorted + "\nsubgroups --> " + subgroups );
    if ( log().fineEnabled() ) {
      log().fine( "\nINITIAL SORT FOR SUBGROUPS:\n" + subgroups + "\n\n" );
    }
    return sorted;
  }

  /** 
   *  Maps clients to a particular ordering of subgroups
   */
  private void mapClientsToGroups( HashMap namemap, Vector subgroups ) {

    if ( log().finestEnabled() ) {
      log().finest( "Mapping clients to subgroups permutation: " + subgroups );
    }

    int mapped = 0;

    for ( Iterator i = subgroups.iterator(); i.hasNext(); ) {
      HydraThreadSubgroup subgroup = (HydraThreadSubgroup) i.next();

      // make sure there are enough clients for the subgroup
      int remaining = numRemainingClients( namemap, subgroup.getClientNames() );
      int needed = subgroup.getTotalThreads();
      if ( remaining < needed ) {
        throw new HydraInternalException
        (
          "Not enough clients (" + remaining + ") to map to: " + subgroup
        );
      }

      // map clients to the subgroup
      if ( log().finestEnabled() ) {
        log().finest( "Mapping clients to subgroup: " + subgroup );
      }
      if ( subgroup.getClientNames() != null
           && subgroup.getTotalVMs() > 0 
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        mapClientsToSubgroupNamedExact( namemap, subgroup );

      } else if ( subgroup.getClientNames() != null
           && subgroup.getTotalVMs() == 0 
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        mapClientsToSubgroupNamed( namemap, subgroup );

      } else if ( subgroup.getTotalVMs() > 0 
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        mapClientsToSubgroupExact( namemap, subgroup );

      } else if ( subgroup.getTotalVMs() == 0 && subgroup.getTotalThreads() > 0
           && ! subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        mapClientsToSubgroupRandom( namemap, subgroup );

      } else if ( subgroup.getName().equals( HydraThreadGroup.DEFAULT_NAME ) ) {
        mapClientsToSubgroupRemaining( namemap, subgroup );

      } else {
        throw new HydraInternalException( "Unexpected case: " + subgroup );
      }
//      log().info( "namemap = " + namemap );
      ++mapped;
    }

    // make sure the mapping is complete
    if ( mapped != subgroups.size() )
      throw new HydraInternalException( "Client mapping skipped one or more subgroups" );
    if ( numRemainingClients( namemap ) != 0 )
      throw new HydraInternalException( "Client mapping skipped " + numRemainingClients( namemap ) + " clients: " + nameMapToString(namemap) );
  }

  /** 
   *  Maps clients as evenly as possible across the specified number of vms with
   *  the specified client names (does not spread evenly across client names).
   */
  private void mapClientsToSubgroupNamedExact( HashMap namemap,
                                               HydraThreadSubgroup subgroup ) {
    int totalThreads   = subgroup.getTotalThreads();
    int totalVMs       = subgroup.getTotalVMs();
    Vector clientNames = subgroup.getClientNames();

    int lowNumThreads  = totalThreads / totalVMs;
    int highNumThreads = lowNumThreads + 1;
    int numHighVMs     = totalThreads % totalVMs;
    int numLowVMs      = totalVMs - numHighVMs;

    // @todo lises spread vms evenly across names

    int mapped = 0;
    for ( Iterator it = subgroup.getClientNames().iterator(); it.hasNext(); ) {
      String clientName = (String) it.next();
      HashMap vmidmap = (HashMap) namemap.get( clientName );
      if ( vmidmap == null )
        throw new HydraInternalException( "No clients with name " + clientName );
      for ( Iterator jt = vmidmap.values().iterator(); jt.hasNext(); ) {
        Collection crs = (Collection) jt.next();
        if ( numHighVMs > 0 && crs.size() >= highNumThreads ) {
          for ( int i = 0; i < highNumThreads; i++ ) {
            mapRandomClientWithVmid( crs, subgroup );
            ++mapped;
          }
          --numHighVMs;
        }
        else if ( numLowVMs > 0 && crs.size() >= lowNumThreads ) {
          for ( int i = 0; i < lowNumThreads; i++ ) {
            mapRandomClientWithVmid( crs, subgroup );
            ++mapped;
          }
          --numLowVMs;
        } // else this vm doesn't work
      }
    }
    // make sure we scheduled them all
    if ( mapped != totalThreads )
      throw new HydraConfigException( "Unable to evenly distribute " + totalThreads + " threads across " + totalVMs + " vms for: " + subgroup );
  }

  /**
   *  Maps clients at random across all vms with the specified client names.
   */
  private void mapClientsToSubgroupNamed( HashMap namemap,
                                          HydraThreadSubgroup subgroup ) {
    int totalThreads = subgroup.getTotalThreads();
    while ( totalThreads > 0 ) {
      mapRandomClientWithNames( namemap, subgroup );
      --totalThreads;
    }
  }

  /**
   *  Maps clients as evenly as possible across the specified number of vms.
   */
  private void mapClientsToSubgroupExact( HashMap namemap,
                                          HydraThreadSubgroup subgroup ) {
    int totalThreads = subgroup.getTotalThreads();
    int totalVMs     = subgroup.getTotalVMs();

    int lowNumThreads  = totalThreads / totalVMs;
    int highNumThreads = lowNumThreads + 1;
    int numHighVMs     = totalThreads % totalVMs;
    int numLowVMs      = totalVMs - numHighVMs;

    int mapped = 0;
    for ( Iterator it = namemap.values().iterator(); it.hasNext(); ) {
      HashMap vmidmap = (HashMap) it.next();
      for ( Iterator jt = vmidmap.values().iterator(); jt.hasNext(); ) {
        Collection crs = (Collection) jt.next();
        if ( numHighVMs > 0 && crs.size() >= highNumThreads ) {
          for ( int i = 0; i < highNumThreads; i++ ) {
            mapRandomClientWithVmid( crs, subgroup );
            ++mapped;
          }
          --numHighVMs;
        }
        else if ( numLowVMs > 0 && crs.size() >= lowNumThreads ) {
          for ( int i = 0; i < lowNumThreads; i++ ) {
            mapRandomClientWithVmid( crs, subgroup );
            ++mapped;
          }
          --numLowVMs;
        } // else this vm doesn't work
      }
    }
    // make sure we scheduled them all
    if ( mapped != totalThreads )
      throw new HydraConfigException( "Unable to evenly distribute " + totalThreads + " threads across " + totalVMs + " vms for: " + subgroup );
  }

  /**
   *  Maps clients at random across all available vms.
   */
  private void mapClientsToSubgroupRandom( HashMap namemap,
                                           HydraThreadSubgroup subgroup ) {
    int totalThreads = subgroup.getTotalThreads();
    while ( totalThreads > 0 ) {
      mapRandomClient( namemap, subgroup );
      --totalThreads;
    }
  }

  /**
   *  Maps all remaining clients.
   */
  private void mapClientsToSubgroupRemaining( HashMap namemap,
                                              HydraThreadSubgroup subgroup ) {
    for ( Iterator i = namemap.values().iterator(); i.hasNext(); ) {
      HashMap vmidmap = (HashMap) i.next();
      for ( Iterator j = vmidmap.values().iterator(); j.hasNext(); ) {
        Collection bucket = (Collection) j.next();
        for ( Iterator k = bucket.iterator(); k.hasNext(); ) {
          ClientRecord cr = (ClientRecord) k.next();
          if ( log().finestEnabled() ) {
            log().finest( "Mapping remaining " + cr + " to " + subgroup );
          }
          mapClient( cr, subgroup, k );
        }
      }
    }
  }

  /**
   *  Maps a random client with one of the specified client names to the
   *  subgroup and removes it from its bucket.
   */
  private void mapRandomClientWithNames( HashMap namemap,
                                         HydraThreadSubgroup subgroup ) {
    Vector clientBucketsWithName = new Vector();
    for ( Iterator i = subgroup.getClientNames().iterator(); i.hasNext(); ) {
      String clientName = (String) i.next();
      HashMap vmidmap = (HashMap) namemap.get( clientName );
      clientBucketsWithName.addAll( vmidmap.values() );
    }
    int numRemaining = 0;
    for ( Iterator i = clientBucketsWithName.iterator(); i.hasNext(); ) {
      Collection crs = (Collection) i.next();
      numRemaining += crs.size();
    }
    GsRandom rng = TestConfig.tab().getRandGen();
    int currentIndex = 0;
    int randomIndex = rng.nextInt( 0, numRemaining - 1 );
    for ( Iterator i = clientBucketsWithName.iterator(); i.hasNext(); ) {
      Collection crs = (Collection) i.next();
      for ( Iterator j = crs.iterator(); j.hasNext(); ) {
        ClientRecord cr = (ClientRecord) j.next();
        if ( currentIndex == randomIndex ) {
          if ( log().finestEnabled() ) {
            log().finest( "Mapping client named " + cr + " to " + subgroup );
          }
          mapClient( cr, subgroup, i );
          // must remove it from the namemap as well as the copy
          if ( log().finestEnabled() ) {
            log().finest( "Removing client " + cr + " in thread group "
                        + cr.getThreadGroupName() + " from namemap" );
          }
          HashMap vmidmap = (HashMap) namemap.get( cr.vm().getClientName() );
          Collection bucket = (Collection) vmidmap.get( cr.vm().getVmid() );
          bucket.remove( cr );
          return;
        } else {
          ++currentIndex;
        }
      }
    }
  }

  /**
   *  Maps a random client with a particular vmid to the subgroup and removes it
   *  from its bucket.
   */
  private void mapRandomClientWithVmid( Collection crs,
                                        HydraThreadSubgroup subgroup ) {
    GsRandom rng = TestConfig.tab().getRandGen();
    int currentIndex = 0;
    int randomIndex = rng.nextInt( 0, crs.size() - 1 );
    for ( Iterator i = crs.iterator(); i.hasNext(); ) {
      ClientRecord cr = (ClientRecord) i.next();
      if ( currentIndex == randomIndex ) {
        if ( log().finestEnabled() ) {
          log().finest( "Mapping exact " + cr + " to " + subgroup );
        }
        mapClient( cr, subgroup, i );
        return;
      } else {
        ++currentIndex;
      }
    }
  }

  /**
   *  Maps a random client to the subgroup and removes it from its bucket.
   */
  private void mapRandomClient( HashMap namemap, HydraThreadSubgroup subgroup ) {
    GsRandom rng = TestConfig.tab().getRandGen();
    int randomIndex = rng.nextInt( 1, numRemainingClients( namemap ) - 1 );
    int currentIndex = 0;
    for ( Iterator i = namemap.values().iterator(); i.hasNext(); ) {
      HashMap vmidmap = (HashMap) i.next();
      for ( Iterator j = vmidmap.values().iterator(); j.hasNext(); ) {
        Collection crs = (Collection) j.next();
        for ( Iterator k = crs.iterator(); k.hasNext(); ) {
          ClientRecord cr = (ClientRecord) k.next();
          if ( currentIndex == randomIndex ) {
            if ( log().finestEnabled() ) {
              log().finest( "Mapping random " + cr + " to " + subgroup );
            }
            mapClient( cr, subgroup, k );
            return;
          } else {
            ++currentIndex;
          }
        }
      }
    }
  }

  /**
   *  Maps the client to the subgroup and removes it from its bucket.
   */
  private void mapClient( ClientRecord cr, HydraThreadSubgroup subgroup,
                          Iterator bucket ) {
    String name = cr.getThreadGroupName();
    if ( name != null ) {
      throw new HydraInternalException
      (
        "Already mapped " + cr + " to " + name + " -- can't map it to "
        + subgroup.getName()
      );
    }
    HydraThreadGroup group =
      (HydraThreadGroup) this.threadGroups.get( subgroup.getName() );
    cr.setThreadGroup( group );
    bucket.remove(); // the iterator is at this client
    if ( log().finestEnabled() ) {
      log().finest( "Removing client " + cr + " in thread group "
                  + cr.getThreadGroupName() + " from bucket" );
    }
  }

  /** 
   *  Sorts the clients into a map of maps of vector buckets keyed by client
   *  name then vmid.
   */
  private HashMap sortClientsByNameAndVmid( Map cvmrs ) {
    HashMap namemap = new HashMap();
    for ( Iterator i = cvmrs.values().iterator(); i.hasNext(); ) {
      ClientVmRecord cvmr = (ClientVmRecord) i.next();
      String clientName = cvmr.getClientName();
      Map vmidmap = (Map) namemap.get( clientName );
      if ( vmidmap == null ) {
        vmidmap = new HashMap();
        namemap.put( clientName, vmidmap );
      }
      vmidmap.put( cvmr.getVmid(), cvmr.getCopyOfClientCollection() );
    }
    return namemap;
  }

  /**
   *  Returns a nicely formatted string that describes the given name map.
   */
  private static String nameMapToString( Map namemap ) {
    StringBuffer sb = new StringBuffer("Name map:\n");

    for (Iterator clientNames = namemap.keySet().iterator();
         clientNames.hasNext(); ) {
      String clientName = (String) clientNames.next();
      sb.append("  ClientName ");
      sb.append(clientName);
      sb.append("\n");

      Map vmidmap = (Map) namemap.get(clientName);
      for (Iterator vmids = vmidmap.keySet().iterator();
           vmids.hasNext(); ) {
        Integer vmid = (Integer) vmids.next();
        sb.append("    vm_");
        sb.append(vmid);
        sb.append(" -> ");

        Collection crs = (Collection) vmidmap.get(vmid);
        for (Iterator iter = crs.iterator(); iter.hasNext(); ) {
          sb.append(iter.next());
          sb.append(" ");
        }

        sb.append("\n");
      }
    }
    return sb.toString();
  }

  /**
   *  Returns the number of remaining clients in the namemap with the given
   *  client names.
   */
  private int numRemainingClients( HashMap namemap, Vector clientNames ) {
    if ( clientNames == null ) { // can use any names
      return numRemainingClients( namemap );
    } else {
      int count = 0;
      for ( Iterator i = clientNames.iterator(); i.hasNext(); ) {
        String clientName = (String) i.next();
        HashMap vmidmap = (HashMap) namemap.get( clientName );
        if ( vmidmap == null ) {
          throw new HydraInternalException( "No clients with name " + clientName );
        } else {
          for ( Iterator j = vmidmap.values().iterator(); j.hasNext(); ) {
            Collection crs = (Collection) j.next();
            count += crs.size();
          }
        }
      }
      return count;
    }
  }

  /**
   *  Returns the number of remaining clients in the namemap.
   */
  private int numRemainingClients( HashMap namemap ) {
    int count = 0;
    for ( Iterator i = namemap.values().iterator(); i.hasNext(); ) {
      HashMap vmidmap = (HashMap) i.next();
      for ( Iterator j = vmidmap.values().iterator(); j.hasNext(); ) {
        Collection crs = (Collection) j.next();
        count += crs.size();
      }
    }
    return count;
  }

  /** 
   * Returns the subgroups from the set of groups
   * @param groups Maps thread group name to its {@link HydraThreadGroup}. 
   * @return A <code>Vector</code> of {@link HydraThreadSubgroup}s.
   */
  private Vector subgroupsFromGroups( Map groups ) {
    Vector subgroups = new Vector();
    for ( Iterator i = groups.values().iterator(); i.hasNext(); ) {
      HydraThreadGroup tg = (HydraThreadGroup) i.next();
      for ( Iterator j = tg.getSubgroups().iterator(); j.hasNext(); ) {
        HydraThreadSubgroup tsg = (HydraThreadSubgroup) j.next();
        subgroups.add( tsg );
      }
    }
    if ( log().fineEnabled() ) {
      log().fine( "\nSUBGROUPS:\n" + subgroups + "\n\n" );
    }
    return subgroups;
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }

  private Map threadGroups;
  private Vector threadSubgroups;
}
