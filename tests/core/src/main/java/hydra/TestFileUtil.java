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

import hydra.HostHelper.OSType;
import java.io.*;
import java.util.*;

/**
 *
 *  Supports runtime and postmortem operations on test files.
 *
 */

public class TestFileUtil
{
  private static String[] DISK_FILE_PREFIXES = {
    "RECOVERY_STATE_" // pre-6.5
  };

  private static String[] DISK_FILE_SUFFIXES = {
    ".krf", // post-6.5
    ".idxkrf", // gxd beta
    ".crf", // post-6.5
    ".db",  // pre-6.5
    ".drf", // post-6.5
    ".hlk", // pre-6.5
    ".if",  // post-6.5
    ".lg",  // pre-6.5
    ".lk",  // pre-6.5 and post-6.5
    ".olg"  // pre-6.5
  };

  //////////////////////////////////////////////////////////////////////////////
  ////    CONFIG FILES                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a list of all hydra configuration files in $JTESTS, $EXTRA_JTESTS.
   * Ignores files named "local.conf".
   */
  public static List getConfigFiles() {
    String jtests = System.getProperty("JTESTS");
    List configFiles = getFilesEndingWith(jtests, ".conf");
    //Add any additional confs
    String extraJtests = System.getProperty("EXTRA_JTESTS");
    if( extraJtests != null ) {
      List extraJtestsFiles = getFilesEndingWith(extraJtests, ".conf");
      // Avoid using addAll() to preserve maximum polymorphism
      for (Iterator i = extraJtestsFiles.iterator(); i.hasNext();) {
        configFiles.add(i.next());
      } 
    }
    return configFiles;
  }
  private static List getFilesEndingWith(String dirname, String suffix) {
    class EndsWithFilter implements FileFilter {
      String suffix;
      public EndsWithFilter(String suffix) {
        this.suffix = suffix;
      }
      public boolean accept(File fn) {
       return !fn.isDirectory() && fn.getName().endsWith(suffix);
      }
    };
    return FileUtil.getFiles(new File(dirname), new EndsWithFilter(suffix), true);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    TEST DIRECTORIES                                                  ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns a (possibly empty) list of all subdirectories of the specified
   *  directory whose names end with the pattern -xxxx-xxxxxx, where x is a
   *  digit, e.g., test-1011-123045.  Searches recursively if told to do so.
   *  <p>
   *  This method attempts to pick up only batterytest-generated test directories.
   */
  public static List getTestDirectories( String dirname, boolean recursive ) {
    return FileUtil.getFiles( new File( dirname ), new TestDirFilter(), recursive );
  }
  public static boolean isTestDirectory( String dirname ) {
    return (new TestDirFilter()).accept( new File( dirname ) );
  }
  public static String getShortTestName( String dirname ) {
    String fn = FileUtil.filenameFor( dirname ); 
    int hyphen2 = fn.lastIndexOf('-');
    int hyphen1 = fn.lastIndexOf('-', hyphen2 - 1);
    return fn.substring( 0, hyphen1 );
  }
  static class TestDirFilter implements FileFilter {
    public TestDirFilter() {}
    public boolean accept( File f ) {
     String fn = f.getName();
     int hyphen2 = fn.lastIndexOf('-');
     int hyphen1 = fn.lastIndexOf('-', hyphen2 - 1);
     return f.isDirectory() && hyphen1 != -1
	 && hyphen1 + 4 + 1 == hyphen2
	 && hyphen2 + 6 + 1 == fn.length()
	 && isDigits( fn.substring( hyphen1 + 1, hyphen2 ) )
	 && isDigits( fn.substring( hyphen2 + 1, fn.length() ) );
    }
  };
  protected static boolean isDigits( String s ) {
    for ( int i = 0; i < s.length(); i++ ) {
      if ( ! Character.isDigit( s.charAt(i) ) ) {
        return false;
      }
    }
    return true;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    SYSTEM DIRECTORIES                                                ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the existing system directories, whether local or remote,
   * as a list.  Returns null if there are no directories.
   */ 
  public static List<String> getSystemDirectoriesAsList(String masterUserDir) {
    SortedMap<String,List<String>> sysdirMap =
                                   getSystemDirectories(masterUserDir);
    return sysdirMap == null ? null : mapOfListsToList(sysdirMap);
  }

  /**
   *  Returns all system directories, whether local or remote, mapped
   *  by logical system name.  Looks in the provided directory as well as the
   *  configured locations.
   */
  private static SortedMap getSystemDirectories(String masterUserDir) {
    SortedMap<String,List<String>> dirmap = new TreeMap();
    String fn = null;
    if (HostHelper.getLocalHostOS() == OSType.unix) {
      fn = masterUserDir + File.separator + Nuker.UNIX_SYSDIRS;
    } else if (HostHelper.getLocalHostOS() == OSType.windows) {
      fn = System.getProperty("user.dir") + File.separator + Nuker.WINDOWS_SYSDIRS;
    } else {
      throw new HydraInternalException("Should not happen");
    }
    List<String> tokens;
    try {
      tokens = FileUtil.getTextAsTokens(fn);
    } catch (FileNotFoundException e) {
      return dirmap; // no directories have been created
    } catch (IOException e) {
      String s = "Unable to open file: " + fn;
      throw new HydraRuntimeException(s);
    }
    if (tokens.size() % 2 != 0) {
      String s = fn + " is corrupt";
      throw new HydraRuntimeException(s);
    }
    for (int i = 0; i < tokens.size(); i=i+2) {
      String name = tokens.get(i);
      String dir = tokens.get(i+1);
      if (!FileUtil.exists(dir)) {
        // might have moved, so try in the master user directory
        String localdir = masterUserDir + "/" + FileUtil.filenameFor(dir);
        if (FileUtil.exists(localdir)) { // use the new location
          dir = localdir;
        } else {
          Log.getLogWriter().warning("Missing directory: " + dir);
          dir = null;
        }
      }
      List<String> dirs = dirmap.get(name);
      if (dirs == null) {
        dirs = new ArrayList();
        dirmap.put(name, dirs);
      }
      if (dir != null) {
        dirs.add(dir);
      }
    }
    return dirmap;
  }

  /**
   *  Returns a list of all subdirectories of the specified directory whose
   *  names start with the given prefix.
   */
  private static List getDirectoriesStartingWith( String dirname, String prefix ) {
    class StartsWithFilter implements FileFilter {
      String prefix;
      public StartsWithFilter( String prefix ) {
        this.prefix = prefix;
      }
      public boolean accept( File fn ) {
       return fn.isDirectory() && fn.getName().startsWith( prefix );
      }
    };
    return FileUtil.getFiles( new File( dirname ), new StartsWithFilter( prefix ), false );
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    DISK DIRECTORIES
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Removes all of the disk files.  For internal use by hydra master only.
   * Looks for disk files in directories recorded with {@link hydra.Nuker}.
   */
  protected static void removeDiskFilesAfterTest() {
    Set dirs = Nuker.getInstance().getDirectories();
    for (Iterator i = dirs.iterator(); i.hasNext();) {
      String dir = (String)i.next();
      removeDiskFiles(new File(dir));
    }
  }

  /**
   *  Removes the disk files from the specified directory.
   */
  private static void removeDiskFiles(File dir) {
    class DiskFileFilter implements FileFilter {
      public DiskFileFilter() {}
      public boolean accept(File fn) {
        String fname = fn.getName();
        for (String prefix : DISK_FILE_PREFIXES) {
          if (fname.startsWith(prefix)) {
            return true;
          }
        }
        for (String suffix : DISK_FILE_SUFFIXES) {
          if (fname.endsWith(suffix)) {
            return true;
          }
        }
        return false;
      }
    };
    List dirs = FileUtil.getFiles(dir, new DiskFileFilter(), true);
    for (Iterator i = dirs.iterator(); i.hasNext();) {
       File diskFile = (File)i.next();
       long sz = diskFile.length();
       boolean result = diskFile.delete();
       if (result) {
         Log.getLogWriter().info("Removed "
            + diskFile.getAbsolutePath() + " at " + sz + " bytes");
       } else {
         Log.getLogWriter().info("Was unable to remove "
            + diskFile.getAbsolutePath() + " at " + sz + " bytes");
       }
    }
  }
  //////////////////////////////////////////////////////////////////////////////
  ////    LOG FILES                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Used when merging log files only.
   *  Returns the existing log files, whether local or remote.  Looks in the
   *  provided directory as well as any remote system directories.
   */
  public static List getMergeLogFiles(String masterUserDir) {
    Set<String> allLogs = new TreeSet();
    SortedMap<String,List<String>> sysdirmap =
                                   getSystemDirectories(masterUserDir);
    for (String name : sysdirmap.keySet()) {
      for (String sysdir : sysdirmap.get(name)) {
        List logs = getFilesEndingWith(sysdir, ".log");
        if (logs.size() != 0) {
          allLogs.addAll(logs);
        }
      }
    }
    {
      List logs = getFilesEndingWith(masterUserDir, ".log");
      allLogs.addAll(logs);
    }
    return allLogs.size() == 0 ? null : new ArrayList(allLogs);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    STATISTIC ARCHIVES                                                ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the statistic archives for a native client test in a single list.
   */ 
  public static List getNativeClientStatisticArchivesAsList(String masterUserDir) {
    List archives = getFilesStartingWithEndingWith(masterUserDir,
                                                  "statArchive", ".gfs");
    return archives.size() == 0 ? null : archives;
  }

  /**
   * Returns the existing statistic archives, whether local or remote,
   * as a list.  Returns null if there are no archives.
   */ 
  public static List<String> getStatisticArchivesAsList(String masterUserDir) {
    SortedMap<String,List<String>> archiveMap =
                                   getStatisticArchives(masterUserDir);
    return archiveMap == null ? null : mapOfListsToList(archiveMap);
  }

  /**
   *  Returns the existing statistic archives, whether local or remote, mapped
   *  by logical system name.
   */
  public static SortedMap<String,List<String>> getStatisticArchives(
                                                  String masterUserDir) {
    SortedMap<String,List<String>> archivemap = new TreeMap();
    SortedMap<String,List<String>> sysdirmap =
                                   getSystemDirectories(masterUserDir);
    for (String name : sysdirmap.keySet()) {
      for (String sysdir : sysdirmap.get(name)) {
        List archives = getFilesStartingWithEndingWith(sysdir,
                                                      "statArchive", ".gfs");
        if (archives.size() != 0) {
          List<String> existingArchives = archivemap.get(name);
          if (existingArchives == null) {
            archivemap.put(name, archives);
          } else {
            existingArchives.addAll(archives);
          }
        }
      }
    }
    return archivemap.size() == 0 ? null : archivemap;
  }

  /**
   * Returns a list of all files in the specified directory whose
   * names start with the given prefix and end with the given suffix.
   */
  private static List getFilesStartingWithEndingWith(String dirname,
                      String prefix, String suffix) {
    class StartsWithEndsWithFilter implements FileFilter {
      String prefix, suffix;
      public StartsWithEndsWithFilter(String prefix, String suffix) {
        this.prefix = prefix;
        this.suffix = suffix;
      }
      public boolean accept(File fn) {
       return fn.getName().startsWith(prefix) && fn.getName().endsWith(suffix);
      }
    };
    return FileUtil.getFiles(new File(dirname),
                             new StartsWithEndsWithFilter(prefix, suffix),
                             true);
  }

  //----------------------------------------------------------------------------
  //  Misc
  //----------------------------------------------------------------------------

  /**
   * Returns the (String,List(String)) map as a list, or null if no content.
   */ 
  private static List<String> mapOfListsToList(
                              SortedMap<String,List<String>> mapOfLists) {
    List<String> list = new ArrayList();
    Collection<List<String>> sublists = mapOfLists.values();
    if (sublists != null) {
      for (List<String> sublist : sublists) {
        if (sublist != null) {
          list.addAll(sublist);
        }
      }
    }
    return list.size() == 0 ? null : list;
  }

  //----------------------------------------------------------------------------
  //  Utilities
  //----------------------------------------------------------------------------

  private static final String OPERATION = "operation";
  private static final String TARGET = "target";
  private static final String LIST = "list";
  private static final String GEN_VSD = "genVSD";
  private static final String SYSDIRS = "sysdirs";
  private static final String STATARCHIVES = "statarchives";

  /**
   * Prints the list of system directories for the test directory, using the
   * local location, if valid, else the remote location.
   */
  private static void listSysDirs(String testDir)
  throws FileNotFoundException, IOException {
    List dirList = getSystemDirectoriesAsList(testDir);
    for (Iterator i = dirList.iterator(); i.hasNext();) {
      System.out.println(i.next());
    }
  }

  private static void listStatArchives(String testDir)
  throws FileNotFoundException, IOException {
    List dirList = getStatisticArchivesAsList(testDir);
    for (Iterator i = dirList.iterator(); i.hasNext();) {
      System.out.println(i.next());
    }
  }

  private static void genVSD(String testDir)
  throws FileNotFoundException, IOException {
    List dirList = getStatisticArchivesAsList(testDir);
    if (dirList == null) {
      System.out.println("No archives yet for " + testDir);
    } else {
      StringBuffer buf = new StringBuffer();
      Platform platform = Platform.getInstance();
      buf.append(platform.getScriptHeader() + platform.getCommentPrefix() + " execute this script to run vsd on all archives, including remote ones, for a test that is still running\nvsd ");
      for (Iterator i = dirList.iterator(); i.hasNext();) {
        buf.append(i.next() + " ");
      }
      String fn = testDir + File.separator + "vsd"
                + platform.getFileExtension();
      FileUtil.appendToFile(fn, buf + "\n");
      platform.setExecutePermission(fn);
      System.out.println("\n\nSee " + fn + "\n");
    }
  }

  /**
   * Prints the given string and usage string and exits.
   */
  private static void usage(String s) {
    System.err.println(s);
    usage();
  }

  /**
   * Prints the usage string and exits.
   */
  private static void usage() {
    System.err.println("Usage:  hydra.TestFileUtil "
           + OPERATION + " " + TARGET + " [<test_directory(default:$PWD)>]"
           + "\n\twhere"
           + "\n\t\t" + OPERATION + "=" + LIST + "|" + GEN_VSD
           + "\n\t\t" + TARGET + "=" + SYSDIRS + "|" + STATARCHIVES);
    System.exit(1);
  }

  /**
   * Utilities for directory operations.
   */
  public static void main(String args[])
  throws FileNotFoundException, IOException {
    Log.createLogWriter( "testFileUtil", "info" );
    if (args.length < 2 || args.length > 3) {
      usage();
    }
    String operation = args[0];
    String target = args[1];
    String testDir = (args.length == 3) ?
                       (new File(args[2])).getAbsolutePath() :
                       System.getProperty("user.dir");
    if (operation.equals(LIST)) {
      if (target.equals(SYSDIRS)) {
        listSysDirs(testDir);
      } else if (target.equals(STATARCHIVES)) {
        listStatArchives(testDir);
      } else {
        usage("Unsupported " + TARGET + ": " + target);
      }
    } else if (operation.equals(GEN_VSD)) {
      if (target.equals(STATARCHIVES)) {
        genVSD(testDir);
      } else if (target.equals(SYSDIRS)) {
        usage("\"" + GEN_VSD + "\" only supports \"" + STATARCHIVES + "\"");
        System.exit(1);
      } else {
        usage("Unsupported " + TARGET + ": " + target);
      }
    } else {
      usage("Unsupported " + OPERATION + ": " + operation);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    SYSTEM DIRECTORIES (for GFE-only backwards compatibility)
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns the existing local gemfire system directories, mapped
   *  by logical system name.  Looks in the provided directory as well as the
   *  configured locations.
   */
  public static SortedMap getSystemDirectories(List<String> logicalSystemNames,
                                               String masterUserDir) {
    SortedMap map = new TreeMap();

    for (String logicalSystemName : logicalSystemNames) {
      // get directories based on master user dir (all local)
      String parentDirectory = masterUserDir;
      List sysdirs = getSystemDirectories(logicalSystemName, parentDirectory);

      // add the resulting directories to the map
      if (sysdirs.size() > 0) {
        map.put(logicalSystemName, sysdirs);
      }
    }
    return map;
  }

  /**
   * Returns the gemfire system directories for the logical system name found
   * in the specified parent directory.
   */
  public static List<String> getSystemDirectories(String logicalSystemName,
                                                  String parentDirectory) {
    List<String> l1 = getDirectory(parentDirectory + File.separator
                                  + logicalSystemName);
    List<String> l2 = getDirectoriesStartingWith(parentDirectory,
                                                 logicalSystemName + "_");
    l1.addAll(l2);
    return l1;
  }

  /**
   * Returns a list containing the specified directory, if it exists.
   */
  private static List getDirectory(String dirname) {
    List l = new ArrayList();
    File dir = new File(dirname);
    if (dir.exists()) {
      l.add(dir);
    }
    return l;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    STATISTIC ARCHIVES (for GFE-only backwards compatibility)
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the gemfire statistic archives in a single list.
   */
  public static List getStatisticArchivesAsList(List logicalSystemNames,
                                                String masterUserDir) {
    List archives = new ArrayList();
    SortedMap archiveMap =
              getStatisticArchives(logicalSystemNames, masterUserDir);
    if (archiveMap != null) {
      for (Iterator i = archiveMap.values().iterator(); i.hasNext();) {
        List archiveList = (List) i.next();
        if (archiveList != null) {
          archives.addAll(archiveList);
        }
      }
    }
    return archives.size() == 0 ? null : archives;
  }

  /**
   * Returns the existing gemfire statistic archives mapped by logical system
   * name.
   */
  public static SortedMap getStatisticArchives(List logicalSystemNames,
                                               String masterUserDir) {
    SortedMap map = new TreeMap();
    SortedMap systemDirectories =
              getSystemDirectories(logicalSystemNames, masterUserDir);
    for (Iterator i = logicalSystemNames.iterator(); i.hasNext();) {
      String logicalSystemName = (String)i.next();
      List parentDirectories = (List)systemDirectories.get(logicalSystemName);
      if (parentDirectories != null) {
        List archives = getStatisticArchives(parentDirectories);
        map.put(logicalSystemName, archives);
      }
    }
    return map;
  }

  /**
   * Returns the gemfire statistic archives in the specified directories.
   */
  private static List getStatisticArchives(List parentDirectories) {
    List archives = new ArrayList();
    for (Iterator i = parentDirectories.iterator(); i.hasNext();) {
      File parentDirectory = (File)i.next();
      List l = getFilesStartingWithEndingWith(parentDirectory.getAbsolutePath(),
                                             "statArchive", ".gfs");
      if (l != null && l.size() != 0) {
        archives.addAll(l);
      }
    }
    return archives.size() == 0 ? null : archives;
  }
}
