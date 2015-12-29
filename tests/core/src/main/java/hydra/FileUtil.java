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

import java.io.*;
import java.util.*;

public class FileUtil
{
  /**
  *
  * Return whether the filename is absolute.
  *
  */
  public static boolean isAbsoluteFilename( String name ) {
    File f = new File( name );
    if ( name.equals( f.getAbsolutePath() ) )
      return true;
    else
      return false;
  }

  /**
  *
  * Return the file with the given name, or null if name is null.
  *
  */
  public static File fileForName( String name ) {
    if ( name == null ) return null;
    File f = new File( name );
    return f;
  }

  /**
  *
  * Return the path for a given absolute filename.
  *
  */
  public static String pathFor( String fullname ) {
    File f = new File( fullname );
    return f.getParent();
  }

  /**
  *
  * Return the absolute filename for a given file.
  *
  */
  public static String absoluteFilenameFor( String name ) {
    File f = new File( name );
    return f.getAbsolutePath();
  }

  /**
  *
  * Return the filename for a given absolute filename.
  *
  */
  public static String filenameFor( String fullname ) {
    File f = new File( fullname );
    return f.getName();
  }

  /**
  *
  * Replace any simple filenames with absolute ones.  It is assumed that
  * the default path includes a terminating file separator.
  *
  */
  public static Vector makeAbsolute( Vector fns, String defaultPath ) {
    if ( fns == null )
      return null;
    else {
      for ( int i = 0; i < fns.size(); i++ ) {
        String fn = (String) fns.elementAt(i);
        String fullfn = makeAbsolute( fn, defaultPath );
        fns.setElementAt( fullfn, i );
      }
      return fns;
    }
  }
  public static String makeAbsolute( String fn, String defaultPath ) {
    return ( isAbsoluteFilename( fn ) ) ? fn : defaultPath + fn;
  }

  /**
   *  Returns the file in the specified directory, if it exists.
   */
  public static File getFile( File dir, String fn ) {
    if ( ! dir.isAbsolute() ) {
      throw new HydraRuntimeException( dir + " is not an absolute path" );
    }
    File f = new File( dir.getAbsolutePath() + File.separator + fn );
    return f.exists() ? f : null;
  }

  /**
   *  Returns a (possibly empty) list of all files or directories in the
   *  directory that satisfy the filter.  Searches recursively if directed
   *  to do so.
   */
  public static List getFiles( File dir, FileFilter filter, boolean recursive ) {
    if ( ! dir.isAbsolute() ) {
      throw new HydraRuntimeException( dir + " is not an absolute path" );
    }
    File[] tmpfiles = dir.listFiles( filter );
    List matches;
    if ( tmpfiles == null ) {
      matches = new ArrayList();
    } else {
      matches = new ArrayList( Arrays.asList( tmpfiles ) );
    }
    if ( recursive ) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (int i = 0; i < files.length; i++) {
          File file = files[i];
          if (file.isDirectory()) {
            matches.addAll(getFiles(file, filter, recursive));
          }
        }
      }
    }
    return matches;
  }

  /**
  *
  * Answer whether the file exists.
  *
  */
  public static boolean exists( File f ) {
    return f.exists();
  }

  /**
  *
  * Answer whether the file exists.
  *
  */
  public static boolean exists( String name ) {
    return exists( new File( name ) );
  }

  /**
  *
  * Answer whether the file exists.  Complain if it's not a file.
  *
  */
  public static boolean fileExists( String name ) {
    File f = new File( name );
    if ( f.exists() ) {
      if ( f.isFile() )
        return true;
      else
        throw new HydraRuntimeException( name + " exists and is not a file" );
    }
    else
      return false;
  }

  /**
  *
  * Return a temporary file name for the specified name.
  *
  */
  public static String tmpNameFor( String name ) {
    File f = new File( name );
    return f.getName() + ".tmp";
  }

  /**
   *  Creates the specified directory and all intervening ones, if they do not
   *  already exist.
   */
  public static void mkdir( String dirname ) {
    mkdir( new File( dirname ) );
  }
  /**
   *  Creates the specified directory and all intervening ones, if they do not
   *  already exist.
   */
  public static void mkdir( File dir ) {
    if ( dir.exists() ) {
      if ( ! dir.isDirectory() )
        throw new Error("Error: " + dir + " already exists but is not a directory");
    } else {
      if ( ! dir.mkdirs() ) {
        if (! dir.mkdir()) {
          throw new Error("Error: Unable to make directory " + dir);
        }
      }
    }
  }

  /**
   * Creates the new file with the specified name.
   * @throws IOException if the file already exists.
   */
  public static void createNewFile(String name) throws IOException {
    File f = new File(name);
    f.createNewFile();
  }

  /**
  *
  * Delete file from directory on host.
  *
  */
  public static void deleteFile( String name ) {
    File f = new File( name );
    if ( f.exists() ) {
      if ( f.isFile() ) {
        if ( ! f.delete() )
          throw new Error("Error: Unable to delete file " + name);
      }
      else
        throw new Error("Error: " + name + " exists and is not a file");
    }
  }

  /**
   * Removes the directory and all its contents.
   * @param name a file or directory to recursively delete
   * @param throwOnFailure specify if an Exeption should be thrown 
   *                       when an error is encountered
   * @return true if all delete operations succeeded, false otherwise
   */
  public static boolean rmdir(String name, boolean throwOnFailure) {
    boolean result = true;
    File f = new File(name);
    if (f.exists()) {
      if (f.isDirectory()) {
        File[] files = f.listFiles();
        for (int i = 0; i < files.length; i++) {
          result = rmdir(files[i].getPath(), throwOnFailure) & result;
        }
      }
      if (!f.delete()) {
        if(throwOnFailure) {
          throw new HydraRuntimeException("Unable to delete " + f);
        } 
        result = false;
      }
    } else {
      if(throwOnFailure) {
        throw new HydraRuntimeException("Not found: " + f);
      }
      result = false;
    }
    return result;
  }

  /**
  *
  * Delete the directory and all its contents.
  *
  */
  public static void deleteDirAndContents( String name ) {
    deleteFilesFromDir( name );
    deleteDir( name );
  }

  /**
  *
  * Delete all files from directory on host.
  *
  */
  public static void deleteFilesFromDir( String name ) {
    File dir = new File( name );
    if ( dir.exists() ) {
      if ( dir.isDirectory() ) {
        File[] files = dir.listFiles();
        if ( files != null )
          for ( int i = 0; i < files.length; i++ )
            if ( ! files[i].delete() )
              throw new Error("Error: Unable to delete file " + files[i].getName());
      }
      else
        throw new Error("Error: " + name + " exists but is not a directory");
    }
  }

  /**
  *  
  *  Delete all files and directories from directory on host.
  *
  */
   public static void deleteFilesFromDirRecursive( String name ) {
    File dir = new File( name );
    if ( dir.exists() ) {
      if ( dir.isDirectory() ) {
        File[] files = dir.listFiles();
        if ( files != null ) {
          for ( int i = 0; i < files.length; i++ ) {
            if ( ! files[i].delete() ) {
              if (files[i].isDirectory()) {
                deleteFilesFromDirRecursive( files[i].getAbsolutePath() );
              } else {
                throw new Error("Error: Unable to delete file " + files[i].getName());
              }
            }
          }
        }
      }
      else
        throw new Error("Error: " + name + " exists but is not a directory");
    }
  }


  /**
  *
  * Delete the directory.
  *
  */
  public static void deleteDir( String name ) {
    File dir = new File( name );
    if ( dir.exists() )
      if ( dir.isDirectory() ) {
        if ( ! dir.delete() )
          throw new Error("Error: Unable to delete directory " + name);
      }
      else
        throw new Error("Error: " + name + " exists and is not a directory");
  }

  /**
  *
  * Move "src" to "dst".
  *
  */
  public static void moveFile(String src, String dst) {
    if ( fileExists( src ) ) {
      boolean moved = ( new File( src ) ).renameTo( new File( dst ) );
      if ( ! moved ) {
        throw new HydraRuntimeException( "Failed to copy " + src + " to " + dst );
      }
    } else {
      throw new HydraRuntimeException( src + " does not exist" );
    }
  }

  /**
  *
  * Copy "src" to "dst".  First remove the dst if it already exists.
  *
  */
  public static void copyFileOverTop(String src, String dst) {
    try {
      if ( fileExists( dst ) )
        deleteFile( dst );
      copyFile( new File(src), new File(dst) );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Failed to copy " + src + " to " + dst, e );
    }
  }

  /**
  *
  * Copy "src" to "dst".
  *
  */
  public static void copyBytesToFile(byte[] src, String dst) {
    try {
      copyBytesToFile( src, new File(dst) );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Failed to copy src to " + dst, e );
    }
  }

  /**
  *
  * Copy "src" to "dst".
  *
  */
  public static void copyFile(String src, String dst) {
    try {
      copyFile( new File(src), new File(dst) );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Failed to copy " + src + " to " + dst, e );
    }
  }

  /**
  *
  * Copy "src" byte array to "dst" file.
  *
  */
  public static void copyBytesToFile(byte[] src, File dst)
  throws IOException
  {
    if (dst.exists()) {
            throw new IOException("The destination file \""
                                  + dst.getPath()
                                  + "\" already exists.");
    }
    dst = dst.getAbsoluteFile();
    File dstParent = dst.getParentFile();
    if (dstParent != null) {
        if (!dstParent.exists()) {
                  throw new IOException("The destination directory \""
                                        + dstParent.getPath()
                                        + "\" does not exist.");
        }
        if (!dstParent.canWrite()) {
                  throw new IOException("The destination directory \""
                                        + dstParent.getPath()
                                        + "\" is not writable.");
        }
    }
    ByteArrayInputStream in = null;
    FileOutputStream out = null;
    try {
        in = new ByteArrayInputStream(src);

        try {
          out = new FileOutputStream(dst);
        } catch (IOException io) {
          throw new IOException("Failed to create \""
                                + dst.getPath()
                                + "\" because: "
                                + io.toString());
        }
        try {
          int bytesRead;
          byte buffer[] = new byte[16184];
          while ((bytesRead = in.read(buffer)) != -1) {
              out.write(buffer, 0, bytesRead);
          }
        } catch (IOException io) {
          throw new IOException(
              "Copying src to \""
              + dst.getPath()
              + "\" failed because: "
              + io.toString());
          }
        } finally {
              if (out != null) try {out.close();} catch (IOException ignore) {}
        }
  }

  /**
  *
  * Copy "src" file to "dst" file.
  *
  */
  private static void copyFile(File src, File dst)
  throws IOException
  {
    if (!src.exists()) {
            throw new IOException("The source file \""
                                  + src.getPath()
                                  + "\" does not exist.");
    }
    if (dst.exists()) {
            throw new IOException("The destination file \""
                                  + dst.getPath()
                                  + "\" already exists.");
    }
    if (!src.isFile()) {
            throw new IOException("The source file \""
                                  + src.getPath()
                                  + "\" is not a file.");
    }
    if (!src.canRead()) {
              throw new IOException("The source file \""
                                    + src.getPath()
                                    + "\" is not readable.");
    }
    dst = dst.getAbsoluteFile();
    File dstParent = dst.getParentFile();
    if (dstParent != null) {
        if (!dstParent.exists()) {
                  throw new IOException("The destination directory \""
                                        + dstParent.getPath()
                                        + "\" does not exist.");
        }
        if (!dstParent.canWrite()) {
                  throw new IOException("The destination directory \""
                                        + dstParent.getPath()
                                        + "\" is not writable.");
        }
    }
    InputStream in = null;
    FileOutputStream out = null;
    try {
        try {
          in = new FileInputStream(src);
        } catch (IOException io) {
          throw new IOException("Failed to open \""
                                + src.getPath()
                                + "\" because: "
                                + io.toString());
        }
        try {
          out = new FileOutputStream(dst);
        } catch (IOException io) {
          throw new IOException("Failed to create \""
                                + dst.getPath()
                                + "\" because: "
                                + io.toString());
        }
        try {
          int bytesRead;
          byte buffer[] = new byte[16184];
          while ((bytesRead = in.read(buffer)) != -1) {
              out.write(buffer, 0, bytesRead);
          }
        } catch (IOException io) {
          throw new IOException(
              "Copying \""
              + src.getPath()
              + "\" to \""
              + dst.getPath()
              + "\" failed because: "
              + io.toString());
          }
        } finally {
              if (in != null) try {in.close();} catch (IOException ignore) {}
              if (out != null) try {out.close();} catch (IOException ignore) {}
        }
  }

  /**
  *
  * Write the text to the file, overwriting if necessary.
  *
  */
  public static void writeToFile( String fn, String txt )
  {
    try {
      FileOutputStream fos = new FileOutputStream( fn );
      fos.write( txt.getBytes() );
      fos.close();
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to write to file: " + fn, e );
    }
  }

  /**
  *
  * Append the text to the file.
  *
  */
  public static void appendToFile( String fn, String txt )
  {
    try {
      boolean append = true;
      FileOutputStream fos = new FileOutputStream( fn, append );
      fos.write( txt.getBytes() );
      fos.close();
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to append to file: " + fn, e );
    }
  }

  /**
  *
  * Answer whether the file has any lines that contain the substring.
  *
  */
  public static boolean hasLinesContaining( String fn, String substring )
  {
    try {
      boolean result = false;
      BufferedReader br = new BufferedReader( new FileReader( fn ) );
      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        if ( line.indexOf( substring ) != -1 ) {
          result = true;
          break;
        }
      }
      br.close();
      return result;
    } catch( FileNotFoundException e ) {
      throw new HydraRuntimeException( "Unable to find file: " + fn, e );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to search file: " + fn, e );
    }
  }

  /**
  *
  * Return the text line starting with the specified prefix.
  *
  */
  public static String readInFile( String fn, String prefix )
  {
    try {
      BufferedReader br = new BufferedReader( new FileReader( fn ) );
      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        if ( line.startsWith( prefix ) )
          break;
      }
      br.close();
      return line;
    } catch( FileNotFoundException e ) {
      throw new HydraRuntimeException( "Unable to find file: " + fn, e );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to read file: " + fn, e );
    }
  }

  /**
  *
  * Replace the text line starting with the specified prefix with the new text.
  *
  */
  public static void replaceInFile( String fn, String prefix, String newtxt )
  {
    try {
      BufferedReader br = new BufferedReader( new FileReader( fn ) );
      Vector lines = new Vector();
      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        if ( line.startsWith( prefix ) ) {
          line = newtxt;
        }
        lines.add( line );
      }
      br.close();

      File tf = File.createTempFile( filenameFor( fn ), ".tmp" );
      String tfn = tf.getAbsolutePath();
      if ( Log.getLogWriter().finestEnabled() ) {
        Log.getLogWriter().finest( "Writing to " + tfn );
      }
      PrintWriter pw = new PrintWriter( new BufferedWriter( new FileWriter( tf ) ) );
      for ( Iterator i = lines.iterator(); i.hasNext(); )
        pw.println( (String) i.next() );
      pw.close();

      copyFileOverTop( tfn, fn );
      deleteFile( tfn );
    } catch( FileNotFoundException e ) {
      throw new HydraRuntimeException( "Unable to find file: " + fn, e );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to modify file: " + fn, e );
    }
  }

  /**
  *
  * Read the file into a string with no newline added.
  *
  */
  public static String getContents( String fn ) throws FileNotFoundException, IOException {
    StringBuffer buf = new StringBuffer(1000);
    BufferedReader br = new BufferedReader( new FileReader( fn ) );
    String s;
    while ( ( s = br.readLine() ) != null ) {
      buf.append( s );
    }
    br.close();
    return buf.toString();
  }

  /**
   * Read the file into a string with a space where newlines have been.
   */
  public static String getContentsWithSpace(String fn)
  throws FileNotFoundException, IOException {
    StringBuffer buf = new StringBuffer(1000);
    BufferedReader br = new BufferedReader(new FileReader(fn));
    String s;
    boolean firstLine = true;
    while ((s = br.readLine()) != null) {
      if (!firstLine) {
        buf.append(" ");
      }
      buf.append(s);
      firstLine = false;
    }
    br.close();
    return buf.toString();
  }

  /**
  *
  * Read the file into a string.
  *
  */
  public static String getText( String fn ) throws FileNotFoundException, IOException {
    StringBuffer buf = new StringBuffer(1000);
    BufferedReader br = new BufferedReader( new FileReader( fn ) );
    String s;
    while ( ( s = br.readLine() ) != null ) {
      buf.append( s );
      buf.append( NEWLINE );
    }
    br.close();
    return buf.toString();
  }

  /**
   * Read the lines of the file into a list.
   */
  public static List<String> getTextAsList(String fn)
  throws FileNotFoundException, IOException {
    List<String> lines = new ArrayList();
    BufferedReader br = new BufferedReader(new FileReader(fn));
    String line;
    while ((line = br.readLine()) != null) {
      lines.add(line);
    }
    br.close();
    return lines;
  }

  /**
  *
  * Read the lines of the file into a sorted set.
  *
  */
  public static SortedSet<String> getTextAsSet( String fn ) throws FileNotFoundException, IOException {
    SortedSet lines = new TreeSet();
//    StringBuffer buf = new StringBuffer(1000);
    BufferedReader br = new BufferedReader( new FileReader( fn ) );
    String line;
    while ( ( line = br.readLine() ) != null ) {
      lines.add( line );
    }
    br.close();
    return lines;
  }

  /**
   * Read the lines of the file into a list of tokens.
   */
  public static List<String> getTextAsTokens(String fn)
  throws FileNotFoundException, IOException {
    String text = getContentsWithSpace(fn);
    List<String> tokens = new ArrayList();
    StringTokenizer tokenizer = new StringTokenizer(text, " ", false);
    while (tokenizer.hasMoreTokens()) {
      tokens.add(tokenizer.nextToken().trim());
    }
    return tokens;
  }

  /**
  *
  * Read the properties file into a sorted map.  Assumes each line of the file is of the form "key=val".
  *
  */
  public static SortedMap<String,String> getPropertiesAsMap( String fn ) throws FileNotFoundException, IOException {
    FileInputStream propFile = new FileInputStream( fn );
    try {
      Properties p = new Properties();
      p.load( propFile );
      SortedMap map = new TreeMap();
      map.putAll( p );
      return map;
    } finally {
      propFile.close();
    }
  }

  /**
  *
  * Read the properties.
  *
  */
  public static Properties getProperties( String fn ) throws FileNotFoundException, IOException {
    FileInputStream propFile = new FileInputStream( fn );
    try {
      Properties p = new Properties();
      p.load( propFile );
      return p;
    } finally {
      propFile.close();
    }
  }

  /**
   * Returns the properties in a resource.  Returns null if the resource is not found.
   */
  public static Properties getPropertiesFromResource(String fn) throws IOException {
    InputStream is = FileUtil.class.getClassLoader().getResourceAsStream(fn);
    if (is == null) {
      return null;
    } else {
      Properties p = new Properties();
      p.load(is);
      return p;
    }
  }

  /**
  *
  * Serialize the object to the specified file.
  *
  */
  public static void serialize( Object obj, String fn )
  {
    try {
      ObjectOutputStream f = new ObjectOutputStream( new FileOutputStream( fn ) );
      f.writeObject( obj );
      f.flush();
      f.close();
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Failed to serialize to: " + fn, e );
    }
  }

  /**
  *
  * Deserialize the object from the specified file.
  *
  */
  public static Object deserialize( String fn )
  {
    try {
      FileInputStream fis = new FileInputStream( fn );
      ObjectInputStream f = new ObjectInputStream( fis );
      Object obj = f.readObject();
      f.close();
      return obj;
    } catch( Exception e ) {
      throw new HydraRuntimeException( "Failed to deserialize from: " + fn, e );
    }
  }

  /**
  *
  * Serialize the specified object to a byte array.
  *
  */
  public static byte[] serialize( Object obj )
  {
    if ( obj == null ) return null;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
      ObjectOutput out = new ObjectOutputStream( baos );
      out.writeObject( obj );
      out.close();
      return baos.toByteArray();
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Failed to serialize a " + obj.getClass().getName(), e );
    }
  }

  /**
  *
  * Deserialize the object from the specified byte array.
  *
  */
  public static Object deserialize( byte[] bytes )
  {
    if ( bytes == null ) return null;
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream( bytes );
      ObjectInputStream in = new ObjectInputStream( bais );
      Object obj = in.readObject();
      in.close();
      return obj;
    } catch( Exception e ) {
      throw new HydraRuntimeException( "Failed to deserialize", e );
    }
  }
  private static final char NEWLINE = '\n';
}
