/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.store.access.FileResource

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.iapi.store.access;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.io.StorageFile;

import java.io.InputStream;

/**
	Management of file resources within	a database. Suitable for jar
	files, images etc.

	<P>A file resource is identified by the pair (name,generationId).
	Name is an arbitrary String supplied by the caller. GenerationId
	is a non-repeating sequence number constructed by the database.
	Within a database a	(name,generationId) pair uniquely identifies
	a version of a file resource for all time. Newer generation
	numbers reflect newer versions of the file.

	<P>A database supports the concept of a designated current version
	of a fileResource. The management of the current version is
	transactional. The following rules apply
	<OL>
	<LI>Adding a FileResource makes the added version the current
	version
	<LI>Removing a FileResource removes the current version of the
	resource. After this operation the database holds no current
	version of the FileResoure.
	<LI>Replacing a FileResource removes the current version of the
	resource.
	</OL>
	
	<P>For the benefit of replication, a database optionally retains 
	historic versions of stored files. These old versions are
	useful when processing old transactions in the stage. 
*/
public interface FileResource {

    /**
       The name of the jar directory
    */
    public static final String JAR_DIRECTORY_NAME = "jar";

	/**
	  Add a file resource, copying from the input stream.
	  
	  The InputStream will be closed by this method.
	  @param name the name of the file resource.
	  @param source an input stream for reading the content of
	         the file resource.
	  @return the generationId for the file resource. This
	  quantity increases when you replace the file resource.

	  @exception StandardException some error occured.
	*/
    // Gemstone changes BEGIN
    // Adding lcc
	public long add(String name, InputStream source, LanguageConnectionContext lcc)
		throws StandardException;
    // Gemstone changes END

	/**
	  Remove the current generation of a file resource from
	  the database.

	  @param name the name of the fileResource to remove.
	  
	  @exception StandardException some error occured.
	  */
	// Gemstone changes BEGIN
	    // Adding lcc
	public void remove(String name, long currentGenerationId, LanguageConnectionContext lcc)
		throws StandardException;
	// Gemstone changes END

	/**
	  Replace a file resource with a new version.

	  <P>The InputStream will be closed by this method.

	  @param name the name of the file resource.
	  @param source an input stream for reading the content of
	  the file resource.
	  @return the generationId for the new 'current' version of the
	          file resource. 
	  @exception StandardException some error occured.
	*/
	// Gemstone changes BEGIN
	// Adding lcc
  public long replace(String name, long currentGenerationId,
      InputStream source, LanguageConnectionContext lcc)
      throws StandardException;
	
	public long add(String name, byte[] source, long id)
			throws StandardException;

		/**
		  Remove the current generation of a file resource from
		  the database.

		  @param name the name of the fileResource to remove.
		  
		  @exception StandardException some error occured.
		  */
		public void remove(String name, long currentGenerationId, boolean remote)
			throws StandardException;

		/**
		  Replace a file resource with a new version.

		  <P>The InputStream will be closed by this method.

		  @param name the name of the file resource.
		  @param source an input stream for reading the content of
		  the file resource.
		  @return the generationId for the new 'current' version of the
		          file resource. 
		  @exception StandardException some error occured.
		*/
		public long replace(String name, long currentGenerationId, long newGenerationId, byte[] source)
			throws StandardException;
		//GEmstone changes END
	/**
	  Get the StorageFile for a file resource.
	  
	  @param name The name of the fileResource
	  @param generationId the generationId of the fileResource
	  
	  @return A StorageFile object representing the file.
	  */
	public StorageFile getAsFile(String name, long generationId);

    /**
     * @return the separator character to be used in file names.
     */
    public char getSeparatorChar();
}
