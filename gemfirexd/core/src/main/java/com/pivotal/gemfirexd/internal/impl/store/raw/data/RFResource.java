/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.store.raw.data.RFResource

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

package com.pivotal.gemfirexd.internal.impl.store.raw.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.services.daemon.Serviceable;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.store.access.AccessFactoryGlobals;
import com.pivotal.gemfirexd.internal.iapi.store.access.FileResource;
import com.pivotal.gemfirexd.internal.iapi.store.raw.xact.RawTransaction;
import com.pivotal.gemfirexd.internal.io.StorageFile;

class RFResource implements FileResource {

	private final BaseDataFileFactory factory;

	RFResource(BaseDataFileFactory dataFactory) {
		this.factory = dataFactory;
	}

	/**
	  @see FileResource#add
	  @exception StandardException Oops
	*/
	public long add(String name, InputStream source, LanguageConnectionContext lcc)
		 throws StandardException
	{
		OutputStream os = null;

		if (factory.isReadOnly())
        {
			throw StandardException.newException(SQLState.FILE_READ_ONLY);
        }

		long generationId = factory.getNextId();

		try
		{
			StorageFile file = getAsFile(name, generationId);
            if (file.exists())
            {
				throw StandardException.newException(
                        SQLState.FILE_EXISTS, file);
            }

            ContextManager cm = 
                ContextService.getFactory().getCurrentContextManager();

            RawTransaction tran = 
                factory.getRawStoreFactory().getXactFactory().findUserTransaction(
                        factory.getRawStoreFactory(), 
                        cm, 
                        AccessFactoryGlobals.USER_TRANS_NAME);
            
            // Block the backup, If backup is already in progress wait 
            // for the backup to finish. Jar files are unlogged but the 
            // changes to the  references to the jar file in the catalogs 
            // is logged. A consistent backup can not be made when jar file 
            // is being added.

            tran.blockBackup(true);

			StorageFile directory = file.getParentDir();
            if (!directory.exists())
			{
                if (!directory.mkdirs())
                {
					throw StandardException.newException(
                            SQLState.FILE_CANNOT_CREATE_SEGMENT, directory);
                }
			}

            os = file.getOutputStream();
			byte[] data = new byte[4096];
			int len;

			factory.writeInProgress();
			try
			{
				while ((len = source.read(data)) != -1) {
					os.write(data, 0, len);
				}
                factory.writableStorageFactory.sync( os, false);
			}
			finally
			{
				factory.writeFinished();
			}
		}

		catch (IOException ioe)
		{
			throw StandardException.newException(
                    SQLState.FILE_UNEXPECTED_EXCEPTION, ioe);
		}

		finally
		{
			try {
				if (os != null) {
					os.close();
				}
			} catch (IOException ioe2) {/*RESOLVE: Why ignore this?*/}

			try {
				if (source != null)source.close();
			} catch (IOException ioe2) {/* RESOLVE: Why ignore this?*/}
		}
		
		return generationId;
	}

	/**
	  @see FileResource#remove
	  @exception StandardException Oops
	  */
	public void remove(String name, long currentGenerationId, LanguageConnectionContext lcc)
		throws StandardException
	{
		if (factory.isReadOnly())
			throw StandardException.newException(SQLState.FILE_READ_ONLY);

			
		ContextManager cm = ContextService.getFactory().getCurrentContextManager();

        RawTransaction tran = 
            factory.getRawStoreFactory().getXactFactory().findUserTransaction(
                        factory.getRawStoreFactory(), 
                        cm, 
                        AccessFactoryGlobals.USER_TRANS_NAME);
                    
        // Block the backup, If backup is already in progress wait 
        // for the backup to finish. Jar files are unlogged but the 
        // changes to the  references to the jar file in the catalogs 
        // is logged. A consistent backup can not be made when jar file 
        // is being removed.

        tran.blockBackup(true);

		tran.logAndDo(new RemoveFileOperation(name, currentGenerationId, true));

		Serviceable s = new RemoveFile(getAsFile(name, currentGenerationId));

	    tran.addPostCommitWork(s);
	}

	/**
	  @see FileResource#replace
	  @exception StandardException Oops
	  */
	public long replace(String name, long currentGenerationId, InputStream source, LanguageConnectionContext lcc)
		throws StandardException
	{
		if (factory.isReadOnly())
			throw StandardException.newException(SQLState.FILE_READ_ONLY);

		remove(name, currentGenerationId, lcc);

		long generationId = add(name, source, lcc);

		return generationId;
	}


	/**
	  @see FileResource#getAsFile
	  */
	public StorageFile getAsFile(String name, long generationId)
	{
		String versionedFileName = factory.getVersionedName(name, generationId);

		return factory.storageFactory.newStorageFile( versionedFileName);
	}

    public char getSeparatorChar()
    {
        return factory.storageFactory.getSeparator();
    }

    // Gemstone changes BEGIN
	@Override
	public long add(String name, byte[] source, long id)
			throws StandardException {
		throw new UnsupportedOperationException("this add overload " +
				"method is supported only for GfxdJarResource");
	}

	@Override
	public void remove(String name, long currentGenerationId, boolean remote)
			throws StandardException {
		throw new UnsupportedOperationException("this remove overload " +
				"method is supported only for GfxdJarResource");
	}

	@Override
	public long replace(String name, long currentGenerationId,
			long newGenerationId, byte[] source) throws StandardException {
		throw new UnsupportedOperationException("this replace overload " +
				"method is supported only for GfxdJarResource");
	}
	// Gemstone changes END
} // end of class RFResource


final class RemoveFile implements Serviceable, PrivilegedExceptionAction
{
	private final StorageFile fileToGo;

	RemoveFile(StorageFile fileToGo)
    {
		this.fileToGo = fileToGo;
	}

	public int performWork(ContextManager context)
        throws StandardException
    {
        try {
            AccessController.doPrivileged(this);
        } catch (PrivilegedActionException e) {
            throw (StandardException) (e.getException());
         }
        return Serviceable.DONE;
	}

	public boolean serviceASAP()
    {
		return false;
	}

    /**
     * File deletion is a quick operation and typically releases substantial
     * amount of space very quickly, this work should be done on the
     * user thread. 
     * @return true, this work needs to done on user thread. 
     */
	public boolean serviceImmediately()
	{
		return true;
	}

    public Object run() throws StandardException {
        // SECURITY PERMISSION - MP1, OP5
        if (fileToGo.exists()) {
            if (!fileToGo.delete()) {
                throw StandardException.newException(
                        SQLState.FILE_CANNOT_REMOVE_FILE, fileToGo);
            }
        }
        return null;
    }	
}
