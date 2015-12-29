/*
  Copyright and Licensing Information for ACE(TM), TAO(TM), CIAO(TM),
  DAnCE(TM), and CoSMIC(TM)

   [1]ACE(TM), [2]TAO(TM), [3]CIAO(TM), DAnCE(TM), and [4]CoSMIC(TM)
   (henceforth referred to as "DOC software") are copyrighted by
   [5]Douglas C. Schmidt and his [6]research group at [7]Washington
   University, [8]University of California, Irvine, and [9]Vanderbilt
   University, Copyright (c) 1993-2015, all rights reserved. Since DOC
   software is open-source, freely available software, you are free to
   use, modify, copy, and distribute--perpetually and irrevocably--the
   DOC software source code and object code produced from the source, as
   well as copy and distribute modified versions of this software. You
   must, however, include this copyright statement along with any code
   built using DOC software that you release. No copyright statement
   needs to be provided if you just ship binary executables of your
   software products.

   You can use DOC software in commercial and/or binary software releases
   and are under no obligation to redistribute any of your source code
   that is built using DOC software. Note, however, that you may not
   misappropriate the DOC software code, such as copyrighting it yourself
   or claiming authorship of the DOC software code, in a way that will
   prevent DOC software from being distributed freely using an
   open-source development model. You needn't inform anyone that you're
   using DOC software in your software, though we encourage you to let
   [10]us know so we can promote your project in the [11]DOC software
   success stories.

   The [12]ACE, [13]TAO, [14]CIAO, [15]DAnCE, and [16]CoSMIC web sites
   are maintained by the [17]DOC Group at the [18]Institute for Software
   Integrated Systems (ISIS) and the [19]Center for Distributed Object
   Computing of Washington University, St. Louis for the development of
   open-source software as part of the open-source software community.
   Submissions are provided by the submitter ``as is'' with no warranties
   whatsoever, including any warranty of merchantability, noninfringement
   of third party intellectual property, or fitness for any particular
   purpose. In no event shall the submitter be liable for any direct,
   indirect, special, exemplary, punitive, or consequential damages,
   including without limitation, lost profits, even if advised of the
   possibility of such damages. Likewise, DOC software is provided as is
   with no warranties of any kind, including the warranties of design,
   merchantability, and fitness for a particular purpose,
   noninfringement, or arising from a course of dealing, usage or trade
   practice. Washington University, UC Irvine, Vanderbilt University,
   their employees, and students shall have no liability with respect to
   the infringement of copyrights, trade secrets or any patents by DOC
   software or any part thereof. Moreover, in no event will Washington
   University, UC Irvine, or Vanderbilt University, their employees, or
   students be liable for any lost revenue or profits or other special,
   indirect and consequential damages.

   DOC software is provided with no support and without any obligation on
   the part of Washington University, UC Irvine, Vanderbilt University,
   their employees, or students to assist in its use, correction,
   modification, or enhancement. A [20]number of companies around the
   world provide commercial support for DOC software, however. DOC
   software is Y2K-compliant, as long as the underlying OS platform is
   Y2K-compliant. Likewise, DOC software is compliant with the new US
   daylight savings rule passed by Congress as "The Energy Policy Act of
   2005," which established new daylight savings times (DST) rules for
   the United States that expand DST as of March 2007. Since DOC software
   obtains time/date and calendaring information from operating systems
   users will not be affected by the new DST rules as long as they
   upgrade their operating systems accordingly.

   The names ACE(TM), TAO(TM), CIAO(TM), DAnCE(TM), CoSMIC(TM),
   Washington University, UC Irvine, and Vanderbilt University, may not
   be used to endorse or promote products or services derived from this
   source without express written permission from Washington University,
   UC Irvine, or Vanderbilt University. This license grants no permission
   to call products or services derived from this source ACE(TM),
   TAO(TM), CIAO(TM), DAnCE(TM), or CoSMIC(TM), nor does it grant
   permission for the name Washington University, UC Irvine, or
   Vanderbilt University to appear in their names.

   If you have any suggestions, additions, comments, or questions, please
   let [21]me know.

   [22]Douglas C. Schmidt
*/

/*
 * Modified Spawn.hpp from ACE for GemFireXD tests.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. licensed under
 * the same license as the original file. All rights reserved.
 */
#ifndef __TEST_FW_SPAWN_H__
#define __TEST_FW_SPAWN_H__
// Spawn.cpp,v 1.4 2004/01/07 22:40:16 shuston Exp

// @TODO, this out this include list..
#include "ace/OS_NS_stdio.h"
#include "ace/OS_NS_fcntl.h"
#include "ace/OS_NS_pwd.h"
#include "ace/os_include/os_pwd.h"
#include "ace/OS_NS_stdlib.h"
#include "ace/OS_NS_string.h"
#include "ace/OS_NS_unistd.h"

#if defined(_WIN32)
#if (FD_SETSIZE != 1024)
  +++ bad fdsetsize...
#endif
#endif

#include "ace/Process.h"
#include "ace/Log_Msg.h"

namespace dunit {

// Listing 1 code/ch10
class Manager : virtual public ACE_Process
{
public:
  Manager (const ACE_TCHAR* program_name)
  : ACE_Process()
  {
    ACE_OS::strcpy (programName_, program_name);
  }

  virtual int doWork (void)
  {
    // Spawn the new process; prepare() hook is called first.
    ACE_Process_Options options;
    pid_t pid = this->spawn (options);
    if (pid == -1)
      ACE_ERROR_RETURN((LM_ERROR, ACE_TEXT ("%p\n"),
                        ACE_TEXT ("spawn")), -1);
    return pid;
  }

  virtual int doWait (void)
  {
    // Wait forever for my child to exit.
    if (this->wait () == -1)
      ACE_ERROR_RETURN ((LM_ERROR, ACE_TEXT ("%p\n"),
                         ACE_TEXT ("wait")), -1);

    // Dump whatever happened.
    this->dumpRun ();
    return 0;
  }
// Listing 1

protected:
  // Listing 3 code/ch10
  virtual int dumpRun (void)
  {
    if (ACE_OS::lseek (this->outputfd_, 0, SEEK_SET) == -1)
      ACE_ERROR_RETURN ((LM_ERROR, ACE_TEXT ("%p\n"),
                         ACE_TEXT ("lseek")), -1);

    char buf[1024];
    int length = 0;

    // Read the contents of the error stream written
    // by the child and print it out.
    while ((length = (int)ACE_OS::read (this->outputfd_,
                                   buf, sizeof(buf)-1)) > 0)
      {
        buf[length] = 0;
        ACE_DEBUG ((LM_DEBUG, ACE_TEXT ("%C\n"), buf));
      }

    ACE_OS::close (this->outputfd_);
    return 0;
  }
 // Listing 3

 // Listing 2 code/ch10
  // prepare() is inherited from ACE_Process.
  virtual int prepare (ACE_Process_Options &options)
  {
    options.command_line ("%s", this->programName_);
    if (this->setStdHandles (options) == -1 ||
        this->setEnvVariable (options) == -1)
      return -1;
    return 0;
  }

  virtual int setStdHandles (ACE_Process_Options &options)
  {
    ACE_OS::unlink (this->programName_ );
    this->outputfd_ =
      ACE_OS::open (this->programName_, O_RDWR | O_CREAT);
    return options.set_handles
      (ACE_STDIN, ACE_STDOUT, this->outputfd_);
  }

  virtual int setEnvVariable (ACE_Process_Options &options)
  {
    return options.setenv ("PRIVATE_VAR=/that/seems/to/be/it");
  }
 // Listing 2

  private:

protected:
  virtual ~Manager( ) { }

private:

  ACE_HANDLE outputfd_;
  ACE_TCHAR programName_[256];
};

}; // namespace dunit.

#endif // __TEST_FW_SPAWN_H__

