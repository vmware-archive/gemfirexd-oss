/*
 * @(#)TestRunnable.java
 *
 * The basics are taken from an article by Andy Schneider
 * andrew.schneider@javaworld.com
 * The article is "JUnit Best Practices"
 * http://www.javaworld.com/javaworld/jw-12-2000/jw-1221-junit_p.html
 *
 * Part of the GroboUtils package at:
 * http://groboutils.sourceforge.net
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"),
 *  to deal in the Software without restriction, including without limitation
 *  the rights to use, copy, modify, merge, publish, distribute, sublicense,
 *  and/or sell copies of the Software, and to permit persons to whom the
 *  Software is furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 *  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */
package junitExt;

import junit.framework.Assert;

/**
 * @author kbanks
 *
 */
public abstract class TestRunnable extends Assert implements Runnable {
  private static final Class THIS_CLASS = TestRunnable.class;
  private static int testCount = 0;
  
  private MultiThreadedTestRunner mttr;
  private int testIndex;
  private boolean ignoreStopErrors = false;

  public TestRunnable()
  {
    synchronized( THIS_CLASS )
    {
     this.testIndex = testCount++;
    }
  }
  TestRunnable( boolean ignoreStopErrors )
  {
    this();
    this.ignoreStopErrors = ignoreStopErrors;
  }
  
  /**
   * Performs the set of processing or checks on the object-under-test,
   * which will be in parallel with other <tt>TestRunnable</tt>
   * instances.
   * <P>
   * The implementation should be responsive to
   * <tt>InterruptedException</tt> exceptions or to the status of
   * <tt>Thread.isInterrupted()</tt>, as that is the signal used to tell
   * running <tt>TestRunnable</tt> instances to halt their processing;
   * instances which do not stop within a reasonable time frame will
   * have <tt>Thread.stop()</tt> called on them.
   * <P>
   * Non-monitor instances must have this method implemented such that
   * it runs in a finite time; if any instance executes over the
   * <tt>MultiThreadedTestRunner</tt> instance maximum time limit, then
   * the <tt>MultiThreadedTestRunner</tt> instance assumes that a
   * test error occurred.
   *
   * @exception Throwable any exception may be thrown and will be
   * reported as a test failure, except for
   * <tt>InterruptedException</tt>s, which will be ignored.
   */
  public abstract void runTest() throws Throwable;
  
  public final void run()
  {
    if (this.mttr == null)
    {
      throw new IllegalStateException(
          "Owning runner never defined. The runnables should only be "+
      "started through the MultiThreadedTestRunner instance." );
    }
    try
    {
      runTest();
    } catch (InterruptedException ie)
    { 
      // ignore these exceptions - they represent the MTTR
      // interrupting the tests.
    }
    catch (MultiThreadedTestRunner.TestDeathException tde)
    {
      // ignore these exceptions as they relate to thread-related
      // exceptions. These represent the MTTR stopping us.
      // Our response is to actually rethrow the exception.
      if (!this.ignoreStopErrors)
      {
        throw tde;
      }
    }
    catch (Throwable t)
    {
      // for any exception, handle it and interrupt the
      // other threads

      // Note that ThreadDeath exceptions must be re-thrown after
      // the interruption has occured.
      this.mttr.handleException( t );
    }
  }


  /**
   * Returns the status of the owning <tt>MultiThreadedTestRunner</tt>
   * instance: <tt>true</tt> means that the tests have completed (monitors
   * may still be active), and <tt>false</tt> means that the tests are
   * still running.
   *
   * @return <tt>true</tt> if the tests have completed their run,
   * otherwise <tt>false</tt>.
   */
  public boolean isDone()
  {
    return this.mttr.areThreadsFinished();
  }

  void setTestRunner( MultiThreadedTestRunner mttr )
  {
    this.mttr = mttr;
  }
}
