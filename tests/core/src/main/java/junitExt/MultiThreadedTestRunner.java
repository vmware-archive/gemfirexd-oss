/*
 * @(#)MultiThreadedTestRunner.java
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
 * A framework which allows for an array of tests to be
 * run asynchronously.  TestCases should reference this class in a test
 * method.
 * <P>
 * <B>Update for July 9, 2003:</B> now, you can also register
 * {@link TestRunnable} instances as monitors (request 771008); these run
 * parallel with the standard <tt>TestRunnable</tt> instances, but they only
 * quit when all of the standard <tt>TestRunnable</tt> instances end.
 * <P>
 * Fixed bugs 771000 and 771001: spawned threads are now Daemon threads,
 * and all "wild threads" (threads that just won't stop) are
 * {@link Thread#stop()}ed.
 * <P>
 * All these changes have made this class rather fragile, as there are
 * many threaded timing issues to deal with.  Expect future refactoring
 * with backwards compatibility.
 *
 * @author Matt Albrecht <a HREF="mailto:groboclown@users.sourceforge.net">groboclown@users.sourceforge.net</a>
 * @since Jan 14, 2002
 * @version $Date: 2003/10/03 14:26:45 $
 */
public class MultiThreadedTestRunner {
  private static final Class  THIS_CLASS = MultiThreadedTestRunner.class;
  private static final String  THIS_CLASS_NAME = THIS_CLASS.getName();
  //private static final Logger LOG = Logger.getLogger( THIS_CLASS );

  private static final long DEFAULT_MAX_FINAL_JOIN_TIME = 30l * 1000l;
  private static final long DEFAULT_MAX_WAIT_TIME = 24l * 60l * 60l * 1000l;
  private static final long MIN_WAIT_TIME = 10l;

  private Object  synch = new Object ();
  private boolean threadsFinished = false;
  private ThreadGroup  threadGroup;
  private Thread  coreThread;
  private Throwable  exception;
  private TestRunnable runners[];
  private TestRunnable monitors[];
  private long maxFinalJoinTime = DEFAULT_MAX_FINAL_JOIN_TIME;
  private long maxWaitTime = DEFAULT_MAX_WAIT_TIME;
  private boolean performKills = true;


  /**
   * Create a new utility instance with the given set of parallel runners.
   * All runners passed into this method must end on their own, else it's
   * an error.
   */
  public MultiThreadedTestRunner( TestRunnable tr[] )
  {
    this( tr, null );
  }


  /**
   * Create a new utility instance with the given set of parallel runners
   * and a set of monitors. The runners must end on their own, but the
   * monitors can run until told to stop.
   *
   * @param runners a non-null, non-empty collection of test runners.
   * @param monitors a list of monitor runners, which may be <tt>null</tt> or
   * empty.
   */
  public MultiThreadedTestRunner( TestRunnable runners[],
      TestRunnable monitors[] )
  {
    if (runners == null)
    {
      throw new IllegalArgumentException ("no null runners");
    }
    int len = runners.length;
    if (len <= 0)
    {
      throw new IllegalArgumentException (
          "must have at least one runnable");
    }
    this.runners = new TestRunnable[ len ];
    System.arraycopy( runners, 0, this.runners, 0, len );

    if (monitors != null)
    {
      len = monitors.length;
      this.monitors = new TestRunnable[ len ];
      System.arraycopy( monitors, 0, this.monitors, 0, len );
    }
    else
    {
      this.monitors = new TestRunnable[ 0 ];
    }
  }


  /**
   * Run each test given in a separate thread. Wait for each thread
   * to finish running, then return.
   * <P>
   * As of July 9, 2003, this method will not wait forever, but rather
   * will wait for the internal maximum run time, which is by default
   * 24 hours; for most unit testing scenarios, this is more than
   * sufficient.
   *
   * @exception Throwable thrown on a test run if a threaded task
   * throws an exception.
   */
  public void runTestRunnables()
  throws Throwable 
  {
    runTestRunnables( -1 );
  }


  /**
   * Runs each test given in a separate thread. Waits for each thread
   * to finish running (possibly killing them), then returns.
   *
   * @param runnables the list of TestCaseRunnable objects to run
   * asynchronously
   * @param maxTime the maximum amount of milliseconds to wait for
   * the tests to run. If the time is &lt;= 0, then the tests
   * will run until they are complete. Otherwise, any threads that
   * don't complete by the given number of milliseconds will be killed,
   * and a failure will be thrown.
   * @exception Throwable thrown from the underlying tests if they happen
   * to cause an error.
   */
  public void runTestRunnables( long maxTime )
  throws Throwable 
  {
    // Ensure we aren't interrupted.
    // This can happen from one test execution to the next, if an
    // interrupt was poorly timed on the core thread. Calling
    // Thread.interrupted() will clear the interrupted status flag.
    Thread.interrupted();

    // initialize the data.
    this.exception = null;
    this.coreThread = Thread.currentThread();
    this.threadGroup = new ThreadGroup ( THIS_CLASS_NAME );
    this.threadsFinished = false;

    // start the monitors before the runners
    Thread  monitorThreads[] = setupThreads(
        this.threadGroup, this.monitors );
    Thread  runnerThreads[] = setupThreads(
        this.threadGroup, this.runners );

    // catch the IE exception outside the loop so that an exception
    // thrown in a thread will kill all the other threads.
    boolean threadsStillRunning;
    try
    {
      threadsStillRunning = joinThreads( runnerThreads, maxTime );
    }
    catch (InterruptedException  ie)
    {
      // Thread join interrupted: some runner or monitor caused an
      // exception. Note that this is NOT a timeout!
      threadsStillRunning = true;
    }
    finally
    {
      synchronized (this.synch)
      {
        if (!this.threadsFinished)
        {
          interruptThreads();
        }
        else
        {
          //LOG.debug( "All threads finished within timeframe." );
        }
      }
    }

    if (threadsStillRunning)
    {
      //LOG.debug( "Halting the test threads." );

      // threads are still running. If no exception was generated,
      // then set a timeout error to indicate some threads didn't
      // end in time.
      setTimeoutError( maxTime );

      // kill any remaining threads
      try
      {
        // but give them one last chance!
        joinThreads( runnerThreads,
            maxFinalJoinTime );
      }
      catch (InterruptedException  ie)
      {
        // someone caused a real exception. This is NOT a timeout!
      }
      int killCount = killThreads( runnerThreads );
      if (killCount > 0)
      {
        //LOG.fatal( killCount+" thread(s) did not stop themselves." );
        setTimeoutError( maxFinalJoinTime );
      }
    }

    // Stop the monitor threads - they have a time limit!
    //LOG.debug("Halting the monitor threads.");
    try
    {
      joinThreads( monitorThreads, maxFinalJoinTime );
    }
    catch (InterruptedException  ex)
    {
      // don't cause a timeout error with monitor threads.
    }
    killThreads( monitorThreads );


    if (this.exception != null)
    {
      // an exception/error occurred during the test, so throw
      // the exception so it is reported by the owning test
      // correctly.
      //LOG.debug( "Exception occurred during testing.", this.exception );
      throw this.exception;
    }
    //LOG.debug( "No exceptions caused during execution." );
  }


  /**
   * Handles an exception by sending them to the test results. Called by
   * runner or monitor threads.
   */
  void handleException( Throwable  t )
  {
    //LOG.warn( "A test thread caused an exception.", t );
    synchronized( this.synch )
    {
      if (this.exception == null)
      {
        //LOG.debug("Setting the exception to:",t);
        this.exception = t;
      }

      if (!this.threadsFinished)
      {
        interruptThreads();
      }
    }

    if (t instanceof ThreadDeath )
    {
      // rethrow ThreadDeath after they have been registered
      // and the threads have been signaled to halt.
      throw (ThreadDeath )t;
    }
  }


  /**
   * Stops all running test threads. Called by runner or monitor threads.
   */
  void interruptThreads()
  {
    //LOG.debug("Forcing all test threads to stop.");
    synchronized (this.synch)
    {
      // interrupt the core thread (that might be doing a join)
      // first, so that it doesn't accidentally do a join on
      // other threads that were interrupted.
      if (Thread.currentThread() != this.coreThread)
      {
        this.coreThread.interrupt();
      }

      this.threadsFinished = true;

      int count = this.threadGroup.activeCount();
      Thread  t[] = new Thread [ count ];
      this.threadGroup.enumerate( t );
      for (int i = t.length; --i >= 0;)
      {
        if (t[i] != null && t[i].isAlive())
        {
          t[i].interrupt();
        }
      }
    }
  }


  /**
   * Used by the TestRunnable instances to tell if the parallel execution
   * has stopped or is stopping.
   */
  boolean areThreadsFinished()
  {
    return this.threadsFinished;
  }


  /**
   * Sets up the threads for the given runnables and starts them.
   */
  private Thread [] setupThreads( ThreadGroup  tg, TestRunnable tr[] )
  {
    int len = tr.length;
    Thread  threads[] = new Thread [ len ];
    for (int i = 0; i < len; ++i)
    {
      tr[i].setTestRunner( this );
      threads[i] = new Thread ( tg, tr[i] );
      threads[i].setDaemon( true );
    }
    for (int i = 0; i < len; ++i)
    {
      threads[i].start();

      // wait for the threads to actually start. If we wait 10
      // times and still no dice, I expect the test already started
      // and finished.
      int count = 0;
      while (!threads[i].isAlive() && count < 10)
      {
        //LOG.debug("Waiting for thread at index "+i+" to start.");
        Thread.yield();
        ++count;
      }
      if (count >= 10)
      {
        //LOG.debug("Assuming thread at index "+i+" already finished.");
      }
    }
    return threads;
  }


  /**
   * This joins all the threads together. If the max time is exceeded,
   * then <tt>true</tt> is returned. This method is only called by the core
   * thread. The thread array will be altered at return time to only contain
   * threads which are still active (all other slots will be <tt>null</tt>).
   * <P>
   * This routine allows us to attempt to collect all the halted threads
   * together, while not waiting forever on threads that poorly don't
   * respond to outside stimuli (and thus require a stop() on the
   * thread).
   */
  private boolean joinThreads( Thread  t[], long waitTime )
  throws InterruptedException 
  {
    // check the arguments
    if (t == null)
    {
      return false;
    }
    int len = t.length;
    if (len <= 0)
    {
      return false;
    }
    if (waitTime < 0 || waitTime > maxWaitTime)
    {
      waitTime = DEFAULT_MAX_WAIT_TIME;
    }

    // slowly halt the threads.
    boolean threadsRunning = true;
    InterruptedException  iex = null;
    long finalTime = System.currentTimeMillis() + waitTime;
    while (threadsRunning && System.currentTimeMillis() < finalTime &&
        iex == null)
    {
      //LOG.debug("Time = "+System.currentTimeMillis()+"; final = "+finalTime);
      threadsRunning = false;

      // There might be circumstances where
      // the time between entering the while loop and entering the
      // for loop exceeds the final time, which can cause an incorrect
      // threadsRunning value. That's why this boolean exists. Note
      // that since we put in the (len <= 0) test above, we don't
      // have to worry about another edge case where the length prevents
      // the loop from being entered.
      boolean enteredLoop = false;

      for (int i = 0;
      i < len && System.currentTimeMillis() < finalTime;
      ++i)
      {
        enteredLoop = true;
        if (t[i] != null)
        {
          try
          {
            // this will yield our time, so we don't
            // need any explicit yield statement.
            t[i].join( MIN_WAIT_TIME );
          }
          catch (InterruptedException  ex)
          {
            //LOG.debug("Join for thread at index "+i+" was interrupted.");
            iex = ex;
          }
          if (!t[i].isAlive())
          {
            //LOG.debug("Joined thread at index "+i);
            t[i] = null;
          }
          else
          {
            //LOG.debug("Thread at index "+i+" still running.");
            threadsRunning = true;
          }
        }
      }

      // If the threadsRunning is true, it remains true. If
      // the enteredLoop is false, this will be true.
      threadsRunning = threadsRunning || !enteredLoop;
    }
    if (iex != null)
    {
      throw iex;
    }
    return threadsRunning;
  }


  /**
   * This will execute a stop() on all non-null, alive threads in the list.
   *
   * @return the number of threads killed
   */
  private int killThreads( Thread [] t )
  {
    int killCount = 0;
    for (int i = 0; i < t.length; ++i)
    {
      if (t[i] != null && t[i].isAlive())
      {
        //LOG.debug("Stopping thread at index "+i);
        ++killCount;
        if (this.performKills)
        {
          // Yes, this is deprecated API, but we give the threads
          // "sufficient" warning to stop themselves.
          int count = 0;
          boolean isAlive = t[i].isAlive();
          while (isAlive && count < 10)
          {
            // send an InterruptedException, as this is handled
            // specially in the TestRunnable.
            t[i].stop(
                new TestDeathException(
                    "Thread "+i+" did not die on its own" ) );
            //LOG.debug( "Waiting for thread at index "+i+" to stop.");
            Thread.yield();
            isAlive = t[i].isAlive();

            if (isAlive)
            {
              // it may have been in a sleep state, so
              // make it shake a leg!
              t[i].interrupt();
            }
            ++count;
          }
          if (count >= 10)
          {
            //LOG.fatal("Thread at index "+i+" did not stop!" );
          }
          t[i] = null;
        }
        else
        {
          //LOG.fatal( "Did not stop thread "+t[i] );
        }
      }
    }
    return killCount;
  }


  private void setTimeoutError( long maxTime )
  {
    Throwable  t = createTimeoutError( maxTime );
    synchronized (this.synch)
    {
      if (this.exception == null)
      {
        //LOG.debug("Setting the exception to a timeout exception.");
        this.exception = t;
      }
    }
  }


  private Throwable  createTimeoutError( long maxTime )
  {
    Throwable  ret = null;
    // need to set the exception to a timeout
    try
    {
      Assert.fail( "Threads did not finish within " +
          maxTime + " milliseconds.");
    }
    catch (ThreadDeath  td)
    {
      // never trap these
      throw td;
    }
    catch (Throwable  t)
    {
      t.fillInStackTrace();
      ret = t;
    }
    return ret;
  }


  /**
   * An exception that declares that the test has been stop()ed.
   */
  public static final class TestDeathException extends RuntimeException 
  {
    protected/*GemStoneAddition*/ TestDeathException( String  msg )
    {
      super( msg );
    }
  }
}
