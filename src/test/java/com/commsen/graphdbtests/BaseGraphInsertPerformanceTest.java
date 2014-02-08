
package com.commsen.graphdbtests;

import java.util.*;
import java.util.concurrent.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public abstract class BaseGraphInsertPerformanceTest extends BaseGraphPerformanceTest {

	public class TestResult {

		long v, e;
		boolean timeout;

        public TestResult(){
            v=0;
            e=0;
            timeout=false;
        }
		public TestResult(long v, long e, boolean timeout) {

			this.v = v;
			this.e = e;
			this.timeout = timeout;
		}

        public void add(TestResult result){
            if(v < result.v)
                v=result.v;
            //if(e < result.e)
            e+=result.e;
            timeout |= result.timeout;
        }

	}

	protected static enum ModelType {
        VERTICES_ONLY, VERTICES_AND_EDGES, EDGES_ONLY
	}

	protected static long[] executionDocumentsAmounts = new long[] {
		1000, 5000, 10000, 50000, 100000, 500000, 1000000
	};

	protected static long[] executionPropertiesAmounts = new long[] {
		/*0,*/ 2, 5//, 10//, 50
	};

    protected static int[] executionThreadsNumber = new int[] {
		1, 2, 4
    };

	protected static long TIMEOUT = 60 * 1000*10; //10min

	protected static long TIMEOUT_CHECK = 100;

	protected ModelType modelType;

	protected long numberOfDocuments;

	protected long numberOfProperties;

    protected int numberOfThreads;

	protected long startTime;

	@Parameters
	public static Collection<Object[]> getParameters() {

		final LinkedList<Object[]> params = new LinkedList<Object[]>();
            for (final Object modelTypes : ModelType.values()) {
                for (final int threads : executionThreadsNumber) {
                    for (final long docs : executionDocumentsAmounts) {
                        for (final long properties : executionPropertiesAmounts) {
                            params.add(new Object[] {
                                docs, properties, modelTypes, threads
                            });
                        }
                    }

                }
            }

		return params;
	}

	public BaseGraphInsertPerformanceTest(final long numberOfDocs, final long numberOfProperties, final ModelType modelType, final int numberOfThreads) {

		super(TestType.INSERT, "Test inserting " + modelType+" threads "+numberOfThreads);

		this.numberOfDocuments = numberOfDocs;
		this.numberOfProperties = numberOfProperties;
		this.modelType = modelType;
        this.numberOfThreads = numberOfThreads;
	}

	protected void printInsertTimeout(final long v, final long e) {

		super.printInsertTimeout((System.currentTimeMillis() - startTime), numberOfDocuments, numberOfProperties, v, e);
	}

	protected void printInsertTime(final long v, final long e) {

		super.printInsertTime((System.currentTimeMillis() - startTime), numberOfDocuments, numberOfProperties, v, e);
	}

	@Test
	public void addDocuments() {

		startTime = System.currentTimeMillis();

        TestResult result;

        result = doAddDocuments_();

        if(result == null)
            return;

		if (result.timeout) {
			printInsertTimeout(result.v, result.e);
		}
		else {
			printInsertTime(result.v, result.e);
		}
		
		if (numberOfProperties == executionPropertiesAmounts[executionPropertiesAmounts.length - 1]) {
			printSeparator();
		}

	}

    private TestResult doAddDocuments_(){
        TestResult result = new TestResult();
        //final BrokenUniqueIdGenerator domainObject = new BrokenUniqueIdGenerator();
        Callable<TestResult> task = new Callable<TestResult>() {
            @Override
            public TestResult call() {
                return doAddDocuments();
            }
        };
        List<Callable<TestResult>> tasks = Collections.nCopies(numberOfThreads, task);
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<TestResult>> futures;
        try {
            futures = executorService.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
        // Check for exceptions
        for (Future<TestResult> future : futures) {
            // Throws an exception if an exception was thrown by the task.
            try {
                result.add(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            } catch (ExecutionException e) {
                e.printStackTrace();
                return null;
            }
        }
        return result;
    }
 	protected abstract TestResult doAddDocuments();

}
