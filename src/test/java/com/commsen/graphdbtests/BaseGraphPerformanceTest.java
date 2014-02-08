
package com.commsen.graphdbtests;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.*;

public abstract class BaseGraphPerformanceTest {

	public static enum TestType {
		INSERT,
		QUERY
	}
	
	private static final String INSERT_TEST_PATTERN_ROW = "| %8d documents with %4d fields each | %15d | %15d | %15d | %20d | %15.2f |\n";

	private static final String INSERT_TEST_PATTERN_ROW_TIMEOUT = "| %8d documents with %4d fields each | %15d | %15d | %15d | (timeout) %10d | %15.2f |\n";

	private static final String INSERT_TEST_PATTERN_ROW_MERGED = "| %-135s |\n";

	private static final String INSERT_TEST_PATTERN_SEPARATOR = "| %40s | %15s | %15s | %15s | %20s | %15s |\n";

	private static final String INSERT_TEST_PATTERN_SEPARATOR_MERGED = "|-%40s---%15s---%15s---%15s---%20s---%15s-|\n";

	private static final Object[] INSERT_TEST_VALUES_SEPARATOR = new String[] {
		"----------------------------------------", "---------------", "---------------", "---------------", "--------------------", "---------------"
	};

	protected static String currentHeader = null;

	protected static TestType currentTestType = null;

	@AfterClass
	public static void printClassFooter() {

		System.out.printf(INSERT_TEST_PATTERN_SEPARATOR_MERGED, INSERT_TEST_VALUES_SEPARATOR);
    }

	protected BaseGraphPerformanceTest(TestType testType, String header) {

		if (!testType.equals(currentTestType) || !header.equals(currentHeader)) {
			currentTestType = testType;
			currentHeader = header;
			printHeader();
		}

	}

	public void printHeader() {

		switch (currentTestType) {
		case INSERT:
			printSeparator();
			System.out.printf(INSERT_TEST_PATTERN_ROW_MERGED, currentHeader);
			printSeparator();
			System.out.printf(INSERT_TEST_PATTERN_SEPARATOR, "test", "documents", "vertices", "edges", "time (ms)", "avg (doc/sec)");
			System.out.printf(INSERT_TEST_PATTERN_SEPARATOR, INSERT_TEST_VALUES_SEPARATOR);
			break;

		default:
			throw new UnsupportedOperationException();
		}
	}

	protected void printSeparator() {

		System.out.printf(INSERT_TEST_PATTERN_SEPARATOR_MERGED, INSERT_TEST_VALUES_SEPARATOR);
	}

	protected void printInsertTime(long time, long expected, long props, long v, long e) {

		printInsert(INSERT_TEST_PATTERN_ROW, time, expected, props, v, e);
	}
	
	protected void printInsertTimeout(long time, long expected, long props, long v, long e) {

		printInsert(INSERT_TEST_PATTERN_ROW_TIMEOUT, time, expected, props, v, e);
	}
	
	protected void printInsert(String pattern, long time, long expected, long props, long v, long e) {
		long docs = v + e;
		Double docsPerSecond = (double) docs / ((double) time / (double) 1000);
		System.out.printf(pattern, expected, props, docs, v, e, time, docsPerSecond);
	}

}
