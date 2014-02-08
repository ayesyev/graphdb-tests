
package com.commsen.graphdbtests.neo4j;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

public class Neo4jUtil {

	static class ShutdownHook extends Thread {

		public void run() {

			db.shutdown();
		}
	}

	public static String dbUrl = "/tmp/graphdb_tests_neo4j";

	private static GraphDatabaseService db = null;

	private static ShutdownHook hook = null;

	public static void createDB() {

		db = new GraphDatabaseFactory().newEmbeddedDatabase(dbUrl);
		hook = new ShutdownHook();
		Runtime.getRuntime().addShutdownHook(hook);
	}

	public static void dropDB()
		throws IOException {

		if (db != null) {
			db.shutdown();
			Runtime.getRuntime().removeShutdownHook(hook);
		}
		FileUtils.deleteRecursively(new File(dbUrl));
	}

	public static synchronized GraphDatabaseService getDatabase() {

		if (db == null) {
			createDB();
		}

		return db;
	}

}
