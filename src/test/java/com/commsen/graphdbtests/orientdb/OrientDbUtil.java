
package com.commsen.graphdbtests.orientdb;

import java.io.IOException;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.storage.OStorage;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class OrientDbUtil {

	public static String dbUrl = "remote:127.0.0.1/graphdb_tests_orient";//"local:/tmp/graphdb_tests_orient";

    //public static String dbRemoteUrl = "remote:127.0.0.1/graphdb_tests_orient";

    public static String dbUser = "root";

	public static String dbPassword = "root";

	//public static OGraphDatabasePool dbConnectionPool = OGraphDatabasePool.global();

    private static final ThreadLocal<OrientGraph> graph = new ThreadLocal<OrientGraph>();
    private static final ThreadLocal<OrientGraphNoTx> graphNoTx = new ThreadLocal<OrientGraphNoTx>();

    private static OrientGraphFactory factory;

	public static synchronized OrientGraph getDatabase() {
        if(graph.get() == null) {
            if(factory == null) {
                factory = new OrientGraphFactory(dbUrl);
                factory.setupPool(5, 10);
            }

            graph.set(factory.getTx());
        }

        return graph.get();
	}

    public static OrientGraphNoTx getDatabaseNoTx() {
        if(graphNoTx.get() == null) {
            if(factory == null) {
                factory = new OrientGraphFactory(dbUrl);
                factory.setupPool(5, 10);
            }

            graphNoTx.set(factory.getNoTx());
        }

        return graphNoTx.get();
    }

	public static void createDB()
		throws IOException {

		if (dbUrl.startsWith("remote")) {
			OServerAdmin server = new OServerAdmin(dbUrl).connect(dbUser, dbPassword);
			if (!server.existsDatabase("plocal")) {
				server.createDatabase("graph", "plocal");

            }
			server.close();
            /*OrientGraph db = getDatabase();
            db.getRawGraph().addCluster("test", OStorage.CLUSTER_TYPE.PHYSICAL);
            db.shutdown();*/
        }else {
            OrientGraph database = new OrientGraph(dbUrl);
            database.shutdown();
		}

	}

	public static void dropDB() throws IOException {

		if (dbUrl.startsWith("remote")) {

            if(factory !=null){
                OrientGraph graph = getDatabase();
                graph.shutdown();
                factory.close();
                factory = null;
            }


            OServerAdmin server = new OServerAdmin(dbUrl).connect(dbUser, dbPassword);
            if (server.existsDatabase("plocal")){
                server = new OServerAdmin(dbUrl).connect(dbUser, dbPassword);
                server.dropDatabase("plocal");
            }
            server.close();
		}
		else {
            OrientGraph database = new OrientGraph(dbUrl);
            database.drop();
        }
	}

}
