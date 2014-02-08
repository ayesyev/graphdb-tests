package com.commsen.graphdbtests.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.TransactionalGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.impl.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Stack;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;

public class TitanDbUtils {

    public static String dbUrl = "/tmp/graphdb_tests_titan";

    public static final String INDEX_NAME = "search";

    //public static final Stack<TitanGraph> graphs = new Stack<TitanGraph>();

    private static TitanGraph graph=null;

    public static void createDB(){
        BaseConfiguration config = new BaseConfiguration();
        //Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configuring local backend
        //config.setProperty("storage.backend", "embeddedcassandra");
        config.setProperty("storage.directory", dbUrl);
        //config.setProperty("storage.cassandra-config-dir", "file://c:/projects/GraphDBs/apache-cassandra-2.0.4/conf/cassandra.yaml");

        // configuring elastic search index
        /*Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
        index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
        index.setProperty("local-mode", true);
        index.setProperty("client-only", false);
        index.setProperty(STORAGE_DIRECTORY_KEY, dbUrl + File.separator + "es");*/

        graph = TitanFactory.open(config);;
        //graphs.push(graph);
        //return graph;

    }

    public static void dropDB() throws IOException {
        if(graph!=null)
            graph.shutdown();
        //graphs.pop();
        boolean done=false;
        while(!done){
            try {
                FileUtils.deleteRecursively(new File(dbUrl));
                done = true;
            }catch(IOException ex){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    public static synchronized TransactionalGraph getDatabase() {
        return graph.newTransaction();
    }
}
