package com.commsen.graphdbtests.titan;


import com.commsen.graphdbtests.BaseGraphInsertPerformanceTest;
import com.thinkaurelius.titan.core.TitanException;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;


import java.io.*;

public class TitanInsertPerformanceTest extends BaseGraphInsertPerformanceTest {

    static Logger logger = Logger.getRootLogger();

    public TitanInsertPerformanceTest(long numberOfDocs, long numberOfProperties, ModelType modelType, int numberOfThreads){
        super(numberOfDocs, numberOfProperties, modelType, numberOfThreads);
        logger.setLevel(Level.ERROR);
        logger.removeAllAppenders();
    }
    @BeforeClass
    public static void redirectOutput(){
        try {
            File file  = new File("titan_results.txt");
            PrintStream printStream = new PrintStream(new FileOutputStream(file));
            System.setOut(printStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void clearData() throws IOException {

        TitanDbUtils.dropDB();
        TitanDbUtils.createDB();
    }
    protected Vertex createVertex(final TransactionalGraph db) {
        Vertex vertex = db.addVertex("OGraphVertex");
        for (int i = 0; i < numberOfProperties; i++) {
            vertex.setProperty("property" + i, "value" + i);
        }

        return vertex;
    }

    protected Edge createEdge(Vertex v1, Vertex v2) {
        Edge e =  v1.addEdge("test", v2);        //OrientEdge e = db.addEdge("test", v1, v2, "test");
        for (int i = 0; i < numberOfProperties; i++) {
            e.setProperty("property" + i, "value" + i);
        }
        return e;
    }

    @Override
    protected TestResult doAddDocuments() {
        long v = 0, e = 0;

        Vertex v1 = null, v2 = null;

        final long startTime = System.currentTimeMillis();

        final TransactionalGraph db = TitanDbUtils.getDatabase();
        //TitanTransaction tx = db.newTransaction();
        TestResult result = null;
        try {
            switch (modelType) {

                case VERTICES_AND_EDGES:
                    v = numberOfDocuments / 2;
                    e = numberOfDocuments - v;
                    break;

                case VERTICES_ONLY:
                    v = numberOfDocuments;
                    break;

                case EDGES_ONLY:
                    e = numberOfDocuments - 2;
                    v1 = createVertex(db);
                    v2 = createVertex(db);
            }

            v = v/numberOfThreads;
            e = e/numberOfThreads;

            for (int i = 0; i < v; i++) {

                if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
                    return new TestResult(i, 0, true);
                }

                final Vertex vertex = createVertex(db);
                if (i == 0)
                    v1 = vertex;
                if (i == 1)
                    v2 = vertex;
            }

            for (int i = 0; i < e; i++) {

                if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
                    return new TestResult(v, i, true);
                }

                createEdge(v1, v2);
            }
            //commit(db);


            result = new TestResult(getVertices(db), getEdges(db), false);
            //tx.commit();
        }
        finally {
            commit(db);
        }

        return result;
    }

    private void commit(final TransactionalGraph db){
        boolean done = false;
        while(!done)
            try{
                //db.commit();
                db.shutdown();
                done = true;
            }catch(TitanException ex){
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }catch(IllegalArgumentException ex){
                done = true;
            }
    }
    /*private void shutdown(final TitanGraph db){
        boolean done = false;
        while(!done)
            try{
                //db.commit();
                db.shutdown();
                done = true;
            }catch(TitanException ex){
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
    }*/
    private long getVertices(final TransactionalGraph db){
        long cnt=0;
        for(Vertex v:db.getVertices())
            cnt++;
        return cnt;
    }
    private long getEdges(final TransactionalGraph db){
        long cnt=0;
        for(Edge e:db.getEdges())
            cnt++;
        return cnt;
    }
}
