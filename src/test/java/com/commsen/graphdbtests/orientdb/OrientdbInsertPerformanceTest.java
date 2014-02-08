
package com.commsen.graphdbtests.orientdb;

import java.io.*;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;
import org.junit.Before;
import org.junit.BeforeClass;

import com.commsen.graphdbtests.BaseGraphInsertPerformanceTest;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

public class OrientdbInsertPerformanceTest extends BaseGraphInsertPerformanceTest {

	public OrientdbInsertPerformanceTest(long numberOfDocs, long numberOfProperties, ModelType modelType, int numberOfThreads) {
		super(numberOfDocs, numberOfProperties, modelType, numberOfThreads);
	}


	protected OrientVertex createVertex(final OrientBaseGraph db) {
        OrientVertex vertex = db.addVertex("OGraphVertex", null);
        for (int i = 0; i < numberOfProperties; i++) {
            vertex.setProperty("property" + i, "value" + i);
		}
        vertex.save();
        return vertex;
	}

	protected OrientEdge createEdge(Vertex v1, Vertex v2, final OrientBaseGraph db) {

		OrientEdge e = db.addEdge(null, v1, v2, "E");
		for (int i = 0; i < numberOfProperties; i++) {
			e.setProperty("property" + i, "value" + i);
		}
		return e;
	}

	/*protected ODocument createEdge(ORID id1, ORID id2, final OGraphDatabase db) {

		ODocument doc = db.createEdge(id1, id2);
		for (int i = 0; i < numberOfProperties; i++) {
			doc.field("property" + i, "value" + i);
		}
		return doc;
	}*/

    @BeforeClass
    public static void redirectOutput(){
        try {
            File file  = new File("orient_results.txt");
            PrintStream printStream = new PrintStream(new FileOutputStream(file));
            System.setOut(printStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

	@Before
	public void clearData()
		throws IOException {

		OrientDbUtil.dropDB();
		OrientDbUtil.createDB();
	}



    @Override
    protected TestResult doAddDocuments() {
		long v = 0, e = 0;

 		OrientVertex v1=null, v2=null;

		OrientBaseGraph db=null;

        db = OrientDbUtil.getDatabase();
		try {
			db.getRawGraph().declareIntent(new OIntentMassiveInsert());

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

                    v1 = db.addVertex(null);
                    v1.save();

                    v2 = db.addVertex(null);
                    v2.save();
            }

            v = v/numberOfThreads;
            e = e/numberOfThreads;

			for (int i = 0; i < v; i++) {

				if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
					return new TestResult(i, 0, true);
				}

				OrientVertex vertex = createVertex(db);
                if (i == 0) {
                    v1 = vertex;
                }
                if (i == 1) {
                    v2 = vertex;
                }
       		}

			for (int i = 0; i < e; i++) {

				if (i % TIMEOUT_CHECK == 0 && System.currentTimeMillis() - startTime > TIMEOUT) {
					return new TestResult(v, i, true);
				}

				OrientEdge edge = createEdge(v1, v2, db);

                edge.save();
			}

			db.getRawGraph().declareIntent(null);

		}catch(Exception ex){
            ex.printStackTrace();
        }
		finally {
			db.getRawGraph().close();
            db.commit();
		}
		
		return new TestResult(db.countVertices(), e/*db.countEdges()*/, false);
	}
}
