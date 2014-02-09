
package com.commsen.graphdbtests.orientdb;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;
import org.junit.Before;
import org.junit.BeforeClass;

import com.commsen.graphdbtests.BaseGraphInsertPerformanceTest;
//import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

public class OrientdbInsertPerformanceTest extends BaseGraphInsertPerformanceTest {

	public OrientdbInsertPerformanceTest(long numberOfDocs, long numberOfProperties, ModelType modelType, int numberOfThreads) {
		super(numberOfDocs, numberOfProperties, modelType, numberOfThreads);
	}


	protected OrientVertex createVertex(final OrientBaseGraph db) {
        Map<String, String> properties = new HashMap<String, String>();
        for (int i = 0; i < numberOfProperties; i++)
            properties.put("property" + i, "value" + i);
        OrientVertex vertex = db.addVertex(null, properties);
/*
        for (int i = 0; i < numberOfProperties; i++) {
            vertex.setProperty("property" + i, "value" + i);
		}
*/
        vertex.save();
        return vertex;
	}

	protected OrientEdge createEdge(Vertex v1, Vertex v2, final OrientBaseGraph db) {
        Map<String, String> properties = new HashMap<String, String>();
        for (int i = 0; i < numberOfProperties; i++)
            properties.put("property" + i, "value" + i);
        OrientEdge e = ((OrientVertex)v1).addEdge(null, (OrientVertex)v2, "E", null, properties);// db.addEdge(null, v1, v2, "E", properties);
        e.save();

		/*for (int i = 0; i < numberOfProperties; i++) {
			e.setProperty("property" + i, "value" + i);
		}*/
		return e;
	}

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
	public void clearData() throws IOException {

		OrientDbUtil.dropDB();
		OrientDbUtil.createDB();
	}



    @Override
    protected TestResult doAddDocuments() {
		long v = 0, e = 0;

 		OrientVertex v1=null, v2=null;

		OrientBaseGraph db;

        db = OrientDbUtil.getDatabase();
		try {
			//db.getRawGraph().declareIntent(new OIntentMassiveInsert());

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
                    db.commit();
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
                    db.commit();
					return new TestResult(v, i, true);
				}

				OrientEdge edge = createEdge(v1, v2, db);
			}

			//db.getRawGraph().declareIntent(null);
            db.commit();
		}catch(Exception ex){
            ex.printStackTrace();
        }
		finally {
			db.getRawGraph().close();

		}
		
		return new TestResult(db.countVertices(), e/*db.countEdges()*/, false);
	}
}
