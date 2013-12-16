import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.client.RexsterClientTokens;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


/**
* ok
*
* @author Puneet Arora
*/
public class TitanBenchmark {

    public static final String INDEX_NAME = "search";

    public static void main(String args[]){
    	RexsterClient client=null;
    	try{
    		client= getRexsterClientConnection();
    		if(client!=null){
    			executeCode(client);
    		}
    	}
    	finally{
			try {
				client.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
    
    public static RexsterClient getRexsterClientConnection(){
    	RexsterClient client=null;
    	try {
    		
    		BaseConfiguration conf = new BaseConfiguration() {{
    		    addProperty(RexsterClientTokens.CONFIG_HOSTNAME, "192.168.0.194");
    		    addProperty(RexsterClientTokens.CONFIG_MESSAGE_RETRY_WAIT_MS, 0);
    		    addProperty(RexsterClientTokens.CONFIG_GRAPH_NAME, "graph");
    		}};
    		//TitanGraph graph = TitanFactory.open(conf);
    		//System.out.println(graph);
    		client = RexsterClientFactory.open(conf);
			//return RexsterClientFactory.open("localhost", "graph");
			
		} catch (RexProException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return client;
    }
    
    public static void executeCode(RexsterClient client){
    	try {

    		List<Map<String,Object>> result;
    		
//    		result= client.execute("g = TinkerGraphFactory.createTinkerGraph()");
//    		System.out.println(result);
//    		client.close();
    	//	client = RexsterClientFactory.open("localhost", "tinkugraph");
    		System.out.println("ok i got it");
    		//Thread.sleep(50000);
    		result= client.execute("g.addVertex(null,[name:\"stephen\"])");
    		System.out.println(result);
    		
    		result= client.execute("g.v(1).values()");
    		System.out.println(result);
    		
    		
    		
//    		
//			
//			result = client.execute("g.V('name','saturn').in('father').map");
//			// result.toString(): [{name="jupiter", type="god"}]
//
//			Map<String,Object> params = new HashMap<String,Object>();
//			params.put("name","saturn");
//			result = client.execute("g.V('name',name).in('father').map",params);
		} catch (RexProException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public static TitanGraph create(final String directory) {
        BaseConfiguration config = new BaseConfiguration();
        
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
       // storage.setProperty(GraphDatabaseConfiguration.STORAGE_CONF_FILE_KEY, arg1)
        // configuring local backend
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "embeddedcassandra");
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, "cassandra.yaml");
        // configuring elastic search index
        Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
        index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
        index.setProperty("local-mode", true);
        index.setProperty("client-only", false);
        index.setProperty(STORAGE_DIRECTORY_KEY, "db/es");
        

        TitanGraph graph = TitanFactory.open(config);

    	
    	    	
        //TitanGraph graph = TitanFactory.open("/home/pld6/titan-all-0.4.1/conf/titan-cassandra-embedded-es.properties");
        //GraphOfTheGodsFactory.load(graph);
        return graph;
    }

    public static void load(final TitanGraph graph) {

        graph.makeKey("name").dataType(String.class).indexed(Vertex.class).unique().make();
        graph.makeKey("age").dataType(Integer.class).indexed(INDEX_NAME, Vertex.class).make();
        graph.makeKey("type").dataType(String.class).make();

        final TitanKey time = graph.makeKey("time").dataType(Integer.class).make();
        final TitanKey reason = graph.makeKey("reason").dataType(String.class).indexed(INDEX_NAME, Edge.class).make();
        graph.makeKey("place").dataType(Geoshape.class).indexed(INDEX_NAME, Edge.class).make();

        graph.makeLabel("father").manyToOne().make();
        graph.makeLabel("mother").manyToOne().make();
        graph.makeLabel("battled").sortKey(time).make();
        graph.makeLabel("lives").signature(reason).make();
        graph.makeLabel("pet").make();
        graph.makeLabel("brother").make();

        graph.commit();

        // vertices

        Vertex saturn = graph.addVertex(null);
        saturn.setProperty("name", "saturn");
        saturn.setProperty("age", 10000);
        saturn.setProperty("type", "titan");

        Vertex sky = graph.addVertex(null);
        ElementHelper.setProperties(sky, "name", "sky", "type", "location");

        Vertex sea = graph.addVertex(null);
        ElementHelper.setProperties(sea, "name", "sea", "type", "location");

        Vertex jupiter = graph.addVertex(null);
        ElementHelper.setProperties(jupiter, "name", "jupiter", "age", 5000, "type", "god");

        Vertex neptune = graph.addVertex(null);
        ElementHelper.setProperties(neptune, "name", "neptune", "age", 4500, "type", "god");

        Vertex hercules = graph.addVertex(null);
        ElementHelper.setProperties(hercules, "name", "hercules", "age", 30, "type", "demigod");

        Vertex alcmene = graph.addVertex(null);
        ElementHelper.setProperties(alcmene, "name", "alcmene", "age", 45, "type", "human");

        Vertex pluto = graph.addVertex(null);
        ElementHelper.setProperties(pluto, "name", "pluto", "age", 4000, "type", "god");

        Vertex nemean = graph.addVertex(null);
        ElementHelper.setProperties(nemean, "name", "nemean", "type", "monster");

        Vertex hydra = graph.addVertex(null);
        ElementHelper.setProperties(hydra, "name", "hydra", "type", "monster");

        Vertex cerberus = graph.addVertex(null);
        ElementHelper.setProperties(cerberus, "name", "cerberus", "type", "monster");

        Vertex tartarus = graph.addVertex(null);
        ElementHelper.setProperties(tartarus, "name", "tartarus", "type", "location");

        // edges

        jupiter.addEdge("father", saturn);
        jupiter.addEdge("lives", sky).setProperty("reason", "loves fresh breezes");
        jupiter.addEdge("brother", neptune);
        jupiter.addEdge("brother", pluto);

        neptune.addEdge("lives", sea).setProperty("reason", "loves waves");
        neptune.addEdge("brother", jupiter);
        neptune.addEdge("brother", pluto);

        hercules.addEdge("father", jupiter);
        hercules.addEdge("mother", alcmene);
        ElementHelper.setProperties(hercules.addEdge("battled", nemean), "time", 1, "place", Geoshape.point(38.1f, 23.7f));
        ElementHelper.setProperties(hercules.addEdge("battled", hydra), "time", 2, "place", Geoshape.point(37.7f, 23.9f));
        ElementHelper.setProperties(hercules.addEdge("battled", cerberus), "time", 12, "place", Geoshape.point(39f, 22f));

        pluto.addEdge("brother", jupiter);
        pluto.addEdge("brother", neptune);
        pluto.addEdge("lives", tartarus).setProperty("reason", "no fear of death");
        pluto.addEdge("pet", cerberus);

        cerberus.addEdge("lives", tartarus);

        // commit the transaction to disk
        graph.commit();
    }
}