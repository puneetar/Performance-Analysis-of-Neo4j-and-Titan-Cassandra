package com.pld.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.client.RexsterClientTokens;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


/**
 * 
 *
 * @author Puneet Arora
 */
public class TitanBenchmark {

	//public static final String INDEX_NAME = "search";
	public final static String INPUT_CSV_FILE="/home/pld6/git/My/My/facebook-sg/newaa";
	public final static String BACTH_LOAD_PROPERTIES="src/com/pld/titan/bachLoading_titan-cassandra-es.properties";
	public final static String BENCHMARKING_PROPERTIES="src/com/pld/titan/benchMarking_titan-cassandra-es.properties";
	static final int final_limit=5000000;
	private final static String[] arr_prop_node=new String[1000];
	private final static String[] arr_prop_edge=new String[1000];


	public static void main(String args[]){
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}

		while(true){
			long startTime = System.currentTimeMillis();

			batchLoadData("titan-cassandra-es.properties",INPUT_CSV_FILE);

			long endTime = System.currentTimeMillis();
			System.out.println("That took " + (endTime - startTime) + " milliseconds");

		}


		//RexsterClient client=null;
		//	try{
		//			client= getRexsterClientConnection();
		//			if(client!=null){
		//				loadFromCSV(client);
		//			}
		//		}
		//		finally{
		//			try {
		//				client.close();
		//			} catch (IOException e) {
		//				// TODO Auto-generated catch block
		//				e.printStackTrace();
		//			}
		//		}
	}

	public static TitanGraph getGraph(String propertiesFile,boolean createKeys){
		TitanGraph graph = /*TitanFactory.open(config);*/TitanFactory.open(propertiesFile);

		if(createKeys){
			graph.makeKey("nproperty").dataType(String.class).make();
			graph.makeKey("userid").dataType(String.class).indexed(Vertex.class).make();
			graph.makeKey("eproperty").dataType(String.class).make();
			graph.makeLabel("KNOWS").make();
		}

		return graph;
	}

	public static TitanGraph batchLoadData(String propertiesFile, String csvFile) {
		//	BaseConfiguration config = new BaseConfiguration();
		//		Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
		//		// storage.setProperty(GraphDatabaseConfiguration.STORAGE_CONF_FILE_KEY, arg1)
		//		// configuring local backend
		//		storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "embeddedcassandra");
		//		storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, "cassandra.yaml");
		//		// configuring elastic search index
		//		Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
		//		index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
		//		index.setProperty("local-mode", true);
		//		index.setProperty("client-only", false);
		//		index.setProperty(STORAGE_DIRECTORY_KEY, "db/es");

		TitanGraph graph=getGraph(propertiesFile, true);
		BatchGraph<TitanGraph> bgraph = new BatchGraph<TitanGraph>(graph, VertexIDType.STRING, 1000);
		//bgraph.setLoadingFromScratch(false);
		//BatchGraph bgraph = BatchGraph.wrap(graph);

		try {
			FileReader fr = new FileReader(csvFile);
			BufferedReader br=new BufferedReader(fr);
			br.readLine();

			System.out.println(Thread.currentThread().getId()+" : "+csvFile);

			String line;
			String arr_token[]=new String[2];
			Vertex[] vertices;
			Edge edge;
			int j=1;
			int k=500000;
			while((line=br.readLine())!=null) {
				arr_token=line.split(" ");

				if(Integer.parseInt(arr_token[0])<final_limit && Integer.parseInt(arr_token[1])<final_limit ){
					System.out.println(Thread.currentThread().getId()+" : "+j+++" : "+line);
					if(j>k){
						k=k+500000;
						System.gc();
						System.out.println("doing system gc");
						Thread.sleep(10000);
					}
					vertices = new Vertex[2];
					for (int i=0;i<2;i++) {
						vertices[i] = bgraph.getVertex(arr_token[i]);

						if (vertices[i]==null){
							vertices[i]=bgraph.addVertex(arr_token[i],"userid",arr_token[i],"nproperty", arr_prop_node[new Random().nextInt(1000)]);
							// 	vertices[i].setProperty("nproperty", arr_prop_node[new Random().nextInt(1000)]);
						}
					}
					edge = bgraph.addEdge(null,vertices[0],vertices[1],"KNOWS","eproperty",arr_prop_edge[new Random().nextInt(1000)]);
					//edge.setProperty("eproperty",arr_prop_edge[new Random().nextInt(1000)]);
				}
			}
			bgraph.commit();
			graph.commit();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return graph;
	}


	public void addSignleNode(){

	}

	public static RexsterClient getRexsterClientConnection(){
		RexsterClient client=null;
		try {

			BaseConfiguration conf = new BaseConfiguration() {{
				addProperty(RexsterClientTokens.CONFIG_HOSTNAME, "192.168.0.56");
				addProperty(RexsterClientTokens.CONFIG_MESSAGE_RETRY_WAIT_MS, 0);
				addProperty(RexsterClientTokens.CONFIG_GRAPH_NAME, "graph");
			}};

			//client = RexsterClientFactory.open("192.168.0.56","graph");
			client=RexsterClientFactory.open(conf);

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

	public static void loadFromCSV(RexsterClient client){
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}
		try {
			List<Map<String,Object>> result;
			//			FileReader fr = new FileReader(INPUT_CSV_FILE);
			//			BufferedReader br=new BufferedReader(fr,10240);
			//			br.readLine();
			//
			//			
			//			for(int i=1;i<=final_limit;i++){
			//				//bw_node.write(i+"\t"+arr_prop_node[new Random().nextInt(1000)]+"\n");
			//				result= client.execute("g.addVertex(['userid':'"+i+"','nproperty':'"+arr_prop_node[new Random().nextInt(1000)]+"'])");
			//				System.out.println(result);
			//			}
			//			
			//			String arr_token[]=new String[2];
			//			String line;
			//			while((line=br.readLine())!=null){
			//				arr_token=line.split(" ");
			//
			//				if(Integer.parseInt(arr_token[0])<final_limit && Integer.parseInt(arr_token[1])<final_limit){
			//					//bw_rels.write(arr_token[0]+"\t"+arr_token[1]+"\t"+"KNOWS"+
			//						//	"\t"+arr_prop_edge[new Random().nextInt(1000)]+"\n");
			//					
			//					result= client.execute("g.addEdge(g.getVertex("+arr_token[0]+"),"+
			//					"g.addEdge(g.getVertex("+arr_token[0]+")"+
			//					",'KNOWS',[eproperty:"+arr_prop_node[new Random().nextInt(1000)]+"])");
			//					System.out.println(result);
			//				}
			//			
			//			}
			//		//	result= client.execute("g.addVertex(['userid':'106','name':'divya'])");
			//		//	System.out.println(result);

			result=client.execute("vs=[] as Set;new File(\""+INPUT_CSV_FILE+"\").eachLine{l->p=l.split(\" \");vs<<p[0];vs<<p[1];}");
			//result= client.execute("g.V.userid");
			System.out.println(result);

			//vs=[] as Set;new File("/home/pld6/git/My/My/facebook-sg/facebook.100").eachLine{l->p=l.split(" ");println "${p[0]}"; vs<<p[0];vs<<p[1];}
			//vs.each{v->g.addVertex(['userid':v,'']v)}
			//	new File("/home/pld6/git/My/My/facebook-sg/facebook.100").eachLine{l->p=l.split(" ");println "adding edge between ${p[0]} : ${p[1]}";g.addEdge(g.getVertex(${p[0]}),g.getVertex(${p[1]}),'friend')}

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


}