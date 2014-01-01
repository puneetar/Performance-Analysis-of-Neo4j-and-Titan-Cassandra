package com.pld.titan;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
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
import org.apache.commons.lang.time.StopWatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


/**
 *
 * @author Puneet Arora
 */

public class TitanBenchmark {

	//public static final String INDEX_NAME = "search";
	public final static String INPUT_CSV_FILE="/home/pld6/git/My/My/facebook-sg/newfull";
	public final static String BACTH_LOAD_PROPERTIES="src/com/pld/titan/batchLoading_titan-cassandra-es.properties";
	public final static String BENCHMARKING_PROPERTIES="src/com/pld/titan/benchMarking_titan-cassandra-es.properties";
	public final static String NODE_PROPERTY="nproperty";
	public final static String EDGE_PROPERTY="eproperty";
	public static HashMap<String,HashSet<String>> currentBenchmarkData;
	static Random random = new Random();

	public static void main(String args[]){
		Scanner in=new Scanner(System.in);
		currentBenchmarkData=loadNewRandomBenchmarkData(100);
		System.out.println("\n Getting connection to Database...");
		TitanGraph graph=getGraph(BENCHMARKING_PROPERTIES, false);
		TitanMicroBenchmarks microBenchmark=new TitanMicroBenchmarks(graph);
		TitanMacroBenchmarks macroBenchmark=new TitanMacroBenchmarks(graph);
		

		while(true){
			System.out.println("\nEnter your choice:\n");
			System.out.println(
					"0. Bulk Load Data \n "
							+ "1. Add node\n "
							+ "2. Get node \n "
							+ "3. Delete Node\n "
							+ "4. Update Node \n "
							+ "5. Add node Property\n "
							+ "6. Get node Property\n "
							+ "7. Delete Node Property\n "
							+ "8. Update Node Property\n "
							+ "9. Add Edge\n "
							+ "10. Get Edge \n "
							+ "11. Delete Edge\n "
							+ "12. Update Edge \n "
							+ "13. Add Edge Property\n "
							+ "14. Get Edge Property\n "
							+ "15. Delete Edge Property\n "
							+ "16. Update Edge Property\n "
							+ "17. Get Nodes With Filter\n "
							+ "18. Get Edges with Filter\n "
							+ "19. K hop traversal\n "
							+ "20. Dijkstra Shorest Path Algo\n "
							+ "q. Quit / Exit"

					);

			String input=in.nextLine();
			if(input.equalsIgnoreCase("q"))
				break;

			int choice = -1;
			try {
				choice = Integer.parseInt(input);
			} catch (NumberFormatException e) {
				System.out.println("Invalid Input : NumberFormatException\n\n");
				//break;
			}


			try{
				switch(choice){
				case 0:
					BatchGraphImpl.batchLoadData(BACTH_LOAD_PROPERTIES,INPUT_CSV_FILE);
					break;


				case 1:
					microBenchmark.addNode();
					break;
				case 2: 
					microBenchmark.getNode(getRandomNode());
					break;
				case 3: 
					microBenchmark.deleteNode(getRandomNode());
					break;
				case 4: 
					microBenchmark.updateNode(getRandomNode());
					break;


				case 5:
					microBenchmark.addNodeProperty(getRandomNode());
					break;
				case 6: 
					microBenchmark.getNodeProperty(getRandomNode());
					break;
				case 7: 
					microBenchmark.deleteNodeProperty(getRandomNode());
					break;
				case 8: 
					microBenchmark.updateNodeProperty(getRandomNode());
					break;


				case 9:
					microBenchmark.addEdge(new String[]{getRandomNode(),getRandomNode()});
					break;
				case 10: 
					microBenchmark.getEdge(getTwoRelatedNodes());
					break;
				case 11: 
					microBenchmark.deleteEdge(getTwoRelatedNodes());
					break;
				case 12: 
					microBenchmark.updateEdge(getTwoRelatedNodes());
					break;


				case 13:
					microBenchmark.addEdgeProperty(getTwoRelatedNodes());
					break;
				case 14: 
					microBenchmark.getEdgeProperty(getTwoRelatedNodes());
					break;
				case 15: 
					microBenchmark.deleteEdgeProperty(getTwoRelatedNodes());
					break;
				case 16: 
					microBenchmark.updateEdgeProperty(getTwoRelatedNodes());
					break;

					
				case 17: 
					macroBenchmark.getNodesWithFilter();
					break;
				case 18: 
					macroBenchmark.getEdgesWithFilter();
					break;
				case 19: 
					macroBenchmark.getKHopNeighbours(2,getRandomNode());
					break;
				
				case 20:
			//		DijkstraAlgorithm dijkstraAlgorithm = new DijkstraAlgorithm(graph);
					DijkstraAlgorithm.getShortestPath(getRandomNode(), getRandomNode());
					break;
			
					
				default:
					System.out.println("Invalid Input\n\n");
					break;
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
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


	public static HashMap<String,HashSet<String>> loadNewRandomBenchmarkData(int numOfDataLoaded){
		System.out.println("Loading Data:\n---------------------");
		//	if(numOfDataLoaded==0 || numOfDataLoaded>100){
		numOfDataLoaded=1000;
		//	}

		HashMap<String,HashSet<String>> hmp_data=new HashMap<String,HashSet<String>>();
		FileReader fr;
		BufferedReader br;
		try {
			fr = new FileReader(INPUT_CSV_FILE);
			br=new BufferedReader(fr,10240);
			String line;
			String arr_token[]=new String[2];
			int j=random.nextInt(1000);
			int i=0;
			while((line=br.readLine())!=null && hmp_data.size()<=numOfDataLoaded) {
				if(i++==j){
					arr_token=line.split(" ");	
					j=j+random.nextInt(1000);	
					System.out.println(line);
					if(hmp_data.containsKey(arr_token[0])){
						HashSet<String> hst=hmp_data.get(arr_token[0]);
						hst.add(arr_token[1]);
						hmp_data.put(arr_token[0],hst);
					}
					else{
						HashSet<String> hst=new HashSet<String>();
						hst.add(arr_token[1]);
						hmp_data.put(arr_token[0], hst);
					}
				}
			}
			br.close();
			fr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return hmp_data;

	}

	public static String getRandomNode(){
		if(currentBenchmarkData==null || currentBenchmarkData.size()==0)
			currentBenchmarkData=loadNewRandomBenchmarkData(0);


		List<String> keys = new ArrayList<String>(currentBenchmarkData.keySet());
		String randomKey = keys.get( random.nextInt(keys.size()) );
		//HashSet<String> value = currentBenchmarkData.get(randomKey);
		return randomKey;
	}

	public static String[] getTwoRelatedNodes(){
		if(currentBenchmarkData==null || currentBenchmarkData.size()==0)
			currentBenchmarkData=loadNewRandomBenchmarkData(0);


		List<String> keys = new ArrayList<String>(currentBenchmarkData.keySet());
		String randomKey = keys.get( random.nextInt(keys.size()) );
		List<String> setOfValue = new ArrayList<String>(currentBenchmarkData.get(randomKey));
		String value=setOfValue.get( random.nextInt(setOfValue.size()) );

		return new String[]{randomKey,value};
	}

	public static void printTime(String st){
		System.out.println("Time Taken : "+st);
	}

}