package com.pld.neo4j;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.time.StopWatch;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.OrderedByTypeExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.tinkerpop.blueprints.TransactionalGraph;

public class EmbeddedNeo4j {

	//private static final String DB_PATH = "test.db";
	private static final String DB_PATH="test.db";

	public String greeting;
	public  static String[] arr_prop_node=new String[1000];
	public static  String[] arr_prop_edge=new String[1000];
	//public static String PATH_CSV_FILE="wikipedia_link_fr.csv";
	public static String PATH_CSV_FILE="out.facebook-sg";

	// START SNIPPET: vars
	public static GraphDatabaseService graphDb;
	HashMap<String, String> haConfig;
	BatchInserter inserter;
	AutoIndexer<Node> indexmgr;
	public static ReadableIndex<Node> index;
	Iterator<Object> itr;

	Relationship rel;
	String  nodeId, nodeId1, nodeId2;
	String valueNode,valueEdge;
	HashMap<String, String> hmp= new HashMap<>();
	public static HashMap<String,HashSet<String>> currentBenchmarkData;
	static Random random = new Random();
	public static int nodeAdded=5000001;

	public static enum RelTypes implements RelationshipType
	{
		KNOWS
	}
	// END SNIPPET: createReltype

	public static void main( String[] args )
	{
		EmbeddedNeo4j hello = new EmbeddedNeo4j();
		hello.createDb();
		// hello.removeData();
		// hello.shutDown();
	}
	public static ReadableIndex<Node> nodeAutoIndex=null;
	void createDb()
	{
		clearDb();
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}


		haConfig = new HashMap<String, String>();


		haConfig.put("ha.server_id", "7");
		haConfig.put("ha.initial_hosts", "192.168.0.51:5001,192.168.0.52:5001,192.168.0.53:5001,192.168.0.51:5002");
		haConfig.put("ha.server", "192.168.0.51:6002");
		haConfig.put("ha.cluster_server", "192.168.0.51:5002");
		haConfig.put("org.neo4j.server.database.mode", "HA");

		graphDb = new HighlyAvailableGraphDatabaseFactory()
		.newHighlyAvailableDatabaseBuilder(DB_PATH)
		.setConfig(haConfig).
		setConfig( GraphDatabaseSettings.node_auto_indexing, "true" ).
		setConfig( GraphDatabaseSettings.node_keys_indexable, "id" ).
		newGraphDatabase();
		registerShutdownHook( graphDb );

		indexmgr = graphDb.index().getNodeAutoIndexer();
		try {

			benchmarks();
		} catch (NumberFormatException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static HashMap<String,HashSet<String>> loadNewRandomBenchmarkData(int numOfDataLoaded){
		System.out.println("Loading Data:\n---------------------");
		if(numOfDataLoaded==0 || numOfDataLoaded>100){
			numOfDataLoaded=100;
		}

		HashMap<String,HashSet<String>> hmp_data=new HashMap<String,HashSet<String>>();
		FileReader fr;
		BufferedReader br;
		try {
			fr = new FileReader(PATH_CSV_FILE);
			br=new BufferedReader(fr,10240);
			String line;
			String arr_token[]=new String[2];
			int j=new Random().nextInt(1000);
			int i=0;
			br.readLine();
			while((line=br.readLine())!=null && hmp_data.size()<=numOfDataLoaded) {
				if(i++==j){
					arr_token=line.split(" ");	
					j=j+numOfDataLoaded;	
					//System.out.println(line);
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

		Random random = new Random();
		List<String> keys = new ArrayList<String>(currentBenchmarkData.keySet());
		String randomKey = keys.get( random.nextInt(keys.size()) );
		List<String> setOfValue = new ArrayList<String>(currentBenchmarkData.get(randomKey));
		String value=setOfValue.get( random.nextInt(setOfValue.size()) );

		return new String[]{randomKey,value};
	}
	Scanner in=new Scanner(System.in);
	Microbenchmarks mb= new Microbenchmarks();
	MacroBenchmarks mab= new MacroBenchmarks();
	Algorithm algo=new Algorithm();
	private void benchmarks() throws IOException{
		int choice;

		while(true){
			//getRandomData();
			currentBenchmarkData=loadNewRandomBenchmarkData(100);
			System.out.println("Enter your choice:");
			System.out.println("1.Add node\n2.Get node\n3.Add Node Property\n4.Get Node Property\n5.Add edge\n6.Get Edge\n7.Remove node"
					+ "\n 8.Remove Node Property\n9.Remove Edge\n10.Remove Edge Property\n11.Add edge property\n12. Update Node"
					+ "\n13.Update Node Property\n14.Update Edge\n15.Get Edge Property\n16.Update Edge Property"
					+"\n17.get edges with filter\n18.get k-hop neighbors\n19 get nodes with filter\n20. Shortest Path");

			String input=in.nextLine();
			choice=Integer.parseInt(input);
			try ( Transaction tx = graphDb.beginTx() ){
				indexmgr.setEnabled(true);
				index=indexmgr.getAutoIndex();	
				tx.success();
			}
			switch(choice){
			case 1:{
				//				System.out.println("Enter node id to be added:");
				//				String nodeId3=in.nextLine();
				for( int i=0;i<20;i++){
				String nodeId=Integer.toString(nodeAdded+1);
				System.out.println("Node id:"+nodeId);
				mb.addSingleNode(nodeId);
				}
				break;
			}
			case 2: {
				for( int i=0;i<20;i++){
				String nodeId=getRandomNode();
				System.out.println("Node is:"+nodeId);
				Node node=mb.getNode(nodeId);
				}
//				try ( Transaction tx = graphDb.beginTx() ){
//					if(node!=null){
//						System.out.println(node.getProperty("id"));
//						System.out.println(node.getProperty("nproperty"));
//						tx.success();
//					}
//					else
//						System.out.println("node not found");
//				}

				break;
			}

			case 3:{ 
				for( int i=0;i<20;i++){
				String nodeId=Integer.toString(nodeAdded++);
				System.out.println("Node is:"+nodeId);
				mb.addNodeProperty(nodeId);
				}
				break;
			}
			case 4: {
				for( int i=0;i<20;i++){
				String nodeId=getRandomNode();
				System.out.println("Node is:"+nodeId);
				String prop=mb.getNodeProperty(nodeId);
				System.out.println("Property is "+prop);
				}
				break;
			}
			case 5: {
				for( int i=0;i<20;i++){
				String nodeId1=getRandomNode();
				String nodeId2=getRandomNode();
				System.out.println("Node1 is:"+nodeId1);
				System.out.println("Node2 is:"+nodeId2);
				mb.addRelationship(nodeId1, nodeId2);
				}
				break;
			}

			case 6: {
				for( int i=0;i<20;i++){
				String[] nodes=getTwoRelatedNodes();
				System.out.println("Node 1 is:"+nodes[0]);
				System.out.println("Node 2 is:"+nodes[1]);

				rel=mb.getRelationship(nodes[0],nodes[1]);
//				try ( Transaction tx = graphDb.beginTx() ){
//					System.out.println("Relationship is :"+ rel.getProperty("eproperty"));
//					tx.success();
//				}
				}
				break;
			}

			case 7: {
				for( int i=0;i<20;i++){
				String nodeId=getRandomNode();
				System.out.println("Node Id:"+nodeId);
				mb.removeNode(nodeId);
				}
				break;
			}
			case 8:{ 
				for( int i=0;i<20;i++){
				String nodeId=getRandomNode();
				System.out.println("Node Id:"+nodeId);
				mb.removeNodeProperty(nodeId);
				}
				break;
			}
			case 9:{ 
				for( int i=0;i<20;i++){
				String[] nodes=getTwoRelatedNodes();
				System.out.println("Node 1 is:"+nodes[0]);
				System.out.println("Node 2 is:"+nodes[1]);
				mb.removeRelationship(nodes[0], nodes[1]);
				}
				break;
				
			}
			case 10:{
				for( int i=0;i<20;i++){
				String[] nodes=getTwoRelatedNodes();
				System.out.println("Node 1 is:"+nodes[0]);
				System.out.println("Node 2 is:"+nodes[1]);
				mb.removeEdgeProperty(nodes[0],nodes[1]);
				}
				break;
			}
			case 11: {
				for( int i=0;i<20;i++){
				String[] nodes=getTwoRelatedNodes();
				System.out.println("Node 1 is:"+nodes[0]);
				System.out.println("Node 2 is:"+nodes[1]);
				mb.addEdgeProperty(nodes[0], nodes[1]);
				}
				break;
			}
			case 12:{
				for( int i=0;i<20;i++){
				String nodeId=getRandomNode();
				System.out.println("Node Id:"+nodeId);
				valueNode=arr_prop_node[new Random().nextInt(1000)];
				mb.updateNode(nodeId,valueNode);
				}
				break;
			}
			case 13:{
				for( int i=0;i<20;i++){
				String nodeId=getRandomNode();
				System.out.println("Node Id:"+nodeId);
				valueNode=arr_prop_node[new Random().nextInt(1000)];
				mb.updateNodeProperty(nodeId,valueNode);
				}
				break;
			}
			case 14:{
				for( int i=0;i<20;i++){
				String[] nodes=getTwoRelatedNodes();
				System.out.println("Node 1 is:"+nodes[0]);
				System.out.println("Node 2 is:"+nodes[1]);
				valueEdge=arr_prop_edge[new Random().nextInt(1000)];
				mb.updateEdge(nodes[0],nodes[1],valueEdge);
				}
				break;
			}
			case 15:{
				for( int i=0;i<20;i++){
				String[] nodes=getTwoRelatedNodes();
				System.out.println("Node 1 is:"+nodes[0]);
				System.out.println("Node 2 is:"+nodes[1]);
				String prop=mb.getEdgeProperty(nodes[0],nodes[1]);
				System.out.println("Edge property is:"+ prop);
				}
				break;

			}
			case 16:{
				for( int i=0;i<20;i++){
				String[] nodes=getTwoRelatedNodes();
				System.out.println("Node 1 is:"+nodes[0]);
				System.out.println("Node 2 is:"+nodes[1]);
				valueEdge=arr_prop_edge[new Random().nextInt(1000)];
				mb.updateEdgeProperty(nodes[0],nodes[1], valueEdge);
				}
				break;

			}
			case 17:{
				for( int i=0;i<20;i++){
				String filter="343";
				//mab.get_neighbors(nodeId, filter);
				mab.getEdges(filter);
				}
				break;
			}
			case 18:{
				for( int i=0;i<20;i++){
				String nodeId= getRandomNode();
				System.out.println("Node Id:"+nodeId);
				System.out.println("Enter depth");
				input=in.nextLine();
				mab.k_hop_neighbors(nodeId, Integer.parseInt(input));
				}
				break;
			}
			case 19:{
				for( int i=0;i<20;i++){
				String filter="343";
				mab.select_nodes(filter);
				}
			}
			case 20:{
				for( int i=0;i<20;i++){
				String nodeId1= getRandomNode();
				String nodeId2= getRandomNode();
				System.out.println("Node 1 is:"+nodeId1);
				System.out.println("Node 2 is:"+nodeId2);
				algo.shortestPath1(nodeId1, nodeId2);
				}
				//algo.shortestPath(nodeId1, nodeId2);
			}

			}
		}

	}



	//	private ArrayList<Node> getNeighbors(String nodeId){
	//		ArrayList<Node> arls= new ArrayList<Node>(); 
	//		try ( Transaction tx = graphDb.beginTx() ){
	//			node=index.get("id",nodeId).getSingle();
	//			itrRel =node.getRelationships(); //use micro-benchmark
	//			itRel=itrRel.iterator();
	//			while(itRel.hasNext()){
	//				rel=itRel.next();
	//				firstNode=rel.getStartNode();
	//				if(!node.equals(firstNode))
	//					arls.add(firstNode);
	//				secondNode=rel.getEndNode();
	//				if(!node.equals(secondNode))
	//					arls.add(secondNode);
	//			}
	//			tx.success();
	//		}
	//		return arls;
	//	}

	//	private void addNodesMultiThread(ReadableIndex<Node> nodeAutoIndex2){
	//
	//		BlockingQueue<String> q = new ArrayBlockingQueue<String>(10240);
	//		Producer p = new Producer(q);
	//		Consumer c1 = new Consumer(q,nodeAutoIndex2);
	//		//	Consumer c2 = new Consumer(q,nodeAutoIndex2);
	//		//	Consumer c3 = new Consumer(q,nodeAutoIndex2);
	//		//Consumer c2 = new Consumer(q);
	//		new Thread(p).start();
	//		new Thread(c1).start();
	//		//	new Thread(c2).start();
	//		//new Thread(c3).start();
	//
	//	}
	//
	//	private void addBatch(){
	//		GraphDatabaseService batchDb =
	//				BatchInserters.batchDatabase( DB_PATH, haConfig );
	//		Label personLabel = DynamicLabel.label( "Person" );
	//		Node mattiasNode = batchDb.createNode( personLabel );
	//		mattiasNode.setProperty( "name", "Mattias" );
	//		Node chrisNode = batchDb.createNode();
	//		chrisNode.setProperty( "name", "Chris" );
	//		chrisNode.addLabel( personLabel );
	//		RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
	//		mattiasNode.createRelationshipTo( chrisNode, knows );
	//		batchDb.shutdown();
	//	}

	//	private void addNodesBatchInsert() throws ConstraintViolationException, IOException{
	//
	//
	//		FileReader fr=new FileReader(PATH_CSV_FILE);
	//		BufferedReader br=new BufferedReader(fr,10240);
	//		String[] arr_token=null;
	//		String line=null;
	//
	//		Label userLabel = DynamicLabel.label( "Users" );
	//		inserter = (BatchInserterImpl)BatchInserters.inserter( DB_PATH);
	//		inserter.createDeferredSchemaIndex( userLabel ).on( "ID" ).create();
	//
	//
	//		Map<String, Object> properties= new HashMap<>();
	//
	//
	//
	//		while((line=br.readLine())!=null)
	//		{
	//			//arr_token=line.split(" ");
	//			System.out.println(line);
	//
	//			//Map<String, Object> properties = new HashMap<>();
	//			properties.clear();
	//			properties.put( "ID", line );
	//			properties.put("property1", arr_prop[new Random().nextInt(1000)] );
	//			long mattiasNode = inserter.createNode( properties, userLabel );
	//
	//
	//			//				properties.put( "ID", "Chris" );
	//			//				long chrisNode = inserter.createNode( properties, userLabel );
	//			//				RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
	//			//				// To set properties on the relationship, use a properties map
	//			//				// instead of null as the last parameter.
	//			//				inserter.createRelationship( mattiasNode, chrisNode, knows, null );
	//
	//		}
	//
	//		inserter.shutdown();
	//	}



	private void clearDb()
	{
		try
		{
			FileUtils.deleteRecursively( new File( DB_PATH ) );
		}
		catch ( IOException e )
		{
			throw new RuntimeException( e );
		}
	}


	void shutDown()
	{
		System.out.println();
		System.out.println( "Shutting down database ..." );
		// START SNIPPET: shutdownServer
		graphDb.shutdown();
		// END SNIPPET: shutdownServer
	}

	// START SNIPPET: shutdownHook
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				graphDb.shutdown();
			}
		} );
	}
}
