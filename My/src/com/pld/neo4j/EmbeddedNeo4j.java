package com.pld.neo4j;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
	private String[] arr_prop_node=new String[1000];
	private String[] arr_prop_edge=new String[1000];
	//public static String PATH_CSV_FILE="wikipedia_link_fr.csv";
	public static String PATH_CSV_FILE="out.facebook-sg";

	// START SNIPPET: vars
	public static GraphDatabaseService graphDb;
	HashMap<String, String> haConfig;
	BatchInserter inserter;
	AutoIndexer<Node> indexmgr;
	ReadableIndex<Node> index;
	IndexHits<Node> indexhits;
	Iterator<Object> itr;
	Node node, firstNode,secondNode;
	Relationship rel;
	Iterable<Relationship> itrRel;
	Iterator<Relationship> itRel;
	String  nodeId, nodeId1, nodeId2;
	String valueNode,valueEdge;
	HashMap<String, String> hmp= new HashMap<>();


	private static enum RelTypes implements RelationshipType
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
		//nodeAutoIndex =graphDb.index().getNodeAutoIndexer().getAutoIndex();
		//
		//		try ( Transaction tx = graphDb.beginTx() ){
		//			index=indexmgr.forNodes("node_auto_index");
		//			//addSingleNode();
		//			tx.success();
		//		}
		try {
			FileReader fr=new FileReader(PATH_CSV_FILE);
			BufferedReader br=new BufferedReader(fr,10240);
			String[] arr_token=null;
			String line=null;
			int count=0;
			line=br.readLine();
			while((line=br.readLine())!=null)
			{
				arr_token= line.split(" ");
				hmp.put(arr_token[0], arr_token[1]);
				if(hmp.size()>1000)
					break;
			}
			br.close();
			benchmarks();
		} catch (NumberFormatException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void getRandomData() throws IOException{

		while(true){
			nodeId=Integer.toString(new Random().nextInt(5000000));
			try ( Transaction tx = graphDb.beginTx() ){
				indexmgr.setEnabled(true);
				index=indexmgr.getAutoIndex();
				node=index.get("id", nodeId).getSingle();

				if(node!=null){
					System.out.println("Node id "+nodeId);
					break;
				}
				tx.success();
			}
		}

		while(true){
			int i=new Random().nextInt(1000);
			int j=new Random().nextInt(1000);
			nodeId1=hmp.get(Integer.toString(i));
			nodeId2=hmp.get(Integer.toString(j));
			if(nodeId1!=null && nodeId2!=null && !nodeId1.equals(nodeId2)){
				System.out.println("Node Id 1 "+nodeId1);
				System.out.println("Node Id 2 "+nodeId2);
				break;
			}
		}
		valueNode=arr_prop_node[new Random().nextInt(1000)];
		valueEdge=arr_prop_edge[new Random().nextInt(1000)];

	}
	private void benchmarks() throws IOException{
		int choice;

		while(true){
			getRandomData();
			System.out.println("Enter your choice:");
			System.out.println("1.Add node\n2.Get node\n3.Add Node Property\n4.Get Node Property\n5.Add edge\n6.Get Edge\n7.Remove node"
					+ "\n 8.Remove Node Property\n9.Remove Edge\n10.Remove Edge Property\n11.Add edge property\n");
			Scanner in=new Scanner(System.in);
			String input=in.nextLine();
			choice=Integer.parseInt(input);

			long startTime = 0,endTime=0;

			switch(choice){
			case 1:{
				System.out.println("Enter node id to be added:");
				String nodeId3=in.nextLine();
				startTime = System.currentTimeMillis();
				addSingleNode(nodeId3,valueNode);
				endTime = System.currentTimeMillis();
				break;
			}
			case 2: {
				System.out.println("Enter node id to get ");
				String nodei=in.nextLine();
				startTime = System.currentTimeMillis();
				Node node=getNode(nodei);
				endTime = System.currentTimeMillis();
				try ( Transaction tx = graphDb.beginTx() ){
					if(node!=null){
						System.out.println(node.getProperty("id"));
						System.out.println(node.getProperty("nproperty"));
						tx.success();
					}
					else
						System.out.println("node not found");
				}

				break;
			}

			case 3:{ 
				startTime = System.currentTimeMillis();
				addProperty(nodeId, valueNode);
				endTime = System.currentTimeMillis();
				break;
			}
			case 4: {
				startTime = System.currentTimeMillis();
				String prop=getProperty(nodeId);
				endTime = System.currentTimeMillis();
				System.out.println("Property is:"+prop);
				break;
			}
			case 5: {
				startTime = System.currentTimeMillis();
				addRelationship(nodeId1, nodeId2, valueEdge);
				endTime = System.currentTimeMillis();
				break;
			}

			case 6: {
				startTime = System.currentTimeMillis();
				getRelationship(nodeId);
				endTime = System.currentTimeMillis();
				break;
			}

			case 7: {
				startTime = System.currentTimeMillis();
				removeNode(nodeId);
				endTime = System.currentTimeMillis();
				break;
			}
			case 8:{ 
				startTime = System.currentTimeMillis();
				removeNodeProperty(nodeId);
				endTime = System.currentTimeMillis();
				break;
			}
			case 9:{ 
				startTime = System.currentTimeMillis();
				removeRelationship(nodeId1, nodeId2);
				endTime = System.currentTimeMillis();
				break;
			}
			case 10:{
				startTime = System.currentTimeMillis();
				removeEdgeProperty(nodeId);
				endTime = System.currentTimeMillis();
				break;
			}
			case 11: {
				startTime = System.currentTimeMillis();
				addEdgeProperty(nodeId1, nodeId2);
				endTime = System.currentTimeMillis();
				break;
			}
			}

			long totalTime = endTime - startTime;
			System.out.println("Time taken is:" +totalTime + " millisecond");
		}

	}

	private Node addSingleNode(String nodeId,String valueNode){
		try ( Transaction tx = graphDb.beginTx() ){
			node = graphDb.createNode();
			node.setProperty("id",nodeId );
			node.setProperty("nproperty",valueNode );
			
			tx.success();
		}
		return node;
	}

	private Node getNode(String nodeId){
		System.out.println("here");
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id", nodeId).getSingle();
			tx.success();
		}
		return node;
	}

	private void addProperty(String nodeId,String value){
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id", nodeId).getSingle();
			node.setProperty("nproperty", value);
			tx.success();
		}

	}

	private String getProperty(String nodeId){
		String prop;
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id",nodeId).getSingle();
			prop=(String) node.getProperty("nproperty");
			tx.success();
		}
		return prop;
	}

	private void addRelationship(String nodeId1, String nodeId2, String relPropValue){
		try ( Transaction tx = graphDb.beginTx() ){
			firstNode=index.get("id",nodeId1).getSingle();
			secondNode=index.get("id",nodeId2).getSingle();
			rel = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
			rel.setProperty("eproperty", relPropValue);
			tx.success();
		}

	}
	private Iterable<Relationship> getRelationship(String nodeId){

		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id",nodeId).getSingle();
			itrRel=node.getRelationships();
			tx.success();
		}
		return itrRel;

	}

	private void removeNode(String nodeId){
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id",nodeId).getSingle();
			node.getSingleRelationship( RelTypes.KNOWS, Direction.BOTH ).delete();
			node.delete();
			tx.success();
		}
	}
	private void removeNodeProperty(String nodeId){
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id",nodeId).getSingle();
			node.removeProperty("nproperty");
			tx.success();
		}
	}
	private void removeEdgeProperty(String nodeId){
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id",nodeId).getSingle();
			rel=node.getSingleRelationship(RelTypes.KNOWS, Direction.BOTH);
			rel.removeProperty("eproperty");
			tx.success();
		}
	}

	private void removeRelationship(String nodeId1, String nodeId2){

		try ( Transaction tx = graphDb.beginTx() ){
			firstNode=index.get("id",nodeId1).getSingle();
			secondNode=index.get("id",nodeId2).getSingle();
			PathExpander expander= Traversal.pathExpanderForAllTypes(Direction.BOTH);
			PathFinder<Path> finder = GraphAlgoFactory.shortestPath(expander,1, 1);
			Path p=finder.findSinglePath(firstNode, secondNode);
			if(p!=null){
				itrRel=p.relationships();
				itRel=	itrRel.iterator();
				while(itRel.hasNext()){
					rel=itRel.next();
					rel.delete();
				}
			}
			tx.success();
		}
	}

	private void removeRelationship2(String nodeId1, String nodeId2){
		try ( Transaction tx = graphDb.beginTx() ){
			firstNode=index.get("id",nodeId1).getSingle();
			secondNode=index.get("id",nodeId2).getSingle();
			itrRel = firstNode.getRelationships();
			itRel=itrRel.iterator();
			while(itRel.hasNext()){
				rel=itRel.next();
				if(rel.getStartNode().equals(firstNode) && rel.getEndNode().equals(secondNode))
					rel.delete();
			}
			tx.success();
		}

	}
	private void addEdgeProperty(String nodeId1, String nodeId2){

	}
	private ArrayList<Node> getNeighbors(String nodeId){
		ArrayList<Node> arls= new ArrayList<Node>(); 
		try ( Transaction tx = graphDb.beginTx() ){
			itrRel =getRelationship(nodeId); //use micro-benchmark
			itRel=itrRel.iterator();
			while(itRel.hasNext()){
				rel=itRel.next();
				firstNode=rel.getStartNode();
				if(!node.equals(firstNode))
					arls.add(firstNode);
				secondNode=rel.getEndNode();
				if(!node.equals(secondNode))
					arls.add(secondNode);
			}
			tx.success();
		}
		return arls;
	}

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
