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
	public static String PATH_CSV_FILE="wikipedia_link_fr.csv";

	// START SNIPPET: vars
	public static GraphDatabaseService graphDb;
	HashMap<String, String> haConfig;
	BatchInserter inserter;
	IndexManager indexmgr;
	Index<Node> index;
	IndexHits<Node> indexhits;
	Iterator<Object> itr;
	Node node, firstNode,secondNode;
	Relationship rel;
	Iterable<Relationship> itrRel;
	Iterator<Relationship> itRel;


	private static enum RelTypes implements RelationshipType
	{
		KNOWS
	}
	// END SNIPPET: createReltype

	public static void main( String[] args )
	{
		EmbeddedNeo4j hello = new EmbeddedNeo4j();
		hello.createDb(args[0]);
		// hello.removeData();
		// hello.shutDown();
	}
	public static ReadableIndex<Node> nodeAutoIndex=null;
	void createDb(String choice)
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
		setConfig( GraphDatabaseSettings.node_keys_indexable, "node_auto_index" ).
		setConfig( GraphDatabaseSettings.node_auto_indexing, "true" ).		
		newGraphDatabase();
		registerShutdownHook( graphDb );

		indexmgr = graphDb.index();
		//nodeAutoIndex =graphDb.index().getNodeAutoIndexer().getAutoIndex();
		//
		try ( Transaction tx = graphDb.beginTx() ){
			index=indexmgr.forNodes("node_auto_index");
			//addSingleNode();
			tx.success();
		}
		


		benchmarks(Integer.parseInt(choice));
		

		//micro-benchmarks
		//addSingleNode(nodeId,valueNode);
		//getNode(nodeId);
		//addProperty(nodeId, valueNode);
		//		getProperty(nodeId);
		//addRelationship(nodeId1, nodeId2, valueEdge);
		//		getRelationship(nodeId);
		//		removeNode(nodeId);
		//		removeNodeProperty(nodeId);
		//		removeEdgeProperty(nodeId);
		//removeRelationship(nodeId1,nodeId2);
		//removeRelationship2(nodeId1,nodeId2);
		//		

		
	}

	private void benchmarks(int choice){
		String nodeId="3";
		String nodeId1="3";
		String nodeId2="1";
		String valueNode=arr_prop_node[new Random().nextInt(1000)];
		String valueEdge=arr_prop_edge[new Random().nextInt(1000)];
		System.out.println("\nEnter your choice:\n");
		System.out.println("1.Add node\n 2.Get node \n 3.Add Node Property\n 4. Get Node Property \n 5. Add edge\n 6. Get Edge\n 7.Remove node"
				+ "\n 8. Remove Nodr Property\n 9. Remove Edge \n 10. Remove Edge Property\n 11. Add edge property\n");
		long startTime,endTime;
		startTime = System.currentTimeMillis();
		switch(choice){
		case 1:addSingleNode(nodeId,valueNode);
		break;
		case 2: getNode(nodeId);
		break;
		case 3: addProperty(nodeId, valueNode);
		break;
		case 4: getProperty(nodeId);
		break;
		case 5: addRelationship(nodeId1, nodeId2, valueEdge);
		break;
		case 6: getRelationship(nodeId);
		break;
		case 7: removeNode(nodeId);
		break;
		case 8: removeNodeProperty(nodeId);
		break;
		case 9: removeRelationship(nodeId1, nodeId2);
		break;
		case 10:removeEdgeProperty(nodeId);
		break;
		case 11: addEdgeProperty(nodeId1, nodeId2);
		}
		endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Time taken is" +totalTime + "millisecond");
	}

	private void addSingleNode(String nodeId,String valueNode){
		try ( Transaction tx = graphDb.beginTx() ){
			node = graphDb.createNode();
			node.setProperty( "id",nodeId );
			node.setProperty( "nproperty",valueNode );	
			tx.success();
		}
	}

	private void getNode(String nodeId){
		System.out.println("here");
		try ( Transaction tx = graphDb.beginTx() ){

			index=indexmgr.forNodes("node_auto_index");
			node=index.get("id", nodeId).getSingle();
			tx.success();
		}
	}

	private void addProperty(String nodeId,String value){
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id", nodeId).getSingle();
			node.setProperty("nproperty", value);
			tx.success();
		}

	}

	private void getProperty(String nodeId){
		try ( Transaction tx = graphDb.beginTx() ){
			node=index.get("id",nodeId).getSingle();
			node.getProperty("nproperty");
			tx.success();
		}
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
