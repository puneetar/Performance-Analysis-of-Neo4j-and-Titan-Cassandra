package com.pld.neo4j;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
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
	private String[] arr_prop=new String[1000];
	public static String PATH_CSV_FILE="final.csv";
	// START SNIPPET: vars
	public static GraphDatabaseService graphDb;
	HashMap<String, String> haConfig;
	BatchInserter inserter;
	Node firstNode;
	Node secondNode;
	Relationship relationship;

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
	ReadableIndex<Node> nodeAutoIndex=null;
	void createDb()
	{
		clearDb();
		for(int i=0;i<1000;i++){
			arr_prop[i]="np"+i;
		}

		haConfig = new HashMap<String, String>();


		haConfig.put("ha.server_id", "7");
		haConfig.put("ha.initial_hosts", "192.168.0.199:5001,192.168.0.198:5001,192.168.0.197:5001,192.168.0.199:5002");
		haConfig.put("ha.server", "192.168.0.199:6002");
		haConfig.put("ha.cluster_server", "192.168.0.199:5002");
		haConfig.put("org.neo4j.server.database.mode", "HA");

		graphDb = new HighlyAvailableGraphDatabaseFactory()
		.newHighlyAvailableDatabaseBuilder(DB_PATH)
		.setConfig(haConfig).
		setConfig( GraphDatabaseSettings.node_keys_indexable, "id" ).
		setConfig( GraphDatabaseSettings.node_auto_indexing, "true" ).		
		newGraphDatabase();
		registerShutdownHook( graphDb );

		//		
		//		
				nodeAutoIndex =graphDb.index().getNodeAutoIndexer().getAutoIndex();
		//	 TransactionalGraph graph = new Neo4jHaGraph("/path/to/ha_db_dir", haConfig);

		try {
			//addNodes();
			//addNodesBatchInsert();
			//addBatch();
			addNodesMultiThread(nodeAutoIndex);
		} catch (ConstraintViolationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		} 
		//		catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
		//		

	}
	
	private void addNodesMultiThread(ReadableIndex<Node> nodeAutoIndex2){

		BlockingQueue<String> q = new ArrayBlockingQueue<String>(1024);
		Producer p = new Producer(q);
		Consumer c1 = new Consumer(q,nodeAutoIndex2);
		//Consumer c2 = new Consumer(q);
		new Thread(p).start();
		new Thread(c1).start();
		//new Thread(c2).start();

	}
	
	private void addBatch(){
		GraphDatabaseService batchDb =
				BatchInserters.batchDatabase( DB_PATH, haConfig );
		Label personLabel = DynamicLabel.label( "Person" );
		Node mattiasNode = batchDb.createNode( personLabel );
		mattiasNode.setProperty( "name", "Mattias" );
		Node chrisNode = batchDb.createNode();
		chrisNode.setProperty( "name", "Chris" );
		chrisNode.addLabel( personLabel );
		RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
		mattiasNode.createRelationshipTo( chrisNode, knows );
		batchDb.shutdown();
	}

	private void addNodesBatchInsert() throws ConstraintViolationException, IOException{


		FileReader fr=new FileReader(PATH_CSV_FILE);
		BufferedReader br=new BufferedReader(fr,10240);
		String[] arr_token=null;
		String line=null;

		Label userLabel = DynamicLabel.label( "Users" );
		inserter = (BatchInserterImpl)BatchInserters.inserter( DB_PATH);
		inserter.createDeferredSchemaIndex( userLabel ).on( "ID" ).create();


		Map<String, Object> properties= new HashMap<>();

		while((line=br.readLine())!=null)
		{
			//arr_token=line.split(" ");
			System.out.println(line);

			//Map<String, Object> properties = new HashMap<>();
			properties.clear();
			properties.put( "ID", line );
			properties.put("property1", arr_prop[new Random().nextInt(1000)] );
			long mattiasNode = inserter.createNode( properties, userLabel );

			//				properties.put( "ID", "Chris" );
			//				
			//				long chrisNode = inserter.createNode( properties, userLabel );
			//				RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
			//				// To set properties on the relationship, use a properties map
			//				// instead of null as the last parameter.
			//				inserter.createRelationship( mattiasNode, chrisNode, knows, null );

		}

		inserter.shutdown();
	}

	private void addNodes(){
		try ( Transaction tx = graphDb.beginTx() ){

			FileReader fr=new FileReader(PATH_CSV_FILE);
			BufferedReader br=new BufferedReader(fr,10240);
			String[] arr_token=null;
			String line=null;
			br.readLine();
			while((line=br.readLine())!=null)
			{
				arr_token=line.split(" ");

				if(nodeAutoIndex.get("ID", arr_token[0]).getSingle()==null){
					firstNode = graphDb.createNode();
					firstNode.setProperty( "ID", arr_token[0] );
					firstNode.setProperty( "property1", arr_prop[new Random().nextInt(1000)] );	
				}

				if(nodeAutoIndex.get("ID", arr_token[1]).getSingle()==null){
					firstNode = graphDb.createNode();
					firstNode.setProperty( "ID", arr_token[1] );
					firstNode.setProperty( "property1", arr_prop[new Random().nextInt(1000)] );	
				}

			}

			tx.success();
			br.close();fr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

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
	void removeData()
	{
		try ( Transaction tx = graphDb.beginTx() )
		{
			// START SNIPPET: removingData
			// let's remove the data
			firstNode.getSingleRelationship( RelTypes.KNOWS, Direction.OUTGOING ).delete();
			firstNode.delete();
			secondNode.delete();
			// END SNIPPET: removingData

			tx.success();
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
