package com.pld.neo4j;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

public class EmbeddedNeo4j {

	private static final String DB_PATH = "data";

	public String greeting;

	// START SNIPPET: vars
	GraphDatabaseService graphDb;
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
		String storeDir="mydata";
		String pathToConfig="./";
		hello.createDb(storeDir,pathToConfig);
		// hello.removeData();
		// hello.shutDown();
	}

	void createDb(String storeDir, String pathToConfig)
	{
		clearDb();

		HashMap<String, String> haConfig = new HashMap<String, String>();

		haConfig.put("ha.server_id", "4");
		haConfig.put("ha.initial_hosts", "192.168.0.199:5001,192.168.0.198:5001,192.168.0.197:5001,192.168.0.199:5002");
		haConfig.put("ha.server", "192.168.0.199:6002");
		haConfig.put("ha.cluster_server", "192.168.0.199:5002");
		haConfig.put("org.neo4j.server.database.mode", "HA");

		graphDb = new GraphDatabaseFactory()
		.newEmbeddedDatabaseBuilder("test.db")
		.setConfig(haConfig)
		.setConfig(GraphDatabaseSettings.node_auto_indexing,"true")
		.setConfig(GraphDatabaseSettings.node_keys_indexable,"id")
		.newGraphDatabase();
		//	        System.out.println(pathToConfig);
		//	        graphDb = new GraphDatabaseFactory()
		//	        .newEmbeddedDatabaseBuilder( storeDir )
		//	        .loadPropertiesFromFile( pathToConfig + "neo4j.properties" )
		//	        .newGraphDatabase();
		// START SNIPPET: startDb
		//graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		registerShutdownHook( graphDb );
		// END SNIPPET: startDb

		// START SNIPPET: transaction

		addNodes();
		// END SNIPPET: transaction
	}

	private void addNodes(){
		try ( Transaction tx = graphDb.beginTx() )
		{
			// Database operations go here
			// END SNIPPET: transaction
			// START SNIPPET: addData
			firstNode = graphDb.createNode();
			firstNode.setProperty( "message", "Hello, " );
			secondNode = graphDb.createNode();
			secondNode.setProperty( "message", "World!" );

			relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
			relationship.setProperty( "message", "brave Neo4j " );
			// END SNIPPET: addData

			// START SNIPPET: readData
			System.out.print( firstNode.getProperty( "message" ) );
			System.out.print( relationship.getProperty( "message" ) );
			System.out.print( secondNode.getProperty( "message" ) );
			// END SNIPPET: readData

			greeting = ( (String) firstNode.getProperty( "message" ) )
					+ ( (String) relationship.getProperty( "message" ) )
					+ ( (String) secondNode.getProperty( "message" ) );

			// START SNIPPET: transaction
			tx.success();
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
