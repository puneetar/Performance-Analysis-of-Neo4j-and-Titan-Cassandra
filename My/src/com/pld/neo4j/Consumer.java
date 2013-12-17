package com.pld.neo4j;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;


public class Consumer implements Runnable {
	private final BlockingQueue<String> queue;
	ReadableIndex<Node> nodeAutoIndex2;

	private String[] arr_prop_node=new String[1000];
	private String[] arr_prop_edge=new String[1000];

	public static boolean flag=true;

	Consumer(BlockingQueue<String> q, ReadableIndex<Node> nodeAutoIndex2) { 
		queue = q; 
		this.nodeAutoIndex2=nodeAutoIndex2;
	}

	Node firstNode,secondNode;

	public void run() {
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}


		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			while (flag && queue.size()!=0) 
			{ 
				consume(queue.take()); 

			}
			tx.success();
		} catch (InterruptedException ex) { }
	}

	private static enum RelTypes implements RelationshipType
	{
		KNOWS
	}

	String[] arr_token=new String[2];
	Relationship relationship;

	void consume(String string) { 

		arr_token=string.split(" ");

		if((firstNode=EmbeddedNeo4j.nodeAutoIndex.get("id", arr_token[0]).getSingle())==null){
			//System.out.println("Size of queue: "+queue.size() + "\t Inserted Node ID : "+arr_token[0]);
			firstNode = EmbeddedNeo4j.graphDb.createNode();
			firstNode.setProperty( "id", arr_token[0] );
			firstNode.setProperty( "n_property", arr_prop_node[new Random().nextInt(1000)] );	
		}

		if((secondNode=EmbeddedNeo4j.nodeAutoIndex.get("id", arr_token[1]).getSingle())==null){
			//System.out.println("Size of queue: "+queue.size() + "\t Inserted Node ID : "+arr_token[1]);
			secondNode = EmbeddedNeo4j.graphDb.createNode();
			secondNode.setProperty( "id", arr_token[1] );
			secondNode.setProperty( "n_property", arr_prop_node[new Random().nextInt(1000)] );	
		}

		System.out.println("creating relationship "+arr_token[0]+ " : "+arr_token[1]);
		relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
		relationship.setProperty( "e_property", arr_prop_edge[new Random().nextInt(1000)] );
	}

}
