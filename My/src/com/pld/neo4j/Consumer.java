package com.pld.neo4j;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;

public class Consumer implements Runnable {
	private final BlockingQueue<String> queue;
	ReadableIndex<Node> nodeAutoIndex2;
	private String[] arr_prop=new String[1000];
	public static boolean flag=true;
	
	Consumer(BlockingQueue<String> q, ReadableIndex<Node> nodeAutoIndex2) { 
		queue = q; 
		this.nodeAutoIndex2=nodeAutoIndex2;
	}

	Node firstNode;

	public void run() {
		for(int i=0;i<1000;i++){
			arr_prop[i]="np"+i;
		}
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			while (flag && queue.size()==0) 
			{ 
				consume(queue.take()); 
				
			}
			tx.success();
		} catch (InterruptedException ex) { }
	}

	void consume(String string) { 
		
		if(nodeAutoIndex2.get("ID", string).getSingle()==null){
			System.out.println("Size of queue: "+queue.size() + "\t Inserted Node ID : "+string);
			firstNode = EmbeddedNeo4j.graphDb.createNode();
			firstNode.setProperty( "ID", string );
			firstNode.setProperty( "property1", arr_prop[new Random().nextInt(1000)] );	
		}
	}
}