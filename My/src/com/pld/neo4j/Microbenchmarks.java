package com.pld.neo4j;

import java.util.Iterator;
import java.util.Random;

import org.apache.commons.lang.time.StopWatch;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Traversal;


public class Microbenchmarks {


	Node node,firstNode, secondNode;
	String valueNode,valueEdge;
	Relationship rel;
	Iterable<Relationship> itrRel;
	Iterator<Relationship> itRel;
	public void initialize(){

	}

	public  void addSingleNode(String nodeId){
		StopWatch watch=new StopWatch();
		valueNode=EmbeddedNeo4j.arr_prop_node[new Random().nextInt(1000)];
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			watch.start();
			node = EmbeddedNeo4j.graphDb.createNode();
			watch.stop();
			node.setProperty("id",nodeId );
			node.setProperty("nproperty", valueNode);
			tx.success();
		}
		System.out.println("Total time taken to add node is :"+watch.getTime()+" milliseconds");
	}

	public Node getNode(String nodeId){
		StopWatch watch=new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			watch.start();
			node=EmbeddedNeo4j.index.get("id", nodeId).getSingle();
			//node=index.get("id", "1287").getSingle();
			watch.stop();
			tx.success();
		}

		System.out.println("Total time taken for getting node is :"+watch.getTime()+" milliseconds");
		return node;
	}

	public void addNodeProperty(String nodeId){
		valueNode=EmbeddedNeo4j.arr_prop_node[new Random().nextInt(1000)];
		StopWatch watch=new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.graphDb.createNode();
			watch.start();
			node.setProperty("id", nodeId);
			watch.stop();
			System.out.println("Total time taken for adding node  id property is :"+watch.getTime()+" milliseconds");
			watch.reset();
			watch.start();
			node.setProperty("nproperty", valueNode);
			watch.stop();
			System.out.println("Total time taken for adding node other property is :"+watch.getTime()+" milliseconds");
			tx.success();
		}


	}

	public String getNodeProperty(String nodeId){
		String prop;
		StopWatch watch=new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.index.get("id", nodeId).getSingle();
			watch.start();
			prop=(String) node.getProperty("nproperty");
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for getting node property is :"+watch.getTime()+" milliseconds");
		return prop;
	}

	public void addRelationship(String nodeId1, String nodeId2){
		StopWatch watch=new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			firstNode=EmbeddedNeo4j.index.get("id", nodeId1).getSingle();
			secondNode=EmbeddedNeo4j.index.get("id", nodeId2).getSingle();
			watch.start();
			rel = firstNode.createRelationshipTo( secondNode, EmbeddedNeo4j.RelTypes.KNOWS );
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for addinge edge is :"+watch.getTime()+" milliseconds");


	}
	public Relationship getRelationship(String nodeId1, String nodeId2){
		StopWatch watch=new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			firstNode=EmbeddedNeo4j.index.get("id",nodeId1).getSingle();
			secondNode=EmbeddedNeo4j.index.get("id",nodeId2).getSingle();
			watch.start();
			PathExpander expander= Traversal.pathExpanderForAllTypes(Direction.BOTH);
			PathFinder<Path> finder = GraphAlgoFactory.shortestPath(expander,1, 1);
			Path p=finder.findSinglePath(firstNode, secondNode);
			if(p!=null){
				itrRel=p.relationships();
				itRel=	itrRel.iterator();
				while(itRel.hasNext()){
					rel=itRel.next();
				}
			}
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for getting edge is :"+watch.getTime()+" milliseconds");
		return rel;

	}

	public void removeNode(String nodeId){
		StopWatch watch= new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.index.get("id",nodeId).getSingle();
			watch.start();
			itRel=node.getRelationships().iterator();
			while(itRel.hasNext()){
				rel=itRel.next();
				rel.delete();
			}
			node.delete();
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for removing node is :"+watch.getTime()+" milliseconds");

	}
	public void removeNodeProperty(String nodeId){
		StopWatch watch= new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.index.get("id",nodeId).getSingle();
			watch.start();
			node.removeProperty("nproperty");
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for removing node property is :"+watch.getTime()+" milliseconds");
	}
	public void removeEdgeProperty(String nodeId1, String nodeId2){
		StopWatch watch= new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			Relationship rel=getRelationship(nodeId1,nodeId2);
			watch.start();
			rel.removeProperty("eproperty");
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for removing edge property  is :"+watch.getTime()+" milliseconds");
	}

	public void removeRelationship(String nodeId1, String nodeId2){
		StopWatch watch= new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			firstNode=EmbeddedNeo4j.index.get("id",nodeId1).getSingle();
			secondNode=EmbeddedNeo4j.index.get("id",nodeId2).getSingle();
			watch.start();
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
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for removing edge is :"+watch.getTime()+" milliseconds");
	}

	public void removeRelationship2(String nodeId1, String nodeId2){
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			firstNode=EmbeddedNeo4j.index.get("id",nodeId1).getSingle();
			secondNode=EmbeddedNeo4j.index.get("id",nodeId2).getSingle();
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
	public void addEdgeProperty(String nodeId1, String nodeId2){
		StopWatch watch=new StopWatch();
		valueEdge=EmbeddedNeo4j.arr_prop_edge[new Random().nextInt(1000)];
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			rel=getRelationship(nodeId1, nodeId2);
			rel.removeProperty("eproperty");
			watch.start();
			rel.setProperty("eproperty",valueEdge );
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for adding edge property is :"+watch.getTime()+" milliseconds");
	}

	public void updateNode(String nodeId, String value){
		updateNodeProperty(nodeId, value);		
	}


	public void updateNodeProperty(String nodeId, String value){
		StopWatch watch= new StopWatch();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.index.get("id",nodeId).getSingle();
			System.out.println("Node previous value is:"+node.getProperty("nproperty"));
			watch.start();
			node.setProperty("nproperty", value);
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for updating node/property is :"+watch.getTime()+" milliseconds");

	}
	public void updateEdge(String nodeId1, String nodeId2, String value){
		updateEdgeProperty(nodeId1, nodeId2, value);
	}
	public String getEdgeProperty(String nodeId1, String nodeId2){
		StopWatch watch= new StopWatch();
		String edgeProp;
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			rel=getRelationship(nodeId1, nodeId2);

			watch.start();
			edgeProp=(String) rel.getProperty("eproperty");
			watch.stop();
			tx.success();
		}
		return edgeProp;
	}
	public void updateEdgeProperty(String nodeId1, String nodeId2, String value){
		StopWatch watch= new StopWatch();
		try ( Transaction tx =EmbeddedNeo4j.graphDb.beginTx() ){
			rel=getRelationship(nodeId1, nodeId2);
			System.out.println("Previous value of edge property is :"+ rel.getProperty("eproperty"));
			watch.start();
			rel.setProperty("eproperty", value);
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for updating edge/property is :"+watch.getTime()+" milliseconds");
	}
}
