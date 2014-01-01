package com.pld.neo4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.time.StopWatch;
import org.apache.velocity.runtime.directive.Evaluate;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import com.pld.neo4j.EmbeddedNeo4j.RelTypes;
public class MacroBenchmarks {

	Node node,pathNode;

	public void k_hop_neighbors(String nodeId, int k){
		StopWatch watch=new StopWatch();
		String output = null;
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.index.get("id", nodeId).getSingle();
			watch.start();
			TraversalDescription td = Traversal.description()
					.depthFirst()
					.relationships( RelTypes.KNOWS, Direction.BOTH )
					.evaluator( Evaluators.excludeStartPosition() )
					.uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
			Traverser traverser= td.evaluator(Evaluators.toDepth(k)).traverse( node );
			//System.out.println("before");
			if(traverser!=null)
				for ( Node currnode  : traverser.nodes())
				{
					output += currnode.getProperty( "id" ) + "\n";

				}
			watch.stop();
			//output += "Number of friends found: " + numberOfFriends + "\n";
			System.out.println(output);
			System.out.println("Total time taken for getting edge is :"+watch.getTime()+" milliseconds");
			tx.success();
		}
	}
	public void k_hop_neighbors1(String nodeId, int k){

	}
	public ArrayList<Node> select_nodes(String filter){
		//		String output="";
		ArrayList<Node> arls= new ArrayList<Node>();
		StopWatch watch=new StopWatch();
		GlobalGraphOperations gb=GlobalGraphOperations.at(EmbeddedNeo4j.graphDb);
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			watch.start();
			
			Iterable<Node> itnode=gb.getAllNodes();
			Iterator<Node> itrNode= itnode.iterator();
			node=itrNode.next();
			while(itrNode.hasNext()){
				node=itrNode.next();
				String prop=(String) node.getProperty("nproperty");
				if(prop.contains(filter)){
					System.out.println(node.getProperty("id")+":"+prop);
					arls.add(node);
				}
			}
			watch.stop();
			tx.success();
		}
		System.out.println("Total time taken for getting edge is :"+watch.getTime()+" milliseconds");
		return arls;
	}
	public ArrayList<String> get_neighbors(String nodeId, String filter){
		//k_hop_neighbors(nodeId, 1);
		ArrayList<String> arls= new ArrayList<String>();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.index.get("id", nodeId).getSingle();
			for (Relationship rel: node.getRelationships()){
				Node neighbour = rel.getOtherNode(node);
				if(neighbour.getProperty("eproperty").equals(filter))
					arls.add((String) neighbour.getProperty("id"));
			}
			tx.success();
		}
		//System.out.println("Total time taken for getting edge is :"+watch.getTime()+" milliseconds");
		return arls;
	}
	public ArrayList<Relationship> getEdges(final String filter){
		StopWatch watch=new StopWatch();
		ArrayList<Relationship> arls= new ArrayList<Relationship>();
		Relationship rel;
		Iterator<Relationship> itr;
		Iterable<Relationship> itrel;
		GlobalGraphOperations gb=GlobalGraphOperations.at(EmbeddedNeo4j.graphDb);
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			watch.start();
			itrel=gb.getAllRelationships();
			itr=itrel.iterator();
			while(itr.hasNext()){
				rel=itr.next();
				String prop=(String) rel.getProperty("eproperty");
				if(prop.contains(filter))
					arls.add(itr.next());
			}
			watch.stop();
			tx.success();
		}
		System.out.println(arls.size());
		System.out.println("Total time taken for getting edge is :"+watch.getTime()+" milliseconds");
		return arls;
	}
}