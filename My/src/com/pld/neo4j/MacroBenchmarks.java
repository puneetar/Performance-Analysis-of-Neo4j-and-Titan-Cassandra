package com.pld.neo4j;

import java.util.HashSet;
import java.util.Iterator;

import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.Traversal;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;

import com.pld.neo4j.EmbeddedNeo4j.RelTypes;
public class MacroBenchmarks {

	Node node,pathNode;
	
	public void k_hop_neighbors(String nodeId, int k){
		HashSet<String> hst= new HashSet<String>();
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			node=EmbeddedNeo4j.index.get("id", nodeId).getSingle();
			int numberOfFriends = 0;
			String output = null;
			TraversalDescription td = Traversal.description()
		            .depthFirst()
		            .relationships( RelTypes.KNOWS, Direction.BOTH )
		            .evaluator( Evaluators.excludeStartPosition() )
		            .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
		    Traverser traverser= td.evaluator(Evaluators.toDepth(k)).traverse( node );
			//System.out.println("before");
		    for ( Node currnode  : traverser.nodes())
		    {
//		    	System.out.println(path);
//		    	Iterable itr=path.nodes();
//		    	Iterator it= itr.iterator();
//		    	while(it.hasNext()){
//		    		Node node=(Node) it.next();
		    		 output += currnode.getProperty( "id" ) + "\n";
		    	//}
		       
		    }
		    //output += "Number of friends found: " + numberOfFriends + "\n";
		    System.out.println(output);
			tx.success();
		}
	}
	public void select_nodes(){

	}
	public void get_neighbors(){

	}
	public void set_edges(){

	}
}
