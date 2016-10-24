# Dynamo Implementation

This is a sample implementation of Amazon Dynamo using 5 android emulators as nodes.

# Design:

	Membership
		Every node knows every other node. This means that each node knows all other nodes in the system 
		and also knows exactly which partition belongs to which node; any node can forward a request to the 
		correct node without using a ring-based routing.
		
	Request routing
		Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which 
		partition belongs to which node.
		Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor 
		of the key), and the coordinator is in charge of serving read/write operations.
	
	Quorum replication
		Linearizability is ensured by implementing a quorum-based replication used by Dynamo.
		The replication degree N is 3. This means that given a key, the key’s coordinator as well as the 
		2 successor nodes in the Dynamo ring  store the key.
		Both the reader quorum size R and the writer quorum size W is 2.
		The coordinator for a get/put request always contacts other two nodes and get a vote from each 
		(i.e., an acknowledgement for a write, or a value for a read).
		For write operations, all objects are versioned in order to distinguish stale copies from the most 
		recent copy.
		For read operations, if the readers in the reader quorum have different versions of the same object, 
		the coordinator picks the most recent version and returns it.
	
	Failure handling
		Just as the original Dynamo, each request is used to detect a node failure.
		If a node does not respond within the timeout interval, it is considered to have failed.
		Since the reader quorum size R and the writer quorum size W is 2, the system can handle 1 failure 
		without impact on availability and linearizability.
		When a coordinator for a request fails and it does not respond to the request, its successor is 
		contacted next for the request.
