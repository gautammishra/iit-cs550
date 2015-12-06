<h3>Evaluation of Distributed Key-Value storage systems </h3>
<b>Evaluated following systems:</b>
<ol>
<li>Redis</li>
<li>MongoDB</li>
<li>Cassandra</li>
<li>Riak</li>
<li>MyDHT (Distributed Hash Table implemented in Assignment 2)</li>
</ol>

<b>Experimental setup for the evaluation was as follows:</b>
<ul>
<li>Cluster of 16 nodes where each node is an Amazon EC2 m3.medium instance</li>
<li>Each node comprises 1 or 2 EC2 instances<sup>#</sup></li>
<li>Random generated key-value pairs.</li>
<li>Key size:  10 bytes, Value size: 90 bytes</li>
<li>Parallel requests from variable no. of clients (1, 2, 4, 8 and 16)</li>
<li>Each node initiates 100,000 requests.</li>
</ul>

<sup>#</sup> Redis uses a master-slave configuration for each node. Therefore, each node of Redis comprises two   Amazon m3.medium instances.
