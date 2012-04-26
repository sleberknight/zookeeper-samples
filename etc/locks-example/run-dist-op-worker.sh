# This script takes one argument, a name to identity the client.
# It runs the code in the com.nearinfinity.examples.zookeeper.lock.WorkerUsingDistributedOperation class,
# which wraps the core ZK recipes WriteLock in BlockingWriteLock to make the client code look synchronous,
# and adds assurance that the lock is released (via the DistributedOperation interface).

java -cp ../../target/classes:../../etc/examples-libs/* com.nearinfinity.examples.zookeeper.lock.WorkerUsingDistributedOperation localhost:2181 /sample-lock $1
