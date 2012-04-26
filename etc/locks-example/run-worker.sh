# This script takes one argument, a name to identity the client.
# It runs the code in the com.nearinfinity.examples.zookeeper.lock.Worker class,
# which uses the core ZK recipes WriteLock and deals with the async behavior.

java -cp ../../target/classes:../../etc/examples-libs/* com.nearinfinity.examples.zookeeper.lock.Worker localhost:2181 /sample-lock $1
