# This script takes one argument, a name to identity the client.
# It runs the code in the com.nearinfinity.examples.zookeeper.lock.WorkerUsingCurator class,
# which uses Curator's InterProcessMutex distributed lock.

# Arguments:
# $1 - name
# $2 - optional max wait time in seconds

java -cp ../../target/classes:../../etc/examples-libs/* com.nearinfinity.examples.zookeeper.lock.WorkerUsingCurator localhost:2181 /curator-lock $1 $2 &
