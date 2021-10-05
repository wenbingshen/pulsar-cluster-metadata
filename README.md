# pulsar-cluster-metadata
Collect metadata of multiple pulsar clusters

# mvn build

mvn clean install


# execute
java -jar pulsar-tools-1.0-SNAPSHOT.jar configFilePath

# example config file
cluster=pulsar_test_cluster_1
pulsar_test_cluster_1.serviceUrl=http://localhost:8080/
pulsar_test_cluster_1.authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
pulsar_test_cluster_1.authParams={"token":"*********"} 

pulsar_test_cluster_1.brokersTask.enable=true
pulsar_test_cluster_1.tenantsTask.enable=true
pulsar_test_cluster_1.namespacesTask.enable=true
pulsar_test_cluster_1.topicsTask.enable=true
