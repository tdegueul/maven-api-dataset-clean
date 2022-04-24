#!/bin/sh

# Start Neo4j
/usr/neo4j/neo4j-community-3.5.18/bin/neo4j start

# Wait until the service is up
sleep 20

# Check Neo4j status
/usr/neo4j/neo4j-community-3.5.18/bin/neo4j status
netstat -l

# Clean DB
cd semver/cypher-queries/
mvn -e exec:java -Dexec.mainClass="mcr.BuildDataset" -Dexec.args="-cleanDB" -Dexec.cleanupDaemonThreads=false -Drelax -gs /usr/semver/shared/.m2/settings-docker.xml

# Check Neo4j status
/usr/neo4j/neo4j-community-3.5.18/bin/neo4j status
netstat -l

sleep infinity
#mvn exec:java -Dexec.mainClass="mcr.BuildDataset" -Dexec.args="-deltas" -Dexec.cleanupDaemonThreads=false -Drelax -gs /usr/semver/shared/.m2/settings-docker.xml
#mvn exec:java -Dexec.mainClass="mcr.BuildDataset" -Dexec.args="-versionsRaemaekers" -Dexec.cleanupDaemonThreads=false -Drelax -gs /usr/semver/shared/.m2/settings-docker.xml