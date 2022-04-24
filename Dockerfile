#---------------------------
# Maven
#---------------------------

FROM maven:3.5.3-jdk-8 AS maven-build

RUN apt-get update

ENV MAVEN_OPTS="-Xmx1024m"

WORKDIR /usr/semver

# Copy Docker settings
COPY settings-docker.xml shared/.m2/settings-docker.xml

# Copy semver.sh
COPY semver.sh .
RUN chmod +x semver.sh

# Download required GitHub projects
RUN git clone https://github.com/crossminer/maracas.git
RUN git clone https://github.com/crossminer/aethereal.git
RUN git clone https://github.com/tdegueul/japicmp.git
COPY code/cypher-queries/ cypher-queries/


#---------------------------
# Semver + Neo4j
#---------------------------

FROM maven-build AS semver

RUN apt-get install xz-utils net-tools -y

WORKDIR /usr

# Download Neo4j Community edition
ADD https://neo4j.com/artifact.php?name=neo4j-community-3.5.18-unix.tar.gz neo4j/neo4j-community-3.5.18-unix.tar.gz
#COPY neo4j-community-3.5.18-unix.tar.gz neo4j/neo4j-community-3.5.18-unix.tar.gz
RUN tar xvf neo4j/neo4j-community-3.5.18-unix.tar.gz -C neo4j/

# Download database
ADD https://zenodo.org/record/1489120/files/maven-data.raw.tar.xz?download=1 neo4j/neo4j-community-3.5.18/data/databases/
#COPY maven-data.raw.tar.xz neo4j/neo4j-community-3.5.18/data/databases/
RUN tar xvfJ neo4j/neo4j-community-3.5.18/data/databases/maven-data.raw.tar.xz -C neo4j/neo4j-community-3.5.18/data/databases

# Increase heap size
RUN echo "dbms.memory.heap.initial_size=512m" >> /usr/neo4j/neo4j-community-3.5.18/conf/neo4j.conf
RUN echo "dbms.memory.heap.max_size=2g" >> /usr/neo4j/neo4j-community-3.5.18/conf/neo4j.conf

# Set initial password
RUN ./neo4j/neo4j-community-3.5.18/bin/neo4j-admin set-initial-password j4oen

# Install maven artifacts
RUN mvn -f semver/aethereal/aethereal/pom.xml clean install -Drelax -gs semver/shared/.m2/settings-docker.xml
RUN mvn -f semver/japicmp/pom.xml clean install -DskipTests -Drelax -gs semver/shared/.m2/settings-docker.xml
RUN mvn -f semver/maracas/maracas/pom.xml -llr clean install -Drelax -gs semver/shared/.m2/settings-docker.xml
RUN mvn -f semver/cypher-queries/pom.xml clean install -Drelax -gs semver/shared/.m2/settings-docker.xml

# EXPOSE 7474
# EXPOSE 7473
# EXPOSE 7687

# Run semver.sh script
CMD /usr/semver/semver.sh

# BUILD
# docker build --tag semver-image .

# RUN
# docker run --name semver semver-image
# docker run -p 7474:7474 -p 7687:7687 --name semver semver-image

# REMOVE
# docker rm --force semver

# SSH CONTAINER
# docker exec -it semver /bin/bash