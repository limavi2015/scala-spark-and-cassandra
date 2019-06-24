# scala-spark-and-cassandra

This project is an exercise in scala to read and write information in Cassandra database using Spark.



* Run docker container
docker run --name cassandra --network bridge -d -p 9042:9042 cassandra:3.11

* To enter the container
docker exec -it cassandra bash
cd bin
cqlsh

* Commands
CREATE KEYSPACE public WITH replication = {'class': 'SimpleStrategy','replication_factor' : 1};

use public;

CREATE TABLE user (usuario text,nombre text,apellido text,pais text,edad int,PRIMARY KEY (usuario));
 
insert into user (usuario, nombre, apellido, pais, edad)values ('usuario1', 'Milu', 'Arevalo', 'Colombia', 3);

select * from user;

