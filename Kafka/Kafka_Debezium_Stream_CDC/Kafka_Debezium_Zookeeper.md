## Setting up Kafka + Debezium + Zookeeper for Data Streaming

The best way to setup this environment is to create a docker-compose file containing images and configurations of the three servers.

One of the techniques to use would be to use **CDC**_(Change Data Capture)_ and then stream the table(s)changes using kafka.
 
The procedure is as follows:-


## ****1. Set up CDC on the Postgres database****
CDC is a feature that allows you to track changes to a database table and capture them as a stream of change events. To set up CDC on the Postgres database, you’ll need to perform the following steps:

	1. Enable CDC on the Postgres database by running the following SQL command:

	        ALTER TABLE my_table REPLICA IDENTITY FULL; 
		This command enables full replica identity on the my_table table, which means that Postgres will include the full old and new row data in each change event.


	2. Create a publication for the desired table(s) by running the following SQL command:

	        ALTER TABLE my_table REPLICA IDENTITY FULL; 

	3. This command creates a publication that will stream the changes made to the expenses table.

			SELECT pg_create_logical_replication_slot('my_table_slot', 'pgoutput');

This should setup my_table for CDC.


## **2. Setup Kafka + Debezium + Zookeeper in a Docker container using docker-compose**


Using the docker-compose file located [`here`](url). The file is self explanatory. 

Download the file into your target directory and from a command line tool, navigate to the directory. To build the container, enter the command below

	docker-compose up
After a few minutes, the container should be built and the servers should be up and running.


## 3. Creating a Kafka connection to the Database Table

The debezium API can be use to see available connectors and also to create new connectors.

Open a tool like Postman and: 

    1. To test if Debezium is working , send a GET request to the url 

			http://<debezium_Server_IP>:8083
		If working, you should see the version and worker name

	2. To see available connectors, send a GET request to the url 

			http://<debezium_Server_IP>:8083/connectors
		If available, you shold see a list of available connectors

	3. To add a connector, send a POST request to the url 

			http://<debezium_Server_IP>:8083/connectors
		The body of the request should be like the JSON below
			
```
     {
         "name": "name_of-connector",
         "config": {
    	 "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    	 "database.hostname": "database_host",
    	 "database.port": "5432",
	     "database.user": "database_user",
    	 "database.password": "database_password",
	     "database.dbname": "database_name",
	     "plugin.name": "pgoutput",
	     "database.server.name": "source",
	     "key.converter.schemas.enable": "false",
	     "value.converter.schemas.enable": "false",
	     "transforms": "unwrap",
	     "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
	     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
	     "key.converter": "org.apache.kafka.connect.json.JsonConverter",
	     "table.include.list": "public.my_table",
	     "slot.name": "dbz_my_table_slot"
 	 }
	}
	
```	
   If the request succeds, you should see the same paylod sent in the response body. 

   For more info about creatin Debezium connectors, go [`here`](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties).

An example Postman collection to illustrate the above can be found [`here`](url).


Once a new connector is created, an associated kafka topic is also authomatically created. This topic can be subscribed(listened) to, get the messages being streamed and process them.

Using PyKafka, we can view our kafka topics [`here`](url) and consumers [`here`](url).
	

For furthe info and reference, chechout these videos

[1 Set up Debezium, Apache Kafka and Postgres for real time Data Streaming | Real Time ETL | ETL 
](https://www.youtube.com/watch?v=9yP_75OBWis)

[2. How to Stream Data using Apache Kafka & Debezium from Postgres | Real Time ETL | ETL | Part 2](https://www.youtube.com/watch?v=xh9rVSqNHMI&amp;t=3s)

