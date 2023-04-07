

[`Image for this set up`](url)


## Setting up Kafka + RabbitMQ + Zookeeper for Data Streaming

To achieve the setup, follow the example mention [`here`](url) and use the docker-compose file located [`here`](url) instead.

There's no debezium needed in this example, instead we use `Kafka-connect`.


## Create queue and test message on RabbitMQ

This uses the Management API which has been enabled on the Docker container automagically.

1. Create the queue

```
curl --user guest:guest \
      -X PUT -H 'content-type: application/json' \
      --data-binary '{"vhost":"/","name":"test-queue-01","durable":"true","auto_delete":"false","arguments":{"x-queue-type":"classic"}}' \
      'http://localhost:15672/api/queues/%2F/test-queue-01'
```

2. Confirm that the queue has been created

```
curl -s --user guest:guest \
        -X GET -H 'content-type: application/json' \
        'http://localhost:15672/api/queues/%2F/' | jq '.[].name'
"test-queue-01"
```
You should get the following output.

	"test-queue-01"

3. Send a test message

```
curl --silent --user guest:guest \
        -X POST -H 'content-type: application/json' \
        --data-binary '{"ackmode":"ack_requeue_true","encoding":"auto","count":"10"}' \
        'http://localhost:15672/api/queues/%2F/test-queue-01/get' | jq '.[].payload|fromjson'
```

You should get the following output.

	{
  	   "transaction": "PAYMENT",
  	   "amount": "$125.0",
  	   "timestamp": "Wed 8 Jan 2020 10:41:45 GMT"
	}


You can see the RabbitMQ Web UI (login guest/guest) at [`http://<Your_Server_IP>:15672`](url)


## Create the Kafka Connect connector

This uses the [RabbitMQ plugin for Kafka Connect](https://docs.confluent.io/kafka-connectors/rabbitmq-source/current/overview.html), which has been installed in the Docker container already. You can install it yourself from [`Confluent Hub`](https://www.confluent.io/hub/).


```
curl -i -X PUT -H  "Content-Type:application/json" \
    http://localhost:8083/connectors/source-rabbitmq-00/config \
    -d '{
        "connector.class" : "io.confluent.connect.rabbitmq.RabbitMQSourceConnector",
        "kafka.topic" : "rabbit-test-00",
        "rabbitmq.queue" : "test-queue-01",
        "rabbitmq.username": "guest",
        "rabbitmq.password": "guest",
        "rabbitmq.host": "rabbitmq",
        "rabbitmq.port": "5672",
        "rabbitmq.virtual.host": "/",
        "confluent.license":"",
        "confluent.topic.bootstrap.servers":"kafka:29092",
        "confluent.topic.replication.factor":1,
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter"
    } '
```

With the connector created we check that it’s running:

```
curl -s "http://localhost:8083/connectors?expand=info&expand=status" | \
           jq '. | to_entries[] | [ .value.info.type, .key, .value.status.connector.state,.value.status.tasks[].state,.value.info.config."connector.class"]|join(":|:")' | \
           column -s : -t| sed 's/\"//g'| sort
```

You should get the following output.

	source  |  source-rabbitmq-00  |  RUNNING  |  RUNNING  |  io.confluent.connect.rabbitmq.RabbitMQSourceConnector


An example Postman collection to illustrate the above can be found [`here`](url).


Once a new connector is created, an associated kafka topic is also authomatically created. This topic can be subscribed(listened) to, get the messages being streamed and process them.

Using PyKafka, we can view our kafka topics [`here`](url) and consumers [`here`](url).


The above setup should be able to get you up and running. For further info you can checkout this [`blog`](https://rmoff.net/2020/01/08/streaming-messages-from-rabbitmq-into-kafka-with-kafka-connect/)
and this [`one too`](https://medium.com/@danieljameskay/a-basic-overview-of-the-kafka-connect-rabbitmq-source-connector-abeba64ba453).




