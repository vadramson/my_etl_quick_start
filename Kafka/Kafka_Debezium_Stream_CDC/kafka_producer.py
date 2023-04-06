# Import KafkaProducer from Kafka library
from kafka import KafkaProducer

# Define server with port
bootstrap_servers = ['localhost:29092']

# Define topic name where the message will publish
topicName = 'testpy'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

# Publish text in defined topic
producer.send(topicName, b'It seems to be good')

# Print message
print("Message Sent")