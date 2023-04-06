from kafka import KafkaConsumer

# Define server with port
bootstrap_servers = ['localhost:29092']

# Define topic name from where the message will recieve
topicName = 'testpy'

# Initialize consumer variable
consumer = KafkaConsumer (topicName , auto_offset_reset='earliest', bootstrap_servers = bootstrap_servers)

print(consumer)
print('Consumer Started')
# Read and print message from consumer
for msg in consumer:
    print("Topic Name=%s,Message=%s" % (msg.topic , msg.value))
