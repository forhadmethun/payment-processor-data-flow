## Payment processor data source application

### Tech stack
- Java dropwizard
- Kafka
- Redis

### Running the project 
##### Run project
``` 
mvn clean package
java -jar target/PaymentProcessorDataFlow-1.0-SNAPSHOT.jar server config.yml
```
##### Create / View kafka topic 
``` 
bin/kafka-console-consumer.sh --topic card-info --from-beginning --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic encrypted-card-info --from-beginning --bootstrap-server localhost:9092
```

### Sample log 
``` 
INFO  [2021-11-24 23:15:46,470] com.tokenizer.rest.producer.EncryptedCardInfoProducer: Encrypted and message sent successfully for txId: 0bdc4ef37eac4c8a935dacaf094a1de4
```
