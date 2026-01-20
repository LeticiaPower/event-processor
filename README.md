# EVENT PROCESSOR

## KEY FUNCTIONALITIES

### PRODUCER

#### KafkaProducer
- `producer` // kafka producer object  
- `topic` // topic name  

#### main
- get the arguments if exist, parse into eventMessage struct and send to the topic  
- if don't exist any argument, create an eventMessage with hardcoded values and send to the topic  


### CONSUMER

#### KafkaConsumer
- `consumer` // kafka consumer object  
- `topic` // topic name  
- `msgCH` // channel for the messages  
- `isReady` // flag to check if the consumer is ready (e.g creating/validating the topic)  

#### initializeKafkaTopic
- connects with admin  
- check if the topic already exist  
- or else, create the topic  

#### ReadMessageLoop
- read the messages from the consumer  
- add kafka messages into the message channel  

#### CommitOffset
- manually commit the kafka message  

#### handleMessage
- parse the message received to the eventMessage struct  
- validate the eventMessage fields  
- save in db an event log, for the success or failed messages  
- create the event message in db to be fetched in the future  
- call the commit for the kafka message to finish the handling  

#### handleEventLog
- send eventLog struct to be saved in db  

## EVENT

### EventMessage
- `EventID`     // unique uuid of the event  
- `EventType`   // type representing the producer  
- `ClientKey`   // client key where the message is coming from  
- `Destination` // client destination  
- `Timestamp`   // timestamp for the message  
- `Message`     // interface message in any structure to send to destination  

### EventLog
- `EventID`      // unique uuid of the event  
- `EventMessage` // event message received  
- `Status`       // status (SAVED/FAILED)  
- `Details`      // details of the errors for example  
- `Timestamp`    // timestamp of the message arrival  


### ValidateEventMessage
- use validator to check the eventMessage struct tags, for example the required fields  
- return each field error in case exists  


### CreateEventMessage
- check if the event already exists by the event_id  
- in case it exists, ignore the creation for a idempotent behavior  
- create the event with the eventMessage struct in db for the future consumption  


### GetEventMessageByEventID
- check if the event item exist in the db  
- if don't exist return a Not Found error  


### LogEventStatus
- save status of the received messages in the db  
- save in case of success (status=SAVED) or in case of errors (status=FAILED)  
- use the eventLog struct  


## STACK

- Go
- LocalStack
  - AWS
    - DynamoDB
- Kafka (confluent-kafka)
- Docker
- Make

## HOW TO RUN

- make build

- make install

- make up

- make run-consumer
	- topic is created
	- tables are created
	- consumer starts listening

### other terminal

- make run-producer
	- will send one hardcoded eventMessage to the topic with a new id and ts each time is executed

- go run ./producer/cmd/main.go '{payload}'
	- send a custom payload to the topic in a eventMessage json format
		- ex: go run ./producer/cmd/main.go '{"client_key":"external_client","destination":"client_02","event_id":"eb2bf52f-94e8-485a-ba5a-bbe5a2e4090909","event_type":"another_app","message":{"name":"Leticia","last_name":"Silva"},"timestamp":"2026-01-19T12:16:56-03:00"}'
