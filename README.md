# parking-space


Create the customer data 
````bat 
curl -X POST -H "Content-Type: application/vnd.kafka.json.v1+json" --data @config/userdata.json "http://localhost:8082/topics/customer-location"
````

configure kafka rest connector
````bat
curl -i -X POST -H "Accept:application/json"  -H  "Content-Type:application/json" http://localhost:8083/connectors -d @config/parkingservice.json
````

Run the kafka stream application
````bat
gradle bootrun
````