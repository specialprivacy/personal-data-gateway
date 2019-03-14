# Personal Data Gateway

This service allows for a single call check on the compliance of a processing action for a data subject. The service will put the request on the application-logs topic in kafka and return when the checked version has been put on the checked-application-logs. There is no exception handling. Any exception will result in a HTTP 500 response.

In case the processing is compliant with the data subjects consent then the response will be a HTTP 204, if not then the response will be a HTTP 401.

# Example

## Scenario
Suppose we want to check whether or not application 1 can perform some administrative analysis on the copied data of data subject user-1 on a server within the EU. 

## request
Then we could make a POST request to 
```
http://localhost:8888/processing
```

where http://localhost should be replaced with the address of the server on which the demonstrator has been deployed. 

For the above example the body should then be:
```
{
  "process": "application1",
  "purpose": "http://www.specialprivacy.eu/vocabs/purposes#Admin",
  "processing": "http://www.specialprivacy.eu/vocabs/processing#Copy",
  "recipient": "http://www.specialprivacy.eu/vocabs/recipientsSame",
  "storage": "http://www.specialprivacy.eu/vocabs/locations#EULike",
  "userID": "user-1",
  "data": ["http://www.specialprivacy.eu/vocabs/data#Derived"]
}
```
## response
If the processing is consentual then the response will be:
```
HTTP 204
```
Otherwise it will be:
```
HTTP 401
```
```
Processing for the request with policy: {'process': 'application1', 'purpose': 'http://www.specialprivacy.eu/vocabs/purposes#Admin', 'processing': 'http://www.specialprivacy.eu/vocabs/processing#Copy', 'recipient': 'http://www.specialprivacy.eu/vocabs/recipientsSame', 'storage': 'http://www.specialprivacy.eu/vocabs/locations#EULike', 'userID': 'user-1', 'data': ['http://www.specialprivacy.eu/vocabs/data#Derived']} has been denied
```

## application-logs
The compliance checker would put a message on the 'checked-application-logs' that looks similar to:
```
{
  'eventID': '246ab165-92be-4190-853d-0a0acab47d82', 
  'timestamp': 1552569626753, 
  'process': 'Application A', 
  'userID': '5d8025c2-7c41-4fa6-a993-b84e6b3fe7ac', 
  'hasConsent': False, 
  'purpose': 'http://www.specialprivacy.eu/vocabs/purposes#Admin', 
  'processing': 'http://www.specialprivacy.eu/vocabs/processing#Analyze', 
  'recipient': 'http://www.specialprivacy.eu/vocabs/recipients#Ours', 
  'storage': 'http://www.specialprivacy.eu/vocabs/locations#EU', 
  'data': ['http://www.specialprivacy.eu/vocabs/data#Anonymized']
}
```

## checked-application-logs
The compliance checker would put a message on the 'checked-application-logs' that looks similar to:
```
{
  'eventID': '246ab165-92be-4190-853d-0a0acab47d82', 
  'timestamp': 1552569626753, 
  'process': 'Application A', 
  'userID': '5d8025c2-7c41-4fa6-a993-b84e6b3fe7ac', 
  'hasConsent': False, 
  'purpose': 'http://www.specialprivacy.eu/vocabs/purposes#Admin', 
  'processing': 'http://www.specialprivacy.eu/vocabs/processing#Analyze', 
  'recipient': 'http://www.specialprivacy.eu/vocabs/recipients#Ours', 
  'storage': 'http://www.specialprivacy.eu/vocabs/locations#EU', 
  'data': ['http://www.specialprivacy.eu/vocabs/data#Anonymized']
}
```
