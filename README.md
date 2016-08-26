# sumologic-aws-lambda-unzip
A Java Lambda function that can uncompress a zip file, read the files inside and send them to Sumo Logic.

##Instructions
1. Downlaod the latest [release](https://github.com/frankreno/sumologic-aws-lambda-unzip/releases/tag/1.0.0).
2. You need to create a configuration JSON file that contains the endpoint.  This file needs to be placed in the same S3Bucket that contains the zip files you want to send to sumo.  An example config is shown below.  Remember to adjust the hostname as needed.
```
{"endpoint":"https://endpoint1.collection.sumologic.com/receiver/v1/http/<YOUR UNIQUE ID HERE>"}
```
3. Create your lambda function.  When configuring the the handler, you need to use **com.sumologic.lambda.SumoUnzipS3EventProcessor::handleRequest**