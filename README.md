# aws-rekognize-plugin

PoC (Proof of Concept) Pentaho PDI Plugin for using AWS Rekognition Facial Analysis services

Developed during HV Engineering Winter Hackathon, Dec/18

BUILD:  

mvn clean install

INSTALLING:

cd target 
cp aws-rekognize-plugin-8.3.0.0-SNAPSHOT.zip $SPOON_DIR/data-integration/plugins
cd $SPOON_DIR/data-integration/plugins
unzip aws-rekognize-plugin-8.3.0.0-SNAPSHOT.zip
rm aws-rekognize-plugin-8.3.0.0-SNAPSHOT.zip

Before running spoon, you need to set your AWS credentials on ~/.aws/credentials .
The "AWS Rekognition Face Analysis" PDI Step allows to specify an AWS bucket and process all images on this bucket using AWS Rekognition Face Analysis API.
No inputs are needed for the step, it outputs results in the following schema: (ImageFile, FaceID, Property, Value, Confidence).


TODO/Functionality
- read s3 URI w/ regexp, limit read to subdir
- read Filename from input
- non s3 URI / local file /vfs support
- add timestamp column
- move aws creds read to environment vars (use AWS DefaultCredentialProvider),support STS
- add other/missing fields
- billable aws API calls in metrics ext I/O
- remove hardcoded params: region, timeout, confid.
- add license file (MIT), user docs
- change to experimental step
- publish on marketplace
- ability to call labels API
- control out fields names
- control what goes on out fields, transpose some rows to columns

TODO/Tech
- unit tests
- check included lib/jars not in main lib for
- check/remove unused/sample code
- refactor/SoC, diff class for aws, use lambda
- check missing class on unit test exec
- move dialog to xul
- use reflection for calls?
- test with parallel exec
- test on ael


LICENSE

Distributed under MIT License - https://opensource.org/licenses/MIT
