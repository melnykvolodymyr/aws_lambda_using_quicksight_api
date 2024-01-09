# 1. AWS lambda function using quicksight api
##  - CRUD datasource of various database in quicksight
##  - CRUD dataset of quicksight

# 2. How do I use layers to integrate the current AWS SDK for JavaScript into my Node.js Lambda function

 - `mkdir -p aws-sdk-layer/nodejs`
 - `cd aws-sdk-layer/nodejs/`
 - `npm install aws-sdk`
 - `zip -r ../package.zip ../`
 - `aws lambda publish-layer-version --layer-name node_sdk --description "My Layer" --license-info "MIT" --compatible-runtimes nodejs18.x --zip-file fileb://../package.zip --region us-west-2`
 - `aws lambda update-function-configuration --function-name CreateQuickSightResources --layers arn:aws:lambda:us-west-2:190600982480:layer:node_sdk:1 --region us-west-2`
