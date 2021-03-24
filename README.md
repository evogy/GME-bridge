# AWS Lambda driver for GME
The Lambda application aim is to download from [GME](https://www.mercatoelettrico.org/it/) FTP the energy price and insert it on a RabbitMQ queue

This Serverless project creates a Lambda functions with two entry point:

- **tomorrow** triggered every day at 4:00PM (UTC time) automatically download the energy price of tomorrow
- **by_date** This function is a Node application that make available the following API endpoints:
    - **[GET] /by_date?date={yyyy-MM-dd}** download the energy price of a specific day

## Configuration

1. Set-up one environment variables:
    - AWS_PROFILE: the AWS CLI profile
    - GME_USER: the ftp GME user
2. Set-up the following  **secured** variables in the AWS Parameter store:
    - **AMQP_URL** The rabbitmq uri
    - **GME_PASSWORD** The ftp GME password
3. Use `npm install`
4. Run `sls deploy` 

To test on local pc you can use:

1. `serverless invoke local --function {function_name}`

if you want to test the http endpoint you must run for example `serverless invoke local --function by_date --data '{ "queryStringParameters": {"date": "2021-03-19"}}'`

2. The serverless offline plugin: `${SLS_PATH}/sls offline start`

