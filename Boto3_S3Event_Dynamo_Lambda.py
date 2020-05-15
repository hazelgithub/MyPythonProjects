#This program reads data from S3 and loads into Dynamo DB

import json
import boto3

s3_Client = boto3.client('s3')

# dynamoClient = boto3.client('dynamodb')

dynamoClient = boto3.resource('dynamodb')


def lambda_handler(event, context):
    # TODO implement

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']

    # print("bucket_name is ",bucket_name)
    # print("file_name is " , file_name)
    #
    # print("event is ",str(event))

    json_object = s3_Client.get_object(Bucket=bucket_name, Key=file_name)

    # print(json_object['Body'].read())

    json_body = json_object['Body'].read()

    print("json_body is ", json_body)

    json_file = json.loads(json_body)   # This will convert JSON file into Dictionary

    print('json_file ', json_file)

    table = dynamoClient.Table('Users')  # Connects with Dynamo DB table

    table.put_item(Item=json_file)  # Inserting record into Table Users

    return 'Data Inserted in DB'
