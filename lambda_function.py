import boto3
import json
import insert_data


def lambda_handler(event, context):
    data = get_data(event)
    insert_data.insert_to_sql(data)


def get_data(event):
    s3_client = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    json_file_name = event['Records'][0]['s3']['object']['key']
    json_object = s3_client.get_object(Bucket=bucket, Key=json_file_name)
    jsonFileReader = json_object['Body'].read()
    jsonDict = json.loads(jsonFileReader)
    return jsonDict
