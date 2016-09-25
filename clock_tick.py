
import json
import boto3
import botocore
import time

def clock_tick(event, context):
    # connect to sns and dynamodb
    sns = boto3.client('sns')
    db = boto3.client('dynamodb')
    
    # get minute timer from database
    response = db.get_item(
        TableName='clock_tick',
        Key={
            'state' : {'S' : 'service_state'}
            }
        )

    # test for success or failure
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        minutes_since_beginning = int(response['Item']['minutes']['N'])
    else:
        sns.publish(
            TopicArn = "arn:aws:sns:us-west-2:356335180012:exception",
            Message = '{"Exception" : "Clock Tick - DynamoDB Read Failure"}',
            Subject = "Exception"
        )
        return

    # increment minutes entry indicating
    # another minute has passed
    response = db.update_item(
        TableName = 'clock_tick',
        Key = {
            'state' : {'S' : 'service_state'}
        },
        UpdateExpression = (
            "set minutes = minutes + :min"
        ),
        ExpressionAttributeValues = {
            ':min' : {'N' : '1'}
        }
    )
    # test for failure
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        sns.publish(
            TopicArn = "arn:aws:sns:us-west-2:356335180012:exception",
            Message = '{"Exception" : "Clock Tick - DynamoDB Update Failure"}',
            Subject = "Exception"
        )
        return

    # publish 1_Minute event
    event_message = '{"minutes" : ' + str(minutes_since_beginning) + '}'
    sns.publish(
        TopicArn = "arn:aws:sns:us-west-2:356335180012:1_Minute",
        Message = event_message,
        Subject = "1_Minute"
    )

    # determine if it is time for other time events
    # 5 minute event
    if minutes_since_beginning % 5 == 0:
        sns.publish(
            TopicArn = "arn:aws:sns:us-west-2:356335180012:5_Minute",
            Message = event_message,
            Subject = "5_Minute"
        )
    
    # 15 minute event
    if minutes_since_beginning % 15 == 0:
        return    
    
    # 30 minute event
    if minutes_since_beginning % 30 == 0:
        return
    
    # 60 minute event
    if minutes_since_beginning % 60 == 0:
        return
    
    # 1 day event
    if minutes_since_beginning % 1440 == 0:
        return

    return
