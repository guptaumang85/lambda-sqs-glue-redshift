import json
import random
import boto3

sqs_queue_url = "https://sqs.ap-south-1.amazonaws.com/370876035393/demo-sqs"
sqs = boto3.client('sqs')
name_list = ["Umang", "Ajay", "Vijay", "Sanjay", "Manish", "Kunal"]
age_list = [27,28,34,43,24,37]
salary_list = [1000,2000,3000,4000,5000,6000]

def lambda_handler(event, context):
    random_index = random.randint(0,5)
    msg_for_sqs = {
        "emp_name": name_list[random_index],
        "emp_age": age_list[random_index],
        "emp_salary": salary_list[random_index]
    }
    
    message_body = json.dumps(msg_for_sqs)
    
    # send message to SQS
    response = sqs.send_message(
        QueueUrl = sqs_queue_url,
        MessageBody=message_body
        )
        
    print(f"Message sent to SQS queue: {response['MessageId']}")
    # TODO implement
    return {
        "statusCode": 200,
        "body": json.dumps("Message sent to SQS queue successfully.")
    }
