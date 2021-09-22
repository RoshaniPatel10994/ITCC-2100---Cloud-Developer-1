import boto3
import json
###########################################
# https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

QUEUENAME = 'Vitamin_Queue'

KEY = "vb#1"
VITAMIN = {
    "Vendor": "Now Foods",
    "Title": "Thiamine B1 Energy Support",
    "Description": "Vitamin B1, Called Thiamine, C12H17N4OS+",
    "Units": "Pills",
    "Weight Per Unit": "100",
    "Weight Unit": "mg",
    "Price":"9.99",
    "Price Units":"USD",
    "Sold in": "USA"
}

###########################################


def SqsCreateQueue():

    # Create SQS resource (not client)
    sqs = boto3.resource('sqs')

    # Create the queue. This returns an SQS.Queue instance
    queue = sqs.create_queue(QueueName=QUEUENAME, Attributes={'ReceiveMessageWaitTimeSeconds': '5'})

    # You can now access identifiers and attributes
    print(queue.url)
    return queue.url

###########################################


def SqsDeleteQueue(url):

    # Create SQS client
    sqs = boto3.client('sqs')

    # Delete SQS queue
    sqs.delete_queue(QueueUrl=url)

###########################################


def SqsListQueues():

    # Create SQS resource (not client)
    sqs = boto3.resource('sqs')

    # Print out each queue name, which is part of its ARN
    for queue in sqs.queues.all():
        print(queue.url)

###########################################


def SqsLookUpQueue():

    # Create SQS client
    sqs = boto3.client('sqs')

    # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName=QUEUENAME)

    # You can now access identifiers and attributes
    print(queue.url)
    print(queue.attributes.get('DelaySeconds'))

    # It is also possible to list all of your existing queues:
    # Print out each queue name, which is part of its ARN
    for queue in sqs.queues.all():
        print(queue.url)

    return queue.url

###########################################


def SqsSendMsg():

    # Create SQS resource (not client)
    sqs = boto3.resource('sqs')

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUENAME)

    # Send 10 messages...
    count = 10
    while count:
        queue.send_message(MessageAttributes={'Purchase': {'StringValue': KEY + str(count), 'DataType': 'String'}}, MessageBody=json.dumps(VITAMIN))
        count -= 1

###########################################
# https://stackoverflow.com/questions/10180851/how-to-get-all-messages-in-amazon-sqs-queue-using-boto-library-in-python

def SqsProcessMsg():

    # Create SQS resource (not client)
    sqs = boto3.resource('sqs')

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=QUEUENAME)

    # Process messages by printing out body and optional author name
    message_bodies = []
    while True:
        messages_to_delete = []
        for message in queue.receive_messages(MaxNumberOfMessages=3, MessageAttributeNames=['Purchase']):

            if message.message_attributes is not None:

                # Get the item key...Not used. For demo purposes..
                key = message.message_attributes.get('Purchase').get('StringValue')
                print(key)

                # Get the message body
                body = json.loads(message.body)
                message_bodies.append(body)

                # add message to delete
                messages_to_delete.append({'Id': message.message_id, 'ReceiptHandle': message.receipt_handle})

        # if you don't receive any notifications the messages_to_delete list will be empty
        if len(messages_to_delete) == 0:
            break
        # delete messages to remove them from SQS queue
        else:
            queue.delete_messages(Entries=messages_to_delete)

    # Return the messages.
    return message_bodies

###########################################


def main():

    SqsListQueues()

    # Note: You must wait 60 seconds before creating a queue if it was just deleted
    url = SqsCreateQueue()
    SqsSendMsg()
    messages = SqsProcessMsg()

    if messages: 
        for message in messages:
            print(json.dumps(message))

    SqsDeleteQueue(url)
    SqsListQueues()

###########################################

main()
print("Program end.")
