# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#sns
import json
import boto3
from botocore.exceptions import ClientError

############################
sns = boto3.client("sns")
SNSTOPIC = "TestTopic"


def GetPhoneNumber():
    return "2692766460"

def GetEmail():
    return "roshanipatel10994@gmail.com"


############################
# Create topic
def SnsCreateTopic(topic):
    response = sns.create_topic(Name=topic)
    topic_arn = response["TopicArn"]
    print(topic_arn)
    return topic_arn


############################
# List topics
def SnsListTopics():
    response = sns.list_topics()
    topics = response["Topics"]
    print(str(topics)[1:-1])


############################
# Delete topics
def SnsDeleteTopics(topic_arn):
    sns.delete_topic(TopicArn=topic_arn)


############################
# Create SMS subscription
def SnsCreateSmsSubscription(topic_arn, phone_number):
    response = sns.subscribe(TopicArn=topic_arn, Protocol="SMS", Endpoint=phone_number)
    subscription_arn = response["SubscriptionArn"]
    print(subscription_arn)
    return subscription_arn


############################
# Create email subscription
def SnsCreateEmailSubscription(topic_arn, email):
    # Email must be confirmed before their subscriptions are active.
    # Pending confirmation" emails will be deleted automatically after 3 days.
    # When a subscription is not confirmed, its Amazon Resource Number (ARN) is set to 'PendingConfirmation'.
    response = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)

    while True:
        response = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
        for subscription in response["Subscriptions"]:

            if subscription["Endpoint"] != email:
                continue

            if subscription["SubscriptionArn"] == "PendingConfirmation":
                input("Please check you email and confirm the subscription...Press enter when complete.")
            else:
                return subscription["SubscriptionArn"]


############################
# List all subscriptions
def SnsListSubscriptions(topic_arn):
    response = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
    subscriptions = response["Subscriptions"]
    print(str(subscriptions)[1:-1])


############################
# Delete subscription
def SnsDeleteSubscription(subscription_arn):
    sns.unsubscribe(SubscriptionArn=subscription_arn)


############################
# Publish to topic to all subscribers
def SnsPublishToTopic(topic_arn, msg, subject):
    # "Subject" used in emails only but required nonetheless
    # MessageStructure='json' allows the publishing to SQS (when available)
    # The JSON object must have at least 'default'.
    jsonMsg = json.dumps({"default": msg})
    sns.publish(TopicArn=topic_arn, Message=jsonMsg, Subject=subject, MessageStructure="json")


############################
# Send a single SMS (no topic, no subscription needed)
# Not used in this demo
def SnsPublishToSMS(number, msg):
    # The endpoint that receives messages, such as a phone number (in E.164 format - "+48123456789") for SMS messages
    sns.publish(PhoneNumber=number, Message=msg)


############################
def main():
    RoshanisTopic="Hello"
    topic_arn = SnsCreateTopic(RoshanisTopic)
    phone=GetPhoneNumber()
    sub_sms=SnsCreateSmsSubscription(topic_arn, phone)
    email=GetEmail()
    sub_email=SnsCreateEmailSubscription(topic_arn, email)
    # subject="Json File"
    # msg={
    # "Vendor": "Now Foods",
    # "Title": "Thiamine B1 Energy Support",
    # "Description": "Vitamin B1, Called Thiamine, C12H17N4OS+",
    # "Units": "Pills",
    # "Weight Per Unit": "100",
    # "Weight Unit": "mg",
    # "Price":"9.99",
    # "Price Units":"USD",
    # "Sold in": "USA"
    # }
    SnsPublishToTopic(topic_arn, "Hello", "My name is Roshani")
    #SnsListTopics()

    #sub_sms = SnsCreateSmsSubscription(topic_arn, GetPhoneNumber())
    #sub_email = SnsCreateEmailSubscription(topic_arn, "john@clearbyte.com")
    #SnsListSubscriptions(topic_arn)

    #SnsPublishToTopic(topic_arn, "This is a test message", "Test subject")

    SnsDeleteSubscription(sub_sms)
    SnsDeleteSubscription(sub_email)
    SnsDeleteTopics(topic_arn)
    print("End of program")


main()