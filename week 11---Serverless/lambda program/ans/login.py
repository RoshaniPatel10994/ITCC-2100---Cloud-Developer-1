from datetime import datetime
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT = "arn:aws:dynamodb:us-east-2:748939641058:table/Vitamins1"

TABLE_NAME = "Vitamins1"

###############################################################################
# Put a DynamoDb Item.
###############################################################################
def log_in_out(pk, sk, login):

    time_stamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

    try:
        db_client = boto3.client("dynamodb")
        db_client.put_item(
            Item={
                "pk": {
                    "S": pk,
                },
                "sk": {
                    "S": sk,
                },
                "login": {
                    "BOOL": login,
                },
                "timestamp": {
                    "S": time_stamp,
                },
            },
            ReturnConsumedCapacity="TOTAL",
            TableName=TABLE_NAME,
        )
        return True
    except Exception as e:
        logging.error(e)
        return False


###############################################################################
# Entrance to the lambda function.
###############################################################################
def lambda_handler(event, context):

    # This is new info....

    logger.info(event)
    logger.info(context)

    pk = "vit"
    sk = "Thiamine"
    if log_in_out(pk, sk, True):
        return {"statusCode": 200, "body": "Successfully logged in!"}

    return {"statusCode": 400, "body": "Error logging in!"}


#### For debugging only
if __name__ == "__main__":
    lambda_handler(None, None)
