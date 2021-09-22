import logging
import requests
import io
import boto3
from botocore.exceptions import ClientError
###############################################################################
# Create an S3 bucket in a specified region.
###############################################################################
def create_bucket(bucket_name, region):

    try:
        s3_client = boto3.client('s3', region_name=region)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    
    print('Bucket created successfully')
    return True

###############################################################################
# Retrieve the list of existing buckets
###############################################################################
def list_all_buckets():
    
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_buckets()
    except ClientError as e:
        logging.error(e)
        return False

    # Output the bucket names
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')
    
    return True
###############################################################################
# Upload a file to a bucket
###############################################################################
def upload_file(bucket_name, file_name, object_name):

    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False

    print('File uploaded successfully')
    return True
###############################################################################
# Upload binary data to a bucket
# Will automatically handle multipart uploads behind the scenes if necessary.
###############################################################################
def upload_file_obj(bucket_name, file_name, binary_data):
    
    try:
        s3_client = boto3.client('s3')
        fo = io.BytesIO(binary_data)
        s3_client.upload_fileobj(fo, bucket_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False

    return True
###############################################################################
# Put an object - lower level API call
# Does not do multi-part uploads
###############################################################################
def put_object(bucket_name, object_name, file_name):

    try:
        s3_client = boto3.client('s3')
        with open(file_name, 'rb') as f:
            content = f.read()

        # Send the to the bucket
        s3_client.put_object(Body=content, Bucket=bucket_name, Key=object_name)

    except ClientError as e:
        logging.error(e)
        return False

    return True        
###############################################################################
# Download a file to the loacal hard drive - upper level API call 
# Will automatically handle multipart downloads behind the scenes if necessary.
###############################################################################
def download_file(bucket_name, object_name, file_name):

    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(bucket_name, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False

    return True        
###############################################################################
# Download a file and save to a file-like object
# Will automatically handle multipart downloads behind the scenes if necessary.
###############################################################################
def download_file_object(bucket_name, object_name, fob):

    try:
        s3_client = boto3.client('s3')
        s3_client.download_fileobj(bucket_name, object_name, fob)
    except ClientError as e:
        logging.error(e)
        return False

    return True        
###############################################################################
# Get an object - lower level API call.
# Does not do multi-part downloads
###############################################################################
def get_object(bucket_name, object_name, file_name):

    try:
        s3_client = boto3.client('s3')
        s3_response_object = s3_client.get_object(Bucket=bucket_name, Key=object_name)

        # Get the "StreamingBody"...
        object_content = s3_response_object['Body'].read()
        
        #Push the "StreamingBody" stream to a file.
        with open(file_name, 'wb') as f:
            f.write(object_content)

    except ClientError as e:
        logging.error(e)
        return False

    return True        
###############################################################################
# List all objects in a bucket.
# Uses paginators. See:https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
###############################################################################
def list_all_objects(bucket_name):

    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name):

            for content in page["Contents"]:
                key = content['Key']    
                print(key)

    except ClientError as e:
        logging.error(e)
        return False

    return True        
###############################################################################
# Delete an object in a bucket.
###############################################################################
def delete_object(bucket_name, object_name):

    try:
        s3_client = boto3.client('s3')
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False

    print('Object deletion successful')
    return True        
###############################################################################
# Delete all objects in a bucket.
# Uses paginators. See:https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
###############################################################################
def delete_all_objects(bucket_name):

    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name):

            for content in page["Contents"]:
                key = content['Key']    
                s3_client.delete_object(Bucket=bucket_name, Key=key)

    except ClientError as e:
        logging.error(e)
        return False

    print('All objects deleted inside of your selected bucket')
    return True        
###############################################################################
# Delete a bucket.
###############################################################################
def delete_bucket(bucket_name):

    try:
        s3_client = boto3.client('s3')
        s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False

    print('Bucket deleted successfully')
    return True        
###############################################################################
# Generate a presigned URL to share an S3 object
###############################################################################    
def create_presigned_url(bucket_name, object_name, expiration=3600):
    
    try:
        s3_client = boto3.client('s3')
        Params={'Bucket': bucket_name,'Key': object_name}
        url = s3_client.generate_presigned_url('get_object', Params, ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None, None

    # Optional - Get the file via the presigned URL.
    if url is not None:
      return url, requests.get(url).content

    return None, None
###############################################################################
## Query the internals of an S3 object.
###############################################################################
def s3_select(bucket_name, file_name, expression):
    try:
        expression = 'select * from s3object'
        s3_client = boto3.client("s3")
        bucket = bucket_name
        key = file_name
        expression_type = "SQL"
        expression = expression
        input_serialization = {"CSV": {"FileHeaderInfo": "USE"}}
        output_serialization = {"JSON": {}}
        response = s3_client.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType=expression_type,
            Expression=expression,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization
        )
    except ClientError as e:
        logging.error(e)
        return False

    for event in response["Payload"]:
        print(event)
    
    return True
###############################################################################
# Exercise the S3 functions.
###############################################################################
def main():

    # sql_espression = """SELECT * FROM S3Object"""
    sql_espression = """SELECT s.Title FROM S3Object s"""
    #Define quite parameter which will terminate the program upon user entering string "Q"
    quit=""
    #While loop that keeps running as long as user inputs something other than "Q"
    while quit!="Q":
    #Print out welcome screen and list options for user to select
        print("Welcome to Amazon S3!")
        print("What would you like to do?")
        print("A. Create a bucket")
        print("B. List all buckets")
        print("C. Upload a file")
        print("D. List all files in a particular bucket")
        print("E. Delete a file")
        print("F. Delete a bucket")
        print("G. List the contents of a file inside of a bucket")
        val=input("Please select what you would like to do A-G: ")
    #If else logic that processes user inputs
        if val=="A" or val=="a":
            #Prompts user for bucket name and region and calls function to create bucket
            bucket_name=input("Please type in a unique bucket name: ")
            region=input("Please type in the appropriate AWS region: ")
            create_bucket(bucket_name,region)
        elif val=="B" or val=="b":
            #Lists all buckets
            list_all_buckets()
        elif val=="C" or val=="c":
            #Prompts user for bucket name and file name they would like to upload into that bucket. Calls function to upload the file
            bucket_name=input("Please type in the bucket name you would like to upload the file to: ")
            upload_file_name=input("Please type in the name of the file you would like to upload: ")
            upload_file(bucket_name, upload_file_name_csv, upload_file_name_csv)
        elif val=="D" or val=="d":
            #Prompts user for bucket name of files they would like to see. Calls functions to list all objects inside bucket
            bucket_name=input("Please type in the name of the bucket which you would like to see all files: ")
            list_all_objects(bucket_name)
        elif val=="E" or val=="e":
            #Prompts user for the bucket name and the object inside of that bucket they would like to delete
            bucket_name=input("Please type in the bucket from which you would like to delete an object: ")
            file_name=input("Please type in the file name you would like to delete: ")
            delete_object(bucket_name,file_name)
        elif val=="F" or val=="f":
            #Prompots user to type in bucket name they would like to delete and calls function to delete the bucket
            bucket_name=input("Please type in the bucket name you would like to delete: ")
            delete_bucket(bucket_name)
        elif val=="G" or val=="g":
            #Prompts user to type in the name of the bucket and the file name inside of the bucket. Calls function to view the specific file inside of the bucket
            bucket_name=input("Please type in the bucket name containing the file you would like to see: ")
            file_name=input("Please type in the name of the file you would like to open: ")
            #expression=input()
            expression=""
            s3_select(bucket_name, file_name, expression)
        else:
            #Final call, asks user to rerun file to try again
            quit=input("Wrong answer, rerun file to try again")
        quit=input("Press Q to quit or any other key to continue")




###############################################################################
# Exercise the S3 functions.
###############################################################################
main()

