import boto3
s3_ob=boto3.resource('s3',aws_access_key_id='AKIA24YCIADRDGEGO4RG',Aws_secret_access_key='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')
for each_b in s3_ob.buckets.all():
    print (each_b.name)