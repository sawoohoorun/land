import boto3

s3_client = boto3.Session().client('s3')
response = s3_client.get_object(Bucket='sentinel-s2-l1c',
                                Key='tiles/7/W/FR/2018/3/31/0/B01.jp2', 
                                RequestPayer='requester')
response_content = response['Body'].read()

with open('./B01.jp2', 'wb') as file:
    file.write(response_content)



