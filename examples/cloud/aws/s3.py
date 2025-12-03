import boto3

def get_public_bucket_keys(bucket_name):
    """
    Lists the keys (object names) in a public S3 bucket.
    """
    s3 = boto3.client('s3')
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                keys.append(obj['Key'])
    return keys

# Example usage:
public_bucket_name = 'your-public-bucket-name'  # Replace with your bucket name
object_keys = get_public_bucket_keys(public_bucket_name)
for key in object_keys:
    print(key)