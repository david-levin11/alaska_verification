# import pandas as pd

# # Load the Parquet file
# df = pd.read_parquet("model/ndfd/alaska_ndfd_wind_forecasts.parquet")

# # Show the first 10 rows
# print(df.head(10))

# # Or filter by station
# print(df[df['station_id'] == 'PAJN'].head())

# # Or filter by date
# print(df[df['valid_time'] > '2025-01-01'].head())



# from datetime import datetime
# import os

# speed_file = 'noaa-ndfd-pds/wmo/wspd/2020/12/29/YCRZ98_KWBN_202012291149'
# dir_file = 'noaa-ndfd-pds/wmo/wdir/2020/12/29/YBRZ98_KWBN_202012291149'

# def extract_timestamp(filename):
#     time_str = os.path.basename(filename).split("_")[-1]
#     return datetime.strptime(time_str, "%Y%m%d%H%M")

# speed_time = extract_timestamp(speed_file)
# dir_time = extract_timestamp(dir_file)

# print("Speed:", speed_time)
# print("Dir  :", dir_time)
# print("Equal?", speed_time == dir_time)


# import boto3
# import fsspec

# # Set up session using profile
# session = boto3.Session(profile_name="default")
# credentials = session.get_credentials().get_frozen_credentials()

# # Print for confirmation (safe to print access key, not secret key!)
# print("Using AWS credentials:", credentials.access_key)

# # Create fsspec S3 filesystem with credentials
# s3_fs = fsspec.filesystem(
#     "s3",
#     key=credentials.access_key,
#     secret=credentials.secret_key,
#     token=credentials.token,  # optional, for session tokens
# )

# # Upload text file to S3
# with s3_fs.open("s3://alaska-verification/ndfd/test.txt", "w") as f:
#     f.write("S3 test upload\n")

# print("✅ Upload complete!")

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# def test_aws_credentials():
#     try:
#         # Create an S3 client using default credentials/profile
#         s3 = boto3.client('s3')
        
#         # Try listing buckets
#         response = s3.list_buckets()
#         print("✅ AWS Credentials are working.")
#         print("Buckets:")
#         for bucket in response['Buckets']:
#             print(f" - {bucket['Name']}")

#     except NoCredentialsError:
#         print("❌ No AWS credentials found.")
#     except PartialCredentialsError:
#         print("❌ Incomplete AWS credentials.")
#     except ClientError as e:
#         print(f"❌ AWS ClientError: {e}")
#     except Exception as e:
#         print(f"❌ Unexpected error: {e}")

# if __name__ == "__main__":
#     test_aws_credentials()
import boto3

session = boto3.Session(profile_name="default")
s3 = session.client("s3")

try:
    response = s3.list_objects_v2(Bucket="alaska-verification", Prefix="ndfd/")
    if "Contents" in response:
        print("✅ S3 objects:")
        for obj in response["Contents"]:
            print(f" - {obj['Key']}")
    else:
        print("✅ Bucket exists but no objects found under 'ndfd/'")
except Exception as e:
    print(f"❌ Error accessing bucket: {e}")