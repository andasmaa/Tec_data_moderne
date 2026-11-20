import os, boto3

s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="minioadmin",
                  aws_secret_access_key="minioadmin")
os.makedirs("notebooks/data/orders", exist_ok=True)
for obj in s3.list_objects_v2(Bucket="bronze", Prefix="parquet/orders/")["Contents"]:
    dest = "notebooks/data/orders/" + os.path.basename(obj["Key"])
    s3.download_file("bronze", obj["Key"], dest)
    print("téléchargé :", dest)
