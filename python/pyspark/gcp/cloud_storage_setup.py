from google.cloud import storage
storage_client = storage.Client()

def implicit():
    buckets = list(storage_client.list_buckets())
    print(buckets)

def create_new_bucket(bucket_name):
    bucket = storage_client.create_bucket(bucket_name)
    print("Bucket {} created.".format(bucket.name))


def list_blobs(bucket_name):
    # blob_name = "your-object-name"
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        print(blob.name)

def file_upload(bucket_name, bucket_file_name, local_file_name):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_file_name)

    with open(local_file_name, 'rb') as f:
        blob.upload_from_file(f)

    print("Upload complete")


if __name__ == "__main__":
    implicit()
    bucket_name = 'spark-cloud-storage'
    create_new_bucket(bucket_name)
    list_blobs(bucket_name)
    file_upload(bucket_name, 'gcp_book.csv', 'data/small.csv')