# Cloud Storage + Spark


## create cloud storage bucket
```bash
gsutil mb gs://BUCKET_NAME

gsutil mb -p PROJECT_ID -c STORAGE_CLASS -l BUCKET_LOCATION -b on gs://BUCKET_NAME
```
```bash
-p: Specify the project with which your bucket will be associated. For example, my-project.
-c: Specify the default storage class of your bucket. For example, NEARLINE.
-l: Specify the location of your bucket. For example, US-EAST1.
-b: Enable uniform bucket-level access for your bucket.
```

## create GCP connection
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/...YOUR_GCP_PROJECT.../spark/python/conf/service_account.json
```

## create bucket & upload file in the bucket
```bash
python python/gcp/cloud_storage_setup.py
```

## run the main.py
```bash
python python/gcp/main.py
```

