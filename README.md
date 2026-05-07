gcloud compute ssh dev-gh-archive-cluster-m --project=gcp_id --zone=asia-south1-c --impersonate-service-account=YOUR_SA_NAME@gcp_id.iam.gserviceaccount.com -- -D 1080 -N



gcloud auth application-default login --impersonate-service-account=nikhil@gcp_id.iam.gserviceaccount.com -
bruin run .




gcloud dataproc jobs submit pyspark cleanupscript.py --cluster=dev-gh-archive-cluster --region=asia-south1 --project=gcp_id --impersonate-service-account=nikhil@gcp_id.iam.gserviceaccount.com -- --year=2024 --month=nr7