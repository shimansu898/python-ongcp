import functions_framework
from google.cloud import storage

# Define the destination bucket for invalid files
INVALID_FILES_BUCKET = "dest-decrypted-files"  # Replace with your bucket name
"""Triggered by a change to a GCS bucket.
Args:
event(dict): Event payload.
context (google. cloud. functions. Context) : Metadatafor the event.


"""
@functions_framework.cloud_event
def check_file_size(cloud_event):

        data = cloud_event.data
        print("**************Function Started*************")
        source_bucket_name = data['bucket']
        file_name = data['name']

        # Initialize GCS client
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(source_bucket_name)
        blob = source_bucket.blob(file_name)

        # Reload blob metadata to ensure size is fetched
        blob.reload()

        file_size = blob.size
        
        one_kb_in_bytes = 1024
        
        #Ensure file size is not none
        if file_size is file_size < one_kb_in_bytes :
            print(f"Unable to detemine the size for {file_name}. Skipping.")
            return

        # Define the maximum file size (2MB)
        max_size = 2 * 1024 * 1024  # 2MB in bytes

        if file_size > max_size:
            # Move the file to the destination bucket
            destination_bucket = storage_client.bucket(INVALID_FILES_BUCKET)
            new_blob_name = file_name 
            new_blob = source_bucket.copy_blob(blob, destination_bucket, new_blob_name)

            # Delete the original file
            blob.delete()
        
            print(f"File {file_name} moved to {INVALID_FILES_BUCKET}/{new_blob_name} because it exceeded 2MB.")
        else:
            print(f"File {file_name} is within the size limit (<= 2MB). No action taken.")
        
        print("**************Function Ended*************")
