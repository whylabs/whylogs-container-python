from typing import List, Optional

import boto3


def create_presigned_upload_url(
    bucket_name: str,
    org_id: str,
    dataset_id: str,
    dataset_timestamp: int,
    prefix: Optional[str] = None,
    tags: Optional[List[str]] = None,
    expiration: int = 3600,
) -> str:
    """Generate a presigned URL to upload a file to S3 with a structured key path

    Args:
        bucket_name (str): S3 bucket name
        dataset_id (str): Identifier for the model
        upload_timestamp (Union[str, datetime]): Timestamp for the upload
        prefix (Optional[str]): Optional S3 prefix/folder path
        tags (Optional[List[str]]): List of tags to include in path
        expiration (int): Time in seconds until URL expires

    Returns:
        Optional[str]: Presigned URL. Returns None if there's an error.
    """
    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]

    # Process tags
    tag_segment = "_".join(tags) if tags else "default"

    # Construct the filename
    filename = f"profile_{org_id}_{dataset_id}_{dataset_timestamp}.bin"

    # Construct the full key with all components
    key_components = [prefix.rstrip("/") if prefix else None, org_id, dataset_id, tag_segment, filename]

    # Filter out None values and join with '/'
    full_key = "/".join(filter(None, key_components))

    return s3_client.generate_presigned_url(
        "put_object",
        Params={
            "Bucket": bucket_name,
            "Key": full_key,
            "ContentType": "application/octet-stream",
        },
        ExpiresIn=expiration,
    )
