import tempfile
from typing import Optional

from langkit.core.metric import Metric
from langkit.metrics.themes.themes import response_refusal_similarity_metric
from langkit.transformer import EmbeddingChoiceArg
from whylogs_container.whylabs.container.aws.s3_sync import get_s3_client


def custom_refusal_metric(
    embedding: EmbeddingChoiceArg = "default",
    additional_data_path: Optional[str] = None,
) -> Metric:
    if additional_data_path and "s3://" in additional_data_path:
        # make sure it ends with .npy
        if not additional_data_path.endswith(".npy"):
            raise ValueError("Additional data path must end with .npy")

        s3_client = get_s3_client()

        # Download the file from S3
        bucket_name, key = additional_data_path.replace("s3://", "").split("/", 1)
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        # It's going to be a binary file
        data = response["Body"].read()
        # write to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".npy") as f:
            f.write(data)
            file_name = f.name

        return response_refusal_similarity_metric(additional_data_path=file_name, embedding=embedding)
    else:
        # Assume its a local file on the container
        return response_refusal_similarity_metric(additional_data_path=additional_data_path, embedding=embedding)
