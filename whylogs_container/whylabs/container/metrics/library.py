import sys
from functools import partial
from typing import Dict, Optional

from langkit.core.metric import MetricCreator
from langkit.core.workflow import Workflow
from langkit.transformer import EmbeddingChoiceArg


class lib:
    @staticmethod
    def all_metrics() -> MetricCreator:
        return [lib.similarity.custom_refusal()]

    class similarity:
        @staticmethod
        def custom_refusal(embedding: EmbeddingChoiceArg = "default", additional_data_path: Optional[str] = None) -> MetricCreator:
            from whylogs_container.whylabs.container.metrics.custom_refusals import custom_refusal_metric

            return partial(custom_refusal_metric, additional_data_path=additional_data_path, embedding=embedding)


if __name__ == "__main__":
    import logging

    from langkit.metrics.library import lib as langkit_lib
    from whylogs_container.whylabs.container.metrics.cached_metrics import get_metric_names

    logging.basicConfig(level=logging.DEBUG)

    # Everything we use from langkit is in here
    metric_names: Dict[str, MetricCreator] = {
        **get_metric_names(),
        # Supply default values for the metrics that have required arguments. This might need to be updated
        # if we add new metrics that have required arguments.
        "CUSTOM_COLUMN.similarity.CUSTOM_COLUMN_2": lambda: langkit_lib.CUSTOM_COLUMN.similarity.CUSTOM_COLUMN_2(
            CUSTOM_COLUMN="prompt", CUSTOM_COLUMN_2="response"
        ),
        "prompt.similarity.CUSTOM_COLUMN": lambda: langkit_lib.prompt.similarity.CUSTOM_COLUMN(CUSTOM_COLUMN="response"),
        "response.similarity.CUSTOM_COLUMN": lambda: langkit_lib.response.similarity.CUSTOM_COLUMN(CUSTOM_COLUMN="prompt"),
    }
    enabled_metrics = list(metric_names.values())

    # Metrics that are not default but we want to cache on the image anyway so they're
    # available from the policy. These are sometimes non-api-key alternatives to defaults.
    additional_metrics = [
        langkit_lib.prompt.topics.medical(),
        langkit_lib.prompt.topics.financial(),
        langkit_lib.prompt.topics.code(),
        langkit_lib.response.topics.medical(),
        langkit_lib.response.topics.financial(),
        langkit_lib.response.topics.code(),
    ]

    wf = Workflow(metrics=[lib.all_metrics(), *enabled_metrics, additional_metrics], cache_assets="--skip-downloads" not in sys.argv)
    # Run it to ensure nothing else ends up getting lazily cached
    wf.run({"prompt": "How are you today?", "response": "I'm doing great!"})
