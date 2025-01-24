import os
from enum import Enum
from typing import Dict, List, cast

import whylogs_container_client.api.llm.evaluate as Evaluate
from tqdm import tqdm
from whylogs_container_client import AuthenticatedClient
from whylogs_container_client.models.evaluation_result import EvaluationResult
from whylogs_container_client.models.llm_validate_request import LLMValidateRequest

from scripts.benchmarking.strings import string_10, string_100, string_1000


class Host(Enum):
    LOCAL = "http://localhost:8000"
    AWS = "http://adc1ec60dbb37441892dc3911b26b983-1511989768.us-west-2.elb.amazonaws.com"


client = AuthenticatedClient(
    base_url=Host.LOCAL.value,
    token="password",
    prefix="",
    auth_header_name="X-API-Key",
)


def benchmark(prompt: str, response: str) -> Dict[str, float]:
    request = LLMValidateRequest(
        prompt=prompt,
        response=response,
        dataset_id="model-131",
    )

    eval_response = Evaluate.sync_detailed(client=client, body=request, perf_info=True)

    if not isinstance(eval_response.parsed, EvaluationResult):
        raise Exception(f"Failed to validate data. Status code: {eval_response.status_code}. {eval_response.parsed}")

    result = eval_response.parsed

    assert result.perf_info
    return cast(
        Dict[str, float],
        result.perf_info.metrics_time_sec.to_dict()
        | result.perf_info.context_time_sec.to_dict()
        | {"total": result.perf_info.workflow_total_sec},
    )


def report_benchmark_restults(result: List[Dict[str, float]]) -> None:
    # First we need to calculate the average of each metric
    metrics = result[0].keys()
    averages = {metric: sum([res[metric] for res in result]) / len(result) for metric in metrics}

    # Then we compute the p90 of each metric
    p90s = {metric: sorted([res[metric] for res in result])[int(0.9 * len(result))] for metric in metrics}

    # Sort the metrics by the average time
    metrics = reversed(sorted(metrics, key=lambda metric: averages[metric]))

    # Then we print it out in a nice format
    print("Avg\tP90\tMetric")
    for metric in metrics:
        print(f"{averages[metric]:.3f}\t{p90s[metric]:.3f}\t{metric}")


def report_benchmark_results_using_pandas(name: str, result: List[Dict[str, float]]) -> None:
    import pandas as pd

    pd.set_option("display.float_format", lambda x: "%.0f" % x)  # pyright: ignore[reportUnknownLambdaType]

    df = pd.DataFrame(result)

    # convert all of the values, which are float seconds, to millisecond ints
    df = df * 1000

    df = df.round().astype(int)

    results = df.describe(percentiles=[0.9, 0.99]).T.sort_values(by="90%", ascending=False)
    print(results)

    # write the results to ./results/{name}.csv
    if not os.path.exists("results"):
        os.makedirs("results")
    results.to_csv(f"results/{name}.csv", sep="\t")


if __name__ == "__main__":
    iterations = 100

    benchmark_10 = [benchmark(string_10, string_10) for _ in tqdm(range(iterations), desc="10 token")]
    benchmark_100 = [benchmark(string_100, string_100) for _ in tqdm(range(iterations), desc="100 token")]
    benchmark_1000 = [benchmark(string_1000, string_1000) for _ in tqdm(range(iterations), desc="1,000 token")]
    benchmark_10_000 = [benchmark(string_1000 * 10, string_1000 * 10) for _ in tqdm(range(iterations), desc="10,000 token")]

    print()
    print("Benchmarks for 10 token prompt+response")
    report_benchmark_results_using_pandas("10_token", benchmark_10)

    print()
    print("Benchmarks for 100 token prompt+response")
    report_benchmark_results_using_pandas("100_token", benchmark_100)

    print()
    print("Benchmarks for 1000 token prompt+response")
    report_benchmark_results_using_pandas("1000_token", benchmark_1000)

    print()
    print("Benchmarks for 10,000 token prompt+response")
    report_benchmark_results_using_pandas("10000_token", benchmark_10_000)
