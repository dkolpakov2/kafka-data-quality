import os
from datadog import initialize, api

options = {
    "api_key": os.getenv("DD_API_KEY"),
    "app_key": os.getenv("DD_APP_KEY"),
}

initialize(**options)

# Send test coverage
api.Metric.send(
    metric="kafka.dq.test.coverage",
    points=float(os.getenv("TEST_COVERAGE", 0)),
    tags=["service:kafka-data-quality"]
)

# Send number of failed tests
failures = int(os.getenv("TEST_FAILURES", "0"))
api.Metric.send(
    metric="kafka.dq.test.failures",
    points=failures,
    tags=["service:kafka-data-quality"]
)

print("Datadog test metrics sent.")
