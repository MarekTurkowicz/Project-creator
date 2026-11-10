"""Request counts and error fractions grouped by HTTP endpoint."""

import pytest

duckdb = pytest.importorskip("duckdb")

from loglens.database import count_requests_by_endpoint, create_schema, insert_events  # noqa: E402
from loglens.models import LogEvent  # noqa: E402


def test_endpoint_counts_include_all_requests_and_error_fatal_fraction():
    samples = [
        ("/orders", "Information"),
        ("/orders", "Error"),
        ("/orders", "Fatal"),
        ("/orders", "Warning"),
        ("/health", "Information"),
        ("/failed", "Error"),
        (" /health ", "Debug"),
        ('/path?name="quoted"', "Information"),
    ]
    with duckdb.connect() as connection:
        create_schema(connection)
        insert_events(connection, [
            LogEvent(level=level, properties={"RequestPath": path})
            for path, level in samples
        ])

        assert count_requests_by_endpoint(connection) == [
            (" /health ", 1, 0.0),
            ("/failed", 1, 1.0),
            ("/health", 1, 0.0),
            ("/orders", 4, 0.5),
            ('/path?name="quoted"', 1, 0.0),
        ]
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone() == (8,)


def test_endpoint_counts_return_no_rows_for_empty_or_non_http_data():
    with duckdb.connect() as connection:
        create_schema(connection)
        assert count_requests_by_endpoint(connection) == []

        properties = [
            {}, {"Elapsed": 10}, {"Http": {"RequestPath": "/nested"}},
            *({"RequestPath": value} for value in [
                None, "", " \t\n", 123, False, [], {},
            ]),
        ]
        insert_events(connection, [
            LogEvent(level="Error", properties=value) for value in properties
        ])

        assert count_requests_by_endpoint(connection) == []
