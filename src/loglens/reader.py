"""Stream CLEF events from files with their source locations."""

from collections.abc import Iterator
from pathlib import Path

from loglens.clef import parse_line
from loglens.models import LogEvent


def read_file(path: str | Path) -> Iterator[LogEvent]:
    """Lazily read UTF-8 CLEF, propagating file and parsing errors.

    Line numbers start at one. The file is closed when iteration finishes,
    parsing fails, or the caller explicitly closes the iterator.
    """
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            event = parse_line(line)
            event.source = str(path)
            event.line_number = line_number
            yield event
