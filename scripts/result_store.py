"""Exclusive artifacts plus append-only CSV/JSONL streams."""
import csv
import json
from pathlib import Path
from lab import save_json


class RunStore:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.files = []

    def json(self, name, data):
        save_json(self.directory / name, data)

    def jsonl(self, name):
        f = (self.directory / name).open('x', encoding='utf-8')
        self.files.append(f)
        def append(data):
            f.write(json.dumps(data, ensure_ascii=False, allow_nan=False) + '\n'); f.flush()
        return append

    def csv(self, name, fields):
        f = (self.directory / name).open('x', newline='', encoding='utf-8')
        self.files.append(f)
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='raise')
        writer.writeheader(); f.flush()
        def append(data):
            writer.writerow(data); f.flush()
        return append

    def close(self):
        for f in self.files:
            f.close()


def read_jsonl(path):
    """An unfinished final line is deferred; malformed completed lines are errors."""
    if not Path(path).exists():
        return []
    lines = Path(path).read_text(encoding='utf-8').splitlines(keepends=True)
    return [json.loads(line) for line in lines if line.endswith('\n')]
