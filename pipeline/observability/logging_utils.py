"""
Thin wrapper so every DAG task gets a consistently-named logger instead of
ad-hoc print() calls. Deliberately does NOT attach a custom handler:
Airflow already attaches its own handler to the root logger for every
task process — that's how a plain httpx INFO line ends up in each task's
attempt=1.log even though httpx has no idea Airflow exists. Adding a
second handler here would either duplicate every line, or, if avoided via
propagate=False, silently stop these log lines from reaching the task log
at all. Just get a logger and call it; Airflow does the rest.
"""
from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
