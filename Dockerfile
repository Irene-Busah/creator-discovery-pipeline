FROM apache/airflow:2.9.3-python3.11

# _PIP_ADDITIONAL_REQUIREMENTS (the previous approach) installs packages
# with no compatibility constraint against Airflow's own pinned
# dependencies — pip just resolves the newest version satisfying our
# spec, which for SQLAlchemy is 2.0.x. Airflow 2.9.3 was built and tested
# against SQLAlchemy 1.4.x, not 2.0 — installing 2.0 breaks Airflow's own
# ORM models (TaskInstance, DagModel, ...), not our code. That's the
# `MappedAnnotationError` / `ArgumentError` seen when relying on
# _PIP_ADDITIONAL_REQUIREMENTS.
#
# The fix: install against Airflow's own constraints file for this exact
# version, which pins every dependency (including SQLAlchemy) to what
# Airflow 2.9.3 actually shipped and tested with.
COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    -r /requirements-airflow.txt
