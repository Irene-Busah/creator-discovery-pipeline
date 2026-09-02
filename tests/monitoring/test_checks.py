from pipeline.monitoring.checks import check_ingestion_freshness, check_quota_exhaustion


class FakeRepoWithFreshness:
    def __init__(self, count):
        self._count = count

    def count_raw_payloads_since(self, cutoff):
        return self._count


def test_ingestion_freshness_passes_when_data_recently_landed():
    result = check_ingestion_freshness(FakeRepoWithFreshness(12), lookback_hours=2)
    assert result.passed is True


def test_ingestion_freshness_fails_when_table_stopped_growing():
    """Direct answer to the brief's 'a flag when a table stops growing' —
    zero new raw_payloads rows in the lookback window means dag_ingestion
    has been stuck or the queue has sat empty for the whole window.
    """
    result = check_ingestion_freshness(FakeRepoWithFreshness(0), lookback_hours=2)
    assert result.passed is False


class FakeRepoWithQuotaErrors:
    def __init__(self, count):
        self._count = count

    def count_quota_exhaustion_errors(self):
        return self._count


def test_quota_exhaustion_passes_under_threshold():
    result = check_quota_exhaustion(FakeRepoWithQuotaErrors(2), max_quota_errors=5)
    assert result.passed is True


def test_quota_exhaustion_fails_over_threshold_and_names_billing():
    """Regression test tied to a real incident: 54 ingestion failures
    during development all traced back to Apify's free tier running out
    (402 Payment Required). This check exists specifically to surface that
    as a distinct, actionable signal instead of a generic failure count.
    """
    result = check_quota_exhaustion(FakeRepoWithQuotaErrors(54), max_quota_errors=5)
    assert result.passed is False
    assert "billing" in result.message.lower()
