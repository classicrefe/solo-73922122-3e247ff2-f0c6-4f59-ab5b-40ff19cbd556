"""缺测核对核心与 Web 层测试。运行：python -m pytest tests/test_gaps.py -q"""

from __future__ import annotations

import csv
import io
import sqlite3
import threading
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app import gaps, lake
from app.lake import Settings
from app.main import create_app

WINDOW_START = "2026-01-01T00:00:00Z"
WINDOW_END = "2026-01-01T01:00:00Z"


# ---------------------------------------------------------------------------
# 夹具与辅助
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    s = Settings(data_dir=tmp_path / "lake", instance_name="gap-test")
    lake.initialize(s)
    return s


@pytest.fixture()
def client(settings: Settings):
    with TestClient(create_app(settings)) as c:
        yield c


def csv_bytes(rows: list[list[str]], header: list[str] | None = None) -> bytes:
    head = header or [lake.STATION_COLUMN, lake.TIME_COLUMN, "temp_c"]
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(head)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def prepare_schema(client: TestClient) -> int:
    resp = client.post(
        "/api/fields", json={"schema_version": 0, "name": "temp_c"}
    )
    assert resp.status_code == 201, resp.text
    return client.get("/api/schema").json()["schema_version"]


def upload_core(settings: Settings, version: int, rows: list[list[str]]) -> lake.Batch:
    return lake.upload_csv(settings, version, csv_bytes(rows))


def create_report(
    client: TestClient,
    station: str = "54511",
    start: str = WINDOW_START,
    end: str = WINDOW_END,
    interval: int = 10,
) -> httpx.Response:
    return client.post(
        "/api/gap-reports",
        json={
            "station_id": station,
            "start_at": start,
            "end_at": end,
            "interval_minutes": interval,
        },
    )


# ---------------------------------------------------------------------------
# 网格、去重、非网格与连续区间
# ---------------------------------------------------------------------------


def test_grid_missing_intervals_and_counts(client: TestClient):
    version = prepare_schema(client)
    upload_core(
        client.app.state.settings,
        version,
        [
            ["54511", "2026-01-01T00:00:00Z", "1"],
            ["54511", "2026-01-01T00:10:00Z", "2"],
            # 非网格时刻：不抵缺测
            ["54511", "2026-01-01T00:07:00Z", "9"],
        ],
    )
    resp = create_report(client)
    assert resp.status_code == 201, resp.text
    report = resp.json()
    assert report["status"] == "current"
    assert report["point_count"] == 7  # 00:00 .. 01:00，10 分钟
    assert report["observed_count"] == 2
    assert report["missing_count"] == 5
    assert report["missing_intervals"] == [
        {
            "start_at": "2026-01-01T00:20:00Z",
            "end_at": "2026-01-01T01:00:00Z",
            "point_count": 5,
        }
    ]
    assert len(report["offgrid_observations"]) == 1
    assert report["offgrid_observations"][0]["observed_at"] == "2026-01-01T00:07:00Z"
    # 逐网格明细
    observed_times = [g["observed_at"] for g in report["grid"] if g["observed"]]
    assert observed_times == ["2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"]


def test_multiple_missing_runs_are_separate_intervals(client: TestClient):
    version = prepare_schema(client)
    upload_core(
        client.app.state.settings,
        version,
        [
            ["54511", "2026-01-01T00:10:00Z", "1"],
            ["54511", "2026-01-01T00:30:00Z", "2"],
        ],
    )
    report = create_report(client).json()
    # 缺测：00:00 / 00:20 / 00:40..01:00 -> 三段
    intervals = report["missing_intervals"]
    assert [(i["start_at"], i["end_at"], i["point_count"]) for i in intervals] == [
        ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", 1),
        ("2026-01-01T00:20:00Z", "2026-01-01T00:20:00Z", 1),
        ("2026-01-01T00:40:00Z", "2026-01-01T01:00:00Z", 3),
    ]
    assert report["missing_count"] == 5


def test_duplicate_grid_time_across_batches_counts_once(client: TestClient):
    settings = client.app.state.settings
    version = prepare_schema(client)
    first = upload_core(
        settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]]
    )
    second = upload_core(
        settings, version, [["54511", "2026-01-01T00:00:00Z", "2"]]
    )
    report = create_report(client).json()
    point_zero = next(
        g for g in report["grid"] if g["observed_at"] == "2026-01-01T00:00:00Z"
    )
    assert point_zero["observed"] is True
    assert sorted(point_zero["batch_ids"]) == sorted([first.id, second.id])
    assert report["observed_count"] == 1  # 重复时刻只算一次
    # 两个批次都进入依据集合
    assert sorted(report["basis_batch_ids"]) == sorted([first.id, second.id])


def test_only_matching_station_rows_count(client: TestClient):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(
        settings,
        version,
        [
            ["54511", "2026-01-01T00:00:00Z", "1"],
            ["99999", "2026-01-01T00:10:00Z", "2"],
            ["54511", "2026-01-01T00:20:00Z", "3"],
        ],
    )
    report = create_report(client, station="54511").json()
    assert report["observed_count"] == 2
    assert report["missing_count"] == 5
    # 只含 54511 行的那一个批次是依据（该批同时含 99999 行仍算依据，因为含本站行）
    assert report["basis_batch_count"] == 1


def test_observations_outside_window_are_ignored(client: TestClient):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(
        settings,
        version,
        [
            ["54511", "2025-12-31T23:50:00Z", "1"],  # 窗口外
            ["54511", "2026-01-01T01:10:00Z", "2"],  # 窗口外
        ],
    )
    report = create_report(client).json()
    assert report["observed_count"] == 0
    assert report["missing_count"] == 7
    assert report["basis_batch_count"] == 0
    assert report["offgrid_observations"] == []


def test_interval_choices_and_24h_boundary(client: TestClient):
    prepare_schema(client)
    # 恰好 24 小时、60 分钟网格：25 个点
    resp = create_report(client, start=WINDOW_START, end="2026-01-02T00:00:00Z", interval=60)
    assert resp.status_code == 201
    assert resp.json()["point_count"] == 25
    # 1 分钟网格、24 小时：1441 个点
    resp = create_report(client, start=WINDOW_START, end="2026-01-02T00:00:00Z", interval=1)
    assert resp.status_code == 201
    assert resp.json()["point_count"] == 1441


# ---------------------------------------------------------------------------
# 参数校验
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate, fragment",
    [
        (lambda b: b.update(station_id="   "), "站号"),
        (lambda b: b.update(interval_minutes=15), "采样间隔"),
        (lambda b: b.update(interval_minutes="x"), "采样间隔"),
        (lambda b: b.update(end_at=WINDOW_START), "结束时刻必须晚于"),
        (lambda b: b.update(start_at="2026-01-01T00:00:30Z"), "精确到分钟"),
        (lambda b: b.update(start_at="garbage"), "ISO 8601"),
        (lambda b: b.update(end_at="2026-01-02T00:01:00Z"), "24 小时"),
        (lambda b: b.update(end_at="2026-01-01T00:25:00Z"), "整除"),
    ],
)
def test_invalid_requests_rejected(client: TestClient, mutate, fragment):
    prepare_schema(client)
    body = {
        "station_id": "54511",
        "start_at": WINDOW_START,
        "end_at": WINDOW_END,
        "interval_minutes": 10,
    }
    mutate(body)
    resp = client.post("/api/gap-reports", json=body)
    assert resp.status_code == 400, resp.text
    assert fragment in resp.json()["detail"]


def test_naive_datetime_interpreted_as_utc(client: TestClient):
    prepare_schema(client)
    resp = client.post(
        "/api/gap-reports",
        json={
            "station_id": "X",
            "start_at": "2026-01-01T00:00",
            "end_at": "2026-01-01T00:05",
            "interval_minutes": 5,
        },
    )
    assert resp.status_code == 201
    assert resp.json()["start_at"] == "2026-01-01T00:00:00Z"


def test_report_not_found(client: TestClient):
    assert client.get("/api/gap-reports/missing").status_code == 404
    assert client.post("/api/gap-reports/missing/refresh").status_code == 404
    page = client.get("/gaps/missing")
    assert page.status_code == 404


# ---------------------------------------------------------------------------
# 过期标记：相关补传立即过期，无关补传不影响
# ---------------------------------------------------------------------------


def test_relevant_upload_marks_report_stale_immediately(client: TestClient):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    report = create_report(client).json()
    rid = report["id"]
    assert report["status"] == "current"

    later = upload_core(
        settings, version, [["54511", "2026-01-01T00:30:00Z", "8"]]
    )
    fetched = client.get(f"/api/gap-reports/{rid}").json()
    assert fetched["status"] == "stale"
    assert fetched["stale_reason_batch_id"] == later.id
    assert fetched["stale_at"] is not None
    # 旧结论原样保留
    assert fetched["missing_count"] == report["missing_count"]
    assert fetched["basis_batch_ids"] == report["basis_batch_ids"]


def test_irrelevant_upload_does_not_stale_report(client: TestClient):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    report = create_report(client).json()
    rid = report["id"]

    # 别的站号
    upload_core(settings, version, [["99999", "2026-01-01T00:30:00Z", "8"]])
    # 同站号但窗口外
    upload_core(settings, version, [["54511", "2026-02-01T00:00:00Z", "8"]])
    fetched = client.get(f"/api/gap-reports/{rid}").json()
    assert fetched["status"] == "current"
    assert fetched["stale_reason_batch_id"] is None


def test_refresh_recomputes_and_clears_stale(client: TestClient):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    rid = create_report(client).json()["id"]
    upload_core(settings, version, [["54511", "2026-01-01T00:30:00Z", "8"]])
    assert client.get(f"/api/gap-reports/{rid}").json()["status"] == "stale"

    refreshed = client.post(f"/api/gap-reports/{rid}/refresh")
    assert refreshed.status_code == 200
    body = refreshed.json()
    assert body["status"] == "current"
    assert body["stale_reason_batch_id"] is None and body["stale_at"] is None
    assert body["observed_count"] == 2 and body["missing_count"] == 5
    assert body["basis_batch_count"] == 2


def test_compute_time_upload_marks_new_report_stale(client: TestClient, monkeypatch):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])

    def upload_during_compute() -> None:
        upload_core(settings, version, [["54511", "2026-01-01T00:30:00Z", "8"]])

    monkeypatch.setattr(gaps, "_during_compute_hook", upload_during_compute)
    resp = create_report(client)
    assert resp.status_code == 201
    body = resp.json()
    # 绝不能把可能过期的结论标成当前
    assert body["status"] == "stale"
    assert body["stale_reason_batch_id"] is not None


def test_compute_time_irrelevant_upload_keeps_report_current(
    client: TestClient, monkeypatch
):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])

    def irrelevant() -> None:
        upload_core(settings, version, [["99999", "2026-01-01T00:30:00Z", "8"]])

    monkeypatch.setattr(gaps, "_during_compute_hook", irrelevant)
    body = create_report(client).json()
    assert body["status"] == "current"


def test_refresh_during_upload_keeps_old_conclusion(client: TestClient, monkeypatch):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    rid = create_report(client).json()["id"]
    old = client.get(f"/api/gap-reports/{rid}").json()

    def upload_during_refresh() -> None:
        upload_core(settings, version, [["54511", "2026-01-01T00:30:00Z", "8"]])

    monkeypatch.setattr(gaps, "_during_compute_hook", upload_during_refresh)
    resp = client.post(f"/api/gap-reports/{rid}/refresh")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "stale"
    # 中间结论不落库：网格结论与时间戳保持刷新前的值
    assert body["missing_count"] == old["missing_count"]
    assert body["computed_at"] == old["computed_at"]
    assert body["basis_batch_ids"] == old["basis_batch_ids"]


# ---------------------------------------------------------------------------
# 失败保留旧报告、重启持久化
# ---------------------------------------------------------------------------


def test_persist_failure_preserves_old_report(client: TestClient, monkeypatch):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    rid = create_report(client).json()["id"]
    before = client.get(f"/api/gap-reports/{rid}").json()

    def boom() -> None:
        raise lake.StorageError("模拟提交失败")

    monkeypatch.setattr(gaps, "_before_persist_hook", boom)
    resp = client.post(f"/api/gap-reports/{rid}/refresh")
    assert resp.status_code == 500
    assert resp.json()["error"] == "storage_error"

    after = client.get(f"/api/gap-reports/{rid}").json()
    assert after["status"] == before["status"]
    assert after["missing_count"] == before["missing_count"]
    assert after["refreshed_at"] == before["refreshed_at"]


def test_sqlite_error_during_persist_is_storage_error(client: TestClient, monkeypatch):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    rid = create_report(client).json()["id"]
    before = client.get(f"/api/gap-reports/{rid}").json()

    def boom_sqlite() -> None:
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(gaps, "_before_persist_hook", boom_sqlite)
    resp = client.post(f"/api/gap-reports/{rid}/refresh")
    assert resp.status_code == 500
    after = client.get(f"/api/gap-reports/{rid}").json()
    assert after["missing_count"] == before["missing_count"]


def test_report_persists_across_restart(settings: Settings):
    app = create_app(settings)
    with TestClient(app) as c:
        version = prepare_schema(c)
        upload_core(
            settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]]
        )
        rid = create_report(c).json()["id"]

    restarted = TestClient(
        create_app(Settings(data_dir=settings.data_dir, instance_name="again"))
    )
    with restarted:
        reports = restarted.get("/api/gap-reports").json()["reports"]
        assert len(reports) == 1
        assert reports[0]["id"] == rid
        detail = restarted.get(f"/api/gap-reports/{rid}").json()
        assert detail["grid"][0]["observed_at"] == "2026-01-01T00:00:00Z"
        assert detail["missing_intervals"][0]["point_count"] == 6


def test_startup_reconcile_marks_missed_stale(settings: Settings, tmp_path):
    """构造“相关批次已存在但报告漏标”的状态，启动核对应补齐过期。"""

    import shutil

    with TestClient(create_app(settings)) as c:
        version = prepare_schema(c)
        real_batch = upload_core(
            settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]]
        )
        rid = create_report(c).json()["id"]

    # 复制真实 Parquet 并直接补登一个批次（绕过置过期联动），报告保持 current：
    # 模拟历史数据 / 异常手工拷贝导致的漏标。
    shutil.copy(
        settings.parquet_dir / real_batch.file_name,
        settings.parquet_dir / "phantom.parquet",
    )
    with lake._write_transaction(settings) as conn:
        conn.execute(
            "INSERT INTO batches(id, created_at, schema_version, row_count, "
            "file_name, file_sha256, schema_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "a" * 32,
                lake._utc_now_iso(),
                version,
                1,
                "phantom.parquet",
                "0",
                "[]",
            ),
        )
        conn.execute(
            "UPDATE gap_reports SET status = 'current' WHERE id = ?", (rid,)
        )

    with TestClient(
        create_app(Settings(data_dir=settings.data_dir, instance_name="reconcile"))
    ) as c2:
        body = c2.get(f"/api/gap-reports/{rid}").json()
        assert body["status"] == "stale"
        assert body["stale_reason_batch_id"] == "a" * 32


# ---------------------------------------------------------------------------
# 网页
# ---------------------------------------------------------------------------


def test_gap_web_pages_render(client: TestClient):
    version = prepare_schema(client)
    settings = client.app.state.settings
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    listing = client.get("/gaps")
    assert listing.status_code == 200
    assert "新建缺测核对" in listing.text
    assert "54511" in listing.text  # 已知站号 datalist

    form = client.post(
        "/gaps",
        data={
            "station_id": "54511",
            "start_at": "2026-01-01T00:00",
            "end_at": "2026-01-01T01:00",
            "interval_minutes": "10",
        },
        follow_redirects=False,
    )
    assert form.status_code == 303
    rid = form.headers["location"].split("?")[0].rstrip("/").split("/")[-1]

    detail = client.get(f"/gaps/{rid}")
    assert detail.status_code == 200
    assert "时间网格明细" in detail.text
    assert "连续缺测区间" in detail.text
    assert "依据批次" in detail.text
    assert "非网格观测" in detail.text

    refresh = client.post(f"/gaps/{rid}/refresh", follow_redirects=False)
    assert refresh.status_code == 303
    followed = client.post(f"/gaps/{rid}/refresh", follow_redirects=True)
    assert followed.status_code == 200


def test_gap_form_validation_error_redirects(client: TestClient):
    prepare_schema(client)
    resp = client.post(
        "/gaps",
        data={
            "station_id": "54511",
            "start_at": "2026-01-01T01:00",
            "end_at": "2026-01-01T00:00",
            "interval_minutes": "10",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "kind=error" in resp.headers["location"]
    # 没有生成任何报告
    assert client.get("/api/gap-reports").json()["reports"] == []


def test_stale_badge_shown_on_listing(client: TestClient):
    settings = client.app.state.settings
    version = prepare_schema(client)
    upload_core(settings, version, [["54511", "2026-01-01T00:00:00Z", "1"]])
    rid = create_report(client).json()["id"]
    upload_core(settings, version, [["54511", "2026-01-01T00:30:00Z", "8"]])
    page = client.get("/gaps").text
    assert "过期" in page
    detail = client.get(f"/gaps/{rid}").text
    assert "本报告已" in detail and "过期" in detail


# ---------------------------------------------------------------------------
# 并发：两个真实线程同时刷新/上传，结论必须来自单一已提交状态
# ---------------------------------------------------------------------------


def test_concurrent_refresh_and_upload_never_mixes_states(
    settings: Settings, monkeypatch
):
    lake.add_field(settings, 0, "temp_c")
    schema = lake.read_schema(settings)
    lake.upload_csv(
        settings,
        schema.version,
        csv_bytes([["54511", "2026-01-01T00:00:00Z", "1"]]),
    )
    report = gaps.create_report(
        settings, "54511", WINDOW_START, WINDOW_END, 10
    )
    rid = report.id
    errors: list[BaseException] = []
    stop = threading.Event()

    def refresher() -> None:
        while not stop.is_set():
            try:
                current = gaps.get_report(settings, rid)
                # 自洽性：状态与结论必须匹配，current 报告的依据必须全部已登记
                state = lake.read_state(settings)
                registered = {b.id for b in state.batches}
                assert set(current.basis_batch_ids) <= registered
                if current.status == gaps.STATUS_CURRENT:
                    # 当前报告引用的批次时刻必须真的在网格上已观测
                    for point in current.grid:
                        if point.observed:
                            assert set(point.batch_ids) <= registered
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)
                return

    thread = threading.Thread(target=refresher, daemon=True)
    thread.start()
    try:
        for minute in (10, 20, 30, 40, 50):
            lake.upload_csv(
                settings,
                schema.version,
                csv_bytes(
                    [["54511", f"2026-01-01T00:{minute:02d}:00Z", str(minute)]]
                ),
            )
            refreshed = gaps.refresh_report(settings, rid)
            # 每次刷新后若为 current，其缺测数必须与依据批次自洽
            if refreshed.status == gaps.STATUS_CURRENT:
                expected_observed = 1 + len([m for m in (10, 20, 30, 40, 50) if m <= minute])
                assert refreshed.observed_count == expected_observed
    finally:
        stop.set()
        thread.join(timeout=5)
    assert errors == []
    final = gaps.refresh_report(settings, rid)
    assert final.status == gaps.STATUS_CURRENT
    assert final.observed_count == 6 and final.missing_count == 1


def test_mark_reports_only_flips_current(settings: Settings):
    lake.add_field(settings, 0, "temp_c")
    schema = lake.read_schema(settings)
    lake.upload_csv(
        settings,
        schema.version,
        csv_bytes([["54511", "2026-01-01T00:00:00Z", "1"]]),
    )
    report = gaps.create_report(settings, "54511", WINDOW_START, WINDOW_END, 10)
    # 第一次相关补传：置过期
    b2 = lake.upload_csv(
        settings,
        schema.version,
        csv_bytes([["54511", "2026-01-01T00:20:00Z", "2"]]),
    )
    stale_once = gaps.get_report(settings, report.id)
    assert stale_once.status == gaps.STATUS_STALE
    assert stale_once.stale_reason_batch_id == b2.id
    # 第二次相关补传：保留最早的过期原因，不覆盖
    b3 = lake.upload_csv(
        settings,
        schema.version,
        csv_bytes([["54511", "2026-01-01T00:30:00Z", "3"]]),
    )
    stale_twice = gaps.get_report(settings, report.id)
    assert stale_twice.status == gaps.STATUS_STALE
    assert stale_twice.stale_reason_batch_id == b2.id
    assert b3.id != b2.id
