"""缺测核对：在 UTC 时间网格上汇总已登记批次，生成并维护缺测报告。

核心规则
--------
* 网格由 ``[start_at, end_at]``（含端点）按 ``interval_minutes`` 等距生成；
  时间跨度必须能被间隔整除，且不超过 24 小时，端点精确到分钟。
* 已登记批次中，站号匹配且时刻落在窗口内（含端点）的行参与汇总；
  同一网格时刻被多个批次登记只算一次（集合语义）；非网格时刻不抵缺测，
  但会作为“非网格观测”随报告保存，其批次仍属于报告的依据批次。
* 报告持久化保存逐网格结论、连续缺测区间与依据批次集合。
* 批次登记与“相关报告置过期”在同一个 SQLite 写事务内完成；报告生成 /
  刷新在写事务内复核计算期间是否出现新的相关批次——因此报告的批次集合
  与缺测结论永远来自同一个已提交状态，过期结论不会被标成当前。
* Parquet 读取（计算）在数据库锁之外进行，核对期间上传不受阻塞；
  任一步失败只回滚本次写入，旧报告及其状态原样保留。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from . import lake
from .lake import (
    STATION_COLUMN,
    TIME_COLUMN,
    LakeError,
    NotFound,
    StorageError,
    ValidationError,
)

log = logging.getLogger("metlake")

ALLOWED_INTERVALS = (1, 5, 10, 60)
MAX_WINDOW = timedelta(hours=24)
MAX_STATION_LENGTH = lake.MAX_STATION_LENGTH

STATUS_CURRENT = "current"
STATUS_STALE = "stale"

# 测试钩子：快照取得之后、Parquet 扫描期间执行（模拟“计算中补传”）。
_during_compute_hook: Callable[[], None] | None = None
# 测试钩子：报告写事务内、INSERT/UPDATE 之前执行（模拟提交失败）。
_before_persist_hook: Callable[[], None] | None = None


def set_during_compute_hook(hook: Callable[[], None] | None) -> None:
    global _during_compute_hook
    _during_compute_hook = hook


def set_before_persist_hook(hook: Callable[[], None] | None) -> None:
    global _before_persist_hook
    _before_persist_hook = hook


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GridPoint:
    observed_at: datetime
    observed: bool
    batch_ids: tuple[str, ...]


@dataclass(frozen=True)
class OffGridPoint:
    observed_at: datetime
    batch_id: str


@dataclass(frozen=True)
class MissingInterval:
    start_at: datetime
    end_at: datetime
    point_count: int


@dataclass(frozen=True)
class GapReport:
    id: str
    station_id: str
    start_at: datetime
    end_at: datetime
    interval_minutes: int
    status: str
    created_at: str
    computed_at: str
    refreshed_at: str
    point_count: int
    observed_count: int
    missing_count: int
    basis_batch_ids: tuple[str, ...]
    grid: tuple[GridPoint, ...]
    offgrid: tuple[OffGridPoint, ...]
    intervals: tuple[MissingInterval, ...]
    stale_reason_batch_id: str | None
    stale_at: str | None
    snapshot_batch_count: int

    @property
    def short_id(self) -> str:
        return self.id[:8]


# ---------------------------------------------------------------------------
# 参数校验
# ---------------------------------------------------------------------------


def parse_station(raw: str | None) -> str:
    if raw is None:
        raise ValidationError("站号不能为空。")
    station = str(raw).strip()
    if not station:
        raise ValidationError("站号不能为空。")
    if len(station) > MAX_STATION_LENGTH:
        raise ValidationError(f"站号最长 {MAX_STATION_LENGTH} 个字符。")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in station):
        raise ValidationError("站号不能包含控制字符。")
    return station


def parse_minute_utc(raw: str | None, field_name: str) -> datetime:
    if raw is None or not str(raw).strip():
        raise ValidationError(f"{field_name}不能为空。")
    text = str(raw).strip()
    try:
        value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError(f"{field_name}“{text}”不是 ISO 8601 时刻。") from None
    if value.tzinfo is None:
        # 网页 datetime-local 控件提交的是无时区值，按 UTC 解释。
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    if value.second != 0 or value.microsecond != 0:
        raise ValidationError(f"{field_name}必须精确到分钟（秒为 00）。")
    return value


def parse_interval(raw: str | int | None) -> int:
    try:
        interval = int(raw)
    except (TypeError, ValueError):
        raise ValidationError(
            f"采样间隔必须是 {', '.join(map(str, ALLOWED_INTERVALS))} 分钟之一。"
        ) from None
    if interval not in ALLOWED_INTERVALS:
        raise ValidationError(
            f"采样间隔只支持 {', '.join(map(str, ALLOWED_INTERVALS))} 分钟。"
        )
    return interval


def validate_window(
    station: str, start: datetime, end: datetime, interval: int
) -> None:
    if end <= start:
        raise ValidationError("UTC 结束时刻必须晚于开始时刻。")
    span = end - start
    if span > MAX_WINDOW:
        raise ValidationError("时间跨度不能超过 24 小时（含端点）。")
    seconds = int(span.total_seconds())
    if seconds % (interval * 60) != 0:
        raise ValidationError(
            f"时间跨度 {seconds // 60} 分钟必须能被采样间隔 {interval} 分钟整除，"
            "否则结束端点不在网格上。"
        )


def build_grid(start: datetime, end: datetime, interval: int) -> list[datetime]:
    step = timedelta(minutes=interval)
    points: list[datetime] = []
    current = start
    while current <= end:
        points.append(current)
        current += step
    return points


# ---------------------------------------------------------------------------
# 批次相关性读取（Parquet 不可变，可在数据库事务之外安全读取）
# ---------------------------------------------------------------------------


def _read_station_times(path_text: str) -> list[tuple[str, datetime]]:
    """读取一个 Parquet 的 (站号, UTC 时刻) 两列；文件异常上抛存储错误。"""

    try:
        table = pq.read_table(path_text, columns=[STATION_COLUMN, TIME_COLUMN])
        stations = table.column(STATION_COLUMN).combine_chunks().cast(pa.string()).to_pylist()
        times = (
            table.column(TIME_COLUMN)
            .combine_chunks()
            .cast(pa.timestamp("us", tz="UTC"))
            .to_pylist()
        )
    except (OSError, pa.ArrowException, KeyError) as exc:
        raise StorageError(f"读取批次 Parquet 失败：{path_text}") from exc
    pairs: list[tuple[str, datetime]] = []
    for station, value in zip(stations, times):
        if value is None or station is None:
            continue
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        pairs.append((str(station), value))
    return pairs


def _file_is_relevant(
    settings: lake.Settings, file_name: str, station: str,
    start: datetime, end: datetime,
) -> bool:
    path = settings.parquet_dir / file_name
    if not path.is_file():
        raise StorageError(f"批次 Parquet 文件缺失：{file_name}")
    for row_station, row_time in _read_station_times(str(path)):
        if row_station == station and start <= row_time <= end:
            return True
    return False


@dataclass(frozen=True)
class ComputedBasis:
    """一次计算的中间结果，连同计算时已登记的批次快照。"""

    snapshot_ids: frozenset[str]
    basis_batch_ids: tuple[str, ...]
    observed_at: dict[datetime, tuple[str, ...]]
    offgrid: tuple[OffGridPoint, ...]


def _collect_basis(
    settings: lake.Settings,
    station: str,
    start: datetime,
    end: datetime,
    interval: int,
) -> ComputedBasis:
    """在读事务取得批次登记快照，然后在锁外扫描不可变 Parquet。"""

    try:
        with lake._read_transaction(settings) as conn:
            rows = conn.execute(
                "SELECT id, file_name FROM batches ORDER BY created_at ASC, id ASC"
            ).fetchall()
            snapshot = [(r["id"], r["file_name"]) for r in rows]
    except sqlite3.Error as exc:
        raise StorageError("读取批次登记失败，未生成报告。") from exc

    snapshot_ids = frozenset(bid for bid, _ in snapshot)
    if _during_compute_hook is not None:
        # 钩子内的提交会被写事务串行化；其新批次将在持久化事务内被复核到。
        _during_compute_hook()

    grid_set = set(build_grid(start, end, interval))
    basis: list[str] = []
    point_batches: dict[datetime, set[str]] = {point: set() for point in grid_set}
    offgrid: list[OffGridPoint] = []

    for batch_id, file_name in snapshot:
        path = settings.parquet_dir / file_name
        if not path.is_file():
            raise StorageError(f"批次 Parquet 文件缺失：{file_name}")
        relevant = False
        for row_station, row_time in _read_station_times(str(path)):
            if row_station != station or not (start <= row_time <= end):
                continue
            relevant = True
            if row_time in grid_set:
                point_batches[row_time].add(batch_id)
            else:
                offgrid.append(OffGridPoint(observed_at=row_time, batch_id=batch_id))
        if relevant:
            basis.append(batch_id)

    observed_at = {point: tuple(sorted(ids)) for point, ids in point_batches.items()}
    return ComputedBasis(
        snapshot_ids=snapshot_ids,
        basis_batch_ids=tuple(basis),
        observed_at=observed_at,
        offgrid=tuple(
            sorted(offgrid, key=lambda item: (item.observed_at, item.batch_id))
        ),
    )


def _build_conclusion(
    start: datetime,
    end: datetime,
    interval: int,
    basis: ComputedBasis,
) -> tuple[tuple[GridPoint, ...], tuple[MissingInterval, ...], int, int]:
    points = build_grid(start, end, interval)
    grid: list[GridPoint] = []
    intervals: list[MissingInterval] = []
    observed_count = 0
    run_start: datetime | None = None
    run_count = 0

    def close_run(at: datetime) -> None:
        nonlocal run_start, run_count
        if run_start is not None:
            intervals.append(
                MissingInterval(
                    start_at=run_start,
                    end_at=at - timedelta(minutes=interval),
                    point_count=run_count,
                )
            )
            run_start = None
            run_count = 0

    for point in points:
        batches = basis.observed_at.get(point, ())
        observed = bool(batches)
        grid.append(
            GridPoint(observed_at=point, observed=observed, batch_ids=batches)
        )
        if observed:
            observed_count += 1
            close_run(point)
        else:
            if run_start is None:
                run_start = point
            run_count += 1
    close_run(end + timedelta(minutes=interval))

    return tuple(grid), tuple(intervals), observed_count, len(points) - observed_count


# ---------------------------------------------------------------------------
# 持久化
# ---------------------------------------------------------------------------


def ensure_schema(conn: sqlite3.Connection) -> None:
    """在初始化写事务内建表（幂等）。"""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gap_reports (
            id TEXT PRIMARY KEY,
            station_id TEXT NOT NULL,
            start_at TEXT NOT NULL,
            end_at TEXT NOT NULL,
            interval_minutes INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('current', 'stale')),
            created_at TEXT NOT NULL,
            computed_at TEXT NOT NULL,
            refreshed_at TEXT NOT NULL,
            point_count INTEGER NOT NULL,
            observed_count INTEGER NOT NULL,
            missing_count INTEGER NOT NULL,
            basis_batch_ids TEXT NOT NULL,
            basis_batch_count INTEGER NOT NULL,
            grid_json TEXT NOT NULL,
            intervals_json TEXT NOT NULL,
            offgrid_json TEXT NOT NULL,
            stale_reason_batch_id TEXT,
            stale_at TEXT,
            snapshot_batch_count INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_gap_reports_station_status "
        "ON gap_reports(station_id, status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_gap_reports_created "
        "ON gap_reports(created_at DESC, id DESC)"
    )


def _grid_to_json(grid: tuple[GridPoint, ...]) -> str:
    return json.dumps(
        {
            "points": [
                {
                    "t": lake.format_utc(point.observed_at),
                    "observed": point.observed,
                    "batches": list(point.batch_ids),
                }
                for point in grid
            ]
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _intervals_to_json(intervals: tuple[MissingInterval, ...]) -> str:
    return json.dumps(
        [
            {
                "start_at": lake.format_utc(item.start_at),
                "end_at": lake.format_utc(item.end_at),
                "point_count": item.point_count,
            }
            for item in intervals
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _offgrid_to_json(offgrid: tuple[OffGridPoint, ...]) -> str:
    return json.dumps(
        [
            {"t": lake.format_utc(item.observed_at), "batch_id": item.batch_id}
            for item in offgrid
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _row_to_report(row: sqlite3.Row) -> GapReport:
    grid_payload = json.loads(row["grid_json"])
    grid = tuple(
        GridPoint(
            observed_at=_parse_iso(item["t"]),
            observed=bool(item["observed"]),
            batch_ids=tuple(item["batches"]),
        )
        for item in grid_payload["points"]
    )
    intervals = tuple(
        MissingInterval(
            start_at=_parse_iso(item["start_at"]),
            end_at=_parse_iso(item["end_at"]),
            point_count=int(item["point_count"]),
        )
        for item in json.loads(row["intervals_json"])
    )
    offgrid = tuple(
        OffGridPoint(observed_at=_parse_iso(item["t"]), batch_id=item["batch_id"])
        for item in json.loads(row["offgrid_json"])
    )
    return GapReport(
        id=row["id"],
        station_id=row["station_id"],
        start_at=_parse_iso(row["start_at"]),
        end_at=_parse_iso(row["end_at"]),
        interval_minutes=int(row["interval_minutes"]),
        status=row["status"],
        created_at=row["created_at"],
        computed_at=row["computed_at"],
        refreshed_at=row["refreshed_at"],
        point_count=int(row["point_count"]),
        observed_count=int(row["observed_count"]),
        missing_count=int(row["missing_count"]),
        basis_batch_ids=tuple(json.loads(row["basis_batch_ids"])),
        grid=grid,
        offgrid=offgrid,
        intervals=intervals,
        stale_reason_batch_id=row["stale_reason_batch_id"],
        stale_at=row["stale_at"],
        snapshot_batch_count=int(row["snapshot_batch_count"]),
    )


def list_reports(settings: lake.Settings) -> tuple[GapReport, ...]:
    try:
        with lake._read_transaction(settings) as conn:
            rows = conn.execute(
                "SELECT * FROM gap_reports ORDER BY created_at DESC, id DESC"
            ).fetchall()
    except sqlite3.Error as exc:
        raise StorageError("读取缺测报告列表失败。") from exc
    return tuple(_row_to_report(row) for row in rows)


def get_report(settings: lake.Settings, report_id: str) -> GapReport:
    try:
        with lake._read_transaction(settings) as conn:
            row = conn.execute(
                "SELECT * FROM gap_reports WHERE id = ?", (report_id,)
            ).fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取缺测报告失败。") from exc
    if row is None:
        raise NotFound("缺测报告不存在。")
    return _row_to_report(row)


def _persist_report(
    settings: lake.Settings,
    *,
    report_id: str | None,
    station: str,
    start: datetime,
    end: datetime,
    interval: int,
    basis: ComputedBasis,
    grid: tuple[GridPoint, ...],
    intervals: tuple[MissingInterval, ...],
    observed_count: int,
    missing_count: int,
    offgrid: tuple[OffGridPoint, ...],
) -> GapReport:
    """在单个写事务内复核并写入报告。

    事务开始后重新读取已登记批次集合：计算期间若有**新**批次提交，
    逐个判定其是否与本站号 / 窗口相关。相关则本次结论只能以 ``stale``
    落库（绝不能把可能过期的结论标成当前）；不相关则不影响报告。
    """

    now_iso = lake._utc_now_iso()
    try:
        with lake._write_transaction(settings) as conn:
            current_rows = conn.execute(
                "SELECT id, file_name FROM batches"
            ).fetchall()
            current = {r["id"]: r["file_name"] for r in current_rows}
            new_ids = [
                bid for bid in current if bid not in basis.snapshot_ids
            ]
            reason_id: str | None = None
            for bid in new_ids:
                if _file_is_relevant(settings, current[bid], station, start, end):
                    reason_id = bid
                    break

            if _before_persist_hook is not None:
                _before_persist_hook()

            if report_id is None:
                new_id = uuid.uuid4().hex
                status = STATUS_STALE if reason_id else STATUS_CURRENT
                conn.execute(
                    """
                    INSERT INTO gap_reports(
                        id, station_id, start_at, end_at, interval_minutes, status,
                        created_at, computed_at, refreshed_at,
                        point_count, observed_count, missing_count,
                        basis_batch_ids, basis_batch_count,
                        grid_json, intervals_json, offgrid_json,
                        stale_reason_batch_id, stale_at, snapshot_batch_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id,
                        station,
                        lake.format_utc(start),
                        lake.format_utc(end),
                        interval,
                        status,
                        now_iso,
                        now_iso,
                        now_iso,
                        len(grid),
                        observed_count,
                        missing_count,
                        json.dumps(list(basis.basis_batch_ids)),
                        len(basis.basis_batch_ids),
                        _grid_to_json(grid),
                        _intervals_to_json(intervals),
                        _offgrid_to_json(offgrid),
                        reason_id,
                        now_iso if reason_id else None,
                        len(basis.snapshot_ids),
                    ),
                )
            else:
                existing = conn.execute(
                    "SELECT id, status FROM gap_reports WHERE id = ?", (report_id,)
                ).fetchone()
                if existing is None:
                    raise NotFound("缺测报告不存在。")
                if reason_id:
                    # 计算期间出现相关补传：保留刷新前的旧结论，只记录过期原因，
                    # 绝不把这次可能过期的结论标成当前。
                    conn.execute(
                        "UPDATE gap_reports SET status = ?, stale_reason_batch_id = ?, "
                        "stale_at = ?, refreshed_at = ? WHERE id = ?",
                        (STATUS_STALE, reason_id, now_iso, now_iso, report_id),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE gap_reports SET
                            status = ?,
                            refreshed_at = ?,
                            computed_at = ?,
                            point_count = ?, observed_count = ?, missing_count = ?,
                            basis_batch_ids = ?, basis_batch_count = ?,
                            grid_json = ?, intervals_json = ?, offgrid_json = ?,
                            stale_reason_batch_id = NULL, stale_at = NULL,
                            snapshot_batch_count = ?
                        WHERE id = ?
                        """,
                        (
                            STATUS_CURRENT,
                            now_iso,
                            now_iso,
                            len(grid),
                            observed_count,
                            missing_count,
                            json.dumps(list(basis.basis_batch_ids)),
                            len(basis.basis_batch_ids),
                            _grid_to_json(grid),
                            _intervals_to_json(intervals),
                            _offgrid_to_json(offgrid),
                            len(basis.snapshot_ids),
                            report_id,
                        ),
                    )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("缺测报告写入失败，已有报告保持不变。") from exc
    return get_report(settings, report_id or new_id)


def create_report(
    settings: lake.Settings,
    raw_station: str,
    raw_start: str,
    raw_end: str,
    raw_interval: str | int,
) -> GapReport:
    station = parse_station(raw_station)
    start = parse_minute_utc(raw_start, "UTC 开始时刻")
    end = parse_minute_utc(raw_end, "UTC 结束时刻")
    interval = parse_interval(raw_interval)
    validate_window(station, start, end, interval)

    basis = _collect_basis(settings, station, start, end, interval)
    grid, intervals, observed_count, missing_count = _build_conclusion(
        start, end, interval, basis
    )
    report = _persist_report(
        settings,
        report_id=None,
        station=station,
        start=start,
        end=end,
        interval=interval,
        basis=basis,
        grid=grid,
        intervals=intervals,
        observed_count=observed_count,
        missing_count=missing_count,
        offgrid=basis.offgrid,
    )
    log.info(
        "gap report %s created station=%s window=%s..%s interval=%sm status=%s "
        "points=%s observed=%s missing=%s basis=%s",
        report.short_id, station, lake.format_utc(start), lake.format_utc(end),
        interval, report.status, report.point_count, observed_count,
        missing_count, len(basis.basis_batch_ids),
    )
    return report


def refresh_report(settings: lake.Settings, report_id: str) -> GapReport:
    previous = get_report(settings, report_id)
    basis = _collect_basis(
        settings,
        previous.station_id,
        previous.start_at,
        previous.end_at,
        previous.interval_minutes,
    )
    grid, intervals, observed_count, missing_count = _build_conclusion(
        previous.start_at, previous.end_at, previous.interval_minutes, basis
    )
    return _persist_report(
        settings,
        report_id=report_id,
        station=previous.station_id,
        start=previous.start_at,
        end=previous.end_at,
        interval=previous.interval_minutes,
        basis=basis,
        grid=grid,
        intervals=intervals,
        observed_count=observed_count,
        missing_count=missing_count,
        offgrid=basis.offgrid,
    )


# ---------------------------------------------------------------------------
# 批次提交时联动置过期（运行在批次登记的同一个写事务内）
# ---------------------------------------------------------------------------


def mark_reports_for_committed_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    station_time_pairs: list[tuple[str, datetime]],
    now_iso: str,
) -> int:
    """批次登记事务内调用：把相关的当前报告原子置为过期。

    相关性 = 报告站号出现在本批，且本批存在落在报告窗口（含端点）内的行。
    只翻转 ``current -> stale``；已过期的报告保留最早的过期原因。
    返回被置过期的报告数。
    """

    if not station_time_pairs:
        return 0
    try:
        rows = conn.execute(
            "SELECT id, station_id, start_at, end_at, status FROM gap_reports"
        ).fetchall()
    except sqlite3.Error:
        raise
    flipped = 0
    for row in rows:
        if row["status"] != STATUS_CURRENT:
            continue
        station = row["station_id"]
        start = _parse_iso(row["start_at"])
        end = _parse_iso(row["end_at"])
        relevant = any(
            row_station == station and start <= row_time <= end
            for row_station, row_time in station_time_pairs
        )
        if not relevant:
            continue
        conn.execute(
            "UPDATE gap_reports SET status = ?, stale_reason_batch_id = ?, "
            "stale_at = ? WHERE id = ? AND status = ?",
            (STATUS_STALE, batch_id, now_iso, row["id"], STATUS_CURRENT),
        )
        flipped += 1
        log.info(
            "gap report %s marked stale by batch %s", row["id"][:8], batch_id[:8]
        )
    return flipped


def reconcile_on_startup(settings: lake.Settings) -> None:
    """启动时防御性核对：当前报告若漏标过期则补齐。

    正常路径下过期标记已随批次事务原子提交；这里兜底处理历史数据 /
    异常手工拷贝等情况。只扫描 ``current`` 报告依据集合之外的批次。
    """

    try:
        reports = [r for r in list_reports(settings) if r.status == STATUS_CURRENT]
        if not reports:
            return
        state = lake.read_state(settings)
    except LakeError:
        log.warning("启动核对跳过：状态暂不可读")
        return

    for report in reports:
        basis = set(report.basis_batch_ids)
        reason: str | None = None
        for batch in state.batches:
            if batch.id in basis:
                continue
            try:
                if _file_is_relevant(
                    settings,
                    batch.file_name,
                    report.station_id,
                    report.start_at,
                    report.end_at,
                ):
                    reason = batch.id
                    break
            except StorageError:
                log.warning("启动核对遇到不可读批次 %s，跳过", batch.short_id)
                break
        if reason is None:
            continue
        try:
            with lake._write_transaction(settings) as conn:
                conn.execute(
                    "UPDATE gap_reports SET status = ?, stale_reason_batch_id = ?, "
                    "stale_at = ? WHERE id = ? AND status = ?",
                    (
                        STATUS_STALE,
                        reason,
                        lake._utc_now_iso(),
                        report.id,
                        STATUS_CURRENT,
                    ),
                )
        except sqlite3.Error:
            log.warning("启动核对更新报告 %s 失败", report.short_id)


def known_stations(settings: lake.Settings) -> list[str]:
    """汇总已登记批次中出现过的站号（只读站号列，供网页选择）。"""

    state = lake.read_state(settings)
    stations: set[str] = set()
    for batch in state.batches:
        path = settings.parquet_dir / batch.file_name
        if not path.is_file():
            continue
        try:
            table = pq.read_table(path, columns=[STATION_COLUMN])
            stations.update(
                value for value in table.column(STATION_COLUMN).to_pylist()
                if value is not None
            )
        except (OSError, pa.ArrowException):
            log.warning("读取站号失败：%s", batch.file_name)
    return sorted(stations)
