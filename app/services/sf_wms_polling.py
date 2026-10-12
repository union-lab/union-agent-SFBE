"""单进程 WMS 查询轮转；不复用业务 updated_at，也不新增数据库字段。"""

from datetime import datetime, timedelta


TERMINAL_TEXTS = frozenset({"已出库", "已发货", "已取消"})


class WmsPollQueue:
    """保存最近一次查询尝试时间（包括失败），防止新单/坏单饿死旧单。

    服务重启后按创建时间从旧到新重新轮转。这里只调度状态查询，不创建单据。
    状态仅适用于当前独立服务的单进程部署，不是跨实例锁。
    """

    def __init__(self):
        self.last_attempt: dict[int, datetime] = {}

    def select(self, records, *, limit: int, now: datetime):
        live_ids = {r["id"] for r in records}
        self.last_attempt = {
            key: value for key, value in self.last_attempt.items() if key in live_ids
        }
        due = []
        for record in records:
            last = self.last_attempt.get(record["id"])
            terminal = record["sf_wms_status_text"] in TERMINAL_TEXTS
            interval = timedelta(hours=2) if terminal else timedelta(minutes=10)
            # 首次检查非终态立即入队，不受业务 updated_at 影响。
            # 手动全量复检仍保留终态记录的两小时保护。
            checked_at = last or (record["updated_at"] if terminal else None)
            if checked_at is not None and now - checked_at < interval:
                continue
            due.append(record)
        due.sort(key=lambda r: (
            self.last_attempt.get(r["id"], r["created_at"]),
            r["created_at"], r["id"],
        ))
        return due[:limit], len(due)

    def mark_attempt(self, record_id: int, now: datetime) -> None:
        self.last_attempt[record_id] = now
