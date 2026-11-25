"""强制关窗（已接线，不再是空桩）。

工程形态：切流账册强制封印——不论缓冲是否满均钉强封指纹，
现行窗按本窗物化（有缓冲）或空封（无缓冲），同事务推进下一窗。

本模块只放强封语义与只读回读；关窗编排、锁与事务在 app.services.关窗。
回读口径须能区分「现行窗号」与「本窗是否有强封」。
"""
from __future__ import annotations
import uuid
from app.db import 连接

NAME = "强制关窗"
CONTRACT = "水位或人工触发可强制关未满窗；不论缓冲是否满均钉强封指纹"
工程形态 = "切流账册强制封印"

HOOKS = (
    "before_load",
    "after_load",
    "decide_window",
    "before_close",
    "after_close",
    "extra_verify_checks",
)


def describe() -> dict:
    return {
        "空桩": NAME,
        "契约": CONTRACT,
        "工程形态": 工程形态,
        "钩子": list(HOOKS),
        "默认启用": True,
        "说明": "已接线：关窗服务持锁单事务写片/清缓冲/封印/推进，同成败",
    }


def 造强封键(窗号: str, 时刻: str) -> str:
    """钉强封指纹：窗号 + 封印时刻 + 随机封印段，全局唯一。"""
    紧凑 = (时刻 or "").replace("-", "").replace(":", "").replace("T", "-").rstrip("Z")
    return f"强封-{窗号}-{紧凑}-{uuid.uuid4().hex[:8]}"


def 最近强封() -> dict:
    """只读接口：回读最近一次强封。

    返回固定四键：窗号、强封键、新窗号、valid。
    从未强关时 valid=False，其余字段为空串，不抛错。
    """
    conn = 连接()
    try:
        row = conn.execute(
            """SELECT 窗号, 报告 FROM 关窗批次
               WHERE json_extract(报告, '$.强封键') IS NOT NULL
               ORDER BY 编号 DESC, rowid DESC LIMIT 1"""
        ).fetchone()
        if row is None:
            return {"窗号": "", "强封键": "", "新窗号": "", "valid": False}
        import json

        报 = json.loads(row["报告"])
        return {
            "窗号": 报.get("窗号", row["窗号"]),
            "强封键": 报.get("强封键", ""),
            "新窗号": 报.get("新窗号", ""),
            "valid": bool(报.get("强封键")),
        }
    finally:
        conn.close()
