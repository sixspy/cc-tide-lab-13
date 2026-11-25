"""关窗：缓冲按运单物化为窗切片。

强制关窗工程形态：切流账册强制封印——
不论缓冲是否满均钉强封指纹（强封键随关窗批次同票落账），
现行窗按本窗物化（有缓冲）或空封（无缓冲），同事务推进到下一窗。
写片、清缓冲、封印、推进同成败（单事务提交/回滚）。
"""
from __future__ import annotations
import json
import threading
import uuid
from collections import defaultdict
from app.db import 连接
from app.seed import 此刻
from app.services import 目录, 时间
from app.modules import 强制关窗

# 进程内关窗/装载串行锁：普通关与强关不双成功，强关与装载不半关现行窗
关窗锁 = threading.RLock()

# 写连接统一加忙等与 WAL：残余文件锁等待有界、自动重试；读不阻塞写，配合进程内单写锁
忙等毫秒 = 5000


def 开连接():
    conn = 连接()
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(f"PRAGMA busy_timeout = {忙等毫秒}")
    return conn


def _读窗规(conn) -> dict:
    rows = conn.execute("SELECT 键,值 FROM 窗规").fetchall()
    return {r["键"]: r["值"] for r in rows}


def _物化(conn, 窗号: str, 窗起: str, 窗止: str, 时刻: str) -> tuple[int, int]:
    """本窗在缓冲事件按运单成片，同步清缓冲命运。返 (成片数, 缓冲消耗)。"""
    rows = conn.execute(
        "SELECT * FROM 入窗缓冲 WHERE 窗号=? AND 命运='在缓冲'", (窗号,)
    ).fetchall()
    by_waybill: dict[str, list] = defaultdict(list)
    for r in rows:
        by_waybill[r["运单键"]].append(r)
    成片 = 0
    for 运单, evs in by_waybill.items():
        切片 = f"片-{窗号}-{运单}-{uuid.uuid4().hex[:6]}"
        conn.execute(
            """INSERT INTO 窗切片账(切片编号,窗号,运单键,事件数,窗起,窗止,物化时,命运)
               VALUES (?,?,?,?,?,?,?,?)""",
            (切片, 窗号, 运单, len(evs), 窗起, 窗止, 时刻, "已成片"),
        )
        for e in evs:
            conn.execute(
                "UPDATE 入窗缓冲 SET 命运=? WHERE 事件编号=?",
                ("已物化", e["事件编号"]),
            )
            conn.execute(
                "UPDATE 观测点 SET 命运=? WHERE 事件编号=?",
                ("已成片", e["事件编号"]),
            )
        成片 += 1
    return 成片, len(rows)


def _执行关窗(强制: bool) -> dict:
    # 锁外先钉住本路意图关的窗号，进事务后比对，并发输家据此认冲突
    期望窗号 = 目录.窗规().get("当前窗号", "W-demo")
    with 关窗锁:
        conn = 开连接()
        try:
            conn.execute("BEGIN IMMEDIATE")
            规 = _读窗规(conn)
            窗号 = 规.get("当前窗号", "W-demo")
            # 冲突一：强关已把现行窗推进到下一窗
            # 冲突二：本窗已有任一关窗批次（普通或强封），不得双关
            已封 = conn.execute(
                "SELECT 1 FROM 关窗批次 WHERE 窗号=? LIMIT 1", (期望窗号,)
            ).fetchone()
            if 窗号 != 期望窗号 or 已封:
                conn.rollback()
                return {
                    "冲突": True,
                    "强制": 强制,
                    "窗号": 期望窗号,
                    "现行窗号": 窗号,
                    "说明": f"窗 {期望窗号} 已关或已推进，拒绝重复关窗",
                }
            窗起 = 规.get("窗起", "")
            窗止 = 规.get("窗止", "")
            时刻 = 此刻()
            成片, 消耗 = _物化(conn, 窗号, 窗起, 窗止, 时刻)
            批 = f"{'强关' if 强制 else '关窗'}-{uuid.uuid4().hex[:8]}"
            报告 = {
                "批次编号": 批,
                "窗号": 窗号,
                "成片数": 成片,
                "缓冲消耗": 消耗,
                "强制": 强制,
            }
            if 强制:
                # 不论缓冲是否满都钉强封指纹；无缓冲即空封
                强封键 = 强制关窗.造强封键(窗号, 时刻)
                新窗起, 新窗止 = 时间.推进窗(窗起, 窗止, int(规.get("窗长秒", 3600)))
                新窗号 = 时间.格式窗号("W", 新窗起)
                conn.execute(
                    "UPDATE 窗规 SET 值=? WHERE 键='当前窗号'", (新窗号,)
                )
                conn.execute("UPDATE 窗规 SET 值=? WHERE 键='窗起'", (新窗起,))
                conn.execute("UPDATE 窗规 SET 值=? WHERE 键='窗止'", (新窗止,))
                报告.update({
                    "强封键": 强封键,
                    "新窗号": 新窗号,
                    "封印": "空封" if 消耗 == 0 else "物化封印",
                })
            # 关窗批次与缓冲清写同票同事务
            conn.execute(
                """INSERT INTO 关窗批次(编号,窗号,时刻,成片数,报告) VALUES (?,?,?,?,?)""",
                (批, 窗号, 时刻, 成片, json.dumps(报告, ensure_ascii=False)),
            )
            conn.commit()
            return 报告
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def 关当前窗() -> dict:
    return _执行关窗(强制=False)


def 强制关当前窗() -> dict:
    return _执行关窗(强制=True)
