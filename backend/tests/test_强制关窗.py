"""强制关窗：封印、窗规推进、并发与贯验。

工程形态：切流账册强制封印——不论缓冲是否满均钉强封指纹，
现行窗按本窗物化（或空封）后同事务推进到下一窗。
"""
from pathlib import Path
import os
import tempfile
import threading
import pytest

_tmp = tempfile.mkdtemp()
os.environ["数据目录"] = _tmp
os.environ["夹具目录"] = str(Path(__file__).resolve().parents[2] / "fixtures")

from app import seed
from app.services import 装载, 关窗, 贯验, 目录, 夹具
from app.modules import 强制关窗


@pytest.fixture(autouse=True)
def _db():
    seed.初始化库()
    yield


def _窗内事件(前缀: str, 运单: str = "运单-F", 窗号: str = "W-demo") -> dict:
    return {
        "事件": [
            {"事件编号": f"{前缀}-1", "运单键": 运单, "网点键": "网点-东站",
             "事件时": "2026-11-24T00:10:00Z"},
            {"事件编号": f"{前缀}-2", "运单键": 运单, "网点键": "网点-西港",
             "事件时": "2026-11-24T00:40:00Z"},
        ]
    }


def test_有缓冲强关成片并推进窗():
    装载.装一批(_窗内事件("F1"))
    规前 = 目录.窗规()
    报 = 关窗.强制关当前窗()

    # 同票：封印指纹、批次与缓冲清写同成败
    assert 报["强制"] is True
    assert 报["强封键"]
    assert 报["窗号"] == 规前["当前窗号"]
    assert 报["缓冲消耗"] == 2
    assert 报["成片数"] == 1
    # 现行窗号推进，且与被封窗号可区分
    规后 = 目录.窗规()
    assert 规后["当前窗号"] != 规前["当前窗号"]
    assert 规后["窗起"] == 规前["窗止"]
    assert 报["新窗号"] == 规后["当前窗号"]
    # 缓冲清空：被封窗不留任何「在缓冲」残留（已物化行留痕，非半关）
    assert not [r for r in 目录.缓冲列表() if r["命运"] == "在缓冲"]
    切片 = 目录.切片列表()
    assert len(切片) == 1 and 切片[0]["窗号"] == 规前["当前窗号"]


def test_空缓冲强关有封印():
    报 = 关窗.强制关当前窗()
    assert 报["强制"] is True
    assert 报["强封键"]
    assert 报["缓冲消耗"] == 0
    assert 报["成片数"] == 0
    # 空窗也要留封印批次，避免误关漏记
    assert 强制关窗.最近强封()["valid"] is True
    assert 强制关窗.最近强封()["强封键"] == 报["强封键"]
    assert 目录.窗规()["当前窗号"] == 报["新窗号"]


def test_强封只读接口字段():
    装载.装一批(_窗内事件("F2", 运单="运单-G"))
    报 = 关窗.强制关当前窗()
    只 = 强制关窗.最近强封()
    assert set(只) >= {"窗号", "强封键", "新窗号", "valid"}
    assert 只["valid"] is True
    assert 只["窗号"] == 报["窗号"]
    assert 只["强封键"] == 报["强封键"]
    assert 只["新窗号"] == 报["新窗号"]


def test_无强封时只读接口invalid():
    只 = 强制关窗.最近强封()
    assert 只["valid"] is False
    assert 只["强封键"] == ""
    assert 只["窗号"] == ""
    assert 只["新窗号"] == ""


def test_强关与普通关并发不双关():
    装载.装一批(_窗内事件("F3", 运单="运单-H"))
    规 = 目录.窗规()
    出发 = threading.Barrier(2)
    回执 = []

    def 强():
        出发.wait()
        回执.append(("强", 关窗.强制关当前窗()))

    def 普():
        出发.wait()
        回执.append(("普", 关窗.关当前窗()))

    t1, t2 = threading.Thread(target=强), threading.Thread(target=普)
    t1.start(); t2.start(); t1.join(); t2.join()

    # 同一窗只允许一个成功者；缓冲只能被物化一次
    成功 = [r for _, r in 回执 if not r.get("冲突")]
    冲突 = [r for _, r in 回执 if r.get("冲突")]
    assert len(成功) == 1
    assert len(冲突) == 1
    from app.db import 连接
    conn = 连接()
    try:
        批数 = conn.execute(
            "SELECT COUNT(*) AS c FROM 关窗批次 WHERE 窗号=?", (规["当前窗号"],)
        ).fetchone()["c"]
        片数 = conn.execute(
            "SELECT COUNT(*) AS c FROM 窗切片账 WHERE 窗号=?", (规["当前窗号"],)
        ).fetchone()["c"]
    finally:
        conn.close()
    assert 批数 == 1
    assert 片数 == 1
    # 被封窗不得有「在缓冲」残留
    assert not [r for r in 目录.缓冲列表() if r["窗号"] == 规["当前窗号"] and r["命运"] == "在缓冲"]


def test_强关与装载并发不得半关现行():
    装载.装一批(_窗内事件("F4", 运单="运单-I"))
    出发 = threading.Barrier(2)
    新事件 = {
        "事件": [
            {"事件编号": "F5-1", "运单键": "运单-J", "网点键": "网点-北仓",
             "事件时": "2026-11-24T00:55:00Z"},
        ]
    }
    回执 = {}

    def 强():
        出发.wait()
        回执["强"] = 关窗.强制关当前窗()

    def 装():
        出发.wait()
        回执["装"] = 装载.装一批(新事件)

    t1, t2 = threading.Thread(target=强), threading.Thread(target=装)
    t1.start(); t2.start(); t1.join(); t2.join()

    规 = 目录.窗规()
    # 现行窗号已推进，新事件不得以「在缓冲」悬挂在被封旧窗
    assert 规["当前窗号"] == 回执["强"]["新窗号"]
    assert not [
        r for r in 目录.缓冲列表()
        if r["窗号"] == 回执["强"]["窗号"] and r["命运"] == "在缓冲"
    ]
    # 要么旧窗成片（关先装后，新事件进新窗缓冲），要么新事件随旧窗成片（装先关后）
    from app.db import 连接
    conn = 连接()
    try:
        旧窗在缓冲 = conn.execute(
            "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 窗号=? AND 命运='在缓冲'",
            (回执["强"]["窗号"],),
        ).fetchone()["c"]
    finally:
        conn.close()
    assert 旧窗在缓冲 == 0


def test_强关后贯验仍对齐():
    装载.装一批(夹具.读取("窗内成片样例"))
    报 = 关窗.强制关当前窗()
    assert 报["成片数"] == 2
    验 = 贯验.跑一次()
    assert 验["是否对齐"] is True
    # 空封后再贯验：无撕裂、无报错
    关窗.强制关当前窗()
    assert 贯验.跑一次()["是否对齐"] is True
