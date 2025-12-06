"""Streamlit user interface for the alerting system."""

from __future__ import annotations

# --- 路径修复 ---
import sys
import os
# 将项目根目录添加到 sys.path，解决模块导入问题
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# ----------------

import csv
import io
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import streamlit as st

from rules.config_loader import load_config
from storage import sqlite_manager


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _dashboard() -> None:
    st.header("行情仪表盘")
    config = load_config()
    cols = st.columns(2)
    with cols[0]:
        st.subheader("最新价 / 涨跌")
        rows: List[Dict[str, object]] = []
        for symbol in config.symbols:
            bar = sqlite_manager.fetch_latest_bar("bars_1m", symbol)
            if not bar:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "close": bar["close"],
                    "close_ts": _format_ts(int(bar["close_ts"])),
                }
            )
        st.dataframe(rows)
    with cols[1]:
        st.subheader("最近事件")
        events = sqlite_manager.fetch_undelivered_events(limit=20)
        table = [
            {
                "id": e["id"],
                "symbol": e["symbol"],
                "rule": e["rule"],
                "severity": e["severity"],
                "ts": _format_ts(int(e["ts"])),
            }
            for e in events
        ]
        st.dataframe(table)


def _price_alerts() -> None:
    st.header("价格提醒规则")
    rules = sqlite_manager.list_rules()
    st.dataframe(rules)
    st.caption("规则的 CRUD 可直接通过数据库或后续版本完善。")


def _volume_trend_config() -> None:
    st.header("放量 / 趋势参数")
    config = load_config()
    st.json({"volume_spike": config.volume_spike.mode.value, "trend_channel": config.trend_channel.__dict__})


def _notification_settings() -> None:
    st.header("通知设置")
    config = load_config()
    ding = config.notifiers.dingtalk
    st.subheader("钉钉")
    st.write({"enabled": ding.enabled, "webhook": ding.webhook, "secret": bool(ding.secret)})
    if st.button("测试钉钉推送"):
        st.info("请在服务端运行 alerts.router.dispatch_new_events() 触发测试")
    sound = config.notifiers.local_sound
    st.subheader("本地声音")
    st.write({"enabled": sound.enabled, "sound_file": sound.sound_file, "volume": sound.volume})


def _token_registry() -> None:
    st.header("代币注册表")
    tokens = sqlite_manager.list_tokens()
    st.dataframe(tokens)

    st.subheader("新增或更新代币")
    with st.form("token_form"):
        selected_id = st.selectbox(
            "选择已有代币 (可留空新增)",
            ["<新增>"] + [token["id"] for token in tokens],
        )
        existing: Optional[Dict[str, object]] = None
        if selected_id != "<新增>":
            existing = next(token for token in tokens if token["id"] == selected_id)
        token_id = st.text_input("唯一 ID", value=(existing["id"] if existing else ""))
        symbol = st.text_input("交易对符号", value=(existing["symbol"] if existing else ""))
        exchange = st.text_input("交易所", value=(existing["exchange"] if existing else "pancake"))
        chain = st.text_input("链", value=(existing["chain"] if existing else "BNB"))
        token_address = st.text_input(
            "Token 地址",
            value=(existing["token_address"] if existing else ""),
        )
        pool_address = st.text_input(
            "Pool 地址",
            value=(existing.get("pool_address") if existing else ""),
        )
        base = st.text_input("基础资产", value=(existing.get("base") if existing else ""))
        quote = st.text_input("计价资产", value=(existing.get("quote") if existing else "USDT"))
        decimals = st.number_input(
            "小数位",
            value=int(existing.get("decimals", 18) if existing else 18),
            min_value=0,
            max_value=36,
            step=1,
        )
        enabled = st.checkbox("启用", value=bool(existing and existing.get("enabled")))
        extra_json = st.text_area(
            "附加信息(JSON)",
            value=(existing.get("extra_json") if existing else "{}"),
        )
        submitted = st.form_submit_button("保存")
        if submitted:
            if not token_id or not symbol:
                st.error("ID 与 symbol 为必填字段")
            else:
                payload = {
                    "id": token_id,
                    "source": "dex",
                    "exchange": exchange,
                    "chain": chain,
                    "symbol": symbol,
                    "base": base,
                    "quote": quote,
                    "token_address": token_address,
                    "pool_address": pool_address or None,
                    "decimals": int(decimals),
                    "enabled": 1 if enabled else 0,
                    "extra_json": extra_json or "{}",
                    "created_at": int(existing.get("created_at", time.time()) if existing else time.time()),
                }
                sqlite_manager.upsert_token(payload)
                st.success("保存成功")
                st.rerun()  # 修复：使用 st.rerun()

    st.subheader("批量导入 CSV")
    uploaded = st.file_uploader("选择 CSV 文件", type=["csv"])
    if uploaded is not None:
        try:
            content = uploaded.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))
            imported = 0
            for row in reader:
                payload = {
                    "id": row.get("id") or f"token-{imported}",
                    "source": row.get("source", "dex"),
                    "exchange": row.get("exchange", "pancake"),
                    "chain": row.get("chain", "BNB"),
                    "symbol": row.get("symbol", ""),
                    "base": row.get("base", ""),
                    "quote": row.get("quote", "USDT"),
                    "token_address": row.get("token_address", ""),
                    "pool_address": row.get("pool_address"),
                    "decimals": int(row.get("decimals", 18) or 18),
                    "enabled": 1 if str(row.get("enabled", "1")) in {"1", "true", "True"} else 0,
                    "extra_json": row.get("extra_json", "{}"),
                    "created_at": int(time.time()),
                }
                sqlite_manager.upsert_token(payload)
                imported += 1
            st.success(f"已导入 {imported} 条记录")
            st.rerun()  # 修复：使用 st.rerun()
        except Exception as exc:  # pragma: no cover - Streamlit runtime
            st.error(f"导入失败: {exc}")


def main() -> None:
    st.set_page_config(page_title="Alert Service", layout="wide")
    page = st.sidebar.selectbox(
        "功能模块",
        (
            "仪表盘",
            "价格提醒",
            "放量/趋势配置",
            "通知设置",
            "代币注册表",
        ),
    )
    if st.sidebar.button("刷新配置"):
        st.rerun()  # 修复：使用 st.rerun()

    if page == "仪表盘":
        _dashboard()
    elif page == "价格提醒":
        _price_alerts()
    elif page == "放量/趋势配置":
        _volume_trend_config()
    elif page == "通知设置":
        _notification_settings()
    else:
        _token_registry()


if __name__ == "__main__":
    main()