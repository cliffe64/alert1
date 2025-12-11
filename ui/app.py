"""Streamlit user interface for the alerting system."""

from __future__ import annotations

# --- 路径修复 ---
import sys
import os
# 将项目根目录添加到 sys.path，解决模块导入问题
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# ----------------

import asyncio
import csv
import io
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

import streamlit as st

from alerts import dingtalk, local_sound
from connectors.binance_provider import BinanceFuturesProvider
from connectors.onchain_provider import OnChainProvider
from core.config_models import EndpointEntry, MonitoredTarget, ThresholdRule
from core.health import Endpoint, EndpointPool
from core.health_checker import probe_endpoints
from core.providers import EndpointConfig, TokenDescriptor
from storage import sqlite_manager
from storage import app_config_store


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _dashboard() -> None:
    st.header("行情仪表盘")
    config = st.session_state.get("config") or app_config_store.load_app_config()
    cols = st.columns(2)
    with cols[0]:
        st.subheader("最新价 / 涨跌")
        rows: List[Dict[str, object]] = []
        for target in config.targets:
            symbol = target.token.symbol
            bar = sqlite_manager.fetch_latest_bar("bars_1m", symbol)
            if bar:
                rows.append(
                    {
                        "symbol": symbol,
                        "close": bar["close"],
                        "close_ts": _format_ts(int(bar["close_ts"])),
                    }
                )
        st.dataframe(rows or [{"提示": "请先在监控配置中添加合约/代币"}])
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


def _log_panel(latest_health: Optional[List[Dict[str, object]]] = None) -> None:
    st.subheader("运行日志与状态")
    events = sqlite_manager.fetch_undelivered_events(limit=50)
    st.write("价格/系统事件 (最新 50 条)：")
    st.dataframe(
        [
            {
                "id": e["id"],
                "type": e.get("event_type", "price"),
                "symbol": e.get("symbol"),
                "severity": e.get("severity"),
                "message": e.get("message"),
                "ts": _format_ts(int(e["ts"])),
            }
            for e in events
        ]
    )

    if latest_health:
        st.write("Endpoint 健康检查结果：")
        st.dataframe(latest_health)


def _notifier_settings(config) -> None:
    st.header("告警通道开关与测试")
    st.caption("三个通道独立开关，点击按钮立即测试。Telegram 预留接口。")
    for notifier in config.notifiers:
        col1, col2, col3 = st.columns([1, 2, 2])
        with col1:
            enabled = st.checkbox(f"{notifier.name} 启用", value=notifier.enabled, key=f"{notifier.name}_enabled")
        with col2:
            st.write("可测试" if notifier.testable else "预留")
        with col3:
            if notifier.testable and st.button(f"测试 {notifier.name}", key=f"btn_{notifier.name}"):
                _run_notifier_test(notifier.name)
        if enabled != notifier.enabled:
            updated = app_config_store.update_notifier(config, notifier.name, enabled)
            st.success("已更新通知开关")
            st.session_state["config"] = updated


def _run_notifier_test(channel: str) -> None:
    try:
        if channel == "dingtalk":
            webhook = os.environ.get("DINGTALK_WEBHOOK")
            secret = os.environ.get("DINGTALK_SECRET")
            if not webhook:
                st.error("未配置 DINGTALK_WEBHOOK")
                return
            title = "[TEST] 系统告警通道自检"
            text = "# 通知自检\n- 频道: 钉钉\n- 结果: 成功触发"
            asyncio.run(dingtalk.send_markdown(title, text, webhook, secret))
            st.success("钉钉消息已发送")
        elif channel == "local_sound":
            local_sound.play(None)
            st.success("已触发本地声音")
        else:
            st.info("Telegram 通道预留，待补充凭据后启用。")
    except Exception as exc:  # pragma: no cover - runtime feedback
        st.error(f"测试失败: {exc}")


def _endpoint_pool_panel(config) -> Optional[List[Dict[str, object]]]:
    st.header("Endpoint 池配置与健康检查")
    st.caption("前端可新增/编辑/删除/排序接口，运行时自动切换。")
    st.dataframe([asdict(ep) for ep in config.endpoints])
    with st.form("endpoint_form"):
        name = st.text_input("名称", value="")
        base_url = st.text_input("Base URL", value="https://fapi.binance.com")
        api_key = st.text_input("API Key", value="", type="password")
        priority = st.number_input("优先级(数字越小优先)", value=0, step=1)
        submitted = st.form_submit_button("保存/更新")
        if submitted and name and base_url:
            entry = EndpointEntry(name=name, base_url=base_url, api_key=api_key or None, priority=int(priority))
            updated = app_config_store.upsert_endpoint(config, entry)
            st.success("已保存 Endpoint")
            st.session_state["config"] = updated
            st.rerun()

    delete_name = st.selectbox("删除 endpoint", options=["<选择>"] + [ep.name for ep in config.endpoints])
    if delete_name != "<选择>" and st.button("删除所选"):
        updated = app_config_store.delete_endpoint(config, delete_name)
        st.success("已删除")
        st.session_state["config"] = updated
        st.rerun()

    if st.button("立即健康检查"):
        pool = EndpointPool(
            Endpoint(name=ep.name, base_url=ep.base_url, api_key=ep.api_key, priority=ep.priority)
            for ep in app_config_store.load_app_config().endpoints
        )
        results = asyncio.run(probe_endpoints(pool))
        latest_health = [
            {
                "name": r.endpoint.name,
                "base_url": r.endpoint.base_url,
                "ok": r.ok,
                "reason": r.reason,
                "latency_ms": r.latency_ms,
                "failures": r.endpoint.consecutive_failures,
            }
            for r in results
        ]
        st.session_state["latest_health"] = latest_health
        st.success("健康检查完成")
        return latest_health
    return st.session_state.get("latest_health")


def _target_rules_panel(config) -> None:
    st.header("监控对象与规则")
    provider = BinanceFuturesProvider()
    onchain_provider = OnChainProvider()
    provider.configure_endpoints(
        EndpointConfig(name=ep.name, base_url=ep.base_url, api_key=ep.api_key, priority=ep.priority)
        for ep in config.endpoints
    )
    onchain_provider.configure_endpoints(
        EndpointConfig(name=ep.name, base_url=ep.base_url, api_key=ep.api_key, priority=ep.priority)
        for ep in config.endpoints
    )

    st.subheader("通过名称搜索币安合约")
    query = st.text_input("输入名称/符号搜索", key="futures_query")
    if query:
        try:
            matches = provider.search_tokens(query)
            if matches:
                options = {f"{m.symbol} | {m.name}": m for m in matches}
                choice = st.selectbox("搜索结果", list(options.keys()))
                if st.button("添加为监控目标"):
                    token = options[choice]
                    target = MonitoredTarget(token=token, rules=[], enabled=True)
                    updated = app_config_store.upsert_target(config, target)
                    st.success("已添加监控对象")
                    st.session_state["config"] = updated
                    st.rerun()
            else:
                st.info("未找到匹配合约，可尝试输入合约地址兜底。")
        except Exception as exc:
            st.error(f"搜索失败: {exc}")

    st.subheader("通过名称搜索链上代币")
    onchain_query = st.text_input("输入名称/符号搜索链上代币", key="onchain_query")
    if onchain_query:
        try:
            matches = onchain_provider.search_tokens(onchain_query)
            if matches:
                options = {f"{m.symbol} | {m.name}": m for m in matches}
                choice = st.selectbox("链上搜索结果", list(options.keys()), key="onchain_choice")
                if st.button("添加链上监控目标"):
                    token = options[choice]
                    target = MonitoredTarget(token=token, rules=[], enabled=True)
                    updated = app_config_store.upsert_target(config, target)
                    st.success("已添加链上监控对象")
                    st.session_state["config"] = updated
                    st.rerun()
            else:
                st.info("未找到匹配代币，可尝试输入合约地址兜底。")
        except Exception as exc:
            st.error(f"搜索失败: {exc}")

    st.subheader("地址兜底添加链上代币")
    addr = st.text_input("合约地址", key="address_add")
    chain = st.text_input("链(可选)", key="address_chain")
    name = st.text_input("名称(可选)", key="address_name")
    if st.button("验证地址"):
        if not addr:
            st.warning("请输入要验证的合约地址")
        else:
            token = onchain_provider.resolve_token(addr)
            if token:
                st.success("地址格式有效，可添加监控")
                st.json(token.__dict__)
            else:
                st.error("无法解析该地址")
    if st.button("地址添加"):
        token = TokenDescriptor(
            identifier=addr,
            name=name or addr,
            symbol=(name or addr)[:10],
            chain=chain or None,
            address=addr,
        )
        target = MonitoredTarget(token=token, rules=[], enabled=True)
        updated = app_config_store.upsert_target(config, target)
        st.success("已通过地址添加")
        st.session_state["config"] = updated
        st.rerun()

    st.subheader("已配置监控对象")
    if not config.targets:
        st.info("暂无监控对象")
    for target in config.targets:
        with st.expander(f"{target.token.symbol} / {target.token.name}"):
            st.json(target.token.__dict__)
            st.write("规则列表")
            if target.rules:
                st.table([rule.__dict__ for rule in target.rules])
            with st.form(f"rule_form_{target.token.identifier}"):
                rule_id = st.text_input("规则 ID", key=f"rule_id_{target.token.identifier}")
                compare = st.selectbox("比较方式", ["gt", "lt", "cross_up", "cross_down"], key=f"cmp_{target.token.identifier}")
                threshold = st.number_input("阈值", value=0.0, key=f"thr_{target.token.identifier}")
                freq = st.number_input("触发频率(秒)", value=60, step=1, key=f"freq_{target.token.identifier}")
                cooldown = st.number_input("冷却时间(秒)", value=300, step=1, key=f"cool_{target.token.identifier}")
                submitted = st.form_submit_button("追加规则")
                if submitted and rule_id:
                    new_rule = ThresholdRule(
                        rule_id=rule_id,
                        compare=compare,
                        threshold=float(threshold),
                        frequency_sec=int(freq),
                        cooldown_sec=int(cooldown),
                    )
                    updated_rules = target.rules + [new_rule]
                    updated_target = MonitoredTarget(token=target.token, rules=updated_rules, enabled=target.enabled)
                    updated = app_config_store.upsert_target(config, updated_target)
                    st.success("规则已添加")
                    st.session_state["config"] = updated
                    st.rerun()
            if st.button("删除该监控对象", key=f"del_{target.token.identifier}"):
                updated = app_config_store.delete_target(config, target.token.identifier)
                st.success("已删除监控对象")
                st.session_state["config"] = updated
                st.rerun()


def main() -> None:
    st.set_page_config(page_title="Alert Service", layout="wide")
    if "config" not in st.session_state:
        st.session_state["config"] = app_config_store.load_app_config()
    config = st.session_state["config"]

    page = st.sidebar.selectbox(
        "功能模块",
        (
            "仪表盘",
            "监控配置",
            "Endpoint 池",
            "告警通道",
            "日志",
        ),
    )
    if st.sidebar.button("刷新配置"):
        st.session_state["config"] = app_config_store.load_app_config()
        st.rerun()

    if page == "仪表盘":
        _dashboard()
    elif page == "监控配置":
        _target_rules_panel(config)
    elif page == "Endpoint 池":
        health = _endpoint_pool_panel(config)
        _log_panel(latest_health=health)
    elif page == "告警通道":
        _notifier_settings(config)
    else:
        _log_panel(latest_health=st.session_state.get("latest_health"))


if __name__ == "__main__":
    main()