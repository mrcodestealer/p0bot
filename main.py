#!/usr/bin/env python3
"""
grafanagamebot — Lark + Grafana「Online Number」主面板（``GRAFANA_PANEL_TITLE``）、默认端口 **5088**。

- **配置**：文件顶部 ``_CFG``；可用环境变量覆盖同名键。
- **HTTP**：``LARK_EVENT_MODE=http`` 时 ``POST /webhook/event``；可选 ``ws`` 长连接（见 ``LARK_EVENT_MODE``）。
- **命令**：``MONITORING_TRIGGER_REQUIRES_AT_BOT=1`` 时 **群聊**须 @ **本**机器人再发 ``/mo``；**私聊 p2p** 可直接发 ``/mo``（无 @）。**``/m``、``/c`` 与 ``/mo`` 共用同一套 @ 判定**（群内裸发 ``/m`` 不会触发）。与 **Platform 同群**时 **Game 与 Platform 均须** ``MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW=0``（默认），并在 ``MONITORING_PEER_BOT_OPEN_IDS`` 填对方 ``open_id``；否则 explicit peer-only mentions + 正文 ``@_user_N`` 占位可能错误落到对方。**explicit meta peer-only** 且正文 **无** peer 的强 ``<at>`` 确认时 **直接 skip**，不再 fall through 到弱路径。``MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER`` 仍用于 **mentions 完全为空** 的兜底。**勿**再搞「单条 peer ``open_id`` + ``@_user_N`` 即强制触发本 bot」。mention 行带本应用 ``app_id`` 时走 ``row_app_id_is_self``。未配 ``LARK_BOT_OPEN_ID`` 时会尝试 ``GET bot/v3/info``。机器人回复为英文。

依赖：Playwright 截图见 ``GRAFANA_SCREENSHOT_ENABLE``；详见 ``_CFG`` 内注释。
默认 ``MONITORING_MESSAGE_CARD_ENABLE=1``：交互卡片；``MONITORING_CARD_EMBED_SCREENSHOT=1``（默认）时截图嵌卡片内 — **一条消息**；embed=``0`` 则卡片 + 单独图片两条。``MONITORING_MESSAGE_CARD_BUTTON_ENABLE=1`` 时有 **Resend screenshot**。若 ``MONITORING_MESSAGE_CARD_ENABLE=0`` 则为纯文字 + 独立图。
"""

import base64
import copy
import csv
import hashlib
import json
import logging
import math
import os
import queue
import subprocess
from urllib.parse import urlencode
from datetime import datetime
import re
import shlex
import shutil
import tempfile
import threading
import time
import warnings
import wave
from typing import Any, Dict, Generator, Iterator, List, Optional, Set, Tuple

import requests
from flask import Flask, Response, g, jsonify, request

# ---------------------------------------------------------------------------
# 单一配置：只改这里（也可用 systemd Environment= 覆盖同名变量，无需 .env）
# 勿将含真实密钥的 main.py 提交到公开仓库；泄露请到飞书/Grafana 后台轮换。
# ---------------------------------------------------------------------------
_CFG: Dict[str, Any] = {
    "PORT": 5088,
    "HTTP_SERVER": "flask",
    "LARK_EVENT_MODE": "http",
    "ENABLE_HTTP": "1",
    "WAITRESS_THREADS": 24,
    "LARK_HOST": "https://open.larksuite.com",
    "LARK_WEBHOOK_PUBLIC_URL": "http://127.0.0.1:5088/webhook/event",
    "GRAFANA_BASE_URL": "https://grafana.client8.me",
    "GRAFANA_DASHBOARD_PATH": "/d/fe70d4bd-4729-471f-9ede-e981ad277963/online-number",
    "GRAFANA_DASHBOARD_UID": "fe70d4bd-4729-471f-9ede-e981ad277963",
    # --- 判警涉及 5 个面板（标题须与 Grafana 完全一致）---
    "GRAFANA_PANEL_TITLE": "LiveSlots Online Number",
    "GRAFANA_PANEL_TITLE_EGAME_ONLINE": "Egame Online Number",
    "GRAFANA_PANEL_TITLE_EGAMES_BET": "Egames 下注Bet/min",
    # 与浏览器面板标题一致（HTML: Liveslot 下注Bet/min）；API 模型里可能仍是 Liveslots-Spin-Bet，见 _find_panel 别名
    "GRAFANA_PANEL_TITLE_LIVESLOT_BET": "Liveslot 下注Bet/min",
    "GRAFANA_PANEL_TITLE_LIVESLOT_BET_ALIASES": "Liveslot 下注Bet/min Liveslots-Spin-Bet",
    # Liveslots-Spin-Bet 面板：仅监控 spin_count，值为 0 持续超过 2 分钟告警
    "GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET": "Liveslots-Spin-Bet",
    "MONITORING_LIVESLOT_SPIN_COUNT_ENABLE": "1",
    "MONITORING_LIVESLOT_SPIN_COUNT_SERIES_INCLUDE": "spin_count",
    "MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS": "120",
    "MONITORING_EGAME_ONLINE_SERIES_KEYWORD": "",
    "MONITORING_EGAMES_BET_SERIES_KEYWORD": "",
    # 逗号/空格分隔；仅分析图例名包含下列子串的序列（空=该面板全部序列）
    "MONITORING_EGAMES_BET_SERIES_INCLUDE": "EcallTW,Sinonet",
    "GRAFANA_DASHBOARD_FROM": "now-1h",
    "GRAFANA_DASHBOARD_TO": "now",
    "GRAFANA_QUERY_STEP": 60,
    "GRAFANA_QUERY_LOOKBACK_SECONDS": 900,
    # Prometheus 最近分钟桶常未跑完；query_range 的 end 用「现在 − 该秒数」，最新点落在「约前两分钟」
    "GRAFANA_QUERY_END_LAG_SECONDS": 60,
    # 二者均 >0 且 START>END 时：不用 LOOKBACK+LAG，改用对齐窗口（见 MONITORING_TIME_BUCKET_TZ）
    # start = 当前日历分钟起点 − START 分钟，end = 当前日历分钟起点 − END 分钟（均为 …:00）。
    # 例 NOW=5:35:23 → cur_min=5:35:00，START=6 END=1 → start=5:29:00 end=5:34:00（最后一桶 5:34:00 非 5:34:23）
    # 设 START=0 或 END=0 则退回 GRAFANA_QUERY_LOOKBACK_SECONDS + GRAFANA_QUERY_END_LAG_SECONDS
    "MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES": "6",
    "MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES": "1",
    # 合并后再丢掉尾部 N 个「分钟桶」；0=不丢。判窗已用 EVAL_END_OFFSET 排除当前不完整分钟，勿与 DROP 叠加以免多等一整分钟。
    "MONITORING_DROP_LAST_MERGED_MINUTES": "0",
    # /mo 与告警正文里的 time/value 表只展示「最新 N 行」（DROP/SPIKE 仍基于窗口内完整 merged 序列）
    "MONITORING_TABLE_TAIL_ROWS": "5",
    # 1=KEYWORD 为空时每条 Grafana 序列单独算 DROP/SPIKE（多序列同框）；0=合并为一条 merged 序列
    "MONITORING_PER_SERIES_ANALYSIS": "1",
    # 无头截图（Playwright）：0=关；1=文字后发 PNG（需 ``pip install playwright`` + ``playwright install chromium``）
    "GRAFANA_SCREENSHOT_ENABLE": "1",
    "GRAFANA_SCREENSHOT_WIDTH": 1400,
    "GRAFANA_SCREENSHOT_HEIGHT": 1080,
    "GRAFANA_SCREENSHOT_TIMEOUT_MS": 90000,
    "GRAFANA_SCREENSHOT_FULL_PAGE": "1",
    # 截图前点 Grafana「Dock menu」收起左侧导航（Grafana 12 mega-menu）；0=跳过
    "GRAFANA_SCREENSHOT_DOCK_NAV": "1",
    # 空=与浏览器一致（保留顶栏、时间选择器、Refresh 1m）；tv=全屏 kiosk（与网站截图外观不同）
    "GRAFANA_SCREENSHOT_KIOSK": "",
    # 写入截图 URL 的 refresh=…（与 Grafana 右上角一致）；空=不写
    "GRAFANA_SCREENSHOT_URL_REFRESH": "1m",
    # 截图前先打开站点根路径再进 dashboard，利于 session 与 SPA bootstrap
    "GRAFANA_SCREENSHOT_BOOT_WARM": "1",
    # 0=不点 Refresh（page.goto(dashboard) 已带时间范围，避免找不到按钮时 page.reload 二次全页加载）
    "GRAFANA_SCREENSHOT_REFRESH": "0",
    # Refresh 后等 Spinner；0=不等待（配合 REFRESH=0）；有 Refresh 时可酌情调大
    "GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS": "0",
    # 1=点击折叠的 dashboard 行（如只显示 KPI 标题无图时）
    "GRAFANA_SCREENSHOT_EXPAND_ROWS": "1",
    # 1=告警截图前在对应面板 Ctrl+点击图例，只保留触发告警的序列
    "GRAFANA_SCREENSHOT_ALERT_LEGEND_CLICK_ENABLE": "1",
    "GRAFANA_SCREENSHOT_RELATIVE_RANGE": "1",
    # 截图 URL 追加 timezone=…（与 Grafana 时间栏一致）；设为 none / - 可省略该参数
    "GRAFANA_SCREENSHOT_TIMEZONE": "browser",
    # 数字面板分钟聚合 + 告警时间 ``mm-dd HH:MM`` 使用的 IANA 时区（如 Asia/Shanghai）。
    # 与 Grafana 面板时区不一致且 bot 跑在 UTC 上时，不设会导致同一分钟的值错桶（假 SPIKE）。
    # 空 / local / server = 使用进程本地时区。
    "MONITORING_TIME_BUCKET_TZ": "",
    # 1=进程内常驻 Playwright Chromium（启动时预热 Grafana；/monitoring 与告警截图复用，不必每次冷启动）
    "GRAFANA_PERSISTENT_BROWSER": "1",
    # 常驻浏览器空闲 Refresh 间隔（秒）；REFRESH=0 时该调用为 no-op，仅控制队列轮询节奏
    "GRAFANA_PERSISTENT_BROWSER_IDLE_REFRESH_SECONDS": "45",
    # 单次截图任务在 keeper 队列中的最长等待（秒）
    "GRAFANA_PERSISTENT_BROWSER_JOB_TIMEOUT_SECONDS": "180",
    # 常驻浏览器每次截图前：不清空全部 cookie，只追加/覆盖新登录（减轻 SPA 主区闪空白）
    "GRAFANA_PERSISTENT_BROWSER_SOFT_COOKIE": "1",
    # 按快门前等待毫秒数；面板已 ready 后可设 0 省固定延迟（空白时再调回 200–350）
    "GRAFANA_SCREENSHOT_PRE_CAPTURE_MS": "0",
    # 1=快门前再跑一轮整页滚动刷 canvas（更慢但更稳）
    "GRAFANA_SCREENSHOT_PRE_CAPTURE_RESCROLL": "0",
    "GRAFANA_SCREENSHOT_POPULATE_MAX_MS": 12000,
    # 整页截图稳定：默认 1 轮即可；仍无法保证 Prometheus「No data」有曲线
    "GRAFANA_SCREENSHOT_STABILIZE_ROUNDS": 1,
    "GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS": 70,
    "GRAFANA_SCREENSHOT_SETTLE_MS": 0,
    "GRAFANA_SCREENSHOT_SPINNER_MAX_MS": 10000,
    # 至少等到 N 个 .react-grid-item（0=不等待；经典大屏可设 4–8；Scenes 布局可能为 0）
    "GRAFANA_SCREENSHOT_MIN_GRID_ITEMS": 0,
    # 截图前“全面板加载”门槛：已加载面板占比（含图或明确 No data）
    "GRAFANA_SCREENSHOT_PANEL_READY_RATIO": 0.92,
    # 截图前“全面板加载”最少面板数（防小屏/过滤时占比误判）
    "GRAFANA_SCREENSHOT_PANEL_READY_MIN": 7,
    # 全面板加载等待预算（毫秒）；仅当能数到面板头时跑满；Scenes 见下一项
    "GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS": 20000,
    # Scenes 布局 panel roots=0 时额外等待上限（毫秒）
    "GRAFANA_SCREENSHOT_PANEL_READY_ZERO_TOTAL_MAX_MS": 400,
    # Set via environment (systemd Environment=) — do not commit real secrets.
    "GRAFANA_USER": "om_duty",
    "GRAFANA_PASSWORD": "5tgb%TGB094",
    "VERIFICATION_TOKEN": "nzdtU1ZFrMJHz2V6kZeFsrEFa7vs0H3C",
    "APP_ID": "cli_a97490f2bcf89ed2",
    "APP_SECRET": "Uo1YHHiWfDo7MOOYVIUFRgJvxf1VyJFE",
    "MONITORING_TRIGGER": "/mo",
    "MONITORING_MUTE_TRIGGER": "/m",
    "MONITORING_CANCELMUTE_TRIGGER": "/c",
    # 1=仅 @ 机器人且无其它正文也触发（与 MONITORING_TRIGGER 默认 /mo 同）；1+ANY=1 时 @ 且任意正文也跑监控（非命令且带字会先收到命令说明）
    "MONITORING_AT_MENTION_ENABLE": "0",
    "MONITORING_AT_MENTION_ANY_TEXT": "0",
    # 1=发 MONITORING_TRIGGER（如 /mo）时必须 @ 本机器人（LARK_BOT_OPEN_ID）；0=群内任意出现 /mo 即触发（旧行为）
    "MONITORING_TRIGGER_REQUIRES_AT_BOT": "1",
    # 1=仅当 mentions **完全为空** 且正文含 @_user_N 时兜底触发 /mo（多 bot 同群且 mentions 含别 bot 时不会误触发）；0=禁用该兜底
    "MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER": "1",
    # 0=禁用「非空弱 mentions + 正文 @_user_N」/mo（与 Platform 同群时必须 **0**，否则会 @ Platform 仍落到 Game）
    "MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW": "0",
    # 1=日志打印 primary @ / mentions / explicit_ids 解析（排查 @ 错 Platform）
    "MONITORING_LOG_PRIMARY_AT": "1",
    # 已移除：/mo 被 @ 门控拒绝时不再给用户发「/mo skipped: ...」说明（代码里已硬关，此项无效）
    "MONITORING_AT_GATE_USER_FEEDBACK": "0",
    # 1=sole mention 的 open_id 落在 peer，但正文仅有 @_user_N、无 <at user_id>，且 mention.name 命中 SUBSTRINGS 时把 primary 纠正为本 bot（默认关；子串要独特）
    "MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_FALLBACK": "0",
    "MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_SUBSTRINGS": "",
    # 本仓库 = Grafana **Game** Bot：解析到明确 ou_/cli_ @ 目标时须与本 bot 的 **任一** canonical id 相交才跑 /mo
    "MONITORING_CANONICAL_BOT_OPEN_ID": "ou_1830c6697311e779471888a420233eed",
    # 同一 Game 应用在飞书里可能出现的其它 open_id（HTTP mentions 仍可能带旧 ou_）
    "MONITORING_CANONICAL_BOT_OPEN_IDS": "ou_848fc4640b48b9845cbc5b0cfa2f1af1 ou_ee1af664e18d9c2d25e0ab6fded66388",
    # Platform 机器人可能出现的全部 ou_（含历史 alternate）；须列全以便 primary / peer-only 判定
    "MONITORING_PEER_BOT_OPEN_IDS": "ou_0bfd185231d6beb669425fdf8f13e9df ou_a51dad55e46f665d740b85c5ae22f940 ou_04878d0cdae2ca774e1d4a1716fa9ac3",
    "LARK_ENCRYPT_KEY": "",
    "LARK_BOT_OPEN_ID": "ou_1830c6697311e779471888a420233eed",
    "LARK_WS_LOG_LEVEL": "INFO",
    "LARK_WS_USE_HTTP_KEYS": "0",
    "LARK_WS_EXTRA_IM_TYPES": "",
    # 1=同时订阅 im.message.receive_v2（易与 v1 对同一条消息各投递一次 → 两条回复）；默认 0
    "LARK_WS_REGISTER_IM_MESSAGE_V2": "0",
    # 同一 chat+发送者+触发正文在 N 秒内只跑一次监控任务；0=关闭（默认 5）
    "MONITORING_IM_DEBOUNCE_SECONDS": "5",
    # 同一会话在 N 秒内仅接受一次 monitoring 触发（在启动后台线程前兜底，拦同秒双 envelope）
    "MONITORING_CHAT_TRIGGER_DEBOUNCE_SECONDS": "0",
    # 同一触发在 N 秒内只允许 **一次** 真正发到飞书（拦双 POST / 双进程竞态）；0=关闭（默认 12）
    "MONITORING_SEND_COALESCE_SECONDS": "12",
    # 同一会话(chat_id/open_id)在 N 秒内只允许一次用户可见发送（兜底拦截同秒双 envelope）；0=关闭
    "MONITORING_CHAT_COALESCE_SECONDS": "0",
    # 0=ws+HTTP 并存时也处理 webhook 上的 im.message（推荐；靠 message_id 去重）。1=ws 收到 DATA 后丢弃 HTTP IM（易导致 /mo 无回复）
    "LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS": "0",
    # 1=当配置 ws 模式但尚未收到任何 WS DATA 帧时，允许 HTTP IM 回退处理（避免 200 但无回复）
    "LARK_HTTP_IM_FALLBACK_WHEN_WS_NO_DATA": "1",
    # ws 模式：若超过该秒数未在 WS 上收到 im.message，则仍处理 HTTP webhook 上的 IM（避免有 DATA 帧但无 IM → /mo 被吞）
    "LARK_HTTP_IM_WS_FALLBACK_GRACE_SECONDS": "120",
    # 1=监控摘要一条交互卡片（须保持 1 才有下方 MONITORING_MESSAGE_CARD_BUTTON_*「Resend screenshot」）
    "MONITORING_MESSAGE_CARD_ENABLE": "1",
    # /mo 路径：**先**发卡/字再截图（避免 Playwright 卡住导致长时间无回复）；截图单独一条。Watchdog 告警仍可先截再发。
    "MONITORING_CARD_EMBED_SCREENSHOT": "1",
    # 1=在监控卡片底部展示 callback 按钮（实现方式参考 Chatbox/jenkinsupdate 的 card JSON 2.0）
    "MONITORING_MESSAGE_CARD_BUTTON_ENABLE": "1",
    "MONITORING_MESSAGE_CARD_BUTTON_TEXT": "Resend screenshot",
    # 飞书交互卡片正文上限；超出时先发卡片再自动拆成多条文字消息补全（避免只看到 ~10 条序列）
    "MONITORING_MESSAGE_CARD_REPLY_MAX_CHARS": "28000",
    "MONITORING_MESSAGE_CARD_TRUNCATE": "1",
    # 续传文字消息单段上限（总长仍会再按飞书限制二次切块）
    "MONITORING_MESSAGE_OVERFLOW_TEXT_CHUNK_CHARS": "12000",
    "LARK_WS_TRANSPORT_LOG": "1",
    "LARK_WS_BOOTSTRAP_FRAMES": 16,
    "LARK_WS_LOG_FRAME_METHOD": "0",
    "LARK_WS_SDK_DEBUG": "0",
    "LARK_WEBHOOK_WSGI_LOG": "0",
    "LARK_WEBHOOK_TIMING_LOG": "0",
    # Fast = 约 3 分钟内最大跌/涨（各面板可单独设 drop/spike %）；连续单调段默认关闭（inf）。
    "MONITORING_ALERT_WINDOW_SECONDS": 180,
    "MONITORING_EGAME_FAST_DROP_ALERT_PCT": 25,
    "MONITORING_EGAME_FAST_SPIKE_ALERT_PCT": 25,
    "MONITORING_EGAMES_BET_FAST_DROP_ALERT_PCT": 25,
    "MONITORING_EGAMES_BET_FAST_SPIKE_ALERT_PCT": 25,
    # 0=临时关闭 Liveslot 下注监控与告警（/mo 不拉该面板）；恢复时设 1 或 MONITORING_LIVESLOT_BET_ENABLE=1
    "MONITORING_LIVESLOT_BET_ENABLE": "0",
    # Liveslot 下注 / Liveslots-Spin-Bet：仅窗口内急跌（fast drop）≥50%；不告警上涨 spike、不告警 continuous
    "MONITORING_LIVESLOT_BET_FAST_DROP_ALERT_PCT": 50,
    "MONITORING_LIVESLOT_BET_FAST_SPIKE_ALERT_PCT": "inf",
    # 只分析 Grafana 图例「total spins across all machines」；勿对全面板逐序列判警（易误报）
    "MONITORING_LIVESLOT_BET_SERIES_INCLUDE": "total spins",
    # 关闭 baseline 滤低点：滤掉中间低桶会把曲线接成假急跌
    "MONITORING_LIVESLOT_BET_BASELINE_FILTER": "0",
    # 急跌两端须 ≥ median×ratio 且绝对跌幅 ≥ MIN_ABS_DROP，否则视为误报
    "MONITORING_LIVESLOT_BET_DROP_ENDPOINT_MIN_MEDIAN_RATIO": "0.35",
    "MONITORING_LIVESLOT_BET_MIN_ABS_DROP": "500",
    "MONITORING_LIVESLOTS_FAST_DROP_ALERT_PCT": 25,
    "MONITORING_LIVESLOTS_FAST_SPIKE_ALERT_PCT": 50,
    "MONITORING_GAME_ALERT_CONTINUOUS_PCT": 30,
    # 1=alert text skips Fast/Continuous SPIKE/DROP lines; only a short time/value tail (Grafana-like).
    "MONITORING_SIMPLE_ALERT_TEXT": "0",
    # 1=/mo hides extra-panel ``within Xm drop/spike`` footer lines; tables only.
    "MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS": "0",
    "MONITORING_WATCH_ENABLE": "1",
    "MONITORING_WATCH_INTERVAL_SECONDS": "20",
    # 自动告警最短间隔（防刷屏）；默认 300=5 分钟
    "MONITORING_WATCH_ALERT_COOLDOWN_SECONDS": "300",
    # Watchdog 专用：合并后再丢尾部 N 分钟桶（默认 0；/mo 仍用 MONITORING_DROP_LAST_MERGED_MINUTES）
    "MONITORING_WATCH_DROP_LAST_MERGED_MINUTES": "0",
    # 对齐判窗时：最后一桶（floor(now)−END_OFFSET 分钟）距现在至少该秒数才跑分析，替代「多丢一桶」等 Prometheus scrape
    "MONITORING_WATCH_MIN_LAST_BUCKET_AGE_SECONDS": "90",
    # Watchdog：Prometheus 判窗对齐到整分钟，相对「当前分钟起点」向回偏移（分钟）。例：7 与 2 → 12:46:xx 只评 12:39:00..12:44:00（5 分钟窗，末桶更旧以减少 Pushgateway 迟到回填假跌）
    "MONITORING_WATCH_EVAL_START_OFFSET_MINUTES": "7",
    "MONITORING_WATCH_EVAL_END_OFFSET_MINUTES": "2",
    # 首次越阈后冻结判窗、等待该秒数再拉同一窗口复核；0=立即告警（关闭确认）。缓解「当分钟 sum 随后被 Prometheus 回填修正」导致的误报
    "MONITORING_WATCH_CONFIRM_SECONDS": "60",
    # Watchdog 判警是否使用与 /monitoring 相同的拉数窗口（默认 0：窄窗口 eval；设为 1 则与报表一致，避免「报表有大波动但自动告警未扫到」）
    "MONITORING_WATCH_MATCH_REPORT_WINDOW": "0",
    # Watchdog 告警附带截图的 Grafana URL（相对时间）；与判窗数据窗口无关，默认最近 1 小时整页
    "MONITORING_WATCH_SCREENSHOT_FROM": "now-1h",
    "MONITORING_WATCH_SCREENSHOT_TO": "now",
    "MONITORING_WATCH_SCREENSHOT_TIMEZONE": "browser",
    # 每日静默：该时段内不拉数、不判警（``MONITORING_TIME_BUCKET_TZ`` 或服务器本地时间）
    # 窗口 1：19:59～20:15；窗口 2：00:00～00:15
    "MONITORING_WATCH_QUIET_WINDOW_ENABLE": "1",
    "MONITORING_WATCH_QUIET_START_HOUR": "19",
    "MONITORING_WATCH_QUIET_START_MINUTE": "59",
    "MONITORING_WATCH_QUIET_END_HOUR": "20",
    "MONITORING_WATCH_QUIET_END_MINUTE": "15",
    "MONITORING_WATCH_QUIET2_ENABLE": "1",
    "MONITORING_WATCH_QUIET2_START_HOUR": "0",
    "MONITORING_WATCH_QUIET2_START_MINUTE": "0",
    "MONITORING_WATCH_QUIET2_END_HOUR": "0",
    "MONITORING_WATCH_QUIET2_END_MINUTE": "15",
    # Tag person / alert group — set via environment.
    "TARGET_USER_OPEN_ID": "",
    # 告警 / 超阈值 /mo 文末仅 @ 此人时追加的说明（空=只 @ 不追加句子）
    "MONITORING_ALERT_AT_USER_NOTE": "It might be event started or false alert kindly check",
    # Person tagging in alerts is DISABLED by default. Set =1 to restore @TARGET_USER_OPEN_ID.
    "MONITORING_ALERT_AT_USER_ENABLE": "0",
    # ---- AI second-review gate (local Ollama vision model / Qwen) ----
    # After a threshold fires (first review), the (series-isolated) alert screenshot is sent to a
    # local Ollama model for a SECOND review; the alert is posted to the group only when the model
    # judges it ABNORMAL, and the AI explanation is appended to the message body.
    "MONITORING_AI_GATE_ENABLE": "1",
    "MONITORING_AI_OLLAMA_URL": "http://localhost:11434",
    "MONITORING_AI_MODEL": "qwen3.6:35b-a3b",
    "MONITORING_AI_TIMEOUT_SECONDS": "120",
    # AI unreachable / undecided: 1=send anyway (no missed alerts), 0=suppress
    "MONITORING_AI_GATE_FAIL_OPEN": "1",
    # fail-open 时在正文末尾追加的说明（空=不追加）
    "MONITORING_AI_FAIL_OPEN_NOTE": "🤖 AI review unavailable — alert sent without AI explanation.",
    # 自定义判定提示词（留空用内置默认；可用 {alert} 占位符插入告警正文）
    "MONITORING_AI_PROMPT": "",
    "JUNCHEN": "",
    "MONITORING_ALERT_CHAT_ID": "oc_51b6fbf2636525acfb4ead3afa3c93ce",
    # @ bot + "git pull … and restart …" (or "/deploy") — git pull origin main then
    # systemctl restart, for authorized senders only.
    "DEPLOY_ENABLE": "1",
    # DELIBERATELY EMPTY on this fork. An open_id is scoped to the APP that saw it, so the
    # grafanagamebot-namespace id this file used to ship could never match under p0bot's
    # credentials — deploy just answered "not authorized" forever. Empty means fail-closed
    # (nobody can deploy), and the denial reply prints the sender's own p0bot-namespace id so
    # it can be pasted straight in here. `/whoami` prints the same id on demand.
    "DEPLOY_ALLOWED_USER_OPEN_ID": "",
    # Optional extra allow-list (space/comma separated open_ids)
    "DEPLOY_ALLOWED_USER_OPEN_IDS": "",
    # Empty = directory containing this main.py; override on server if repo lives elsewhere
    "DEPLOY_GIT_REPO_PATH": "",
    "DEPLOY_SYSTEMD_SERVICE": "p0bot",
    "DEPLOY_TRIGGER": "/deploy",
    # ---- p0bot: Lark wiki doc Q&A via local Ollama / Qwen ----
    # Read a Lark wiki page (and, optionally, its whole subtree), cache the plain
    # text, and answer questions about it. Nothing is fine-tuned — "learning the
    # doc" = giving the model that text as grounding context on every question.
    "P0_DOC_QA_ENABLE": "0",
    # Wiki node token = the segment after /wiki/ in the doc URL (…/wiki/<TOKEN>?…)
    "P0_WIKI_NODE_TOKEN": "",
    # Full wiki URL (optional; when set and P0_WIKI_NODE_TOKEN empty, token is parsed from it)
    "P0_WIKI_URL": "",
    # 1=also fetch every descendant page under the node ("read all"); 0=just this page
    "P0_WIKI_INCLUDE_CHILDREN": "1",
    "P0_WIKI_MAX_NODES": "200",
    # Command trigger for an explicit question, e.g. "/ask how do I …"
    "P0_ASK_TRIGGER": "/ask",
    # Command trigger to re-fetch the doc from Lark, e.g. "/reload"
    "P0_RELOAD_TRIGGER": "/reload",
    # 1=answer any direct (p2p) message without a command/@; 0=require /ask
    "P0_QA_ANSWER_DM": "1",
    # 1=in group chats, answer when the bot is @-mentioned (any text); 0=require /ask
    "P0_QA_AT_MENTION_ENABLE": "1",
    # Ollama endpoint + model for Q&A (fall back to MONITORING_AI_* when empty)
    "P0_QA_OLLAMA_URL": "",
    "P0_QA_MODEL": "",
    "P0_QA_TIMEOUT_SECONDS": "180",
    # Ollama context window; MUST be large enough to hold the doc + question
    # (Ollama defaults to 4096, which would silently truncate the documentation).
    "P0_QA_NUM_CTX": "32768",
    "P0_QA_TEMPERATURE": "0.2",
    # Max characters of doc context passed to the model (keyword-selected when the doc is larger).
    # Keep this comfortably below P0_QA_NUM_CTX in TOKENS — CJK text is ~1.3-1.7 chars/token, so
    # 24000 chars ≈ 15-18k tokens, leaving room for the question + answer inside a 32768 window.
    "P0_DOC_MAX_CHARS": "24000",
    # Re-fetch the doc every N seconds in the background; 0=only on startup + /reload
    "P0_DOC_REFRESH_SECONDS": "0",
    # ---- Local contact directory (always folded into the Q&A context, on top of the wiki) ----
    # A CSV of name,team,phone so p0bot can answer "who to contact for <team>" / "<name>'s number"
    # regardless of the wiki. Reloaded automatically when the file changes; no /reload needed.
    "P0_CONTACTS_ENABLE": "1",
    # Path to the CSV; empty = contacts.csv next to main.py (ships in the repo, deploys via git pull).
    "P0_CONTACTS_FILE": "",
    # After answering, if the answer names anyone in the directory, append their phone(s). 1=on.
    "P0_CONTACTS_APPEND_ENABLE": "1",
    # ---- /whotalk — who-said-what transcript of a recorded meeting (Lark Minutes ASR ----
    # for speaker names + raw text, then the local Qwen cleans zh/en errors and translates;
    # Lark's own translation is not used). Requires tenant Minutes enabled + minutes scopes.
    "P0_WHOTALK_ENABLE": "1",
    "P0_WHOTALK_TRIGGER": "/whotalk",
    # Extra/override instruction for the model (empty = built-in bilingual cleanup prompt).
    "P0_WHOTALK_PROMPT": "",
    # Max transcript characters per model pass; longer transcripts are processed in chunks.
    # A pass that times out is automatically split in half and retried before falling back to raw.
    "P0_WHOTALK_CHUNK_CHARS": "6000",
    # Timeout per /whotalk cleanup pass (regenerating text + translations is slower than Q&A).
    "P0_WHOTALK_QA_TIMEOUT_SECONDS": "900",
    # Lookback (hours) when resolving a bare meeting number to its meeting id via list_by_no.
    "P0_WHOTALK_LOOKBACK_HOURS": "72",
    # ---- "p0" keyword detection: ask "is this a P0?" via a card, confirm by replying ----
    # Real Yes/Cancel BUTTONS need a callback round-trip to the bot (HTTP webhook, which this
    # bot doesn't run — ENABLE_HTTP=0, WS-only, no public port — or a CARD frame over the long
    # connection, which the pinned lark-oapi client v1.7.1, the newest release available, silently
    # discards in its WS loop before any handler sees it). So confirmation is a plain text reply
    # over the already-proven IM pipeline: whole-word "p0" in a watched chat -> card asking to
    # reply P0_DETECT_CONFIRM_TRIGGER within the confirm window -> tags on-duty + auto-/openmeeting.
    "P0_DETECT_ENABLE": "1",
    # Comma/semicolon-separated chat_ids to watch; empty = P0_OPENMEETING_ANNOUNCE_CHAT_ID.
    "P0_DETECT_CHAT_IDS": "",
    # Don't re-show the card in the same chat within this many seconds of the last one.
    "P0_DETECT_COOLDOWN_SECONDS": "2700",
    # How long a shown card stays confirmable via P0_DETECT_CONFIRM_TRIGGER.
    "P0_DETECT_CONFIRM_WINDOW_SECONDS": "900",
    "P0_DETECT_CONFIRM_TRIGGER": "/confirmp0",
    "P0_DETECT_CARD_TEMPLATE": "red",
    # Real 【Confirm】/【Cancel】 BUTTONS on the P0 card. Card-click callbacks arrive as WS "card"
    # frames, which the pinned lark-oapi client discards (elif MessageType.CARD: return); we re-type
    # those frames to "event" so the SDK routes them to the registered card.action.trigger handler
    # and sends the toast/card-update reply back over the WS. REQUIRES the Developer Console
    # 「事件与回调 → 回调配置 → 使用长连接接收事件」 to be enabled. 0 = buttons off (text /confirmp0 only).
    "P0_CARD_BUTTONS_ENABLE": "1",
    # ---- /whotalk hybrid LOCAL ASR: the bot downloads the recording audio and transcribes it ----
    # itself (SenseVoiceSmall via sherpa-onnx) instead of using Lark's ASR text. Speaker names +
    # timestamps still come from the Minutes SRT export; only the heard TEXT is local. Setup:
    # deploy/setup-whotalk-asr.sh (installs ffmpeg + sherpa-onnx + the model), then set =1.
    # Extra scope needed: minutes:minutes.media:export (console + P0_VC_OAUTH_SCOPES + re-/vcauth).
    # Falls back to the Lark transcript automatically on any failure.
    "P0_WHOTALK_ASR_ENABLE": "0",
    # Engine: sensevoice (fast, ~1GB) or whisper (faster-whisper; slower, strong MY/SG code-switching).
    "P0_WHOTALK_ASR_ENGINE": "sensevoice",
    # Model dir holding model(.int8).onnx + tokens.txt; empty = <app>/models/sensevoice.
    "P0_WHOTALK_ASR_MODEL_DIR": "",
    "P0_WHOTALK_ASR_THREADS": "4",
    # Padding added around each speaker segment when slicing audio (ms) — clamped so segments
    # never overlap a neighboring turn (prevents one speaker's words leaking into another's line).
    "P0_WHOTALK_ASR_SEG_PAD_MS": "150",
    # Merge consecutive same-speaker subtitles only when the gap is under this (ms), and cap the
    # merged turn length (s). Smaller values keep rapid exchanges attributed to the right person.
    "P0_WHOTALK_ASR_MERGE_GAP_MS": "800",
    "P0_WHOTALK_ASR_MAX_TURN_SECONDS": "15",
    # faster-whisper settings (engine=whisper): model size/path, forced base language, and the
    # bilingual initial prompt that locks Whisper into mixed zh+en transcription.
    "P0_WHOTALK_WHISPER_MODEL": "medium",
    "P0_WHOTALK_WHISPER_LANG": "zh",
    # Chinese-DOMINANT prompt with a code-switch example: keeps output in 汉字 for Chinese speech
    # (an English-leading prompt makes Whisper translate instead of transcribe) while allowing
    # inline English words.
    "P0_WHOTALK_WHISPER_PROMPT": "以下是一段中英混合的工作会议对话，请按原话记录：中文写汉字，英文单词保留英文。例如：我们现在 check 一下这个 server 的 status，然后 update 给大家。",
    # Refuse to download recordings larger than this (MB). 0 = no limit.
    "P0_WHOTALK_ASR_MAX_MEDIA_MB": "1024",
    # Keep the downloaded media/wav files for debugging (default: delete after use).
    "P0_WHOTALK_ASR_KEEP_MEDIA": "0",
    # ffmpeg binary; empty = try <app>/bin/ffmpeg then PATH.
    "P0_FFMPEG_BIN": "",
    # ---- /p0docs — fill a P0 incident doc from a meeting transcript ----
    # Usage: /p0docs <meeting link|9-digit no|minutes link> <wiki/docx doc link>
    # The bot reads the doc's blocks, asks Qwen to fill ONLY the fields the transcript answers
    # (unknown fields left untouched), and patches those blocks in place.
    # Needs scope docx:document (edit; add + PUBLISH) and the doc/wiki shared to the app as EDITABLE.
    "P0_P0DOCS_ENABLE": "1",
    "P0_P0DOCS_TRIGGER": "/p0docs",
    # Extra/override instruction for the fill model (empty = built-in).
    "P0_P0DOCS_PROMPT": "",
    # Use the local ASR transcript (slow) instead of Lark's text when filling. Default off: doc
    # filling wants speed; names/times come through fine in Lark's transcript.
    "P0_P0DOCS_USE_LOCAL_ASR": "0",
    # Max transcript characters passed to the fill model (head+tail kept when longer).
    "P0_P0DOCS_TRANSCRIPT_CHARS": "12000",
    # ---- /osemeeting — write the bilingual OSE/weekly meeting minutes doc from a recording ----
    # Usage (either order, one per line):
    #   /osemeeting
    #   <meeting link|9-digit no|minutes link>
    #   <wiki/docx doc link>
    # Three models in one pass: OpenAI ASR hears the audio, qwen2.5vl watches the video and keeps
    # only the frames that carry information (shared screens, dashboards, errors, configs), and
    # qwen3.6:35b-a3b turns both into the doc's own layout — the Overview table plus the
    # "English Version" / "中文版" numbered discussion topics, with the kept frames embedded
    # under the topic they belong to.
    # Needs scope docx:document (edit; add + PUBLISH) with the doc shared to the app as EDITABLE,
    # minutes:minutes.media:export for the recording download, and drive:drive to upload images.
    "P0_OSEMEETING_ENABLE": "1",
    "P0_OSEMEETING_TRIGGER": "/osemeeting",
    # ---- audio → text: OpenAI ASR, falling back to the local engine, then Lark's own text ----
    # openai | local | lark. "openai" still falls back down the chain on any failure.
    "P0_OSEMEETING_ASR_PROVIDER": "openai",
    # Empty = read OPENAI_API_KEY from the environment instead.
    "P0_OSEMEETING_OPENAI_API_KEY": "",
    "P0_OSEMEETING_OPENAI_BASE_URL": "https://api.openai.com/v1",
    # whisper-1 by default because it is the only transcription model that still returns per-segment
    # timestamps (response_format=verbose_json), and those timestamps are what let the bot line each
    # sentence up with the right speaker from the Minutes SRT. gpt-4o-transcribe returns text only,
    # so with it speaker attribution degrades to "this chunk = whoever spoke most in it".
    "P0_OSEMEETING_OPENAI_ASR_MODEL": "whisper-1",
    # Force a base language (ISO-639-1, e.g. zh / en); empty = let the model auto-detect.
    "P0_OSEMEETING_ASR_LANG": "",
    "P0_OSEMEETING_ASR_PROMPT": "Mixed Chinese/English OSE operations meeting. Keep English technical terms in English; write Chinese speech in 汉字.",
    # A single OpenAI request is hard-capped at 25 MB, so the audio is cut into chunks first.
    # 600 s of 32 kbps mono mp3 is ~2.4 MB — far under the cap even for a long meeting.
    "P0_OSEMEETING_ASR_CHUNK_SECONDS": "600",
    "P0_OSEMEETING_ASR_BITRATE": "32k",
    "P0_OSEMEETING_ASR_TIMEOUT_SECONDS": "300",
    # ---- video → pictures: qwen2.5vl keeps the frames worth putting in the doc ----
    "P0_OSEMEETING_VISION_ENABLE": "1",
    "P0_OSEMEETING_VISION_MODEL": "qwen2.5vl:3b",
    # Sample one frame every N seconds; near-identical consecutive frames are dropped before the
    # model ever sees them (a screen share that sits still for 5 minutes costs one look, not 15).
    "P0_OSEMEETING_FRAME_INTERVAL_SECONDS": "20",
    "P0_OSEMEETING_FRAME_WIDTH": "1280",
    # Mean 0..255 pixel distance (on a 16x16 grey thumbnail) below which two sampled frames count
    # as the same screen. Raise it to dedupe harder, lower it to keep subtler changes.
    "P0_OSEMEETING_FRAME_DEDUPE_THRESHOLD": "3.0",
    # Hard caps: distinct frames shown to the vision model, and images finally embedded in the doc.
    "P0_OSEMEETING_MAX_FRAMES": "120",
    "P0_OSEMEETING_MAX_IMAGES": "8",
    "P0_OSEMEETING_VISION_TIMEOUT_SECONDS": "120",
    "P0_OSEMEETING_VISION_PROMPT": "",
    # ---- text → minutes: the writer model (empty = P0_QA_MODEL / MONITORING_AI_MODEL) ----
    "P0_OSEMEETING_WRITER_MODEL": "",
    "P0_OSEMEETING_PROMPT": "",
    # Max transcript characters handed to the writer (head+tail kept when longer).
    "P0_OSEMEETING_TRANSCRIPT_CHARS": "24000",
    # Max discussion topics written per language section (the template ships with 4 slots; extra
    # topics get new headings appended, unused slots are left untouched).
    "P0_OSEMEETING_MAX_TOPICS": "10",
    # Overview table values the meeting itself cannot supply. Empty "prepared by" = the bot's name.
    "P0_OSEMEETING_PREPARED_BY": "",
    "P0_OSEMEETING_PARTICIPANTS_FALLBACK": "MY OSE",
    # Keep the downloaded recording + extracted frames for debugging (default: delete after use).
    "P0_OSEMEETING_KEEP_MEDIA": "0",
    # OSE duty roster (wiki sheet): months in row 1, day numbers in row 2, names in column A,
    # D/N marks per day. Meeting start 07:00-19:00 → that day's D people; otherwise N (a start
    # before 07:00 belongs to the previous day's N shift). Fills "OSE On-duty".
    "P0_DUTY_WIKI_TOKEN": "O4Dfw4DVTiPpFukn801l5z3WgMd",
    "P0_DUTY_SHEET_ID": "AS33r7",
    # Optional override of this bot's open_id (else resolved via bot/v3/info with these creds)
    "P0_BOT_OPEN_ID": "",
    # Optional custom system prompt for answers; use {doc} where the documentation should be injected
    "P0_QA_SYSTEM_PROMPT": "",
    # ---- p0bot message reactions (ACK while Qwen thinks, DONE when the answer is sent) ----
    "P0_REACT_ENABLE": "1",
    # Lark emoji_type keys (see Lark reaction emoji list). ACK shows while processing.
    "P0_REACT_ACK_EMOJI": "OK",
    "P0_REACT_DONE_EMOJI": "DONE",
    # 1=remove the ACK reaction once DONE is added (leaves only the ✅); 0=keep both
    "P0_REACT_REMOVE_ACK": "1",
    # ---- p0bot answer formatting: render answers as a clean interactive card ----
    "P0_ANSWER_CARD": "1",
    "P0_CARD_TITLE": "📖 p0bot",
    "P0_CARD_TEMPLATE": "blue",
    # Per-card body budget; a longer answer is split across multiple cards (part n/N)
    "P0_CARD_MAX_CHARS": "8000",
    # ---- p0bot meeting attendance (Mode C: on-demand attendance report by meeting no.) ----
    # A bot can only see meetings it OWNS, so live "who joined" for arbitrary meetings is not
    # possible. Instead, "/meeting <link-or-number>" pulls the attendance report for that
    # 9-digit meeting number via GET /vc/v1/participant_list (works for ongoing + ended
    # meetings) and posts a card. Requires the app to hold the VC meeting-management report
    # permission (admin-granted); some tenants require a user_access_token for this endpoint.
    "P0_MEETING_ENABLE": "0",
    "P0_MEETING_TRIGGER": "/meeting",
    # Look-back window for the report; capped to 24h (Lark caps end-start to 1 day).
    "P0_MEETING_LOOKBACK_HOURS": "6",
    "P0_MEETING_MAX_ROWS": "200",
    "P0_MEETING_CARD_TEMPLATE": "turquoise",
    # ---- Admin OAuth for /meeting (user_access_token; clears the 121005 admin-role gate) ----
    # An admin runs /vcauth, authorizes in the browser, and pastes the code back with /vccode.
    # The bot stores + auto-refreshes that admin's user token and uses it for the report.
    # redirect_uri must be registered in the app's Security → Redirect URLs (localhost is fine;
    # the code is copied from the address bar, so the URL need not actually be served).
    "P0_VC_REDIRECT_URI": "http://localhost:5088/oauth/callback",
    "P0_VC_OAUTH_SCOPES": "vc:rooms.room.detailinfo:read offline_access contact:contact.base:readonly contact:user.employee_id:readonly",
    # REQUIRED to use /vcauth //vccode: space/comma-separated admin open_ids allowed to authorize
    # (the stored token is shared, so a stray authorize would clobber it). Empty = nobody
    # (fail-closed); run /vcauth once and the bot replies with your open_id — set it here + restart.
    "P0_VC_ADMIN_OPEN_IDS": "",
    # ---- Group members: "/members" in a group lists who's IN THE CHAT GROUP (not the video call) ----
    # Works with the bot's own token + im:chat:readonly; the bot only needs to be a MEMBER of the group.
    "P0_MEMBERS_ENABLE": "1",
    "P0_MEMBERS_TRIGGER": "/members",
    "P0_MEMBERS_CARD_TEMPLATE": "blue",
    "P0_MEMBERS_MAX_ROWS": "500",
    # ---- Bot-hosted meeting (/openmeeting): reserve → live join/leave → auto-record → recording ----
    # The bot reserves a meeting it owns (needs a real user as owner/host), posts the join link, and
    # announces joins/leaves live via VC events. Scopes: vc:reserve, vc:meeting:readonly, vc:meeting,
    # vc:record:readonly (+ contact:contact.base:readonly for names). Cloud recording must be enabled
    # for the tenant. /endmeeting needs the host's user token (via /vcauth) AND the host in the call;
    # otherwise the host ends it in the Lark client.
    "P0_OPENMEETING_ENABLE": "0",
    "P0_OPENMEETING_TRIGGER": "/openmeeting",
    "P0_ENDMEETING_TRIGGER": "/endmeeting",
    # /checkmeeting <name> — search bot-hosted meetings for participants whose name matches,
    # showing join/leave times (empty name = list everyone currently tracked).
    "P0_CHECKMEETING_TRIGGER": "/checkmeeting",
    # Meeting owner + assigned host + recording recipient — a real Lark user open_id (REQUIRED).
    "P0_MEETING_HOST_OPEN_ID": "ou_5f660c0fb0769d184aca635d02209272",
    # Where joins/leaves/end are announced; empty = the chat where /openmeeting was run.
    "P0_OPENMEETING_ANNOUNCE_CHAT_ID": "oc_ad9b5bdbb2826ba2ee9730920ef25432",
    "P0_OPENMEETING_AUTO_RECORD": "1",
    "P0_OPENMEETING_TOPIC": "p0bot meeting",
    "P0_OPENMEETING_DURATION_HOURS": "4",
    "P0_OPENMEETING_ANNOUNCE_JOINS": "1",
    "P0_OPENMEETING_ANNOUNCE_LEAVES": "1",
    "P0_OPENMEETING_CARD_TEMPLATE": "turquoise",
    # Who may run /openmeeting //endmeeting (space/comma open_ids); empty = anyone in the chat.
    "P0_OPENMEETING_ALLOWED_OPEN_IDS": "",
}


def _cfg_raw(key: str) -> Any:
    """``os.environ`` wins (systemd), else ``_CFG``."""
    if key in os.environ and str(os.environ.get(key, "")).strip() != "":
        return os.environ[key]
    return _CFG.get(key)


def _cfg_str(key: str, default: str = "") -> str:
    v = _cfg_raw(key)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _cfg_int(key: str, default: int) -> int:
    v = _cfg_raw(key)
    if v is None or (isinstance(v, str) and not str(v).strip()):
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _cfg_float(key: str, default: float) -> float:
    v = _cfg_raw(key)
    if v is None or (isinstance(v, str) and not str(v).strip()):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _cfg_listen_port() -> int:
    """grafanagamebot always binds **5088**; ``PORT`` / ``LARKBOT_PORT`` / ``_CFG`` are ignored."""
    return 5088


# ``lark_oapi`` → ``ws/pb/google/__init__.py`` uses ``pkg_resources.declare_namespace`` (no upstream fix yet).
warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API",
    category=UserWarning,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _lark_env_truthy(key: str) -> bool:
    v = _cfg_raw(key)
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _lark_env_truthy_or_default(key: str, *, default: bool) -> bool:
    """Like :func:`_lark_env_truthy` but ``default`` when the key is unset (``_cfg_raw`` is None)."""
    v = _cfg_raw(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


app = Flask(__name__)


class _WsgiWebhookDiagMiddleware:
    """
    Optional WSGI logging — **default off**: sync writes to journald on every webhook can add latency.
    Feishu URL verification is often quoted as **~1s total budget** (RTT + handler); enable only when debugging::

      LARK_WEBHOOK_WSGI_LOG=1
    """

    def __init__(self, flask_app: Any):
        self.flask_app = flask_app

    def __call__(self, environ: Any, start_response: Any):
        path = environ.get("PATH_INFO") or ""
        if path.rstrip("/") == "/webhook/event" and _lark_env_truthy("LARK_WEBHOOK_WSGI_LOG"):
            logger.info(
                "WSGI enter %s %s content_length=%s expect=%r remote=%s",
                environ.get("REQUEST_METHOD"),
                path,
                environ.get("CONTENT_LENGTH"),
                environ.get("HTTP_EXPECT"),
                environ.get("REMOTE_ADDR"),
            )
        return self.flask_app(environ, start_response)


app.wsgi_app = _WsgiWebhookDiagMiddleware(app.wsgi_app)


def _request_is_webhook_event() -> bool:
    return (request.path or "").rstrip("/") == "/webhook/event"


@app.before_request
def _lark_webhook_request_timer_start():
    if (
        _request_is_webhook_event()
        and request.method == "POST"
        and _lark_env_truthy("LARK_WEBHOOK_TIMING_LOG")
    ):
        g._lark_wh_t0 = time.perf_counter()


@app.after_request
def _lark_webhook_request_timer_end(response: Response):
    """Optional timing log — ``LARK_WEBHOOK_TIMING_LOG=1``. Default off to avoid journald latency on hot path."""
    if not (
        _request_is_webhook_event()
        and request.method == "POST"
        and _lark_env_truthy("LARK_WEBHOOK_TIMING_LOG")
    ):
        return response
    t0 = getattr(g, "_lark_wh_t0", None)
    if t0 is not None:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        xff = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        remote = xff or (request.remote_addr or "")
        ua = (request.headers.get("User-Agent") or "")[:160]
        if elapsed_ms > 1000:
            logger.warning(
                "webhook/event POST slow elapsed_ms=%.1f status=%s remote=%s ua=%r",
                elapsed_ms,
                response.status_code,
                remote,
                ua,
            )
        else:
            logger.info(
                "webhook/event POST elapsed_ms=%.1f status=%s remote=%s",
                elapsed_ms,
                response.status_code,
                remote,
            )
    return response


# Lark duplicate pushes (same message_id) — align with Chatbox processed_messages pattern.
_processed_lark_message_ids: set = set()
_PROCESSED_LARK_IDS_CAP = 4000
_monitoring_reply_dispatch_lock = threading.Lock()
_monitoring_im_trigger_last: Dict[str, float] = {}
_monitoring_chat_trigger_last: Dict[str, float] = {}
_monitoring_inflight_keys: set = set()
_processed_lark_im_event_ids: set = set()
_PROCESSED_IM_EVENT_IDS_CAP = 4000
_monitoring_user_reply_sent_at: Dict[str, float] = {}
_monitoring_user_send_in_progress: set = set()
_monitoring_chat_reply_sent_at: Dict[str, float] = {}
_monitoring_chat_send_in_progress: set = set()
_monitoring_card_action_event_ids: set = set()
_monitoring_watch_last_alert_at: float = 0.0
_monitoring_watch_started: bool = False
_monitoring_watch_pending_confirm: Optional[Tuple[int, int, float]] = None
_lark_bot_open_id_resolve_lock = threading.Lock()
# None = not requested yet; "" = bot/v3/info failed or no APP_ID/SECRET
_lark_bot_open_id_api_cache: Optional[str] = None
# --- Alert mute (「告警静音」：进程内；按监控通道粒度，供 watchdog / 告警转发过滤) ---
_MONITORING_MUTE_UNTIL: Dict[str, float] = {}
_mute_pending_selections: Dict[str, Set[str]] = {}
_grafana_pw_keeper: Optional[Any] = None
_grafana_pw_keeper_lock = threading.Lock()
_grafana_pw_keeper_start_attempted: bool = False
_lark_oapi_client: Optional[Any] = None
_lark_oapi_client_lock = threading.Lock()
# Set when WebSocket picks a working open.feishu.cn vs open.larksuite.com (``_get_lark_oapi_client`` must match).
_lark_open_api_domain_override: Optional[str] = None
_lark_ws_transport_log_installed: bool = False
_lark_ws_recv_method_log_installed: bool = False
_lark_ws_saw_data_frame: bool = False
_lark_ws_last_im_monotonic: float = 0.0
# First N inbound protobuf frames logged at INFO (CONTROL vs DATA) without setting LARK_WS_LOG_FRAME_METHOD.
_LARK_WS_BOOTSTRAP_FRAMES_DEFAULT = 16
_lark_ws_bootstrap_frames_left: int = 0

GRAFANA_BASE_URL = _cfg_str("GRAFANA_BASE_URL", "https://grafana.client8.me").rstrip("/")
GRAFANA_DASHBOARD_PATH = _cfg_str(
    "GRAFANA_DASHBOARD_PATH",
    "/d/fe70d4bd-4729-471f-9ede-e981ad277963/online-number",
)
GRAFANA_DASHBOARD_UID = _cfg_str(
    "GRAFANA_DASHBOARD_UID", "fe70d4bd-4729-471f-9ede-e981ad277963"
)
GRAFANA_PANEL_TITLE = _cfg_str("GRAFANA_PANEL_TITLE", "LiveSlots Online Number")
GRAFANA_PANEL_TITLE_EGAME_ONLINE = _cfg_str(
    "GRAFANA_PANEL_TITLE_EGAME_ONLINE", "Egame Online Number"
)
GRAFANA_PANEL_TITLE_EGAMES_BET = _cfg_str(
    "GRAFANA_PANEL_TITLE_EGAMES_BET", "Egames 下注Bet/min"
)
GRAFANA_PANEL_TITLE_LIVESLOT_BET = _cfg_str(
    "GRAFANA_PANEL_TITLE_LIVESLOT_BET", "Liveslot 下注Bet/min"
)
_LIVESLOT_BET_PANEL_TITLES: Tuple[str, ...] = tuple(
    dict.fromkeys(
        t.strip()
        for t in re.split(
            r"[\s,;|]+",
            _cfg_str(
                "GRAFANA_PANEL_TITLE_LIVESLOT_BET_ALIASES",
                "Liveslot 下注Bet/min Liveslots-Spin-Bet",
            ).strip(),
        )
        if t.strip()
    )
)
GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET = _cfg_str(
    "GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET", "Liveslots-Spin-Bet"
)
# ``extraPanels[*].kind`` — /m 静音通道 id；须与 ``fetch_monitoring_payload`` 写入一致
MONITORING_EXTRA_KIND_EGAME_ONLINE = "egame_online"
MONITORING_EXTRA_KIND_EGAMES_BET = "egames_bet"
MONITORING_EXTRA_KIND_LIVESLOT_BET = "liveslot_bet"
MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT = "liveslot_spin_count"
# Deprecated ``kind`` strings (仍识别旧 payload / 旧静音键)
_MONITORING_EXTRA_KIND_EGAME_ONLINE_LEGACY = "9280_push"
_MONITORING_EXTRA_KIND_EGAMES_BET_LEGACY = "provider_jili"
_MONITORING_EXTRA_KIND_LIVESLOT_BET_LEGACY = "withdraw"


def _extra_panel_logical_kind(kind: str) -> str:
    """Normalize ``extraPanels`` ``kind`` to the current channel id (accepts legacy values)."""
    k = (kind or "").strip()
    if k == MONITORING_EXTRA_KIND_EGAME_ONLINE or k == _MONITORING_EXTRA_KIND_EGAME_ONLINE_LEGACY:
        return MONITORING_EXTRA_KIND_EGAME_ONLINE
    if k == MONITORING_EXTRA_KIND_EGAMES_BET or k == _MONITORING_EXTRA_KIND_EGAMES_BET_LEGACY:
        return MONITORING_EXTRA_KIND_EGAMES_BET
    if k == MONITORING_EXTRA_KIND_LIVESLOT_BET or k == _MONITORING_EXTRA_KIND_LIVESLOT_BET_LEGACY:
        return MONITORING_EXTRA_KIND_LIVESLOT_BET
    if k == MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT:
        return MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT
    return k


def _monitoring_extra_channel_muted(raw_kind: str) -> bool:
    """True if this extra panel's alerts are muted (new or legacy mute key)."""
    lg = _extra_panel_logical_kind(raw_kind)
    if lg == MONITORING_EXTRA_KIND_EGAME_ONLINE:
        return _monitoring_alert_channel_muted(MONITORING_EXTRA_KIND_EGAME_ONLINE) or _monitoring_alert_channel_muted(
            _MONITORING_EXTRA_KIND_EGAME_ONLINE_LEGACY
        )
    if lg == MONITORING_EXTRA_KIND_EGAMES_BET:
        return _monitoring_alert_channel_muted(MONITORING_EXTRA_KIND_EGAMES_BET) or _monitoring_alert_channel_muted(
            _MONITORING_EXTRA_KIND_EGAMES_BET_LEGACY
        )
    if lg == MONITORING_EXTRA_KIND_LIVESLOT_BET:
        if not MONITORING_LIVESLOT_BET_ENABLE:
            return True
        return _monitoring_alert_channel_muted(MONITORING_EXTRA_KIND_LIVESLOT_BET) or _monitoring_alert_channel_muted(
            _MONITORING_EXTRA_KIND_LIVESLOT_BET_LEGACY
        )
    if lg == MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT:
        if not MONITORING_LIVESLOT_SPIN_COUNT_ENABLE:
            return True
        return _monitoring_alert_channel_muted(MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT)
    return _monitoring_alert_channel_muted(raw_kind)


MONITORING_EGAME_ONLINE_SERIES_KEYWORD = (
    _cfg_str("MONITORING_EGAME_ONLINE_SERIES_KEYWORD", "").strip()
    or _cfg_str("MONITORING_9280_SERIES_KEYWORD", "").strip()
)
MONITORING_EGAMES_BET_SERIES_KEYWORD = (
    _cfg_str("MONITORING_EGAMES_BET_SERIES_KEYWORD", "").strip()
    or _cfg_str("MONITORING_PROVIDER_JILI_SERIES_KEYWORD", "").strip()
)
MONITORING_EGAMES_BET_SERIES_INCLUDE = _cfg_str(
    "MONITORING_EGAMES_BET_SERIES_INCLUDE", "EcallTW,Sinonet"
).strip()
# Browser URL time range for screenshots (default last 30 minutes — match Grafana dashboard picker).
GRAFANA_DASHBOARD_FROM = _cfg_str("GRAFANA_DASHBOARD_FROM", "now-1h")
GRAFANA_DASHBOARD_TO = _cfg_str("GRAFANA_DASHBOARD_TO", "now")
# Prometheus query_range step (seconds); 60 → up to 15 buckets in 15m when lookback=900
GRAFANA_QUERY_STEP = _cfg_int("GRAFANA_QUERY_STEP", 60)
GRAFANA_QUERY_LOOKBACK_SECONDS = _cfg_int("GRAFANA_QUERY_LOOKBACK_SECONDS", 900)
GRAFANA_QUERY_END_LAG_SECONDS = _cfg_int("GRAFANA_QUERY_END_LAG_SECONDS", 120)
MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES = max(
    0, _cfg_int("MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES", 0)
)
MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES = max(
    0, _cfg_int("MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES", 0)
)
GRAFANA_SCREENSHOT_WIDTH = _cfg_int("GRAFANA_SCREENSHOT_WIDTH", 1400)
GRAFANA_SCREENSHOT_HEIGHT = _cfg_int("GRAFANA_SCREENSHOT_HEIGHT", 1080)
GRAFANA_SCREENSHOT_TIMEOUT_MS = _cfg_int("GRAFANA_SCREENSHOT_TIMEOUT_MS", 90000)
GRAFANA_SCREENSHOT_STABILIZE_ROUNDS = max(
    1, min(8, _cfg_int("GRAFANA_SCREENSHOT_STABILIZE_ROUNDS", 1))
)
GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS = max(
    60, min(3000, _cfg_int("GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS", 100))
)
GRAFANA_SCREENSHOT_SETTLE_MS = max(
    0, min(120_000, _cfg_int("GRAFANA_SCREENSHOT_SETTLE_MS", 300))
)
GRAFANA_SCREENSHOT_SPINNER_MAX_MS = max(
    2000, min(60_000, _cfg_int("GRAFANA_SCREENSHOT_SPINNER_MAX_MS", 7000))
)
GRAFANA_SCREENSHOT_POPULATE_MAX_MS = max(
    1500, min(90_000, _cfg_int("GRAFANA_SCREENSHOT_POPULATE_MAX_MS", 4500))
)
GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS = max(
    0, min(30_000, _cfg_int("GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS", 1600))
)
GRAFANA_SCREENSHOT_MIN_GRID_ITEMS = max(
    0, min(200, _cfg_int("GRAFANA_SCREENSHOT_MIN_GRID_ITEMS", 0))
)
GRAFANA_SCREENSHOT_PANEL_READY_RATIO = max(
    0.5, min(1.0, _cfg_float("GRAFANA_SCREENSHOT_PANEL_READY_RATIO", 0.92))
)
GRAFANA_SCREENSHOT_PANEL_READY_MIN = max(
    0, min(300, _cfg_int("GRAFANA_SCREENSHOT_PANEL_READY_MIN", 8))
)
GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS = max(
    2000, min(120_000, _cfg_int("GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS", 12000))
)
GRAFANA_SCREENSHOT_PANEL_READY_ZERO_TOTAL_MAX_MS = max(
    400,
    min(60_000, _cfg_int("GRAFANA_SCREENSHOT_PANEL_READY_ZERO_TOTAL_MAX_MS", 1400)),
)
GRAFANA_SCREENSHOT_KIOSK = _cfg_str("GRAFANA_SCREENSHOT_KIOSK", "").strip()
GRAFANA_SCREENSHOT_URL_REFRESH = _cfg_str("GRAFANA_SCREENSHOT_URL_REFRESH", "1m").strip()
GRAFANA_SCREENSHOT_RELATIVE_RANGE = _lark_env_truthy("GRAFANA_SCREENSHOT_RELATIVE_RANGE")
# Screenshot URL ``timezone=`` (e.g. browser); none / - / off → omit parameter
GRAFANA_SCREENSHOT_TIMEZONE = _cfg_str("GRAFANA_SCREENSHOT_TIMEZONE", "browser").strip()
GRAFANA_USER = (
    _cfg_str("GRAFANA_USER")
    or _cfg_str("GRAFANA_ID")
    or _cfg_str("grafanaid")
)
GRAFANA_PASSWORD = _cfg_str("GRAFANA_PASSWORD") or _cfg_str("grafanapassword")
VERIFICATION_TOKEN = _cfg_str("VERIFICATION_TOKEN", "").strip()
# For Open API (e.g. send message) — see Lark auth tenant_access_token_internal
APP_ID = _cfg_str("APP_ID", "").strip() or None
APP_SECRET = _cfg_str("APP_SECRET", "").strip() or None
# Default matches ``lark_oapi.core.const.FEISHU_DOMAIN`` — 国际 Lark 用 ``https://open.larksuite.com``（见 ``_CFG``）
LARK_HOST = _cfg_str("LARK_HOST", "https://open.feishu.cn").rstrip("/")
MONITORING_TRIGGER = _cfg_str("MONITORING_TRIGGER", "/mo")
MONITORING_MUTE_TRIGGER = _cfg_str("MONITORING_MUTE_TRIGGER", "/m").strip()
MONITORING_CANCELMUTE_TRIGGER = _cfg_str("MONITORING_CANCELMUTE_TRIGGER", "/c").strip()
TARGET_USER_OPEN_ID = _cfg_str("TARGET_USER_OPEN_ID", _cfg_str("JUNCHEN", "")).strip()
DEPLOY_ENABLE = _lark_env_truthy_or_default("DEPLOY_ENABLE", default=True)
DEPLOY_ALLOWED_USER_OPEN_ID = _cfg_str("DEPLOY_ALLOWED_USER_OPEN_ID", "").strip()
DEPLOY_ALLOWED_USER_OPEN_ID_SET: Set[str] = set()
if DEPLOY_ALLOWED_USER_OPEN_ID:
    DEPLOY_ALLOWED_USER_OPEN_ID_SET.add(DEPLOY_ALLOWED_USER_OPEN_ID)
for _dep_uid in re.split(
    r"[\s,;]+", _cfg_str("DEPLOY_ALLOWED_USER_OPEN_IDS", "").strip()
):
    if _dep_uid.strip():
        DEPLOY_ALLOWED_USER_OPEN_ID_SET.add(_dep_uid.strip())
DEPLOY_GIT_REPO_PATH = _cfg_str("DEPLOY_GIT_REPO_PATH", "").strip()
DEPLOY_SYSTEMD_SERVICE = _cfg_str("DEPLOY_SYSTEMD_SERVICE", "p0bot").strip()
DEPLOY_TRIGGER = _cfg_str("DEPLOY_TRIGGER", "/deploy").strip()
_DEPLOY_REQUEST_RE = re.compile(r"git\s+pull\b.*\brestart\b", re.IGNORECASE)
MONITORING_ALERT_AT_USER_NOTE = _cfg_str(
    "MONITORING_ALERT_AT_USER_NOTE",
    "It might be event started or false alert kindly check",
).strip()
MONITORING_EGAME_FAST_ALERT_PCT = _cfg_float("MONITORING_EGAME_FAST_ALERT_PCT", 25.0)
MONITORING_EGAMES_BET_FAST_ALERT_PCT = _cfg_float("MONITORING_EGAMES_BET_FAST_ALERT_PCT", 25.0)
MONITORING_GAME_ALERT_CONTINUOUS_PCT = _cfg_float("MONITORING_GAME_ALERT_CONTINUOUS_PCT", 30.0)
# Fast-only panels use ``inf`` for continuous so only drop/spike % below apply.
MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT = float("inf")
MONITORING_EGAME_FAST_DROP_ALERT_PCT = _cfg_float(
    "MONITORING_EGAME_FAST_DROP_ALERT_PCT", MONITORING_EGAME_FAST_ALERT_PCT
)
MONITORING_EGAME_FAST_SPIKE_ALERT_PCT = _cfg_float(
    "MONITORING_EGAME_FAST_SPIKE_ALERT_PCT", MONITORING_EGAME_FAST_ALERT_PCT
)
MONITORING_EGAMES_BET_FAST_DROP_ALERT_PCT = _cfg_float(
    "MONITORING_EGAMES_BET_FAST_DROP_ALERT_PCT", MONITORING_EGAMES_BET_FAST_ALERT_PCT
)
MONITORING_EGAMES_BET_FAST_SPIKE_ALERT_PCT = _cfg_float(
    "MONITORING_EGAMES_BET_FAST_SPIKE_ALERT_PCT", MONITORING_EGAMES_BET_FAST_ALERT_PCT
)
MONITORING_LIVESLOT_BET_ENABLE = _lark_env_truthy_or_default(
    "MONITORING_LIVESLOT_BET_ENABLE", default=False
)
MONITORING_LIVESLOT_BET_FAST_DROP_ALERT_PCT = _cfg_float(
    "MONITORING_LIVESLOT_BET_FAST_DROP_ALERT_PCT", 50.0
)
MONITORING_LIVESLOT_BET_FAST_SPIKE_ALERT_PCT = _cfg_float(
    "MONITORING_LIVESLOT_BET_FAST_SPIKE_ALERT_PCT", float("inf")
)
MONITORING_LIVESLOT_BET_SERIES_INCLUDE = _cfg_str(
    "MONITORING_LIVESLOT_BET_SERIES_INCLUDE", "total spins"
)
MONITORING_LIVESLOT_BET_BASELINE_FILTER = _lark_env_truthy_or_default(
    "MONITORING_LIVESLOT_BET_BASELINE_FILTER", default=False
)
MONITORING_LIVESLOT_BET_DROP_ENDPOINT_MIN_MEDIAN_RATIO = max(
    0.05,
    min(1.0, _cfg_float("MONITORING_LIVESLOT_BET_DROP_ENDPOINT_MIN_MEDIAN_RATIO", 0.35)),
)
MONITORING_LIVESLOT_BET_MIN_ABS_DROP = max(
    0.0, _cfg_float("MONITORING_LIVESLOT_BET_MIN_ABS_DROP", 500.0)
)
MONITORING_LIVESLOT_SPIN_COUNT_ENABLE = _lark_env_truthy_or_default(
    "MONITORING_LIVESLOT_SPIN_COUNT_ENABLE", default=True
)
MONITORING_LIVESLOT_SPIN_COUNT_SERIES_INCLUDE = _cfg_str(
    "MONITORING_LIVESLOT_SPIN_COUNT_SERIES_INCLUDE", "spin_count"
)
MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS = max(
    60, _cfg_int("MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS", 120)
)
MONITORING_LIVESLOTS_FAST_DROP_ALERT_PCT = _cfg_float(
    "MONITORING_LIVESLOTS_FAST_DROP_ALERT_PCT", 25.0
)
MONITORING_LIVESLOTS_FAST_SPIKE_ALERT_PCT = _cfg_float(
    "MONITORING_LIVESLOTS_FAST_SPIKE_ALERT_PCT", 50.0
)
MONITORING_ALERT_WINDOW_SECONDS = max(60, _cfg_int("MONITORING_ALERT_WINDOW_SECONDS", 180))
# 1=在 /mo 与告警中包含主面板（LiveSlots）；0 时主面板表与 JSON 端点可关闭
MONITORING_HTTP_PRIMARY_ENABLE = _lark_env_truthy_or_default(
    "MONITORING_HTTP_PRIMARY_ENABLE", default=True
)
MONITORING_DROP_LAST_MERGED_MINUTES = max(
    0, min(60, _cfg_int("MONITORING_DROP_LAST_MERGED_MINUTES", 0))
)
MONITORING_WATCH_DROP_LAST_MERGED_MINUTES = max(
    0, min(60, _cfg_int("MONITORING_WATCH_DROP_LAST_MERGED_MINUTES", 0))
)
MONITORING_WATCH_MIN_LAST_BUCKET_AGE_SECONDS = max(
    0.0, _cfg_float("MONITORING_WATCH_MIN_LAST_BUCKET_AGE_SECONDS", 90.0)
)
MONITORING_WATCH_CONFIRM_SECONDS = max(
    0.0, _cfg_float("MONITORING_WATCH_CONFIRM_SECONDS", 60.0)
)
_tls_analysis_drop = threading.local()


def _analysis_drop_n() -> int:
    """Watchdog thread sets ``_tls_analysis_drop.watchdog`` so trim uses watch-specific drop count."""
    if getattr(_tls_analysis_drop, "watchdog", False):
        return MONITORING_WATCH_DROP_LAST_MERGED_MINUTES
    return MONITORING_DROP_LAST_MERGED_MINUTES


MONITORING_TABLE_TAIL_ROWS = max(1, min(99, _cfg_int("MONITORING_TABLE_TAIL_ROWS", 5)))
MONITORING_PER_SERIES_ANALYSIS = _lark_env_truthy_or_default(
    "MONITORING_PER_SERIES_ANALYSIS",
    default=True,
)
MONITORING_SIMPLE_ALERT_TEXT = _lark_env_truthy("MONITORING_SIMPLE_ALERT_TEXT")
MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS = _lark_env_truthy(
    "MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS"
)
MONITORING_TIME_BUCKET_TZ = _cfg_str("MONITORING_TIME_BUCKET_TZ", "").strip()


def _parse_monitoring_zoneinfo() -> Optional[Any]:
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        try:
            from backports.zoneinfo import ZoneInfo  # type: ignore
        except ImportError:
            logger.warning(
                "zoneinfo unavailable (Python 3.9+ has it in stdlib; else: pip install "
                "backports.zoneinfo). MONITORING_TIME_BUCKET_TZ ignored — local process time for buckets."
            )
            return None

    tzn = MONITORING_TIME_BUCKET_TZ.strip()
    if not tzn or tzn.lower() in ("local", "server", "-", "none"):
        return None
    try:
        return ZoneInfo(tzn)
    except Exception:
        logger.warning(
            "MONITORING_TIME_BUCKET_TZ=%r invalid; using process local time for buckets",
            tzn,
        )
        return None


MONITORING_ZONEINFO: Optional[Any] = _parse_monitoring_zoneinfo()


def _monitoring_calendar_dt(ts: float) -> datetime:
    zi = MONITORING_ZONEINFO
    if zi is not None:
        return datetime.fromtimestamp(float(ts), tz=zi)
    return datetime.fromtimestamp(float(ts))


def _bucket_ts_monitoring_minute(ts: float) -> float:
    dt = _monitoring_calendar_dt(ts).replace(second=0, microsecond=0)
    return dt.timestamp()


def _snap_series_to_monitoring_minutes(
    points: List[Tuple[float, float]],
    *,
    how: str,
    tol_sec: float = 0.5,
) -> List[Tuple[float, float]]:
    """
    One point per calendar minute (``MONITORING_ZONEINFO`` / local), keyed at minute start ``b``.

    - If any sample lies within ``tol_sec`` of ``b``, prefer those (``max`` or ``sum`` of their values).
    - Otherwise fall back so Prometheus offsets (:30 / :45) still produce data: ``max`` → value at the
      timestamp **closest** to ``b``; ``sum`` → **sum every** sample in that minute (HTTP additive).
    """
    by_b: Dict[float, List[Tuple[float, float]]] = {}
    for ts, val in points:
        try:
            tsf = float(ts)
            v = float(val)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(tsf) or not math.isfinite(v):
            continue
        b = _bucket_ts_monitoring_minute(tsf)
        by_b.setdefault(b, []).append((tsf, v))
    out: List[Tuple[float, float]] = []
    tol = float(tol_sec)
    for b in sorted(by_b.keys()):
        cand = by_b[b]
        near = [(t, v) for t, v in cand if abs(t - b) <= tol]
        if near:
            if how == "sum":
                out.append((b, sum(v for _, v in near)))
            else:
                out.append((b, max(v for _, v in near)))
        elif how == "sum":
            out.append((b, sum(v for _, v in cand)))
        else:
            _t_pick, v_pick = min(cand, key=lambda x: abs(x[0] - b))
            out.append((b, v_pick))
    return out


def _trim_trailing_minute_buckets(
    points: List[Tuple[float, float]],
    n: int,
) -> List[Tuple[float, float]]:
    """
    Drop the newest ``n`` minute buckets after snap — closing incomplete Prometheus tail rows.
    Keeps at least ``n + 3`` points when possible so short windows do not go empty.
    """
    if n <= 0 or not points:
        return points
    if len(points) <= n + 2:
        return points
    return points[:-n]


LARK_ENCRYPT_KEY = (
    _cfg_str("LARK_ENCRYPT_KEY")
    or _cfg_str("ENCRYPT_KEY")
    or _cfg_str("FEISHU_ENCRYPT_KEY")
    or ""
).strip()

MONITORING_PEER_BOT_OPEN_ID_SET: Set[str] = {
    p.strip()
    for p in re.split(r"[\s,;]+", _cfg_str("MONITORING_PEER_BOT_OPEN_IDS", "").strip())
    if p.strip()
}

# Grafana Game Bot：合并进 canonical；用于纠正误把 Platform ou_ 写进 MONITORING_CANONICAL_* 的 systemd 配置。
_GAME_EMBEDDED_CANONICAL_IDS: Tuple[str, ...] = (
    "ou_1830c6697311e779471888a420233eed",
    "ou_848fc4640b48b9845cbc5b0cfa2f1af1",
    "ou_ee1af664e18d9c2d25e0ab6fded66388",
)

# grafanaplatformbot canonical open_ids — systemd 误套 Platform 模板时用于纠正本进程的 LARK_/MONITORING_CANONICAL_*。
_PLATFORM_BOT_OPEN_IDS_EMBEDDED_SET: Set[str] = {
    "ou_0bfd185231d6beb669425fdf8f13e9df",
    "ou_a51dad55e46f665d740b85c5ae22f940",
    "ou_04878d0cdae2ca774e1d4a1716fa9ac3",
}

if MONITORING_PEER_BOT_OPEN_ID_SET and MONITORING_PEER_BOT_OPEN_ID_SET <= set(_GAME_EMBEDDED_CANONICAL_IDS):
    logger.warning(
        "MONITORING_PEER_BOT_OPEN_IDS=%s looks like Game bot ids — peer must list Platform bot ou_; "
        "routing will mis-handle @Platform vs @Game until fixed.",
        sorted(MONITORING_PEER_BOT_OPEN_ID_SET),
    )

_lark_oid_cfg = _cfg_str("LARK_BOT_OPEN_ID", "").strip()
if not _lark_oid_cfg or _lark_oid_cfg in MONITORING_PEER_BOT_OPEN_ID_SET:
    LARK_BOT_OPEN_ID = "ou_1830c6697311e779471888a420233eed"
    if _lark_oid_cfg and _lark_oid_cfg in MONITORING_PEER_BOT_OPEN_ID_SET:
        logger.warning(
            "LARK_BOT_OPEN_ID=%r is a Platform peer id on grafanagamebot — using embedded Game bot open_id",
            _lark_oid_cfg,
        )
else:
    LARK_BOT_OPEN_ID = _lark_oid_cfg

if (LARK_BOT_OPEN_ID or "").strip() in _PLATFORM_BOT_OPEN_IDS_EMBEDDED_SET:
    logger.warning(
        "LARK_BOT_OPEN_ID=%r is a Platform bot open_id on grafanagamebot — likely swapped with Platform "
        "deployment template; using embedded Game bot open_id",
        LARK_BOT_OPEN_ID,
    )
    LARK_BOT_OPEN_ID = "ou_1830c6697311e779471888a420233eed"

MONITORING_AT_MENTION_ENABLE = _lark_env_truthy("MONITORING_AT_MENTION_ENABLE")
MONITORING_AT_MENTION_ANY_TEXT = _lark_env_truthy("MONITORING_AT_MENTION_ANY_TEXT")
MONITORING_TRIGGER_REQUIRES_AT_BOT = _lark_env_truthy("MONITORING_TRIGGER_REQUIRES_AT_BOT")
MONITORING_LOG_PRIMARY_AT = _lark_env_truthy("MONITORING_LOG_PRIMARY_AT")
# Feature removed: never send the "/mo skipped: ..." @-gate feedback reply.
# Hard-disabled in code so it stays off even if systemd sets the env var.
MONITORING_AT_GATE_USER_FEEDBACK = False
_monitoring_at_gate_tls = threading.local()
MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER = _lark_env_truthy_or_default(
    "MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER",
    default=True,
)
MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW = _lark_env_truthy_or_default(
    "MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW",
    default=False,
)
MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_FALLBACK = _lark_env_truthy_or_default(
    "MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_FALLBACK",
    default=False,
)
MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_SUBSTRINGS: Tuple[str, ...] = tuple(
    p.strip()
    for p in re.split(
        r"[|,]",
        _cfg_str("MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_SUBSTRINGS", "").strip(),
    )
    if p.strip()
)

_cfg_canon_primary = _cfg_str("MONITORING_CANONICAL_BOT_OPEN_ID", "").strip()
if not _cfg_canon_primary or _cfg_canon_primary in MONITORING_PEER_BOT_OPEN_ID_SET:
    MONITORING_CANONICAL_BOT_OPEN_ID = "ou_1830c6697311e779471888a420233eed"
    if _cfg_canon_primary and _cfg_canon_primary in MONITORING_PEER_BOT_OPEN_ID_SET:
        logger.warning(
            "MONITORING_CANONICAL_BOT_OPEN_ID=%r is a Platform peer id — using embedded Game canonical",
            _cfg_canon_primary,
        )
else:
    MONITORING_CANONICAL_BOT_OPEN_ID = _cfg_canon_primary

if (MONITORING_CANONICAL_BOT_OPEN_ID or "").strip() in _PLATFORM_BOT_OPEN_IDS_EMBEDDED_SET:
    logger.warning(
        "MONITORING_CANONICAL_BOT_OPEN_ID=%r is a Platform bot open_id on grafanagamebot — likely swapped "
        "with Platform template; using embedded Game canonical",
        MONITORING_CANONICAL_BOT_OPEN_ID,
    )
    MONITORING_CANONICAL_BOT_OPEN_ID = "ou_1830c6697311e779471888a420233eed"

_extra_canon_cfg = {
    p.strip()
    for p in re.split(r"[\s,;]+", _cfg_str("MONITORING_CANONICAL_BOT_OPEN_IDS", "").strip())
    if p.strip()
}
MONITORING_CANONICAL_BOT_OPEN_ID_EXTRA_SET = set(_GAME_EMBEDDED_CANONICAL_IDS) | _extra_canon_cfg

MONITORING_ALERT_CHAT_ID = _cfg_str("MONITORING_ALERT_CHAT_ID", "").strip()
MONITORING_MESSAGE_CARD_ENABLE = _lark_env_truthy_or_default(
    "MONITORING_MESSAGE_CARD_ENABLE",
    default=True,  # unset must not imply plain-text-only (same as Platform)
)

# 群聊里富媒体等类型仍可能带可解析文本；仅跳过明显无 /monitoring 的类型。
_SKIP_IM_MESSAGE_TYPES = frozenset(
    {
        "image",
        "file",
        "audio",
        "media",
        "sticker",
        "location",
        "folder",
        "system",
        "hongbao",
        "share_chat",
        "share_user",
    }
)


def _lark_dict_pick_str(d: Any, *keys: str) -> str:
    """Lark payloads may use snake_case (HTTP) or camelCase (WebSocket / international)."""
    if not isinstance(d, dict):
        return ""
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _lark_message_chat_id(msg: Dict[str, Any]) -> str:
    """Group / topic chat id for ``create_message`` (``receive_id_type=chat_id``)."""
    cid = _lark_dict_pick_str(msg, "chat_id", "chatId", "open_chat_id", "openChatId")
    if cid:
        return cid
    c = msg.get("container")
    if isinstance(c, dict):
        return _lark_dict_pick_str(c, "chat_id", "chatId", "open_chat_id", "openChatId")
    return ""


def _lark_message_chat_id_aliases(msg: Dict[str, Any]) -> List[str]:
    """Collect all chat id aliases (``chat_id`` / ``open_chat_id`` from message and container)."""
    out: List[str] = []

    def _add(v: Any) -> None:
        s = (str(v).strip() if v is not None else "")
        if s and s not in out:
            out.append(s)

    if isinstance(msg, dict):
        for k in ("chat_id", "chatId", "open_chat_id", "openChatId"):
            _add(msg.get(k))
        c = msg.get("container")
        if isinstance(c, dict):
            for k in ("chat_id", "chatId", "open_chat_id", "openChatId"):
                _add(c.get(k))
    return out


def _lark_im_message_dedupe_id(msg: Dict[str, Any]) -> str:
    return _lark_dict_pick_str(
        msg, "message_id", "messageId", "open_message_id", "openMessageId"
    )


def _lark_im_payload_event_id(data: Dict[str, Any]) -> str:
    """Feishu may put ``event_id`` at top level, under ``header``, or under ``event`` depending on schema/version."""
    if not isinstance(data, dict):
        return ""
    top = _lark_dict_pick_str(data, "event_id", "eventId", "uuid")
    if top:
        return top
    h = data.get("header") if isinstance(data.get("header"), dict) else {}
    x = _lark_dict_pick_str(h, "event_id", "eventId", "event_uuid", "eventUuid", "uuid")
    if x:
        return x
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    return _lark_dict_pick_str(ev, "event_id", "eventId")


def _lark_im_message_time_token(msg: Dict[str, Any]) -> str:
    return _lark_dict_pick_str(msg, "create_time", "createTime", "update_time", "updateTime")


def _monitoring_processed_stick(
    mid: str,
    im_event_id: str,
    chat_id: str,
    sender_debounce: str,
    msg_time: str,
) -> str:
    """Stable id for ``_processed_lark_message_ids`` when ``message_id`` is missing in one POST duplicate."""
    m = (mid or "").strip()
    if m:
        return m
    e = (im_event_id or "").strip()
    if e:
        return f"evt:{e}"
    if (msg_time or "").strip() and ((chat_id or "").strip() or (sender_debounce or "").strip()):
        return f"tm:{(chat_id or '').strip()}:{msg_time.strip()}:{sender_debounce}"
    return ""


def _monitoring_try_begin_user_send(dispatch_key: str) -> bool:
    """
    Serialize user-visible sends for the same ``dispatch_key`` (HTTP double-post / race).
    Returns False if another send is in progress or completed within the coalesce window.
    """
    dk = (dispatch_key or "").strip()
    if not dk:
        return True
    sec = _cfg_float("MONITORING_SEND_COALESCE_SECONDS", 12.0)
    if sec <= 0:
        return True
    now = time.monotonic()
    with _monitoring_reply_dispatch_lock:
        if dk in _monitoring_user_send_in_progress:
            return False
        prev = _monitoring_user_reply_sent_at.get(dk, 0.0)
        if prev > 0.0 and (now - prev) < sec:
            return False
        _monitoring_user_send_in_progress.add(dk)
        if len(_monitoring_user_reply_sent_at) > 800:
            for k, t1 in sorted(_monitoring_user_reply_sent_at.items(), key=lambda kv: kv[1])[:300]:
                try:
                    del _monitoring_user_reply_sent_at[k]
                except KeyError:
                    pass
    return True


def _monitoring_end_user_send(dispatch_key: str, success: bool) -> None:
    dk = (dispatch_key or "").strip()
    if not dk:
        return
    with _monitoring_reply_dispatch_lock:
        _monitoring_user_send_in_progress.discard(dk)
        if success:
            _monitoring_user_reply_sent_at[dk] = time.monotonic()


def _monitoring_try_begin_chat_send(chat_key: str) -> bool:
    """
    Coarse safety gate by conversation key (chat/open_id).
    This blocks envelope variants that accidentally bypass dispatch-key dedupe.
    """
    ck = (chat_key or "").strip()
    if not ck:
        return True
    sec = _cfg_float("MONITORING_CHAT_COALESCE_SECONDS", 10.0)
    if sec <= 0:
        return True
    now = time.monotonic()
    with _monitoring_reply_dispatch_lock:
        if ck in _monitoring_chat_send_in_progress:
            return False
        prev = _monitoring_chat_reply_sent_at.get(ck, 0.0)
        if prev > 0.0 and (now - prev) < sec:
            return False
        _monitoring_chat_send_in_progress.add(ck)
        if len(_monitoring_chat_reply_sent_at) > 800:
            for k, t1 in sorted(_monitoring_chat_reply_sent_at.items(), key=lambda kv: kv[1])[:300]:
                try:
                    del _monitoring_chat_reply_sent_at[k]
                except KeyError:
                    pass
    return True


def _monitoring_end_chat_send(chat_key: str, success: bool) -> None:
    ck = (chat_key or "").strip()
    if not ck:
        return
    with _monitoring_reply_dispatch_lock:
        _monitoring_chat_send_in_progress.discard(ck)
        if success:
            _monitoring_chat_reply_sent_at[ck] = time.monotonic()


def _lark_skip_http_im_message_when_ws_mode() -> bool:
    if not _lark_env_truthy("LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS"):
        return False
    # HTTP sidecar is listening — always accept IM on POST /webhook/event (dedupe prevents double reply).
    if _cfg_str("ENABLE_HTTP", "1").strip().lower() in ("1", "true", "yes", "on"):
        return False
    if _cfg_str("LARK_EVENT_MODE", "http").strip().lower() != "ws":
        return False
    # Fail-safe: if WS path is configured but we haven't seen any WS DATA frame yet,
    # do not drop HTTP IM events (otherwise webhook returns 200 but bot never replies).
    if _lark_env_truthy("LARK_HTTP_IM_FALLBACK_WHEN_WS_NO_DATA") and not _lark_ws_saw_data_frame:
        logger.warning(
            "ws mode configured but no WS DATA frame observed yet — allowing HTTP IM fallback "
            "(set LARK_HTTP_IM_FALLBACK_WHEN_WS_NO_DATA=0 to force skip)."
        )
        return False
    grace = max(0.0, _cfg_float("LARK_HTTP_IM_WS_FALLBACK_GRACE_SECONDS", 120.0))
    global _lark_ws_last_im_monotonic
    last_im = float(_lark_ws_last_im_monotonic or 0.0)
    if grace > 0 and (last_im <= 0.0 or (time.monotonic() - last_im) > grace):
        logger.info(
            "webhook: HTTP IM allowed in ws mode — no im.message on WS for %.0fs (grace=%.0fs); "
            "Feishu may be POSTing IM to Request URL only",
            (time.monotonic() - last_im) if last_im > 0 else -1.0,
            grace,
        )
        return False
    return True


def _lark_ws_mark_im_received() -> None:
    global _lark_ws_last_im_monotonic
    _lark_ws_last_im_monotonic = time.monotonic()


def _lark_im_sender_debounce_token(sender: Dict[str, Any], open_id: str) -> str:
    u = _lark_dict_pick_str(sender, "union_id", "unionId")
    if u:
        return u
    o = (open_id or "").strip()
    if o:
        return o
    return _lark_dict_pick_str(sender, "user_id", "userId")


def _feishu_decrypt_encrypt_field(ciphertext_b64: str, encrypt_key: str) -> str:
    """Decrypt Lark ``encrypt`` field (AES-256-CBC + PKCS7), same as Feishu open-platform samples."""
    try:
        from Crypto.Cipher import AES
    except ImportError as e:
        raise ImportError("pip install pycryptodome") from e

    bs = AES.block_size
    key = hashlib.sha256(encrypt_key.encode("utf-8")).digest()
    enc = base64.b64decode(ciphertext_b64)
    iv = enc[:bs]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    raw = cipher.decrypt(enc[bs:])
    pad_len = raw[-1]
    if pad_len < 1 or pad_len > bs:
        raise ValueError("invalid PKCS7 padding")
    raw = raw[:-pad_len]
    return raw.decode("utf-8")


def _feishu_maybe_decrypt_webhook_payload(raw: Any) -> Any:
    """
    When 开发者后台 → 事件与回调 enables Encrypt Key, POST body is only ``{"encrypt":"..."}``.
    Set LARK_ENCRYPT_KEY to the same key (or turn encryption off in console).
    """
    if not isinstance(raw, dict) or "encrypt" not in raw:
        return raw
    if not LARK_ENCRYPT_KEY:
        logger.warning(
            "Lark POST has `encrypt` but LARK_ENCRYPT_KEY is unset — "
            "set it or disable encryption in 事件与回调; events will be ignored."
        )
        return raw
    try:
        plain = _feishu_decrypt_encrypt_field(str(raw["encrypt"]), LARK_ENCRYPT_KEY)
        if plain.startswith("\ufeff"):
            plain = plain.lstrip("\ufeff")
        return json.loads(plain)
    except ImportError as e:
        logger.error("%s — encrypted webhooks need pycryptodome.", e)
        return raw
    except Exception as e:
        logger.exception("Lark decrypt failed: %s", e)
        return raw


def _lark_legacy_event_callback_message_to_v2(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Old ``type: event_callback`` + ``event.type: message`` → schema-2-like dict for one code path."""
    if data.get("type") != "event_callback":
        return None
    ev = data.get("event")
    if not isinstance(ev, dict) or ev.get("type") != "message":
        return None
    token = str(data.get("token") or (data.get("header") or {}).get("token") or "")
    chat_id = ev.get("open_chat_id") or ev.get("chat_id") or ""
    text_raw = ev.get("text_without_at_bot") or ev.get("text") or ""
    if not text_raw and ev.get("content"):
        try:
            c = json.loads(ev["content"])
            text_raw = c.get("text") or ""
        except (json.JSONDecodeError, TypeError):
            text_raw = ""
    msg_type = (ev.get("msg_type") or "text").lower()
    return {
        "schema": "2.0",
        "header": {"event_type": "im.message.receive_v1", "token": token},
        "event": {
            "message": {
                "chat_id": chat_id,
                "chat_type": ev.get("chat_type") or "group",
                "message_type": "text" if msg_type == "text" else msg_type,
                "content": json.dumps({"text": text_raw}),
                "mentions": ev.get("mentions") or [],
            },
            "sender": {"sender_id": {"open_id": ev.get("open_id") or ""}},
        },
    }


def _lark_normalize_webhook(data: Dict[str, Any]) -> Dict[str, Any]:
    legacy = _lark_legacy_event_callback_message_to_v2(data)
    return legacy if legacy else data


def _lark_safe_parse_json_body(req: Any) -> Optional[Dict[str, Any]]:
    """Prefer ``get_json``; fallback to raw body (some proxies strip / alter Content-Type). Same idea as Chatbox."""
    raw = req.get_json(silent=True)
    if isinstance(raw, dict):
        return raw
    b = req.get_data(cache=False)
    if not b:
        return None
    if b.startswith(b"\xef\xbb\xbf"):
        b = b[3:]
    try:
        parsed = json.loads(b.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _lark_is_schema_v2(data: Any) -> bool:
    """Schema may arrive as str ``2.0`` or occasionally non-string — same guard as Chatbox."""
    if not isinstance(data, dict):
        return False
    s = data.get("schema")
    return s == "2.0" or str(s).strip() == "2.0"


def _lark_looks_like_lark_card_update_credential(token_str: Any) -> bool:
    """
    Flat ``card.action.trigger_v1`` uses top-level ``token`` = card credential (``c-``/``d-``), not Verification Token.
    Do not treat that as verification (Chatbox :func:`_lark_looks_like_lark_card_update_credential`).
    """
    s = (str(token_str or "")).strip()
    if not s:
        return False
    return s.startswith("c-") or s.startswith("d-")


def _lark_extract_verification_token(data: Any) -> Optional[str]:
    """
    App Verification Token: schema 2.0 ``header.token``; some payloads ``verification_token``.
    Same extraction order as Chatbox :func:`_lark_extract_verification_token`.
    """
    if not isinstance(data, dict):
        return None
    h = data.get("header")
    if isinstance(h, dict):
        for key in ("token", "Token", "verification_token"):
            t = h.get(key)
            if t is not None:
                return str(t).strip()
    vt = data.get("verification_token")
    if vt is not None:
        return str(vt).strip()
    t2 = data.get("token")
    if t2 is None:
        return None
    ts = str(t2).strip()
    if _lark_looks_like_lark_card_update_credential(ts):
        return None
    return ts


def _lark_coerce_event_dict(data: Any) -> Any:
    """Some gateways deliver ``event`` as a JSON string — normalize to dict (Chatbox :func:`_lark_coerce_event_dict`)."""
    if not isinstance(data, dict):
        return data
    ev = data.get("event")
    if isinstance(ev, str):
        try:
            parsed = json.loads(ev)
            data["event"] = parsed if isinstance(parsed, dict) else {}
        except Exception:
            data["event"] = {}
    elif ev is None and isinstance(data, dict):
        het = _lark_header_event_type(data)
        if isinstance(het, str) and het.startswith("card.action"):
            data["event"] = {}
    return data


def _lark_header_event_type(data: Dict[str, Any]) -> str:
    """``header.event_type`` or top-level ``event_type`` (proxies sometimes flatten the body)."""
    h = data.get("header")
    if isinstance(h, dict):
        et = h.get("event_type")
        if et is not None:
            return str(et).strip()
    et2 = data.get("event_type")
    if et2 is not None:
        return str(et2).strip()
    return ""


def _lark_collect_post_text(obj: Any, out: List[str]) -> None:
    """Depth-first collect human text from rich post / mixed blocks."""
    if isinstance(obj, dict):
        tag = obj.get("tag")
        if tag == "text" and "text" in obj:
            t = obj.get("text")
            if t is not None:
                out.append(str(t))
        elif tag in ("a", "code") and "text" in obj:
            t = obj.get("text")
            if t is not None:
                out.append(str(t))
        for v in obj.values():
            _lark_collect_post_text(v, out)
    elif isinstance(obj, list):
        for x in obj:
            _lark_collect_post_text(x, out)


def _lark_extract_plain_text_from_message(msg: Dict[str, Any]) -> str:
    """Support ``text`` and rich ``post`` bodies (common when @mentioning in mobile clients)."""
    if not isinstance(msg, dict):
        return ""
    raw_c = msg.get("content")
    if raw_c is None:
        raw_c = msg.get("Content")
    if raw_c is None:
        raw_c = msg.get("body")
    if isinstance(raw_c, dict):
        content_str = json.dumps(raw_c, ensure_ascii=False)
    elif isinstance(raw_c, str):
        content_str = raw_c or "{}"
    else:
        content_str = "{}"
    mtype = (_lark_dict_pick_str(msg, "message_type", "messageType") or "").lower()
    try:
        obj = json.loads(content_str)
    except (json.JSONDecodeError, TypeError):
        return ""

    if not mtype:
        if "text" in obj and isinstance(obj.get("text"), str):
            mtype = "text"
        elif any(k in obj for k in ("zh_cn", "en_us", "ja_jp")) or isinstance(obj.get("content"), list):
            mtype = "post"

    if mtype == "text":
        return obj.get("text") or ""

    if mtype == "post":
        for locale_key in ("zh_cn", "en_us", "ja_jp"):
            block = obj.get(locale_key)
            if not isinstance(block, dict):
                continue
            parts: List[str] = []
            for row in block.get("content") or []:
                if isinstance(row, list):
                    for cell in row:
                        if isinstance(cell, dict) and cell.get("tag") == "text":
                            parts.append(cell.get("text") or "")
                elif isinstance(row, dict) and row.get("tag") == "text":
                    parts.append(row.get("text") or "")
            if parts:
                return "".join(parts)
        parts2: List[str] = []
        _lark_collect_post_text(obj, parts2)
        if parts2:
            return "".join(parts2)
        return obj.get("text") or ""

    parts3: List[str] = []
    _lark_collect_post_text(obj, parts3)
    if parts3:
        return "".join(parts3)
    return obj.get("text") or ""


def _lark_collect_im_message_mentions(msg: Dict[str, Any], event: Dict[str, Any]) -> List[Any]:
    """
    Merge @mention metadata from ``message``, ``event``, and parsed ``content`` JSON.

    HTTP ``im.message`` payloads sometimes omit ``message.mentions`` while still encoding
    ``{"text":"@_user_1 ...","mentions":[...]}`` inside ``content`` — without this, @-gates see an empty list.
    """
    out: List[Any] = []
    seen: Set[str] = set()

    def _add(lst: Any) -> None:
        if not isinstance(lst, list):
            return
        for m in lst:
            if not isinstance(m, dict):
                continue
            sig = json.dumps(m, sort_keys=True, ensure_ascii=False)
            if sig in seen:
                continue
            seen.add(sig)
            out.append(m)

    if isinstance(msg, dict):
        _add(msg.get("mentions"))
        _add(msg.get("Mentions"))
    if isinstance(event, dict):
        _add(event.get("mentions"))
        _add(event.get("Mentions"))
    raw_c = None
    if isinstance(msg, dict):
        raw_c = msg.get("content")
        if raw_c is None:
            raw_c = msg.get("Content")
    if isinstance(raw_c, dict):
        _add(raw_c.get("mentions"))
    elif isinstance(raw_c, str):
        try:
            obj = json.loads(raw_c or "{}")
            if isinstance(obj, dict):
                _add(obj.get("mentions"))
        except (json.JSONDecodeError, TypeError):
            pass
    return out


def _lark_raw_text_has_feishu_at_placeholder(raw_text: str) -> bool:
    """HTTP payloads often omit ``mentions`` but keep ephemeral ``@_user_N`` tokens in the text body."""
    return bool(re.search(r"@_user_\d+", raw_text or ""))


def _lark_clean_command_text(raw_text: str, mentions: Any) -> str:
    """Remove @ placeholders so ``/monitoring`` survives after <at>...</at> blocks."""
    text = raw_text or ""
    if isinstance(mentions, list):
        for m in mentions:
            if isinstance(m, dict):
                k = m.get("key")
                if k:
                    text = text.replace(str(k), "")
    text = re.sub(r"@_user_\d+", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[\u200b-\u200f\u2060\uFEFF\u00A0]", "", text)
    for ch in ("\uff0f", "\u2215", "\u2044", "\u29f8"):
        text = text.replace(ch, "/")
    text = text.replace("／", "/").replace("＼", "\\")
    text = re.sub(r"\s+", " ", text).strip()
    # Rich-text-only payloads may leave ``@Bot Display Name`` before ``/mo`` when ``mentions=[]``.
    triggers = sorted(
        {
            ((MONITORING_TRIGGER or "").strip() or "/mo"),
            ((MONITORING_MUTE_TRIGGER or "").strip() or "/m"),
            ((MONITORING_CANCELMUTE_TRIGGER or "").strip() or "/c"),
            ((DEPLOY_TRIGGER or "").strip() or "/deploy"),
        },
        key=len,
        reverse=True,
    )
    for tri in triggers:
        if len(tri) < 2 or not tri.startswith("/"):
            continue
        m_cmd = re.search(re.escape(tri) + r"(?:\s|$)", text, flags=re.IGNORECASE)
        if m_cmd:
            return text[m_cmd.start() :].strip()
    tl = text.casefold()
    for tri in triggers:
        if len(tri) < 2 or not tri.startswith("/"):
            continue
        tri_cf = tri.casefold()
        start = 0
        while True:
            pos = tl.find(tri_cf, start)
            if pos < 0:
                break
            prev_ok = pos == 0 or tl[pos - 1].isspace()
            endpos = pos + len(tri_cf)
            next_ok = endpos >= len(tl) or tl[endpos].isspace()
            if prev_ok and next_ok:
                return text[pos:].strip()
            start = pos + 1
    return text


def _im_command_matches(clean: str, cmd: str) -> bool:
    """
    True when ``clean`` is exactly ``cmd`` or starts with ``cmd`` + whitespace.
    Avoids ``/mo`` being mistaken for ``/m`` (prefix match without boundary).
    """
    c = re.sub(r"\s+", " ", (clean or "").strip().lower())
    tri = (cmd or "").strip().lower()
    if not tri or not c:
        return False
    if c == tri:
        return True
    return c.startswith(tri + " ") or c.startswith(tri + "\t")


def _text_has_monitoring_trigger(raw_text: str, clean: str) -> bool:
    _ = raw_text
    return _im_command_matches(clean or "", MONITORING_TRIGGER)


def _lark_iter_mention_scalar_strings(obj: Any, depth: int = 0, *, max_depth: int = 8) -> Iterator[str]:
    """Yield stripped strings from nested dict/list (Feishu mention shapes evolve; ids may sit deeper than ``id.open_id``)."""
    if depth > max_depth:
        return
    if isinstance(obj, str):
        s = obj.strip()
        if s:
            yield s
        return
    if isinstance(obj, bool):
        return
    if isinstance(obj, int):
        yield str(obj)
        return
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _lark_iter_mention_scalar_strings(v, depth + 1, max_depth=max_depth)
    elif isinstance(obj, list):
        for it in obj:
            yield from _lark_iter_mention_scalar_strings(it, depth + 1, max_depth=max_depth)


def _lark_message_mentions_bot(mentions: Any) -> bool:
    """True when ``mentions`` includes this app bot (``LARK_BOT_OPEN_ID`` or ``bot/v3/info``), or ``APP_ID`` on mention."""
    if not isinstance(mentions, list) or not mentions:
        return False
    app = (str(APP_ID).strip() if APP_ID else "") or ""
    bot = _lark_effective_bot_open_id()
    canon_self = _monitoring_canonical_open_id_match_set()
    if not bot and not app and not canon_self:
        return False
    for m in mentions:
        if not isinstance(m, dict):
            continue
        row_oid = _lark_mention_row_main_open_id(m)
        if row_oid and row_oid in canon_self:
            return True
        if app:
            for ak in ("app_id", "appId"):
                av = m.get(ak)
                if av and str(av).strip() == app:
                    return True
        ido = m.get("id")
        if isinstance(ido, str) and bot and ido.strip() == bot:
            return True
        if isinstance(ido, dict):
            if app:
                for ak in ("app_id", "appId"):
                    av = ido.get(ak)
                    if av and str(av).strip() == app:
                        return True
            if bot:
                for k in ("open_id", "openId", "user_id", "userId", "union_id", "unionId"):
                    v = ido.get(k)
                    if v and str(v).strip() == bot:
                        return True
        if bot:
            for k in ("open_id", "openId", "user_id", "userId"):
                v = m.get(k)
                if v and str(v).strip() == bot:
                    return True
        # Nested / newer payload shapes (still one mention entity — avoid missing open_id under unknown keys).
        if bot or app or canon_self:
            n = 0
            for s in _lark_iter_mention_scalar_strings(m):
                n += 1
                if n > 200:
                    break
                if bot and s == bot:
                    return True
                if app and s == app:
                    return True
                if (
                    canon_self
                    and _lark_string_is_strong_feishu_at_target(s)
                    and s in canon_self
                ):
                    return True
    return False


def _lark_im_bot_addressed_in_mentions_or_body(
    mentions: Any,
    content_at_entity_ids: Optional[List[str]],
) -> bool:
    """
    True when ``mentions[]`` encodes this bot **or** parsed body/post lists this app's canonical ``open_id``
    / ``APP_ID``. Mobile ``post`` messages often omit usable ``mentions[]`` while ``{\"tag\":\"at\"}`` cells
    carry ``user_id``.
    """
    if _lark_message_mentions_bot(mentions):
        return True
    canon = _monitoring_canonical_open_id_match_set()
    app = str(APP_ID or "").strip()
    for x in content_at_entity_ids or []:
        s = str(x).strip()
        if not s:
            continue
        if s in canon:
            return True
        if app and s == app:
            return True
    return False


def _lark_mentions_any_row_matches_app(mentions_list: List[Any], app: str) -> bool:
    """
    True when any ``mentions[]`` row carries this Lark app's ``app_id``.

    Feishu occasionally binds ``@_user_N`` / ``open_id`` to a peer bot while the row still encodes this
    app's ``app_id`` — strict ``primary open_id`` would skip incorrectly.
    """
    ap = (app or "").strip()
    if not ap:
        return False
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        for ak in ("app_id", "appId"):
            if str(m.get(ak) or "").strip() == ap:
                return True
        ido = m.get("id")
        if isinstance(ido, dict):
            for ak in ("app_id", "appId"):
                if str(ido.get(ak) or "").strip() == ap:
                    return True
    return False


def _lark_collect_mention_identity_strings_for_at_conflict(m: dict) -> List[str]:
    """Id-like strings from ``id`` / standard keys only (skip ``name`` / ``tenant_key`` subtrees)."""
    out: List[str] = []
    skip = frozenset({"tenant_key", "tenantKey", "name", "Name"})

    def walk(o: Any, depth: int = 0) -> None:
        if depth > 10:
            return
        if isinstance(o, str):
            s = o.strip()
            if s:
                out.append(s)
            return
        if isinstance(o, bool):
            return
        if isinstance(o, int):
            out.append(str(o))
            return
        if not isinstance(o, dict):
            return
        for k, v in o.items():
            if k in skip:
                continue
            walk(v, depth + 1)

    ido = m.get("id")
    if isinstance(ido, str) and ido.strip():
        out.append(ido.strip())
    elif isinstance(ido, dict):
        walk(ido, 0)
    for k in ("open_id", "openId", "user_id", "userId", "union_id", "unionId", "app_id", "appId"):
        v = m.get(k)
        if v is None or isinstance(v, bool):
            continue
        if isinstance(v, int):
            out.append(str(v))
        elif isinstance(v, str) and v.strip():
            out.append(v.strip())
    return out


def _lark_string_is_strong_feishu_at_target(s: str) -> bool:
    """
    User/bot ``open_id`` (``ou_``) or app id (``cli_``) for the @ target.

    Do **not** treat ``oc_`` (chat) / ``om_`` (message) / ``on_`` as @ targets — Feishu often copies
    those into ``mentions`` JSON and would falsely block ``@_user_N`` ``/mo`` fallback.
    """
    x = (s or "").strip()
    return bool(x) and x.startswith(("ou_", "cli_"))


def _lark_mentions_carry_strong_identity_other_than_bot(bot: str, app: str, mentions: Any) -> bool:
    """
    True if some mention carries a strong id (``ou_``/``cli_``) that clearly targets **another** app/bot.

    Weak payloads (only ``@_user_N`` / display name) → False so ``@_user_N`` placeholder can still fire ``/mo``.

    When **bot open_id is unknown** (empty ``LARK_BOT_OPEN_ID`` and ``bot/v3/info`` failed), any ``ou_`` in
    the payload might still be this bot — we **do not** treat ``ou_`` as conflicting in that case; only a
    ``cli_`` different from ``APP_ID`` blocks (otherwise ``mentions_other_ou_cli`` stays stuck True forever).
    """
    if not isinstance(mentions, list):
        return False
    bot = (bot or "").strip()
    app_s = (str(app).strip() if app else "") or ""
    for m in mentions:
        if not isinstance(m, dict):
            continue
        for s in _lark_collect_mention_identity_strings_for_at_conflict(m):
            if not _lark_string_is_strong_feishu_at_target(s):
                continue
            if bot and s == bot:
                continue
            if app_s and s == app_s:
                continue
            if not bot:
                if s.startswith("cli_") and (not app_s or s != app_s):
                    return True
                continue
            return True
    return False


_LARK_AT_ENTITY_ID_IN_CONTENT_RE = re.compile(
    r"<at\b[^>]*?\b(?:user_id|open_id|openId|userId)\s*=\s*[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
_LARK_AT_ID_ATTR_OU_CLI_RE = re.compile(
    r"<at\b[^>]*?\bid\s*=\s*[\"']((?:ou_|cli_)[^\"']+)[\"']",
    re.IGNORECASE,
)


def _lark_im_content_blobs_for_at_parse(msg: Dict[str, Any]) -> List[str]:
    """
    User-visible text blobs that may contain ``<at …>`` tags.

    Do **not** scan ``json.dumps(content)`` or the raw JSON envelope: Feishu may embed duplicate
    ``<at user_id=…>`` fragments under metadata keys that serialize **before** the real ``text`` field,
    so a whole-blob regex falsely picks another bot as primary @.
    """
    blobs: List[str] = []
    if not isinstance(msg, dict):
        return blobs
    vis = _lark_extract_plain_text_from_message(msg)
    if (vis or "").strip():
        blobs.append(vis)
    for k in ("text", "Text", "body"):
        v = msg.get(k)
        if not isinstance(v, str) or not v.strip():
            continue
        if v == vis:
            continue
        blobs.append(v)
    # Escaped JSON string sometimes keeps ``<at user_id=…>`` while extracted plain text is placeholder-only.
    for k in ("content", "Content"):
        v = msg.get(k)
        if not isinstance(v, str) or not v.strip():
            continue
        if v in blobs:
            continue
        blobs.append(v)
    return blobs


_LARK_AT_OPEN_TAG_RE = re.compile(r"<at\b[^>]*>", re.IGNORECASE)


def _lark_ordered_strong_ids_from_at_tags(blob: str) -> List[str]:
    """Strong ``ou_``/``cli_`` ids in **document order** (first ``<at…>`` first)."""
    ordered: List[str] = []
    seen: Set[str] = set()
    for tm in _LARK_AT_OPEN_TAG_RE.finditer(blob or ""):
        tag = tm.group(0) or ""
        for m in _LARK_AT_ENTITY_ID_IN_CONTENT_RE.finditer(tag):
            s = m.group(1).strip()
            if s and _lark_string_is_strong_feishu_at_target(s) and s not in seen:
                seen.add(s)
                ordered.append(s)
        for m in _LARK_AT_ID_ATTR_OU_CLI_RE.finditer(tag):
            s = m.group(1).strip()
            if s and s not in seen:
                seen.add(s)
                ordered.append(s)
    return ordered


def _lark_im_message_has_visible_strong_at_html(msg: Optional[Dict[str, Any]]) -> bool:
    """True when IM body blobs include ``<at …>`` carrying a strong ``ou_``/``cli_`` id."""
    if not isinstance(msg, dict):
        return False
    for blob in _lark_im_content_blobs_for_at_parse(msg):
        if _lark_ordered_strong_ids_from_at_tags(blob):
            return True
    return False


def _lark_primary_strong_at_from_im_message(
    msg: Optional[Dict[str, Any]],
    mentions_list: Optional[List[Any]] = None,
) -> Optional[str]:
    """First strong bot/user id from ``<at>`` tags or post JSON ``{\"tag\":\"at\"}`` cells (document order)."""
    if not isinstance(msg, dict):
        return None
    ml = (
        mentions_list
        if mentions_list is not None
        else _lark_collect_im_message_mentions(msg, {})
    )
    for blob in _lark_im_content_blobs_for_at_parse(msg):
        ids = _lark_ordered_strong_ids_from_at_tags(blob)
        if ids:
            return ids[0]
    root = _lark_im_parsed_content_root(msg)
    if isinstance(root, dict):
        ordered = _lark_ordered_post_at_strong_ids_from_root(root, ml)
        if ordered:
            return ordered[0]
        post_first: List[str] = []
        post_seen: Set[str] = set()
        _lark_collect_post_at_user_ids(root, post_first, post_seen, mentions_list=ml)
        if post_first:
            return post_first[0]
    return None


def _lark_visible_bot_like_at_chain(
    msg: Optional[Dict[str, Any]],
    bot_like_bag: Set[str],
    mentions_list: Optional[List[Any]] = None,
) -> List[str]:
    """Strong ``ou_``/``cli_`` from visible ``<at>`` tags (document order), filtered to ``bot_like_bag``."""
    if not isinstance(msg, dict) or not bot_like_bag:
        return []
    out: List[str] = []
    for blob in _lark_im_content_blobs_for_at_parse(msg):
        for x in _lark_ordered_strong_ids_from_at_tags(blob):
            t = str(x).strip()
            if t and _lark_string_is_strong_feishu_at_target(t) and t in bot_like_bag:
                out.append(t)
        if out:
            return out
    ml = (
        mentions_list
        if mentions_list is not None
        else _lark_collect_im_message_mentions(msg, {})
    )
    root = _lark_im_parsed_content_root(msg)
    if isinstance(root, dict):
        for x in _lark_ordered_post_at_strong_ids_from_root(root, ml):
            t = str(x).strip()
            if t and _lark_string_is_strong_feishu_at_target(t) and t in bot_like_bag:
                out.append(t)
        if out:
            return out
    return []


def _lark_primary_strong_from_mentions_order(mentions_list: List[Any]) -> Optional[str]:
    """First strong ``ou_``/``cli_`` in ``mentions[]`` iteration order (fallback when body tags omit ids)."""
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        for s in _lark_collect_mention_identity_strings_for_at_conflict(m):
            t = str(s).strip()
            if t and _lark_string_is_strong_feishu_at_target(t):
                return t
    return None


def _lark_distinct_strong_bot_like_ids_in_mentions(
    mentions_list: List[Any],
    *,
    canon_ids: Set[str],
    peer_ids: Set[str],
) -> Set[str]:
    """Strong ids in mentions that look like this app or a configured peer bot (multi-bot guard)."""
    out: Set[str] = set()
    bag = canon_ids | peer_ids
    if not bag:
        return out
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        for s in _lark_collect_mention_identity_strings_for_at_conflict(m):
            t = str(s).strip()
            if t and _lark_string_is_strong_feishu_at_target(t) and t in bag:
                out.add(t)
    return out


def _lark_primary_strong_from_feishu_user_placeholders(
    raw_text: str, mentions_list: List[Any]
) -> Optional[str]:
    """
    Resolve the first ``@_user_N`` in visible text to a strong ``open_id``.

    Uses ``mentions[].key`` / ``mention_key`` when present; otherwise maps ``@_user_N`` to ``mentions[N-1]`` (1-based).
    """
    if not raw_text or not mentions_list:
        return None
    key_to_oid: Dict[str, str] = {}
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        oid = _lark_mention_row_main_open_id(m)
        if not oid or not _lark_string_is_strong_feishu_at_target(oid):
            continue
        for fk in ("key", "Key", "mention_key", "mentionKey"):
            k = m.get(fk)
            if not k:
                continue
            ks = str(k).strip()
            if ks:
                key_to_oid[ks] = oid
                if ks.startswith("@"):
                    key_to_oid.setdefault(ks[1:], oid)
                elif ks.startswith("_user_"):
                    key_to_oid.setdefault("@" + ks, oid)

    for mm in re.finditer(r"@_user_(\d+)", raw_text):
        tok = mm.group(0)
        n = int(mm.group(1))
        idx = n - 1
        oid_idx: Optional[str] = None
        if 0 <= idx < len(mentions_list):
            mrow = mentions_list[idx]
            if isinstance(mrow, dict):
                o2 = _lark_mention_row_main_open_id(mrow)
                if o2 and _lark_string_is_strong_feishu_at_target(o2):
                    oid_idx = o2
        oid_key = key_to_oid.get(tok) or key_to_oid.get(tok.lstrip("@"))
        oid: Optional[str] = oid_key or oid_idx
        if oid:
            return oid
    return None


def _lark_primary_strong_from_mentions_visible_order(
    raw_text: str, mentions_list: List[Any]
) -> Optional[str]:
    """
    Feishu often shows ``@_user_N`` in visible text while ``mentions[]`` array order **does not** match the UI.
    Pick the strong ``open_id`` whose mention ``key`` (or ``@name``) appears **leftmost** in ``raw_text``.
    """
    if not raw_text or not mentions_list:
        return None
    rt = raw_text
    candidates: List[Tuple[int, str]] = []
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        keys_to_try: List[str] = []
        for fk in ("key", "Key", "mention_key", "mentionKey"):
            k = m.get(fk)
            if not k:
                continue
            ks = str(k).strip()
            if not ks:
                continue
            keys_to_try.append(ks)
            if ks.startswith("_user_"):
                keys_to_try.append("@" + ks)
        nm = m.get("name") or m.get("Name")
        if nm:
            keys_to_try.append(f"@{nm}")
        pos: Optional[int] = None
        for kt in keys_to_try:
            p = rt.find(kt)
            if p >= 0 and (pos is None or p < pos):
                pos = p
        if pos is None:
            continue
        oid = _lark_mention_row_main_open_id(m)
        if oid and _lark_string_is_strong_feishu_at_target(oid):
            candidates.append((pos, oid))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _monitoring_resolved_primary_at_target(
    msg: Optional[Dict[str, Any]],
    mentions_list: List[Any],
    raw_text: str = "",
    *,
    bot_like_bag: Optional[Set[str]] = None,
) -> Optional[str]:
    """
    Resolve primary @ target for shared multi-bot groups:

    1. When **two or more** bot-like ids appear in ``mentions[]``, prefer **document-order** strong ``<at …>`` /
       post cells filtered to the canon ∪ peer bag.
    2. Else **rich-text / post body**: first strong ``<at …>`` whose id is in the bot_like bag — **before**
       ``@_user_N`` → ``mentions[]`` mapping (Feishu often binds ``@_user_1`` to the wrong row while HTML keeps the tapped bot).
    3. Leftmost visible ``key`` / ``mention_key`` / ``@name``.
    4. First ``@_user_N`` via ``mentions[].key`` / index; then ``mentions[]`` row order.

    Set ``MONITORING_LOG_PRIMARY_AT=1`` for one-line diagnostics on the server.
    """
    rt = raw_text or ""
    if isinstance(msg, dict):
        alt = _lark_extract_plain_text_from_message(msg) or ""
        if "@_user_" in alt:
            rt = alt
        elif not (rt or "").strip():
            rt = alt

    distinct_bot_like: Set[str] = set()
    body_chain: List[str] = []
    if bot_like_bag:
        distinct_bot_like = _lark_distinct_strong_bot_like_ids_in_mentions(
            mentions_list,
            canon_ids=bot_like_bag,
            peer_ids=bot_like_bag,
        )
        if len(distinct_bot_like) >= 2:
            body_chain = _lark_visible_bot_like_at_chain(msg, bot_like_bag, mentions_list)

    body_first_bot_like = _lark_primary_strong_at_from_im_message(msg, mentions_list)
    vis_early = _lark_primary_strong_from_mentions_visible_order(rt, mentions_list)
    ph = _lark_primary_strong_from_feishu_user_placeholders(rt, mentions_list)

    if MONITORING_LOG_PRIMARY_AT:
        logger.info(
            "monitoring primary-at dbg rt=%r mentions_n=%s distinct_bot_like=%s body_at_chain=%s "
            "body_first_bot_like=%s ph=%s vis_early=%s",
            (rt[:200] + ("…" if len(rt) > 200 else "")),
            len(mentions_list),
            sorted(distinct_bot_like),
            body_chain,
            body_first_bot_like,
            ph,
            vis_early,
        )

    if bot_like_bag and len(distinct_bot_like) >= 2 and body_chain:
        return body_chain[0]

    if (
        body_first_bot_like
        and bot_like_bag
        and body_first_bot_like in bot_like_bag
    ):
        return body_first_bot_like

    if vis_early:
        return vis_early
    if ph:
        return ph
    if body_first_bot_like:
        return body_first_bot_like
    return _lark_primary_strong_from_mentions_order(mentions_list)


def _lark_mention_row_main_open_id(m: dict) -> str:
    """Chatbox-style: ``mentions[].id.open_id`` for one @ row."""
    ido = m.get("id")
    if isinstance(ido, dict):
        return str(ido.get("open_id") or ido.get("openId") or "").strip()
    if isinstance(ido, str):
        return ido.strip()
    return ""


def _lark_ordered_strong_open_ids_from_mentions_rows(mentions_list: List[Any]) -> List[str]:
    """Strong ``ou_``/``cli_`` open ids in Feishu ``mentions[]`` order (one per row)."""
    out: List[str] = []
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        oid = _lark_mention_row_main_open_id(m)
        if oid and _lark_string_is_strong_feishu_at_target(oid):
            out.append(oid)
    return out


def _monitoring_at_gate_reason_set(msg: str) -> None:
    _monitoring_at_gate_tls.reason = (msg or "").strip()


def _monitoring_at_gate_reason_take() -> str:
    r = getattr(_monitoring_at_gate_tls, "reason", "") or ""
    _monitoring_at_gate_tls.reason = ""
    return r


def _monitoring_at_gate_reason_clear() -> None:
    _monitoring_at_gate_tls.reason = ""


def _monitoring_send_at_gate_feedback_worker(
    chat_id: str, open_id: str, text: str, debounce_key: str
) -> None:
    try:
        if (chat_id or "").strip():
            _lark_send_text("chat_id", chat_id.strip(), text)
        elif (open_id or "").strip():
            _lark_send_text("open_id", open_id.strip(), text)
    except Exception:
        logger.exception("at-gate user feedback send failed")
    finally:
        if debounce_key:
            with _monitoring_reply_dispatch_lock:
                _monitoring_inflight_keys.discard(debounce_key)


def _monitoring_maybe_send_at_gate_feedback(
    *,
    chat_id: str,
    open_id: str,
    clean: str,
    raw_text: str,
    chat_type: str,
) -> None:
    """When /mo was seen but @ gate rejected, tell the user why (not silent ignore)."""
    if not MONITORING_AT_GATE_USER_FEEDBACK:
        return
    if not _text_has_monitoring_trigger(raw_text, clean):
        return
    reason = _monitoring_at_gate_reason_take()
    if not reason:
        ct = (chat_type or "").strip().lower()
        if ct in ("group", "topic") and MONITORING_TRIGGER_REQUIRES_AT_BOT:
            reason = (
                "In group chats, @ **Grafana Game Bot** (not Platform Bot) then /mo. "
                "Bare /mo without @ only works in private (PM) chat."
            )
        else:
            reason = "Could not confirm this /mo was addressed to Grafana Game Bot."
    tri = (MONITORING_TRIGGER or "/mo").strip()
    text = f"{tri} skipped: {reason}"
    debounce_key = f"{(chat_id or '').strip() or ('open:' + (open_id or '').strip())}\n__at_gate_fb__"
    with _monitoring_reply_dispatch_lock:
        if debounce_key in _monitoring_inflight_keys:
            return
        _monitoring_inflight_keys.add(debounce_key)
    threading.Thread(
        target=_monitoring_send_at_gate_feedback_worker,
        args=(chat_id, open_id, text, debounce_key),
        daemon=True,
        name="at-gate-feedback",
    ).start()


def _monitoring_group_multi_bot_first_mention_gate(
    *,
    chat_type: str,
    mentions_list: List[Any],
    primary: Optional[str],
    canon_ids: Set[str],
) -> bool:
    """
    Chatbox ``main.py`` only checks ``mention.open_id == BOT_OPEN_ID``; with **two** Lark apps in one group,
    Feishu often puts **both** bot ``open_id``\\ s in ``mentions[]``, so each app still sees itself as mentioned.

    For **group/topic** chats, when ``mentions[]`` contains **2+** bot-like ids (union of canonical + peer sets),
    require the **first** such id to belong to **this** app, unless ``primary`` (body ``<at>``) already picks a
    bot-like target — then **primary** must be in ``canon_ids``.

    **Requires** each bot's ``MONITORING_PEER_BOT_OPEN_IDS`` to list the other app's bot ``ou_``\\ s; otherwise
    the second bot id may be invisible to this gate.
    """
    ct = (chat_type or "").strip().lower()
    if ct not in ("group", "topic"):
        return True
    if not canon_ids:
        return True
    bot_bag = canon_ids | MONITORING_PEER_BOT_OPEN_ID_SET
    if len(bot_bag) < 2:
        return True
    ordered = _lark_ordered_strong_open_ids_from_mentions_rows(mentions_list)
    bots_in_order = [x for x in ordered if x in bot_bag]
    if len(bots_in_order) < 2:
        return True
    if primary and primary in bot_bag:
        ok = primary in canon_ids
        if not ok:
            logger.info(
                "monitoring: skip (group multi-bot) — primary %r is another bot / not canonical",
                primary,
            )
            _monitoring_at_gate_reason_set(
                "Primary @ is not Game Bot in a group with two bots — @ Grafana Game Bot, then /mo."
            )
        return ok
    first = bots_in_order[0]
    if first not in canon_ids:
        logger.info(
            "monitoring: skip (group multi-bot, Chatbox-order) — first bot-like mention %r not this app; "
            "order=%s (set MONITORING_PEER_BOT_OPEN_IDS on both bots)",
            first,
            bots_in_order,
        )
        _monitoring_at_gate_reason_set(
            "Platform Bot is listed first in Feishu mentions — @ Grafana Game Bot **before** typing /mo."
        )
        return False
    return True


def _lark_im_parsed_content_root(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parsed ``message.content`` / ``body`` JSON object when it is (or parses to) a dict."""
    raw_c = msg.get("content") or msg.get("Content") or msg.get("body")
    if isinstance(raw_c, dict):
        return raw_c
    if isinstance(raw_c, str):
        raw_cs = raw_c.strip()
        if not raw_cs:
            return None
        try:
            o = json.loads(raw_cs)
            return o if isinstance(o, dict) else None
        except (json.JSONDecodeError, TypeError):
            return None
    return None


_LARK_POST_DOC_ORDER_KEYS_FIRST: Tuple[str, ...] = (
    "content",
    "elements",
    "children",
    "rows",
    "body",
)
_LARK_POST_SKIP_RECURSE_KEYS = frozenset({"mentions", "Mentions"})


def _lark_post_cell_raw_at_ref(cell: Dict[str, Any]) -> Optional[str]:
    """Raw ``user_id`` / ``open_id`` from a post cell (may be ``union_id`` / internal id per Feishu docs)."""
    tag_s = str(cell.get("tag") or cell.get("Tag") or "").strip().lower()
    if tag_s not in ("at", "mention"):
        return None
    for key in ("user_id", "userId", "open_id", "openId"):
        v = cell.get(key)
        if v:
            s = str(v).strip()
            if s and s.lower() != "all":
                return s
    user = cell.get("user") or cell.get("User")
    if isinstance(user, dict):
        for key in ("open_id", "openId", "user_id", "userId", "union_id", "unionId", "id", "Id"):
            v = user.get(key)
            if v:
                s = str(v).strip()
                if s and s.lower() != "all":
                    return s
    return None


def _lark_resolve_feishu_at_ref_to_strong_open_id(ref: str, mentions_list: List[Any]) -> Optional[str]:
    """Map post ``user_id`` field (open_id / union_id / etc.) to ``ou_``/``cli_`` using ``mentions[]`` rows."""
    r = (ref or "").strip()
    if not r or r.lower() == "all":
        return None
    if _lark_string_is_strong_feishu_at_target(r):
        return r
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        id_strings = _lark_collect_mention_identity_strings_for_at_conflict(m)
        if r not in id_strings:
            continue
        oid = _lark_mention_row_main_open_id(m)
        if oid and _lark_string_is_strong_feishu_at_target(oid):
            return oid
        for s in id_strings:
            t = str(s).strip()
            if t and _lark_string_is_strong_feishu_at_target(t):
                return t
    return None


def _lark_post_cell_resolved_strong_at_user_id(cell: Dict[str, Any], mentions_list: List[Any]) -> Optional[str]:
    raw = _lark_post_cell_raw_at_ref(cell)
    if not raw:
        return None
    return _lark_resolve_feishu_at_ref_to_strong_open_id(raw, mentions_list)


def _lark_ordered_post_at_strong_ids_from_root(
    root: Dict[str, Any],
    mentions_list: Optional[List[Any]] = None,
) -> List[str]:
    """Document-order ``@`` targets in post/rich-text JSON (DFS). Skips embedded ``mentions`` metadata subtrees."""
    ml = mentions_list or []
    out: List[str] = []
    seen: Set[str] = set()

    def visit(obj: Any, depth: int = 0) -> None:
        if depth > 18 or obj is None:
            return
        if isinstance(obj, dict):
            uid = _lark_post_cell_resolved_strong_at_user_id(obj, ml)
            if uid and uid not in seen:
                seen.add(uid)
                out.append(uid)
            keys = list(obj.keys())
            priority = [k for k in _LARK_POST_DOC_ORDER_KEYS_FIRST if k in obj]
            rest = [k for k in keys if k not in priority]
            for k in priority + rest:
                if k in _LARK_POST_SKIP_RECURSE_KEYS:
                    continue
                visit(obj[k], depth + 1)
        elif isinstance(obj, list):
            for x in obj:
                visit(x, depth + 1)

    visit(root, 0)
    return out


def _lark_collect_post_at_user_ids(
    obj: Any,
    out: List[str],
    seen: Set[str],
    depth: int = 0,
    *,
    mentions_list: Optional[List[Any]] = None,
) -> None:
    """Collect resolved ``ou_`` / ``cli_`` from nested post / rich-text JSON (DFS)."""
    ml = mentions_list or []
    if depth > 14 or obj is None:
        return
    if isinstance(obj, dict):
        uid = _lark_post_cell_resolved_strong_at_user_id(obj, ml)
        if uid and uid not in seen:
            seen.add(uid)
            out.append(uid)
        for k, v in obj.items():
            if k in _LARK_POST_SKIP_RECURSE_KEYS:
                continue
            _lark_collect_post_at_user_ids(v, out, seen, depth + 1, mentions_list=ml)
    elif isinstance(obj, list):
        for x in obj:
            _lark_collect_post_at_user_ids(x, out, seen, depth + 1, mentions_list=ml)


def _lark_extract_at_entity_ids_from_im_message(
    msg: Dict[str, Any],
    *,
    mentions_list: Optional[List[Any]] = None,
    event: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """
    Parse ``<at …>`` ids from **message body fields only** (``content`` / ``text`` / ``body``).

    Also walks **post** rich-text JSON cells ``{\"tag\": \"at\", \"user_id\": \"ou_…\"}`` (mobile clients),
    which omit HTML ``<at>`` and plain ``@_user_N`` in extracted text.

    Do **not** scan ``json.dumps(msg)``: the envelope repeats ``mentions[]`` open_ids and falsely looks like a
    peer ``<at>`` in the visible text — Game then peer-skips ``@_user_1 /mo`` even when the user @'d Game.

    Pass the same ``mentions_list`` as IM handling (``message`` + ``event`` merged) so post cells whose
    ``user_id`` is ``union_id`` / internal id can be mapped via ``mentions[]`` rows (HTTP often puts rows on ``event``).
    """
    blobs = _lark_im_content_blobs_for_at_parse(msg)
    out: List[str] = []
    seen: Set[str] = set()
    for b in blobs:
        for m in _LARK_AT_ENTITY_ID_IN_CONTENT_RE.finditer(b or ""):
            s = m.group(1).strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        for m in _LARK_AT_ID_ATTR_OU_CLI_RE.finditer(b or ""):
            s = m.group(1).strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)

    if mentions_list is not None:
        mentions_for_resolve = mentions_list
    else:
        ev = event if isinstance(event, dict) else {}
        mentions_for_resolve = _lark_collect_im_message_mentions(msg, ev)
    root = _lark_im_parsed_content_root(msg)
    if isinstance(root, dict):
        for uid in _lark_ordered_post_at_strong_ids_from_root(root, mentions_for_resolve):
            if uid not in seen:
                seen.add(uid)
                out.append(uid)
        _lark_collect_post_at_user_ids(root, out, seen, mentions_list=mentions_for_resolve)

    return out


def _mo_peer_at_blocks_weak_nonempty_mo(
    content_at_entity_ids: Optional[List[str]],
    *,
    self_bot: str,
    self_app: str,
    peer_open_ids: Set[str],
) -> bool:
    """
    True when ``content`` encodes an @-target that is **only** peer bot(s) (not this bot), so we skip
    weak-nonempty ``/mo`` on **this** worker.
    """
    if not peer_open_ids:
        return False
    ids = [str(x).strip() for x in (content_at_entity_ids or []) if str(x).strip()]
    if not ids:
        return False
    sb = (self_bot or "").strip()
    sa = (str(self_app).strip() if self_app else "") or ""
    if sb and sb in ids:
        return False
    if sa and sa in ids:
        return False
    return any(i in peer_open_ids for i in ids)


def _monitoring_canonical_open_id_match_set() -> Set[str]:
    """Configured canonical id(s) plus ``_lark_effective_bot_open_id()`` when known (covers Feishu vs console id mismatch)."""
    s = set(MONITORING_CANONICAL_BOT_OPEN_ID_EXTRA_SET)
    c = (MONITORING_CANONICAL_BOT_OPEN_ID or "").strip()
    if c:
        s.add(c)
    eff = (_lark_effective_bot_open_id() or "").strip()
    if eff:
        s.add(eff)
    return s


def _lark_collect_explicit_bot_at_ids(
    mentions_list: List[Any],
    content_at_entity_ids: Optional[List[str]],
) -> Set[str]:
    """``ou_``/``cli_`` from mention payloads plus parsed ``<at>`` ids — used with ``MONITORING_CANONICAL_BOT_OPEN_ID``."""
    out: Set[str] = set()
    for x in content_at_entity_ids or []:
        s = str(x).strip()
        if s:
            out.add(s)
    for m in mentions_list:
        if not isinstance(m, dict):
            continue
        for s in _lark_collect_mention_identity_strings_for_at_conflict(m):
            t = str(s).strip()
            if t and _lark_string_is_strong_feishu_at_target(t):
                out.add(t)
    return out


def _lark_body_peer_only_strong_at_targets(
    content_at_entity_ids: Optional[List[str]],
    peer_open_ids: Set[str],
) -> bool:
    """
    True when ``content`` JSON yields at least one strong ``ou_``/``cli_`` id and **all** such ids lie in
    ``peer_open_ids``. Used so we only **hard-skip** peer-only when the body ``<at>`` proves the user @'d the
    peer bot — Feishu often carries the peer ``open_id`` in ``mentions[]`` alone while the visible text is
    ``@_user_N`` for **this** bot (Game weak-nonempty path).
    """
    if not peer_open_ids:
        return False
    body_ids: Set[str] = set()
    for x in content_at_entity_ids or []:
        t = str(x).strip()
        if t and _lark_string_is_strong_feishu_at_target(t):
            body_ids.add(t)
    if not body_ids:
        return False
    return body_ids <= peer_open_ids


def _monitoring_at_bot_requirement_satisfied(
    raw_text: str,
    mentions: Any,
    *,
    content_at_entity_ids: Optional[List[str]] = None,
    msg: Optional[Dict[str, Any]] = None,
    chat_type: str = "",
) -> bool:
    """
    Same @-target rules as ``/mo`` when ``MONITORING_TRIGGER_REQUIRES_AT_BOT=1``.
    Used for ``/m`` / ``/c`` so mute commands in a shared group only hit the bot that was actually @'d.

    **p2p / private**: no @ picker — explicit ``/mo`` (or ``/m`` / ``/c``) is always to this bot; skip @ gate.

    When Lark delivers one payload to multiple apps, ``mentions[]`` may list **both** bots; we resolve the
    **primary** @ target from body ``<at>`` order (then mentions order) so only the addressed bot replies.
    """
    _monitoring_at_gate_reason_clear()
    ct = (chat_type or "").strip().lower()
    if ct in ("p2p", "private"):
        logger.info("monitoring: @ gate bypass — %s chat (PM has no @ target)", ct or "p2p")
        return True
    if isinstance(mentions, list):
        mentions_list = mentions
    elif isinstance(mentions, dict) and mentions:
        mentions_list = [mentions]
    else:
        mentions_list = []
    canon_ids = _monitoring_canonical_open_id_match_set()
    explicit_ids = _lark_collect_explicit_bot_at_ids(mentions_list, content_at_entity_ids)
    primary = _monitoring_resolved_primary_at_target(
        msg,
        mentions_list,
        raw_text,
        bot_like_bag=canon_ids | MONITORING_PEER_BOT_OPEN_ID_SET,
    )
    root = _lark_im_parsed_content_root(msg) if isinstance(msg, dict) else None
    post_at_ordered = (
        _lark_ordered_post_at_strong_ids_from_root(root, mentions_list) if root else []
    )
    if post_at_ordered:
        # Mobile post: trust document-order @ in body over mentions[] order (often lists both bots).
        primary = post_at_ordered[0]

    app_dbg = str(APP_ID or "").strip()
    if MONITORING_LOG_PRIMARY_AT:
        logger.info(
            "monitoring @-gate dbg chat_type=%r primary=%r explicit_ids=%s canon=%s "
            "mentions_n=%s row_app_id_is_self=%s",
            ct or None,
            primary,
            sorted(explicit_ids) if explicit_ids else [],
            sorted(canon_ids) if canon_ids else [],
            len(mentions_list),
            bool(app_dbg and _lark_mentions_any_row_matches_app(mentions_list, app_dbg)),
        )

    # Plain ``@_user_N`` text often has no ``<at user_id=…>``; Feishu may put the peer ``open_id`` on the sole
    # mention row while ``name`` still matches the bot the user picked — optional substring match (off by default).
    if (
        MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_FALLBACK
        and MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_SUBSTRINGS
        and primary
        and primary in MONITORING_PEER_BOT_OPEN_ID_SET
        and _lark_raw_text_has_feishu_at_placeholder(raw_text)
        and len(mentions_list) == 1
        and isinstance(mentions_list[0], dict)
        and isinstance(msg, dict)
        and not _lark_im_message_has_visible_strong_at_html(msg)
    ):
        nm = str(mentions_list[0].get("name") or mentions_list[0].get("Name") or "").strip()
        matched_sub = ""
        for sub in MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_SUBSTRINGS:
            ss = sub.strip()
            if ss and ss.casefold() in nm.casefold():
                matched_sub = ss
                break
        if matched_sub:
            eff = (_lark_effective_bot_open_id() or "").strip()
            if eff and eff in canon_ids:
                logger.info(
                    "monitoring: primary retarget peer=%r → self=%r mention.name=%r matched=%r "
                    "(MONITORING_PLACEHOLDER_PEER_PRIMARY_NAME_FALLBACK)",
                    primary,
                    eff,
                    nm,
                    matched_sub,
                )
                primary = eff

    app = str(APP_ID or "").strip()
    row_app_id_is_self = bool(app and _lark_mentions_any_row_matches_app(mentions_list, app))
    # Never auto-trigger on «sole mention open_id ∈ peer + @_user_N + /mo|/m|/c»: same payload when user
    # correctly @ only the peer bot → the other bot would also reply (dual mute/monitoring).

    strong_body = {
        str(x).strip()
        for x in (content_at_entity_ids or [])
        if _lark_string_is_strong_feishu_at_target(str(x).strip())
    }
    if (
        MONITORING_PEER_BOT_OPEN_ID_SET
        and strong_body
        and strong_body <= MONITORING_PEER_BOT_OPEN_ID_SET
        and not (strong_body & canon_ids)
    ):
        logger.info(
            "monitoring: skip — body/post @ ids %s are peer-only (no canonical id in body for this app)",
            sorted(strong_body),
        )
        _monitoring_at_gate_reason_set(
            "Message body @ targets Platform bot only — pick Grafana Game Bot in the @ picker, then /mo."
        )
        return False

    if canon_ids and primary and primary not in canon_ids:
        if row_app_id_is_self:
            logger.info(
                "monitoring: primary @ open_id=%r not in canonical but mention row app_id matches "
                "this app — continuing (Feishu open_id/placeholder skew)",
                primary,
            )
        elif primary in MONITORING_PEER_BOT_OPEN_ID_SET:
            logger.info(
                "monitoring: skip — primary @ target %r is the configured peer bot (Platform), "
                "not Game (canonical=%s). Feishu mapped @_user_N / mentions to the peer id — "
                "@ Grafana Game Bot in the picker, not Grafana Platform Bot.",
                primary,
                sorted(canon_ids),
            )
            _monitoring_at_gate_reason_set(
                "Primary @ resolved to Grafana Platform Bot — @ **Grafana Game Bot** in the picker, then /mo."
            )
        else:
            logger.info(
                "monitoring: skip — primary @ target %r is not this bot (canonical=%s)",
                primary,
                sorted(canon_ids),
            )
            _monitoring_at_gate_reason_set(
                f"Primary @ target is not Grafana Game Bot (got {primary!r}). @ Game Bot then /mo."
            )
        if not row_app_id_is_self:
            return False

    if MONITORING_TRIGGER_REQUIRES_AT_BOT and not _monitoring_group_multi_bot_first_mention_gate(
        chat_type=chat_type,
        mentions_list=mentions_list,
        primary=primary,
        canon_ids=canon_ids,
    ):
        return False

    if canon_ids:
        if explicit_ids and not explicit_ids.isdisjoint(canon_ids):
            if primary and primary in canon_ids:
                logger.info(
                    "monitoring /mo: trigger — primary @ %r matches canonical open_id set",
                    primary,
                )
                return True
            if not primary:
                strong_x = {
                    str(x).strip()
                    for x in explicit_ids
                    if _lark_string_is_strong_feishu_at_target(str(x).strip())
                }
                bot_like = strong_x & (canon_ids | MONITORING_PEER_BOT_OPEN_ID_SET)
                if len(bot_like) >= 2:
                    logger.info(
                        "monitoring /mo: skip — multiple bot ids in explicit set %s without body primary @",
                        sorted(bot_like),
                    )
                    _monitoring_at_gate_reason_set(
                        "Both bots appear in @ metadata — @ Grafana Game Bot first, then /mo."
                    )
                    return False
                if strong_x & canon_ids:
                    logger.info(
                        "monitoring /mo: trigger — explicit @ intersects canonical (single bot-like id)"
                    )
                    return True
        if explicit_ids and explicit_ids.isdisjoint(canon_ids):
            peer_only = (
                bool(MONITORING_PEER_BOT_OPEN_ID_SET)
                and explicit_ids <= MONITORING_PEER_BOT_OPEN_ID_SET
            )
            if peer_only:
                body_peer_only = _lark_body_peer_only_strong_at_targets(
                    content_at_entity_ids,
                    MONITORING_PEER_BOT_OPEN_ID_SET,
                )
                if body_peer_only:
                    logger.info(
                        "monitoring /mo: skip — explicit @ targets %s peer-only and body <at> confirms peer",
                        sorted(explicit_ids),
                    )
                    _monitoring_at_gate_reason_set(
                        "You @'d Platform Bot (body <at> confirms). @ Grafana Game Bot, then /mo."
                    )
                    return False
                logger.info(
                    "monitoring /mo: skip — explicit meta peer-only %s and body lacks "
                    "peer <at> confirmation; treating as peer-addressed (MONITORING_PEER_BOT_OPEN_IDS).",
                    sorted(explicit_ids),
                )
                _monitoring_at_gate_reason_set(
                    "Feishu @ metadata points to Platform Bot — pick Grafana Game Bot in the @ list, then /mo."
                )
                return False
            else:
                logger.info(
                    "monitoring /mo: skip — explicit @ targets %s disjoint from canonical %s "
                    "(not subset of MONITORING_PEER_BOT_OPEN_IDS; user @'d another bot/app)",
                    sorted(explicit_ids),
                    sorted(canon_ids),
                )
                _monitoring_at_gate_reason_set("This @ target is not Grafana Game Bot.")
                return False

    distinct_bot_like = _lark_distinct_strong_bot_like_ids_in_mentions(
        mentions_list,
        canon_ids=canon_ids,
        peer_ids=MONITORING_PEER_BOT_OPEN_ID_SET,
    )
    if len(distinct_bot_like) >= 2 and not primary:
        logger.info(
            "monitoring /mo: skip — mentions encode multiple bots %s with no resolvable primary @",
            sorted(distinct_bot_like),
        )
        _monitoring_at_gate_reason_set(
            "Both Game and Platform bots in mentions — @ Grafana Game Bot first, then /mo."
        )
        return False

    if _lark_message_mentions_bot(mentions):
        if primary and canon_ids:
            if primary in canon_ids:
                logger.info(
                    "monitoring /mo: trigger — mentions include this bot and primary @ matches canonical"
                )
                return True
            if row_app_id_is_self:
                logger.info(
                    "monitoring /mo: trigger — mentions include this bot (app_id) while primary "
                    "open_id=%r is not canonical (Feishu skew)",
                    primary,
                )
                return True
            logger.info(
                "monitoring /mo: skip — mentions include this bot but primary @ %r is not canonical",
                primary,
            )
            _monitoring_at_gate_reason_set(
                f"Mention maps to wrong bot (primary={primary!r}). @ Grafana Game Bot, then /mo."
            )
            return False
        return True
    cat_ids = [str(x).strip() for x in (content_at_entity_ids or []) if str(x).strip()]
    sb = (_lark_effective_bot_open_id() or "").strip()
    sa = (str(APP_ID).strip() if APP_ID else "") or ""
    if sb and sb in cat_ids:
        logger.info("monitoring /mo: trigger via content <at> matching this bot open_id")
        return True
    if sa and sa in cat_ids:
        logger.info("monitoring /mo: trigger via content <at> matching APP_ID")
        return True
    body_ph = _lark_raw_text_has_feishu_at_placeholder(raw_text)
    conflict_other = (
        _lark_mentions_carry_strong_identity_other_than_bot(
            _lark_effective_bot_open_id(),
            str(APP_ID).strip() if APP_ID else "",
            mentions_list,
        )
        if mentions_list
        else False
    )
    if mentions_list:
        if (
            MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW
            and body_ph
            and not conflict_other
        ):
            if _mo_peer_at_blocks_weak_nonempty_mo(
                content_at_entity_ids,
                self_bot=_lark_effective_bot_open_id(),
                self_app=str(APP_ID).strip() if APP_ID else "",
                peer_open_ids=MONITORING_PEER_BOT_OPEN_ID_SET,
            ):
                logger.info(
                    "monitoring /mo: skip — content <at> targets MONITORING_PEER_BOT_OPEN_IDS "
                    "(weak-nonempty path disabled for peer @)"
                )
                _monitoring_at_gate_reason_set(
                    "Body @ points to Platform Bot. @ Grafana Game Bot (not Platform), then /mo."
                )
            else:
                logger.info(
                    "monitoring /mo: allowed via Feishu @_user_N + weak/non-conflicting mentions "
                    "(MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW=1 MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER=%s)",
                    MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER,
                )
                return True
        _monitoring_at_gate_reason_set(
            "Ambiguous @ in a shared group — @ Grafana Game Bot explicitly, then /mo "
            "(not Platform Bot)."
        )
        return False
    if (
        (not mentions_list)
        and explicit_ids
        and canon_ids
        and explicit_ids.isdisjoint(canon_ids)
    ):
        logger.info(
            "monitoring /mo: skip — mentions empty but explicit @ ids %s disjoint from canonical; "
            "refuse @_user_N placeholder-only (wrong bot in same group)",
            sorted(explicit_ids),
        )
        _monitoring_at_gate_reason_set(
            "@ metadata does not match Game Bot — use the @ picker and choose Grafana Game Bot."
        )
        return False
    if MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER and body_ph:
        logger.info(
            "monitoring /mo: allowed via Feishu @_user_N placeholder "
            "(mentions list empty; MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER=1)"
        )
        return True
    _monitoring_at_gate_reason_set(
        "In group chats, @ Grafana Game Bot then /mo. Bare /mo without @ only works in private (PM) chat."
    )
    return False


def _text_should_run_monitoring(
    raw_text: str,
    clean: str,
    mentions: Any,
    *,
    content_at_entity_ids: Optional[List[str]] = None,
    msg: Optional[Dict[str, Any]] = None,
    chat_type: str = "",
) -> bool:
    """
    Run the same job as ``/monitoring`` when the command is present, or when the user @mentions
    the bot (see ``MONITORING_AT_MENTION_ENABLE`` / ``MONITORING_AT_MENTION_ANY_TEXT``).

    When ``MONITORING_TRIGGER_REQUIRES_AT_BOT`` is true, an explicit trigger (e.g. ``/mo``) runs only if
    :func:`_monitoring_at_bot_requirement_satisfied` passes (primary @, multi-bot order, explicit ids).
    **p2p PM** is exempt — bare ``/mo`` in a private chat always targets this bot.

    ``MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER``: when ``mentions`` is **empty**, body ``@_user_N`` may still
    trigger ``/mo`` if enabled.

    ``MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW``: default **off** on Game when sharing a group with Platform.
    When **on**, non-empty ``mentions`` + body ``@_user_N`` can trigger ``/mo`` — risky with Feishu placeholders.

    ``MONITORING_PEER_BOT_OPEN_IDS``: required peer ``ou_`` / ``cli_`` set. If ``explicit_ids`` from mentions
    are **peer-only** but parsed body ``<at>`` does **not** confirm peer (placeholder-only body),
    we **skip** — no fall-through to weak-nonempty (prevents @ Platform → Game).

    If ``content`` JSON contains ``<at …>`` with **this** bot's strong id, ``/mo`` can still trigger when
    weak-nonempty is off.

    ``MONITORING_CANONICAL_BOT_OPEN_ID`` / extras: merged canonical set for intersection checks with
    ``explicit_ids`` and primary resolution.
    """
    if _text_has_monitoring_trigger(raw_text, clean):
        if not MONITORING_TRIGGER_REQUIRES_AT_BOT:
            return True
        return _monitoring_at_bot_requirement_satisfied(
            raw_text,
            mentions,
            content_at_entity_ids=content_at_entity_ids,
            msg=msg,
            chat_type=chat_type,
        )
    if not MONITORING_AT_MENTION_ENABLE:
        return False
    if not _lark_message_mentions_bot(mentions):
        return False
    if MONITORING_AT_MENTION_ANY_TEXT:
        return True
    return not (clean or "").strip()


def _monitoring_dispatch_body_key(clean: str, raw_text: str, mentions: Any) -> str:
    """
    Normalize IM debounce key so explicit commands / @-mention variants share one key
    (avoids two background workers when ``clean`` whitespace or mention markup differs slightly).
    """
    tri = (MONITORING_TRIGGER or "/mo").strip().lower()
    cl = re.sub(r"\s+", " ", (clean or "").strip().lower())
    if _im_command_matches(clean or "", MONITORING_TRIGGER):
        return tri
    if MONITORING_AT_MENTION_ENABLE and _lark_message_mentions_bot(mentions):
        if MONITORING_AT_MENTION_ANY_TEXT:
            return f"__at_any__:{cl[:240]}"
        if not cl.strip():
            return "__at_only__"
    return cl[:320] or "__body__"


def grafana_login_session() -> requests.Session:
    if not GRAFANA_USER or not GRAFANA_PASSWORD:
        raise ValueError("Set GRAFANA_USER and GRAFANA_PASSWORD in .env")

    session = requests.Session()
    login_url = f"{GRAFANA_BASE_URL}/login"
    resp = session.post(
        login_url,
        json={"user": GRAFANA_USER, "password": GRAFANA_PASSWORD},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    # Grafana sets grafana_session cookie on success
    if "grafana_session" not in session.cookies.get_dict():
        logger.warning("Login returned 200 but no grafana_session cookie; check credentials / SSO")
    return session


def fetch_grafana_dashboard(
    session: Optional[requests.Session] = None,
    extra_query: Optional[Dict[str, str]] = None,
) -> requests.Response:
    """GET dashboard HTML after login (same as opening the link in a browser)."""
    if session is None:
        session = grafana_login_session()
    params = {
        "orgId": "1",
        "from": GRAFANA_DASHBOARD_FROM,
        "to": GRAFANA_DASHBOARD_TO,
        "timezone": "browser",
        "refresh": "5s",
    }
    if extra_query:
        params.update(extra_query)
    url = f"{GRAFANA_BASE_URL}{GRAFANA_DASHBOARD_PATH}"
    resp = session.get(url, params=params, timeout=60)
    return resp


def _lark_is_url_verification_payload(data: Dict[str, Any]) -> bool:
    """True for challenge/URL verification POST (several Feishu/Lark body shapes)."""
    if not isinstance(data, dict):
        return False
    if _lark_header_event_type(data) == "url_verification":
        return True
    if data.get("type") == "url_verification":
        return True
    ev = data.get("event")
    if isinstance(ev, dict) and str(ev.get("type") or "").strip() == "url_verification":
        return True
    return False


def _extract_url_verification(data: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Return (token_hint, challenge) for Lark URL verification.

    Challenge may live in ``event.challenge``, or top-level ``challenge`` if a proxy
    flattened the JSON. Token: prefer :func:`_lark_extract_verification_token` at call site.
    """
    if not isinstance(data, dict):
        return None
    if not _lark_is_url_verification_payload(data):
        return None

    if data.get("type") == "url_verification":
        return (str(data.get("token") or ""), str(data.get("challenge") or ""))

    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    ch = ev.get("challenge")
    if ch is None:
        ch = data.get("challenge")
    if ch is None:
        return None

    tok = ev.get("token")
    if tok is None or (isinstance(tok, str) and not str(tok).strip()):
        h = data.get("header") if isinstance(data.get("header"), dict) else {}
        tok = h.get("token") or h.get("Token") or h.get("verification_token")
    return (str(tok or ""), str(ch))


def _lark_ack_only_event_type(het: str) -> bool:
    """Subscribed but not handled — still HTTP 200 (Chatbox :func:`_lark_ack_only_event_type`)."""
    if not het:
        return False
    h = het.lower()
    if h.startswith("meeting_room.") or h.startswith("vc.meeting."):
        return True
    return False


def _lark_min_json_response(payload: Dict[str, Any], status: int = 200) -> Response:
    """Tight JSON body + explicit length — return before logging for URL verification."""
    body = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return Response(
        body,
        status=status,
        mimetype="application/json; charset=utf-8",
        headers={"X-Accel-Buffering": "no"},
    )


# Pre-built body avoids json.dumps on the hot ACK path (tiny win; no computation before flush).
_FEISHU_WEBHOOK_ACK_EMPTY_BODY = b"{}"


def _lark_feishu_webhook_ack_immediate() -> Response:
    """Feishu event/card HTTP callbacks should get 200 within ~3s; empty body is accepted after ACK."""
    return Response(
        _FEISHU_WEBHOOK_ACK_EMPTY_BODY,
        status=200,
        mimetype="application/json; charset=utf-8",
        headers={
            "Content-Length": "2",
            "X-Accel-Buffering": "no",
        },
    )


def _lark_webhook_url_verification_response_or_none(data: Dict[str, Any]) -> Optional[Response]:
    """If payload is Feishu URL verification / challenge, return minimal JSON immediately."""
    if data.get("type") == "url_verification":
        ch0 = data.get("challenge", "")
        return _lark_min_json_response({"challenge": str(ch0) if ch0 is not None else ""})
    uv = _extract_url_verification(data)
    if not uv:
        return None
    token_from_event, challenge = uv
    if VERIFICATION_TOKEN:
        effective_tok = _lark_extract_verification_token(data) or str(token_from_event or "").strip()
        if effective_tok != VERIFICATION_TOKEN:
            logger.warning(
                "url_verification token mismatch (exp_len=%s got_len=%s)",
                len(VERIFICATION_TOKEN),
                len(effective_tok or ""),
            )
            return _lark_min_json_response({"error": "invalid verification token"}, status=403)
    logger.debug("url_verification OK, challenge len=%s", len(str(challenge)))
    return _lark_min_json_response({"challenge": str(challenge)})


def _fast_plaintext_url_verification_response(raw_in: Dict[str, Any]) -> Optional[Response]:
    """
    Return Flask response for URL verification **before** decrypt/normalize pipeline.
    Uses :class:`Response` (not ``jsonify``) and no success logging so bytes leave ASAP.
    """
    if "encrypt" in raw_in:
        return None
    work = dict(raw_in)
    _lark_coerce_event_dict(work)
    if work.get("type") == "url_verification":
        ch0 = work.get("challenge", "")
        return _lark_min_json_response({"challenge": str(ch0) if ch0 is not None else ""})
    uv = _extract_url_verification(work)
    if not uv:
        return None
    token_from_event, challenge = uv
    if VERIFICATION_TOKEN:
        effective_tok = _lark_extract_verification_token(work) or str(token_from_event or "").strip()
        if effective_tok != VERIFICATION_TOKEN:
            logger.warning(
                "url_verification token mismatch (fast path) exp_len=%s got_len=%s",
                len(VERIFICATION_TOKEN),
                len(effective_tok or ""),
            )
            return _lark_min_json_response({"error": "invalid verification token"}, status=403)
    return _lark_min_json_response({"challenge": str(challenge)})


def _walk_panels(panels: Optional[List[Dict[str, Any]]]) -> Generator[Dict[str, Any], None, None]:
    for p in panels or []:
        yield p
        if p.get("type") == "row" and p.get("panels"):
            yield from _walk_panels(p["panels"])


def _datasource_uid(ds: Any) -> Optional[str]:
    if isinstance(ds, dict):
        uid = ds.get("uid")
        if uid:
            return str(uid)
    return None


def _find_panel(dashboard: Dict[str, Any], title: str) -> Optional[Dict[str, Any]]:
    want = (title or "").strip()
    if not want:
        return None
    panels = list(_walk_panels(dashboard.get("panels")))
    titles_to_try: List[str] = [want]
    liveslot_alias_cf = {t.casefold() for t in _LIVESLOT_BET_PANEL_TITLES}
    if want.casefold() in liveslot_alias_cf:
        for alt in _LIVESLOT_BET_PANEL_TITLES:
            if alt not in titles_to_try:
                titles_to_try.append(alt)

    for cand in titles_to_try:
        for p in panels:
            if (p.get("title") or "").strip() == cand:
                if cand != want:
                    logger.info('panel matched exact title %r (requested %r)', cand, want)
                return p
        want_cf = cand.casefold()
        for p in panels:
            if (p.get("title") or "").strip().casefold() == want_cf:
                if cand != want:
                    logger.info('panel matched title %r (requested %r)', cand, want)
                return p

    # Legacy fuzzy: liveslot + bet/spin in title
    for cand in titles_to_try:
        want_cf = cand.casefold()
        if "liveslot" in want_cf and ("bet" in want_cf or "spin" in want_cf):
            for p in panels:
                t = (p.get("title") or "").strip()
                tl = t.casefold()
                if "liveslot" in tl and ("bet" in tl or "spin" in tl):
                    logger.info(
                        'panel title fallback: requested %r → using %r',
                        cand,
                        t,
                    )
                    return p
    return None


def _fetch_dashboard_model(session: requests.Session, uid: str) -> Dict[str, Any]:
    r = session.get(
        f"{GRAFANA_BASE_URL}/api/dashboards/uid/{uid}",
        params={"orgId": "1"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("dashboard") or {}


def _fetch_library_panel_model(session: requests.Session, library_uid: str) -> Dict[str, Any]:
    """Best-effort fetch of Grafana library panel model by uid."""
    u = (library_uid or "").strip()
    if not u:
        return {}
    try:
        r = session.get(f"{GRAFANA_BASE_URL}/api/library-elements/{u}", params={"orgId": "1"}, timeout=60)
        r.raise_for_status()
        j = r.json() or {}
        # Common shapes: {"result":{"model":{...}}} or {"model":{...}}
        if isinstance(j.get("result"), dict) and isinstance(j["result"].get("model"), dict):
            return j["result"]["model"] or {}
        if isinstance(j.get("model"), dict):
            return j["model"] or {}
    except Exception:
        logger.exception("library panel fetch failed uid=%r", u[:32])
    return {}


def _panel_query_model(session: requests.Session, panel: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return panel model carrying query targets.
    For library panels, Grafana dashboard JSON may only keep a lightweight ref with no targets.
    """
    if not isinstance(panel, dict):
        return {}
    targets = panel.get("targets") or []
    if isinstance(targets, list) and len(targets) > 0:
        return panel
    lp = panel.get("libraryPanel") if isinstance(panel.get("libraryPanel"), dict) else {}
    lib_uid = str(lp.get("uid") or "").strip()
    if lib_uid:
        m = _fetch_library_panel_model(session, lib_uid)
        if m:
            return m
    return panel


def _prometheus_query_range(
    session: requests.Session,
    datasource_uid: str,
    expr: str,
    start_unix: int,
    end_unix: int,
    step: int,
) -> Dict[str, Any]:
    base = f"{GRAFANA_BASE_URL}/api/datasources/proxy/uid/{datasource_uid}/api/v1/query_range"
    params = {
        "query": expr,
        "start": str(start_unix),
        "end": str(end_unix),
        "step": str(step),
    }
    r = session.get(base, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def _grafana_ds_query(
    session: requests.Session,
    datasource_uid: str,
    target: Dict[str, Any],
    start_unix: int,
    end_unix: int,
    step: int,
) -> Dict[str, Any]:
    """
    Generic Grafana datasource query fallback (for non-Prometheus targets, e.g. SQL/log-like queries).
    Returns Grafana /api/ds/query JSON.
    """
    q = copy.deepcopy(target or {})
    q["datasource"] = {"uid": datasource_uid}
    q["refId"] = str(q.get("refId") or "A")
    q["intervalMs"] = max(1000, int(step) * 1000)
    q["maxDataPoints"] = max(200, int((max(1, end_unix - start_unix) // max(1, step)) + 8))
    payload = {
        "from": str(int(start_unix) * 1000),
        "to": str(int(end_unix) * 1000),
        "queries": [q],
    }
    r = session.post(
        f"{GRAFANA_BASE_URL}/api/ds/query",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    r.raise_for_status()
    return r.json() or {}


def _ds_query_to_prometheus_like(raw: Dict[str, Any], ref_id: str) -> Dict[str, Any]:
    """
    Convert Grafana /api/ds/query frames into a Prometheus-like shape:
    {"data":{"result":[{"metric": {...}, "values":[[ts,val], ...]}, ...]}}
    so existing merge/analyze logic can stay unchanged.
    """
    out: List[Dict[str, Any]] = []
    results = raw.get("results") if isinstance(raw.get("results"), dict) else {}
    bucket = results.get(ref_id) if isinstance(results.get(ref_id), dict) else {}
    frames = bucket.get("frames") if isinstance(bucket.get("frames"), list) else []

    for fr in frames:
        if not isinstance(fr, dict):
            continue
        schema = fr.get("schema") if isinstance(fr.get("schema"), dict) else {}
        fields = schema.get("fields") if isinstance(schema.get("fields"), list) else []
        data = fr.get("data") if isinstance(fr.get("data"), dict) else {}
        cols = data.get("values") if isinstance(data.get("values"), list) else []
        if not fields or not cols:
            continue
        n = min(len(fields), len(cols))
        if n <= 0:
            continue

        names: List[str] = []
        field_objs: List[Dict[str, Any]] = []
        for i in range(n):
            f = fields[i] if isinstance(fields[i], dict) else {}
            field_objs.append(f)
            names.append(str(f.get("name") or f"f{i}"))

        def _field_series_name(idx: int, fallback: str) -> str:
            if idx < 0 or idx >= n:
                return fallback
            f = field_objs[idx] if isinstance(field_objs[idx], dict) else {}
            cfg = f.get("config") if isinstance(f.get("config"), dict) else {}
            labels = f.get("labels") if isinstance(f.get("labels"), dict) else {}
            for k in ("displayName", "displayNameFromDS"):
                v = cfg.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            for lk in ("series", "name", "providerid", "provider_id", "gameid", "game_id"):
                if lk in labels and str(labels.get(lk) or "").strip():
                    return str(labels.get(lk)).strip()
            if labels:
                if len(labels) == 1:
                    try:
                        only_v = next(iter(labels.values()))
                        if str(only_v or "").strip():
                            return str(only_v).strip()
                    except Exception:
                        pass
                compact = ",".join(
                    f"{str(k).strip()}={str(v).strip()}"
                    for k, v in labels.items()
                    if str(k).strip() and str(v).strip()
                ).strip(",")
                if compact:
                    return compact
            return fallback

        row_len = 0
        for i in range(n):
            c = cols[i]
            if isinstance(c, list):
                row_len = max(row_len, len(c))
        if row_len <= 0:
            continue

        ts_idx = -1
        val_idx = -1
        label_idx = -1
        for i, nm in enumerate(names):
            nl = nm.strip().lower()
            if ts_idx < 0 and nl in ("time", "t", "ts", "timestamp", "datetime"):
                ts_idx = i
            if val_idx < 0 and nl in ("value", "val", "count", "total"):
                val_idx = i
            if label_idx < 0 and nl in ("gameid", "game_id", "providerid", "provider_id", "name", "series"):
                label_idx = i
        if ts_idx < 0:
            for i in range(n):
                c = cols[i]
                if not isinstance(c, list) or not c:
                    continue
                vv = c[0]
                try:
                    fv = float(vv)
                except (TypeError, ValueError):
                    continue
                if fv > 1e9:
                    ts_idx = i
                    break
        if val_idx < 0:
            for i in range(n):
                if i == ts_idx:
                    continue
                c = cols[i]
                if not isinstance(c, list) or not c:
                    continue
                try:
                    float(c[0])
                    val_idx = i
                    break
                except (TypeError, ValueError):
                    continue
        if ts_idx < 0 or val_idx < 0:
            continue

        by_label: Dict[str, List[List[float]]] = {}
        numeric_idxs: List[int] = []
        for i in range(n):
            if i == ts_idx or i == label_idx:
                continue
            c = cols[i]
            if not isinstance(c, list) or not c:
                continue
            sample = None
            for sv in c:
                if sv is None or sv == "":
                    continue
                sample = sv
                break
            if sample is None:
                continue
            try:
                float(sample)
            except (TypeError, ValueError):
                continue
            numeric_idxs.append(i)
        wide_mode = label_idx < 0 and len(numeric_idxs) > 1

        for r_i in range(row_len):
            try:
                ts_raw = cols[ts_idx][r_i]
            except Exception:
                continue
            try:
                ts = float(ts_raw)
            except (TypeError, ValueError):
                continue
            if ts > 1e12:
                ts = ts / 1000.0
            if wide_mode:
                for vi in numeric_idxs:
                    try:
                        val = float(cols[vi][r_i])
                    except Exception:
                        continue
                    lbl = _field_series_name(vi, str(names[vi]).strip() or "value")
                    by_label.setdefault(lbl, []).append([ts, val])
            else:
                try:
                    val_raw = cols[val_idx][r_i]
                    val = float(val_raw)
                except Exception:
                    continue
                lbl = "value"
                if label_idx >= 0:
                    try:
                        lbl = str(cols[label_idx][r_i]).strip() or "value"
                    except Exception:
                        lbl = "value"
                elif 0 <= val_idx < len(names):
                    lbl = _field_series_name(val_idx, str(names[val_idx]).strip() or "value")
                by_label.setdefault(lbl, []).append([ts, val])

        for lbl, pairs in by_label.items():
            pairs.sort(key=lambda p: p[0])
            out.append({"metric": {"series": lbl, "name": lbl}, "values": pairs})

    return {"data": {"result": out}}


def _fetch_panel_series_by_title(
    panel_title: str,
    session: Optional[requests.Session] = None,
    start_unix: Optional[int] = None,
    end_unix: Optional[int] = None,
) -> Dict[str, Any]:
    if start_unix is not None and end_unix is not None:
        start = int(start_unix)
        end = int(end_unix)
        if start >= end:
            raise ValueError("query window: start_unix must be < end_unix")
    else:
        ao = int(MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES)
        bo = int(MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES)
        if ao > 0 and bo > 0 and ao > bo:
            cur_m = float(_bucket_ts_monitoring_minute(time.time()))
            end = int(cur_m - bo * 60)
            start = int(cur_m - ao * 60)
            if start >= end:
                raise ValueError(
                    "aligned query window: start must be < end "
                    f"(start_offset={ao} end_offset={bo} start={start} end={end})"
                )
        else:
            if ao > 0 or bo > 0:
                logger.warning(
                    "MONITORING_QUERY_ALIGNED_* ignored (need START>0, END>0, START>END); "
                    "using GRAFANA_QUERY_LOOKBACK_SECONDS + GRAFANA_QUERY_END_LAG_SECONDS"
                )
            lag = max(0, int(GRAFANA_QUERY_END_LAG_SECONDS))
            end = int(time.time()) - lag
            start = end - GRAFANA_QUERY_LOOKBACK_SECONDS
    sess = session or grafana_login_session()
    dash = _fetch_dashboard_model(sess, GRAFANA_DASHBOARD_UID)
    panel = _find_panel(dash, panel_title)
    if not panel:
        known = sorted(
            {
                (p.get("title") or "").strip()
                for p in _walk_panels(dash.get("panels"))
                if (p.get("title") or "").strip()
            }
        )
        raise ValueError(
            f'Panel titled "{panel_title}" not found on dashboard {GRAFANA_DASHBOARD_UID}. '
            f"Known titles: {known!r}"
        )

    q_panel = _panel_query_model(sess, panel)
    panel_ds = _datasource_uid(q_panel.get("datasource")) or _datasource_uid(panel.get("datasource"))
    series_out: List[Dict[str, Any]] = []
    for t in q_panel.get("targets") or []:
        expr = (
            (t.get("expr") or "")
            or (t.get("query") or "")
            or (t.get("rawSql") or "")
        )
        expr = str(expr).strip()
        if not expr:
            continue
        ds_uid = _datasource_uid(t.get("datasource")) or panel_ds
        if not ds_uid:
            logger.warning("skip target without datasource uid: %s", t.get("refId"))
            continue
        try:
            raw = _prometheus_query_range(sess, ds_uid, expr, start, end, GRAFANA_QUERY_STEP)
        except requests.HTTPError as e:
            # Non-Prometheus datasource often returns 405 on /api/v1/query_range.
            code = int(getattr(getattr(e, "response", None), "status_code", 0) or 0)
            if code in (400, 404, 405, 415, 422):
                logger.info(
                    'panel "%s" target %s query_range HTTP %s -> fallback api/ds/query',
                    panel_title,
                    t.get("refId"),
                    code,
                )
                raw_ds = _grafana_ds_query(sess, ds_uid, t, start, end, GRAFANA_QUERY_STEP)
                raw = _ds_query_to_prometheus_like(raw_ds, str(t.get("refId") or "A"))
            else:
                raise
        series_out.append(
            {
                "refId": t.get("refId"),
                "legendFormat": t.get("legendFormat"),
                "expr": expr,
                "datasourceUid": ds_uid,
                "prometheus": raw,
            }
        )
    if not series_out:
        logger.warning(
            'No queryable Prometheus targets on panel "%s" (direct targets=%s, library_uid=%r)',
            panel_title,
            len(panel.get("targets") or []),
            ((panel.get("libraryPanel") or {}).get("uid") if isinstance(panel.get("libraryPanel"), dict) else ""),
        )
    return {
        "panelTitle": panel_title,
        "dashboardUid": GRAFANA_DASHBOARD_UID,
        "window": {"startUnix": start, "endUnix": end, "stepSeconds": GRAFANA_QUERY_STEP},
        "series": series_out,
    }


def fetch_request_total_1m_series(
    session: Optional[requests.Session] = None,
    start_unix: Optional[int] = None,
    end_unix: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Primary monitoring panel (default title ``LiveSlots Online Number``) via Prometheus ``query_range``.

    Default window (unless ``start_unix``/``end_unix`` passed, or watchdog overrides):

    - If ``MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES`` and ``…_END_…`` are both **> 0** and
      **START > END**: use minute-aligned bounds from :func:`_bucket_ts_monitoring_minute` —
      ``start = cur_min − START×60``, ``end = cur_min − END×60`` (both ``…:00`` in the configured TZ).
    - Else: ``end = now − GRAFANA_QUERY_END_LAG_SECONDS``, ``start = end − GRAFANA_QUERY_LOOKBACK_SECONDS``.

    Step is ``GRAFANA_QUERY_STEP``. Watchdog passes explicit ``start_unix``/``end_unix`` unchanged.
    """
    return _fetch_panel_series_by_title(
        GRAFANA_PANEL_TITLE,
        session=session,
        start_unix=start_unix,
        end_unix=end_unix,
    )


def _monitoring_watch_quiet_tod_bounds_from_parts(
    start_hour: int,
    start_minute: int,
    end_hour: int,
    end_minute: int,
) -> Tuple[int, int]:
    sh = max(0, min(23, int(start_hour)))
    sm = max(0, min(59, int(start_minute)))
    eh = max(0, min(23, int(end_hour)))
    em = max(0, min(59, int(end_minute)))
    return sh * 3600 + sm * 60, eh * 3600 + em * 60


def _monitoring_watch_tod_in_quiet_bounds(tod: int, start_sec: int, end_sec: int) -> bool:
    if start_sec < end_sec:
        return start_sec <= tod < end_sec
    if start_sec == end_sec:
        return False
    return tod >= start_sec or tod < end_sec


def _monitoring_watch_daily_quiet_windows() -> List[Tuple[int, int]]:
    """
    Daily quiet windows as ``(start_tod_seconds, end_tod_seconds)`` with **end exclusive**.
    Default: [19:59, 20:15) and [00:00, 00:15). Returns empty when master enable is off.
    """
    if not _lark_env_truthy("MONITORING_WATCH_QUIET_WINDOW_ENABLE"):
        return []
    windows: List[Tuple[int, int]] = [
        _monitoring_watch_quiet_tod_bounds_from_parts(
            _cfg_int("MONITORING_WATCH_QUIET_START_HOUR", 19),
            _cfg_int("MONITORING_WATCH_QUIET_START_MINUTE", 59),
            _cfg_int("MONITORING_WATCH_QUIET_END_HOUR", 20),
            _cfg_int("MONITORING_WATCH_QUIET_END_MINUTE", 15),
        )
    ]
    if _lark_env_truthy_or_default("MONITORING_WATCH_QUIET2_ENABLE", default=True):
        windows.append(
            _monitoring_watch_quiet_tod_bounds_from_parts(
                _cfg_int("MONITORING_WATCH_QUIET2_START_HOUR", 0),
                _cfg_int("MONITORING_WATCH_QUIET2_START_MINUTE", 0),
                _cfg_int("MONITORING_WATCH_QUIET2_END_HOUR", 0),
                _cfg_int("MONITORING_WATCH_QUIET2_END_MINUTE", 15),
            )
        )
    return windows


def _monitoring_watch_daily_quiet_tod_bounds() -> Tuple[int, int]:
    """
    First quiet window only (legacy helper). Prefer :func:`_monitoring_watch_daily_quiet_windows`.
    Returns ``(-1, -1)`` when ``MONITORING_WATCH_QUIET_WINDOW_ENABLE=0``.
    """
    wins = _monitoring_watch_daily_quiet_windows()
    if not wins:
        return -1, -1
    return wins[0]


def _monitoring_watch_in_daily_quiet_local(now: Optional[float] = None) -> bool:
    """True if current time lies in any configured daily quiet window."""
    windows = _monitoring_watch_daily_quiet_windows()
    if not windows:
        return False
    t = time.time() if now is None else float(now)
    lt = _monitoring_calendar_dt(t)
    tod = lt.hour * 3600 + lt.minute * 60 + lt.second
    return any(_monitoring_watch_tod_in_quiet_bounds(tod, s, e) for s, e in windows)


def _monitoring_watch_eval_window_unix(now: Optional[float] = None) -> Tuple[int, int]:
    """
    Minute-aligned Prometheus window for **watchdog only** (exclude current incomplete minute by default).
    Defaults: start = floor_to_minute(now) − 7m, end = floor_to_minute(now) − 2m
    (e.g. at 12:46:30 → 12:39:00 .. 12:44:00 unix, inclusive for query_range with step 60).
    """
    t = time.time() if now is None else float(now)
    end_off = max(0, _cfg_int("MONITORING_WATCH_EVAL_END_OFFSET_MINUTES", 2))
    start_off = max(end_off + 1, _cfg_int("MONITORING_WATCH_EVAL_START_OFFSET_MINUTES", 7))
    t_floor = int(t // 60) * 60
    end_unix = t_floor - end_off * 60
    start_unix = t_floor - start_off * 60
    return start_unix, end_unix


def fetch_monitoring_payload(
    session: Optional[requests.Session] = None,
    *,
    for_watchdog: bool = False,
    start_unix: Optional[int] = None,
    end_unix: Optional[int] = None,
) -> Dict[str, Any]:
    sess = session or grafana_login_session()
    w_start: Optional[int] = None
    w_end: Optional[int] = None
    if for_watchdog:
        if _lark_env_truthy("MONITORING_WATCH_MATCH_REPORT_WINDOW"):
            logger.info(
                "fetch_monitoring_payload watchdog eval uses **report** window "
                "(MONITORING_WATCH_MATCH_REPORT_WINDOW=1; same lookback/lag as /monitoring)"
            )
        elif start_unix is not None and end_unix is not None:
            w_start, w_end = int(start_unix), int(end_unix)
            logger.info(
                "fetch_monitoring_payload watchdog eval **frozen** window unix %s..%s (confirm re-query)",
                w_start,
                w_end,
            )
        else:
            w_start, w_end = _monitoring_watch_eval_window_unix()
            logger.info(
                "fetch_monitoring_payload watchdog eval window unix %s..%s (aligned minutes)",
                w_start,
                w_end,
            )
    primary = fetch_request_total_1m_series(session=sess, start_unix=w_start, end_unix=w_end)
    extra: List[Dict[str, Any]] = []
    try:
        p_eg = _fetch_panel_series_by_title(
            GRAFANA_PANEL_TITLE_EGAME_ONLINE,
            session=sess,
            start_unix=w_start,
            end_unix=w_end,
        )
        extra.append({"kind": MONITORING_EXTRA_KIND_EGAME_ONLINE, "payload": p_eg})
    except Exception:
        logger.exception("fetch Egame Online Number panel failed (optional monitor)")
    try:
        p_bet = _fetch_panel_series_by_title(
            GRAFANA_PANEL_TITLE_EGAMES_BET,
            session=sess,
            start_unix=w_start,
            end_unix=w_end,
        )
        extra.append({"kind": MONITORING_EXTRA_KIND_EGAMES_BET, "payload": p_bet})
    except Exception:
        logger.exception("fetch Egames 下注Bet/min panel failed (optional monitor)")
    if MONITORING_LIVESLOT_BET_ENABLE:
        try:
            p_ls = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_LIVESLOT_BET,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": MONITORING_EXTRA_KIND_LIVESLOT_BET, "payload": p_ls})
        except Exception:
            logger.exception("fetch %s panel failed (optional monitor)", GRAFANA_PANEL_TITLE_LIVESLOT_BET)
    if MONITORING_LIVESLOT_SPIN_COUNT_ENABLE:
        try:
            p_spin = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT, "payload": p_spin})
        except Exception:
            logger.exception(
                "fetch %s panel failed (optional monitor)", GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET
            )
    if extra:
        primary["extraPanels"] = extra
    return primary


def _lark_api_domain() -> str:
    """Open Platform API host (tenant token + send message); align with working WS region when set."""
    d = (_lark_open_api_domain_override or LARK_HOST or "").strip().rstrip("/")
    return d or "https://open.feishu.cn"


def _get_lark_oapi_client() -> Any:
    """Singleton Feishu/Lark OpenAPI client (``lark-oapi``); token refresh handled by SDK."""
    global _lark_oapi_client
    if not APP_ID or not APP_SECRET:
        raise ValueError("APP_ID and APP_SECRET required for Lark reply")
    try:
        from lark_oapi import Client
    except ImportError as e:
        raise ImportError(
            "Install the Feishu/Lark Python SDK: pip install -U lark-oapi"
        ) from e
    with _lark_oapi_client_lock:
        if _lark_oapi_client is None:
            _lark_oapi_client = (
                Client.builder()
                .app_id(str(APP_ID).strip())
                .app_secret(str(APP_SECRET).strip())
                .domain(_lark_api_domain())
                .timeout(120.0)
                .build()
            )
    return _lark_oapi_client


def _lark_send_text(receive_id_type: str, receive_id: str, text: str) -> None:
    from lark_oapi.api.im.v1.model.create_message_request import CreateMessageRequest
    from lark_oapi.api.im.v1.model.create_message_request_body import CreateMessageRequestBody

    client = _get_lark_oapi_client()
    body = (
        CreateMessageRequestBody.builder()
        .receive_id(receive_id)
        .msg_type("text")
        .content(json.dumps({"text": text}))
        .build()
    )
    req = (
        CreateMessageRequest.builder()
        .receive_id_type(receive_id_type)
        .request_body(body)
        .build()
    )
    resp = client.im.v1.message.create(req)
    if not resp.success():
        raise RuntimeError(
            f"Lark send failed: code={resp.code!r} msg={resp.msg!r} log_id={resp.get_log_id()!r}"
        )


def _split_text_for_lark(text: str, max_chars: int = 3200) -> List[str]:
    """
    Split long text into multiple chunks to avoid platform length truncation.
    Prefer paragraph/line boundaries; hard-cut only when necessary.
    """
    raw = str(text or "")
    if max_chars <= 200:
        max_chars = 200
    if len(raw) <= max_chars:
        return [raw]

    chunks: List[str] = []
    cur = ""

    def _flush() -> None:
        nonlocal cur
        if cur:
            chunks.append(cur)
            cur = ""

    def _push_piece(piece: str, sep: str = "\n\n") -> None:
        nonlocal cur
        if not piece:
            return
        if len(piece) > max_chars:
            _flush()
            lines = piece.split("\n")
            buf = ""
            for ln in lines:
                if len(ln) > max_chars:
                    if buf:
                        chunks.append(buf)
                        buf = ""
                    i = 0
                    while i < len(ln):
                        chunks.append(ln[i : i + max_chars])
                        i += max_chars
                    continue
                trial = f"{buf}\n{ln}" if buf else ln
                if len(trial) <= max_chars:
                    buf = trial
                else:
                    if buf:
                        chunks.append(buf)
                    buf = ln
            if buf:
                chunks.append(buf)
            return

        trial = f"{cur}{sep}{piece}" if cur else piece
        if len(trial) <= max_chars:
            cur = trial
        else:
            _flush()
            cur = piece

    for para in raw.split("\n\n"):
        _push_piece(para, "\n\n")
    _flush()
    return chunks or [raw[:max_chars]]


def _partition_monitoring_reply_for_card(reply: str, max_card: int) -> Tuple[str, str]:
    """
    If ``reply`` exceeds Feishu card budget, put an initial slice in the card and return the remainder
    for follow-up text messages (prefer cutting at ``\\n\\n[`` panel section boundaries).
    """
    raw = reply or ""
    if len(raw) <= max_card or max_card <= 200:
        return raw, ""
    note = "\n\n… *(full report continues in the next message(s))*"
    budget = max(600, max_card - len(note))
    cut = raw.rfind("\n\n[", 0, budget)
    min_cut = max(120, budget // 5)
    if cut < min_cut:
        cut = raw.rfind("\n\n", 0, budget)
    if cut < min_cut:
        cut = budget
    head = raw[:cut].rstrip() + note
    tail = raw[cut:].lstrip()
    if len(head) > max_card:
        cut = budget
        head = raw[:cut].rstrip() + note
        tail = raw[cut:].lstrip()
    return head, tail


def _lark_send_text_auto(receive_id_type: str, receive_id: str, text: str, max_chars: int = 3200) -> None:
    chunks = _split_text_for_lark(text, max_chars=max_chars)
    total = len(chunks)
    for i, c in enumerate(chunks, 1):
        body = c
        if total > 1:
            body = f"[{i}/{total}]\n{c}"
        _lark_send_text(receive_id_type, receive_id, body)


def _lark_tenant_access_token_string() -> str:
    """Same tenant token as SDK; used for multipart image upload (``requests``)."""
    if not APP_ID or not APP_SECRET:
        raise ValueError("APP_ID and APP_SECRET required")
    url = f"{_lark_api_domain()}/open-apis/auth/v3/tenant_access_token/internal"
    r = requests.post(
        url,
        json={"app_id": str(APP_ID).strip(), "app_secret": str(APP_SECRET).strip()},
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"tenant_token: {j}")
    tok = j.get("tenant_access_token")
    if not tok:
        raise RuntimeError(f"no tenant_access_token: {j}")
    return str(tok)


def _lark_resolve_bot_open_id_via_api() -> str:
    """
    When ``LARK_BOT_OPEN_ID`` is unset, resolve this app's bot ``open_id`` via ``GET bot/v3/info``.

    Cached after first attempt so IM hot path does not hammer the API.
    """
    global _lark_bot_open_id_api_cache
    if _lark_bot_open_id_api_cache is not None:
        return _lark_bot_open_id_api_cache
    with _lark_bot_open_id_resolve_lock:
        if _lark_bot_open_id_api_cache is not None:
            return _lark_bot_open_id_api_cache
        oid = ""
        try:
            if not APP_ID or not APP_SECRET:
                logger.warning(
                    "Cannot resolve bot open_id (empty LARK_BOT_OPEN_ID): set LARK_BOT_OPEN_ID or APP_ID/APP_SECRET"
                )
            else:
                tok = _lark_tenant_access_token_string()
                url = f"{_lark_api_domain()}/open-apis/bot/v3/info"
                r = requests.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {tok}",
                        "Content-Type": "application/json; charset=utf-8",
                    },
                    timeout=20,
                )
                r.raise_for_status()
                j = r.json()
                if int(j.get("code", -1)) != 0:
                    logger.warning("bot/v3/info error: %s", j)
                else:
                    data = j.get("data")
                    if isinstance(data, dict):
                        oid = _lark_dict_pick_str(data, "open_id", "openId")
                        if not oid:
                            inner = data.get("bot")
                            if isinstance(inner, dict):
                                oid = _lark_dict_pick_str(inner, "open_id", "openId")
                    oid = (oid or "").strip()
        except Exception:
            logger.exception(
                "bot/v3/info failed — set LARK_BOT_OPEN_ID in config/env or fix APP credentials"
            )
        _lark_bot_open_id_api_cache = oid
        if oid:
            logger.info(
                "Resolved bot open_id via bot/v3/info (override anytime with LARK_BOT_OPEN_ID)"
            )
        return oid


def _lark_effective_bot_open_id() -> str:
    """Configured ``LARK_BOT_OPEN_ID``, else cached result from :func:`_lark_resolve_bot_open_id_via_api`."""
    c = (LARK_BOT_OPEN_ID or "").strip()
    if c:
        return c
    return _lark_resolve_bot_open_id_via_api()


def _lark_upload_png_image_key(png: bytes) -> str:
    tok = _lark_tenant_access_token_string()
    url = f"{_lark_api_domain()}/open-apis/im/v1/images"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        files={"image": ("grafana.png", png, "image/png")},
        data={"image_type": "message"},
        timeout=120,
    )
    r.raise_for_status()
    j = r.json()
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"image upload: {j}")
    key = (j.get("data") or {}).get("image_key")
    if not key:
        raise RuntimeError(f"no image_key: {j}")
    return str(key)


def _lark_send_image_message(receive_id_type: str, receive_id: str, image_key: str) -> None:
    from lark_oapi.api.im.v1.model.create_message_request import CreateMessageRequest
    from lark_oapi.api.im.v1.model.create_message_request_body import CreateMessageRequestBody

    client = _get_lark_oapi_client()
    body = (
        CreateMessageRequestBody.builder()
        .receive_id(receive_id)
        .msg_type("image")
        .content(json.dumps({"image_key": image_key}))
        .build()
    )
    req = (
        CreateMessageRequest.builder()
        .receive_id_type(receive_id_type)
        .request_body(body)
        .build()
    )
    resp = client.im.v1.message.create(req)
    if not resp.success():
        raise RuntimeError(
            f"Lark image send failed: code={resp.code!r} msg={resp.msg!r} log_id={resp.get_log_id()!r}"
        )


def _monitoring_reply_to_card_md(reply: str) -> str:
    """See grafanaplatformbot: Feishu card markdown + dashed separator rows caused oversized headings."""
    out: List[str] = []
    for line in (reply or "").splitlines():
        st = line.strip()
        if st:
            compact = st.replace("|", "").replace(" ", "")
            if compact and all(c == "-" for c in compact):
                continue
        out.append(line)
    return "\n".join(out)


def _monitoring_card_body_md_strip_title(reply: str) -> str:
    r = (reply or "").strip()
    dup = f"[{GRAFANA_PANEL_TITLE}] graph"
    if r.startswith(dup):
        r = r[len(dup) :].lstrip("\n")
    return _monitoring_reply_to_card_md(r)


def _monitoring_card_callback_payload_strings(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Match jenkinsupdate behavior: scalar callback values are stringified for client compatibility."""
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ks = str(k)
        if isinstance(v, (dict, list)):
            out[ks] = v
        elif v is None:
            out[ks] = ""
        else:
            out[ks] = str(v)
    return out


def _monitoring_card_v2_callback_button(
    label: str,
    btn_type: str,
    payload: Dict[str, Any],
    *,
    element_id: str = "mon_rfsh",
) -> Dict[str, Any]:
    btn: Dict[str, Any] = {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label},
        "type": btn_type,
        "behaviors": [{"type": "callback", "value": _monitoring_card_callback_payload_strings(payload)}],
    }
    eid = (element_id or "").strip()[:20]
    if eid:
        btn["element_id"] = eid
    return btn


# ---------------------------------------------------------------------------
# /mute — per-channel alert suppression (in-process)
# ---------------------------------------------------------------------------
_MUTE_DURATION_CHOICES: List[Tuple[str, int]] = [
    ("15 minutes", 900),
    ("30 minutes", 1800),
    ("1 hour", 3600),
    ("2 hours", 7200),
    ("3 hours", 10800),
    ("4 hours", 14400),
    ("8 hours", 28800),
    ("12 hours", 43200),
    ("1 day", 86400),
]


def _monitoring_mutable_channels() -> List[Tuple[str, str]]:
    """(channel_id, display_label) for /m mute; ``http`` = LiveSlots primary payload."""
    return [
        ("http", GRAFANA_PANEL_TITLE),
        (MONITORING_EXTRA_KIND_EGAME_ONLINE, GRAFANA_PANEL_TITLE_EGAME_ONLINE),
        (MONITORING_EXTRA_KIND_EGAMES_BET, GRAFANA_PANEL_TITLE_EGAMES_BET),
        (MONITORING_EXTRA_KIND_LIVESLOT_BET, GRAFANA_PANEL_TITLE_LIVESLOT_BET),
        (MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT, GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET),
    ]


def _monitoring_mutable_channel_ids() -> Set[str]:
    return {c for c, _ in _monitoring_mutable_channels()}


def _mute_session_key(chat_id: str, operator_open_id: str) -> str:
    c = (chat_id or "").strip()
    o = (operator_open_id or "").strip()
    return f"{c}\n{o}"


def _monitoring_alert_channel_muted(channel: str) -> bool:
    until = _MONITORING_MUTE_UNTIL.get((channel or "").strip(), 0.0)
    return time.time() < float(until or 0.0)


def _mute_purge_expired() -> None:
    now = time.time()
    dead = [k for k, t in _MONITORING_MUTE_UNTIL.items() if float(t or 0.0) <= now]
    for k in dead:
        try:
            del _MONITORING_MUTE_UNTIL[k]
        except KeyError:
            pass


def _mute_apply_channels(channels: Set[str], duration_sec: float) -> Dict[str, float]:
    """Returns channel -> expiry unix for confirmation text."""
    now = time.time()
    dur = max(1.0, float(duration_sec))
    applied: Dict[str, float] = {}
    with _monitoring_reply_dispatch_lock:
        allowed = _monitoring_mutable_channel_ids()
        for ch in channels:
            c = (ch or "").strip()
            if c not in allowed:
                continue
            exp = now + dur
            prev = float(_MONITORING_MUTE_UNTIL.get(c, 0.0) or 0.0)
            if prev > exp:
                exp = prev
            _MONITORING_MUTE_UNTIL[c] = exp
            applied[c] = exp
    return applied


def _mute_clear_all_locked() -> None:
    _MONITORING_MUTE_UNTIL.clear()
    _mute_pending_selections.clear()


def _mute_toast_response(content: str, toast_type: str = "info") -> Dict[str, Any]:
    """HTTP card.action synchronous response body (Feishu shows a small toast)."""
    return {"toast": {"type": toast_type, "content": (content or "")[:500]}}


def _mute_channel_display_label(ch: str) -> str:
    for cid, lbl in _monitoring_mutable_channels():
        if cid == ch:
            return lbl
    return ch


def _mute_selection_card_elements(rid_t: str, rid: str) -> List[Dict[str, Any]]:
    rt = (rid_t or "").strip()
    rv = (rid or "").strip()
    base_rid: Dict[str, Any] = {}
    if rt in ("chat_id", "open_id") and rv:
        base_rid = {"rid_t": rt, "rid": rv}

    elements: List[Dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": (
                "**Mute alerts (multi-select)**\n\n"
                "1. Tap a monitor below repeatedly to **add/remove** it from the selection (see toast).\n"
                "2. **Mute all & duration** selects every monitor in this list at once.\n"
                "3. When ready, tap **Next: choose duration**.\n"
                "4. **Cancel** clears this selection session."
            ),
        }
    ]
    for ch, short_lbl in _monitoring_mutable_channels():
        payload = dict(base_rid)
        payload.update({"k": "mute_btn", "v": "toggle", "ch": ch})
        elements.append(
            _monitoring_card_v2_callback_button(
                short_lbl[:80],
                "default",
                _monitoring_card_callback_payload_strings(payload),
                element_id=f"mt_{hashlib.sha256(ch.encode()).hexdigest()[:10]}",
            )
        )

    row_advance = dict(base_rid)
    row_advance.update({"k": "mute_btn", "v": "next"})
    row_all = dict(base_rid)
    row_all.update({"k": "mute_btn", "v": "all"})
    row_cancel = dict(base_rid)
    row_cancel.update({"k": "mute_btn", "v": "cancel_sel"})
    elements.append(
        {
            "tag": "column_set",
            "flex_mode": "none",
            "horizontal_spacing": "default",
            "horizontal_align": "left",
            "columns": [
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "elements": [
                        _monitoring_card_v2_callback_button(
                            "Next: choose duration",
                            "primary",
                            _monitoring_card_callback_payload_strings(row_advance),
                            element_id="mute_next",
                        )
                    ],
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "elements": [
                        _monitoring_card_v2_callback_button(
                            "Mute all & duration",
                            "danger",
                            _monitoring_card_callback_payload_strings(row_all),
                            element_id="mute_all",
                        )
                    ],
                },
            ],
        }
    )
    elements.append(
        _monitoring_card_v2_callback_button(
            "Cancel",
            "default",
            _monitoring_card_callback_payload_strings(row_cancel),
            element_id="mute_cancel",
        )
    )
    return elements


def _mute_duration_card_elements(
    rid_t: str, rid: str, operator_open_id: str, chat_id: str
) -> List[Dict[str, Any]]:
    rt = (rid_t or "").strip()
    rv = (rid or "").strip()
    oid = (operator_open_id or "").strip()
    cid = (chat_id or "").strip()
    base: Dict[str, Any] = {"k": "mute_btn", "oid": oid, "cid": cid}
    if rt in ("chat_id", "open_id") and rv:
        base["rid_t"] = rt
        base["rid"] = rv

    elements: List[Dict[str, Any]] = [
        {"tag": "markdown", "content": "**Choose mute duration** (applies to selected monitors)"},
    ]
    for label, secs in _MUTE_DURATION_CHOICES:
        pl = dict(base)
        pl["v"] = "apply"
        pl["sec"] = str(int(secs))
        elements.append(
            _monitoring_card_v2_callback_button(
                label,
                "primary",
                _monitoring_card_callback_payload_strings(pl),
                element_id=f"mute_d_{secs}"[:20],
            )
        )
    pl_cancel = dict(base)
    pl_cancel["v"] = "cancel_sel"
    elements.append(
        _monitoring_card_v2_callback_button(
            "Back / cancel selection",
            "default",
            _monitoring_card_callback_payload_strings(pl_cancel),
            element_id="mute_back",
        )
    )
    return elements


def _mute_selection_card_dict(rid_t: str, rid: str) -> Dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Mute monitoring alerts"},
            "subtitle": {
                "tag": "plain_text",
                "content": (MONITORING_MUTE_TRIGGER or "/m").strip()[:190],
            },
        },
        "body": {"elements": _mute_selection_card_elements(rid_t, rid)},
    }


def _mute_duration_card_dict(rid_t: str, rid: str, operator_open_id: str, chat_id: str) -> Dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Choose mute duration"},
            "subtitle": {
                "tag": "plain_text",
                "content": (MONITORING_MUTE_TRIGGER or "/m").strip()[:190],
            },
        },
        "body": {"elements": _mute_duration_card_elements(rid_t, rid, operator_open_id, chat_id)},
    }


def _mute_send_duration_card_async(
    rid_t: str, rid: str, operator_open_id: str, chat_id: str
) -> None:
    def _run() -> None:
        try:
            rt = (rid_t or "").strip()
            rv = (rid or "").strip()
            if rt not in ("chat_id", "open_id") or not rv:
                return
            card = _mute_duration_card_dict(rt, rv, operator_open_id, chat_id)
            _lark_send_interactive_card(rt, rv, card)
        except Exception:
            logger.exception("mute: send duration card failed")

    threading.Thread(target=_run, daemon=True, name="mute-duration-card").start()


def _mute_send_selection_card_worker(chat_id: str, open_id: str, debounce_key: str) -> None:
    try:
        rt = "chat_id" if (chat_id or "").strip() else "open_id"
        rv = (chat_id or open_id or "").strip()
        if not rv:
            logger.warning("mute: missing receive_id")
            return
        card = _mute_selection_card_dict(rt, rv)
        _lark_send_interactive_card(rt, rv, card)
    except Exception:
        logger.exception("mute: send selection card failed")
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _cancelmute_worker(chat_id: str, open_id: str, debounce_key: str) -> None:
    try:
        rt = "chat_id" if (chat_id or "").strip() else "open_id"
        rv = (chat_id or open_id or "").strip()
        if not rv:
            return
        with _monitoring_reply_dispatch_lock:
            _mute_clear_all_locked()
        _lark_send_text(
            rt,
            rv,
            "**All** alert mutes have been cleared — new alerts will be delivered normally.\n"
            "(Mute state is kept in memory only and is lost when the process restarts.)",
        )
    except Exception:
        logger.exception("cancelmute failed")
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _deploy_message_text_blobs(
    msg: Dict[str, Any],
    event: Dict[str, Any],
    raw_text: str,
    clean: str,
) -> List[str]:
    """Collect every text surface that might carry the deploy phrase (HTTP / post / mobile skew)."""
    blobs: List[str] = []
    seen: Set[str] = set()

    def _add(t: str) -> None:
        s = re.sub(r"\s+", " ", (t or "").strip())
        if s and s not in seen:
            seen.add(s)
            blobs.append(s)

    _add(clean or "")
    _add(raw_text or "")
    if isinstance(event, dict):
        for k in ("text_without_at_bot", "textWithoutAtBot", "text"):
            _add(_lark_dict_pick_str(event, k))
    if isinstance(msg, dict):
        _add(_lark_extract_plain_text_from_message(msg))
        raw_c = msg.get("content")
        if raw_c is None:
            raw_c = msg.get("Content")
        if isinstance(raw_c, str):
            _add(raw_c)
            try:
                obj = json.loads(raw_c or "{}")
                if isinstance(obj, dict):
                    parts: List[str] = []
                    _lark_collect_post_text(obj, parts)
                    _add("".join(parts))
            except (json.JSONDecodeError, TypeError):
                pass
        elif isinstance(raw_c, dict):
            _add(json.dumps(raw_c, ensure_ascii=False))
            parts2: List[str] = []
            _lark_collect_post_text(raw_c, parts2)
            _add("".join(parts2))
    return blobs


def _im_text_matches_deploy_request(*texts: str) -> bool:
    """True for ``/deploy`` or natural ``git pull … restart …`` phrasing."""
    tri = (DEPLOY_TRIGGER or "/deploy").strip()
    for raw in texts:
        c = re.sub(r"\s+", " ", (raw or "").strip())
        if not c:
            continue
        if tri and _im_command_matches(c, tri):
            return True
        if _DEPLOY_REQUEST_RE.search(c):
            return True
    return False


def _lark_payload_looks_deploy_like(data: Any) -> bool:
    try:
        blob = json.dumps(data, ensure_ascii=False).casefold()
    except Exception:
        return False
    if "git pull" in blob and "restart" in blob:
        return True
    tri = (DEPLOY_TRIGGER or "/deploy").strip().casefold()
    return bool(tri and tri in blob)


def _deploy_sender_id_set(
    sender: Dict[str, Any],
    open_id: str,
    send_wrap: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    ids: Set[str] = set()
    o = (open_id or "").strip()
    if o:
        ids.add(o)
    for root in (sender, send_wrap or {}):
        if not isinstance(root, dict):
            continue
        for k in ("open_id", "openId", "user_id", "userId", "union_id", "unionId"):
            v = root.get(k)
            if v is not None:
                s = str(v).strip()
                if s:
                    ids.add(s)
        sid = root.get("sender_id") or root.get("senderId")
        if isinstance(sid, dict):
            for k in ("open_id", "openId", "user_id", "userId", "union_id", "unionId"):
                v = sid.get(k)
                if v is not None:
                    s = str(v).strip()
                    if s:
                        ids.add(s)
        for s in _lark_iter_mention_scalar_strings(root):
            if s.startswith("ou_"):
                ids.add(s)
    return ids


def _deploy_sender_authorized(
    sender: Dict[str, Any],
    open_id: str,
    send_wrap: Optional[Dict[str, Any]] = None,
) -> bool:
    if not DEPLOY_ALLOWED_USER_OPEN_ID_SET:
        return False
    sender_ids = _deploy_sender_id_set(sender, open_id, send_wrap)
    return bool(sender_ids & DEPLOY_ALLOWED_USER_OPEN_ID_SET)


def _deploy_sender_primary_open_id(
    sender: Dict[str, Any],
    open_id: str,
    send_wrap: Optional[Dict[str, Any]] = None,
) -> str:
    o = (open_id or "").strip()
    if o:
        return o
    for s in sorted(_deploy_sender_id_set(sender, open_id, send_wrap)):
        if s.startswith("ou_"):
            return s
    return "unknown"


def _deploy_bot_addressed(
    raw_text: str,
    mentions: Any,
    content_at_entity_ids: Optional[List[str]],
    msg: Optional[Dict[str, Any]],
    chat_type: str,
) -> bool:
    """True when this deploy should run on **Game** bot (not Platform peer)."""
    ct = (chat_type or "").strip().lower()
    if ct in ("p2p", "private"):
        return True
    if not MONITORING_TRIGGER_REQUIRES_AT_BOT:
        return True
    if _lark_im_bot_addressed_in_mentions_or_body(mentions, content_at_entity_ids):
        return True
    if _monitoring_at_bot_requirement_satisfied(
        raw_text,
        mentions,
        content_at_entity_ids=content_at_entity_ids,
        msg=msg,
        chat_type=chat_type,
    ):
        return True
    ml = mentions if isinstance(mentions, list) else []
    app = str(APP_ID or "").strip()
    if app and _lark_mentions_any_row_matches_app(ml, app):
        return True
    return False


def _deploy_reply(chat_id: str, open_id: str, text: str) -> None:
    rt = "chat_id" if (chat_id or "").strip() else "open_id"
    rv = (chat_id or open_id or "").strip()
    if not rv:
        return
    _lark_send_text(rt, rv, text)


def _deploy_git_repo_path() -> str:
    p = (DEPLOY_GIT_REPO_PATH or "").strip()
    if p:
        return os.path.abspath(p)
    return os.path.dirname(os.path.abspath(__file__))


def _deploy_run_cmd(
    argv: List[str],
    *,
    cwd: Optional[str] = None,
    timeout: int = 300,
) -> Tuple[int, str]:
    try:
        proc = subprocess.run(
            argv,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        out = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, out.strip()
    except subprocess.TimeoutExpired as e:
        partial = ((e.stdout or "") + (e.stderr or "")).strip()
        return 124, partial or f"timed out after {timeout}s"
    except Exception as e:
        return 1, str(e)


def _deploy_schedule_service_restart(svc: str, delay_sec: float = 2.0) -> None:
    """Restart after a short delay so Lark can deliver the final Done message first."""
    name = (svc or "p0bot").strip() or "p0bot"
    delay = max(1.0, min(15.0, float(delay_sec)))
    cmd = f"sleep {int(delay)} && systemctl restart {shlex.quote(name)}"
    subprocess.Popen(
        ["bash", "-c", cmd],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _deploy_git_pull_restart_worker(chat_id: str, open_id: str, debounce_key: str) -> None:
    def _reply(msg: str) -> None:
        try:
            _deploy_reply(chat_id, open_id, msg)
        except Exception:
            logger.exception("deploy reply failed")

    try:
        repo = _deploy_git_repo_path()
        svc = (DEPLOY_SYSTEMD_SERVICE or "p0bot").strip() or "p0bot"
        _reply(f"Starting `git pull origin main` in `{repo}` …")
        rc, out = _deploy_run_cmd(["git", "pull", "origin", "main"], cwd=repo, timeout=300)
        tail = "\n".join((out or "").splitlines()[-12:])
        if rc != 0:
            _reply(f"Deploy failed — `git pull origin main` exit {rc}:\n```\n{tail or '(no output)'}\n```")
            return
        _reply(f"`git pull origin main` OK (exit 0):\n```\n{tail or '(no output)'}\n```")
        _deploy_schedule_service_restart(svc, delay_sec=2.0)
        _reply(
            f"Done — `{svc}` will restart in ~2s and come back shortly."
        )
    except Exception:
        logger.exception("deploy git pull + restart failed")
        _reply("Deploy failed — unexpected error (see server logs).")
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _deploy_try_handle_im_message(
    *,
    msg: Dict[str, Any],
    event: Dict[str, Any],
    data: Dict[str, Any],
    raw_text: str,
    clean: str,
    mentions: Any,
    content_at_entity_ids: Optional[List[str]],
    im_chat_type: str,
    chat_id: str,
    open_id: str,
    sender: Dict[str, Any],
    send_wrap: Dict[str, Any],
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """
    Handle ``/deploy`` or ``git pull … restart …`` for the authorized user.
    Returns True when the message looks like a deploy command (handled or rejected with a reply).
    """
    deploy_blobs = _deploy_message_text_blobs(msg, event, raw_text or "", clean or "")
    if not _im_text_matches_deploy_request(*deploy_blobs):
        return False

    logger.info(
        "deploy phrase detected open_id=%r chat=%r authorized=%s blobs=%r",
        (open_id or "")[:24],
        (chat_id or "")[:16],
        _deploy_sender_authorized(sender, open_id or "", send_wrap),
        [b[:80] for b in deploy_blobs[:4]],
    )

    if not DEPLOY_ENABLE:
        logger.warning("deploy-like message but DEPLOY_ENABLE=0")
        if _deploy_sender_authorized(sender, open_id or "", send_wrap):
            try:
                _deploy_reply(chat_id, open_id, "🚀 Deploy (p0bot): disabled (DEPLOY_ENABLE=0).")
            except Exception:
                logger.exception("deploy disabled feedback failed")
        return True

    if not _deploy_sender_authorized(sender, open_id or "", send_wrap):
        sender_ids = sorted(_deploy_sender_id_set(sender, open_id or "", send_wrap))
        primary = _deploy_sender_primary_open_id(sender, open_id or "", send_wrap)
        logger.info(
            "deploy request denied — sender ids=%r allowed=%r",
            sender_ids,
            sorted(DEPLOY_ALLOWED_USER_OPEN_ID_SET),
        )
        try:
            # open_ids are per-app, so the id below is the ONLY one that works here — print it
            # in full so it can be pasted straight into .env instead of guessed at.
            _deploy_reply(
                chat_id,
                open_id,
                "🚫 Deploy (p0bot): not authorized.\n"
                f"Your p0bot open_id: `{primary}`\n"
                + (f"Allowed: `{', '.join(sorted(DEPLOY_ALLOWED_USER_OPEN_ID_SET))}`"
                   if DEPLOY_ALLOWED_USER_OPEN_ID_SET
                   else "Allowed: (nobody yet — DEPLOY_ALLOWED_USER_OPEN_ID is empty)")
                + "\nTo allow yourself: put that id in `DEPLOY_ALLOWED_USER_OPEN_ID` in "
                  "`/root/p0bot/.env`, then `systemctl restart p0bot`.",
            )
        except Exception:
            logger.exception("deploy unauthorized feedback failed")
        return True

    if not _deploy_bot_addressed(
        raw_text,
        mentions,
        content_at_entity_ids,
        msg,
        im_chat_type,
    ):
        logger.info("deploy request skip — @ not addressed to this bot")
        try:
            _deploy_reply(
                chat_id,
                open_id,
                "🚀 Deploy (p0bot): please @ **p0bot** in this group, then send "
                "`/deploy` — or: git pull origin main and restart service",
            )
        except Exception:
            logger.exception("deploy @-gate feedback failed")
        return True

    processed_stick_d = _monitoring_processed_stick(
        mid, im_event_id, chat_id or "", sender_debounce, msg_time
    )
    debounce_key_d = f"{(chat_id or '').strip()}\n__deploy_cmd__"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            logger.info("duplicate IM event_id=%s — skip (deploy)", im_event_id)
            return True
        if processed_stick_d and processed_stick_d in _processed_lark_message_ids:
            logger.info("duplicate deploy stick=%r — skip", processed_stick_d[:96])
            return True
        if debounce_key_d in _monitoring_inflight_keys:
            logger.info("deploy skip — already in flight (duplicate delivery — no extra reply)")
            return True
        _monitoring_inflight_keys.add(debounce_key_d)
        if processed_stick_d:
            _processed_lark_message_ids.add(processed_stick_d)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    logger.info("deploy command accepted chat=%r open=%r", bool(chat_id), bool(open_id))
    try:
        _deploy_reply(chat_id, open_id, "OK")
    except Exception:
        logger.exception("deploy OK ack failed")
    threading.Thread(
        target=_deploy_git_pull_restart_worker,
        args=(chat_id, open_id, debounce_key_d),
        daemon=True,
        name="deploy-git-restart",
    ).start()
    return True


def _monitoring_at_mention_help_text() -> str:
    mo = (MONITORING_TRIGGER or "/mo").strip()
    m = (MONITORING_MUTE_TRIGGER or "/m").strip()
    c = (MONITORING_CANCELMUTE_TRIGGER or "/c").strip()
    return (
        "Commands:\n"
        f"- `{mo}` — Grafana monitoring summary\n"
        f"- `{m}` — mute alerts (interactive card)\n"
        f"- `{c}` — clear all mutes"
    )


def _monitoring_at_mention_help_worker(chat_id: str, open_id: str, debounce_key: str) -> None:
    try:
        rt = "chat_id" if (chat_id or "").strip() else "open_id"
        rv = (chat_id or open_id or "").strip()
        if not rv:
            return
        _lark_send_text(rt, rv, _monitoring_at_mention_help_text())
    except Exception:
        logger.exception("at-mention command help send failed")
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _mute_card_action_dispatch(data: Dict[str, Any], val: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return HTTP response dict for card.action when toast should be shown synchronously."""
    ev_id = _lark_im_payload_event_id(data)
    with _monitoring_reply_dispatch_lock:
        if ev_id and ev_id in _monitoring_card_action_event_ids:
            return _mute_toast_response("Duplicate click ignored", "info")
        if ev_id:
            _monitoring_card_action_event_ids.add(ev_id)
            if len(_monitoring_card_action_event_ids) > 2000:
                _monitoring_card_action_event_ids.clear()
                _monitoring_card_action_event_ids.add(ev_id)

    chat_id, open_id = _lark_card_action_target_ids(data)
    rid_t = _lark_dict_pick_str(val, "rid_t", "receive_id_type")
    rid = _lark_dict_pick_str(val, "rid", "receive_id")
    if rid_t == "chat_id" and rid:
        chat_id = rid
        open_id = ""
    elif rid_t == "open_id" and rid:
        open_id = rid

    op_open = open_id
    if not op_open:
        ev = data.get("event") if isinstance(data.get("event"), dict) else {}
        op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
        op_id = op.get("operator_id") if isinstance(op.get("operator_id"), dict) else {}
        op_open = _lark_dict_pick_str(op_id, "open_id", "openId", "user_id", "userId")

    sk = _mute_session_key(chat_id, op_open or "")
    v = _lark_dict_pick_str(val, "v")
    allowed = _monitoring_mutable_channel_ids()

    if v == "toggle":
        ch = _lark_dict_pick_str(val, "ch").strip()
        if ch not in allowed:
            return _mute_toast_response("Unknown monitor", "warning")
        with _monitoring_reply_dispatch_lock:
            pend = _mute_pending_selections.setdefault(sk, set())
            if ch in pend:
                pend.discard(ch)
                msg = f"Removed: {_mute_channel_display_label(ch)} ({len(pend)} selected)"
            else:
                pend.add(ch)
                msg = f"Added: {_mute_channel_display_label(ch)} ({len(pend)} selected)"
        return _mute_toast_response(msg, "success")

    if v == "all":
        with _monitoring_reply_dispatch_lock:
            _mute_pending_selections[sk] = set(allowed)
        _mute_send_duration_card_async(rid_t, rid, op_open or "", chat_id)
        return _mute_toast_response("All monitors selected — pick a duration", "success")

    if v == "cancel_sel":
        with _monitoring_reply_dispatch_lock:
            _mute_pending_selections.pop(sk, None)
        return _mute_toast_response("Selection cleared", "info")

    if v == "next":
        with _monitoring_reply_dispatch_lock:
            pend = _mute_pending_selections.get(sk) or set()
            pend = set(pend)
        if not pend:
            return _mute_toast_response("Select at least one monitor first", "warning")
        _mute_send_duration_card_async(rid_t, rid, op_open or "", chat_id)
        return _mute_toast_response("Choose a duration", "success")

    if v == "apply":
        oid = _lark_dict_pick_str(val, "oid").strip()
        cid = _lark_dict_pick_str(val, "cid").strip()
        sk2 = _mute_session_key(cid, oid)
        try:
            sec = int(float(_lark_dict_pick_str(val, "sec") or "0"))
        except (TypeError, ValueError):
            sec = 0
        if sec <= 0:
            return _mute_toast_response("Invalid duration", "warning")
        with _monitoring_reply_dispatch_lock:
            pend = set(_mute_pending_selections.pop(sk2, set()) or set())
        if not pend:
            return _mute_toast_response(
                f"Nothing to mute — run {MONITORING_MUTE_TRIGGER} and select monitors again", "warning"
            )
        applied = _mute_apply_channels(pend, float(sec))
        if not applied:
            return _mute_toast_response("Could not apply mute", "warning")
        lines = [f"Alert mute enabled for {len(applied)} monitor(s):"]
        for ch, exp in sorted(applied.items(), key=lambda kv: kv[0]):
            lines.append(f"- {_mute_channel_display_label(ch)} until {_fmt_ts_short(exp)}")
        summary = "\n".join(lines)
        try:
            rt = (rid_t or "").strip()
            rv = (rid or "").strip()
            if rt in ("chat_id", "open_id") and rv:
                _lark_send_text(rt, rv, summary)
        except Exception:
            logger.exception("mute: confirmation text send failed")
        return _mute_toast_response("Mute applied", "success")

    return _mute_toast_response("Unknown action", "warning")


def _lark_dispatch_card_action(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Route card.action; optional dict → merge into HTTP 200 JSON (toast)."""
    val = _lark_card_action_value(data)
    k = _lark_dict_pick_str(val, "k")
    if k == "mute_btn":
        _mute_purge_expired()
        return _mute_card_action_dispatch(data, val)
    if k == "monitoring_btn":
        _handle_monitoring_card_action(data)
        return None
    logger.info("card.action ignored value=%r", val or None)
    return None


def _monitoring_interactive_card_dict(
    reply: str,
    receive_id_type: str,
    receive_id: str,
    lark_img_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Feishu card JSON v2 — markdown card, optional embedded PNG."""
    title = "📊 GRAFANA GAME GRAPH"
    is_alert_card = (reply or "").lstrip().startswith("[ALERT]")
    elements: List[Dict[str, Any]] = [
        {"tag": "markdown", "content": _monitoring_card_body_md_strip_title(reply)},
    ]
    ik = (lark_img_key or "").strip()
    if ik:
        elements.append(
            {
                "tag": "img",
                "img_key": ik,
                "alt": {"tag": "plain_text", "content": "Grafana"},
            }
        )
    if _lark_env_truthy("MONITORING_MESSAGE_CARD_BUTTON_ENABLE"):
        cb_payload: Dict[str, Any] = {"k": "monitoring_btn", "v": "refresh"}
        rt = (receive_id_type or "").strip()
        rv = (receive_id or "").strip()
        if rt in ("chat_id", "open_id") and rv:
            cb_payload["rid_t"] = rt
            cb_payload["rid"] = rv
        elements.append(
            _monitoring_card_v2_callback_button(
                _cfg_str("MONITORING_MESSAGE_CARD_BUTTON_TEXT", "Resend screenshot")[:40],
                "primary",
                cb_payload,
                element_id="mon_rfsh",
            )
        )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": title[:190]},
            "subtitle": {
                "tag": "plain_text",
                "content": "Alert Triggered" if is_alert_card else "Grafana · monitoring",
            },
        },
        "body": {"elements": elements},
    }


def _lark_send_interactive_card(receive_id_type: str, receive_id: str, card: Dict[str, Any]) -> None:
    """Send ``msg_type=interactive`` via HTTP ``im/v1/messages`` (reliable JSON encoding)."""
    tok = _lark_tenant_access_token_string()
    url = f"{_lark_api_domain()}/open-apis/im/v1/messages"
    content_str = json.dumps(card, ensure_ascii=False)
    payload = {"receive_id": receive_id, "msg_type": "interactive", "content": content_str}
    r = requests.post(
        url,
        params={"receive_id_type": receive_id_type},
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    j = r.json()
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"im/v1/messages interactive failed: {j}")


def _lark_send_monitoring_user_message(
    receive_id_type: str,
    receive_id: str,
    reply: str,
    lark_img_key: Optional[str] = None,
) -> Tuple[bool, bool]:
    """
    Send monitoring summary to the user: interactive card (optional embedded PNG) or plain text.

    Returns ``(sent_interactive_card_ok, embedded_png_in_card)``.
    """
    rid = (receive_id or "").strip()
    if not rid:
        raise ValueError("empty receive_id for monitoring message")
    raw_reply = reply or ""
    max_card = _cfg_int("MONITORING_MESSAGE_CARD_REPLY_MAX_CHARS", 28000)
    if max_card <= 0:
        max_card = 3000
    overflow_chunk = max(2000, _cfg_int("MONITORING_MESSAGE_OVERFLOW_TEXT_CHUNK_CHARS", 12000))

    reply_for_card = raw_reply
    overflow_tail = ""
    if len(raw_reply) > max_card:
        if MONITORING_MESSAGE_CARD_ENABLE and _lark_env_truthy_or_default(
            "MONITORING_MESSAGE_CARD_TRUNCATE",
            default=True,
        ):
            reply_for_card, overflow_tail = _partition_monitoring_reply_for_card(raw_reply, max_card)
            logger.warning(
                "monitoring interactive card: body %s chars exceeds MONITORING_MESSAGE_CARD_REPLY_MAX_CHARS=%s "
                "— card has %s chars, sending %s more via text message(s)",
                len(raw_reply),
                max_card,
                len(reply_for_card),
                len(overflow_tail),
            )
        else:
            _lark_send_text_auto(receive_id_type, rid, raw_reply, max_chars=3200)
            return False, False

    if MONITORING_MESSAGE_CARD_ENABLE:
        try:
            card = _monitoring_interactive_card_dict(
                reply_for_card, receive_id_type, rid, lark_img_key
            )
            _lark_send_interactive_card(receive_id_type, rid, card)
            if overflow_tail.strip():
                _lark_send_text_auto(
                    receive_id_type,
                    rid,
                    overflow_tail,
                    max_chars=min(20000, overflow_chunk),
                )
            return True, bool((lark_img_key or "").strip())
        except Exception as e:
            logger.warning(
                "monitoring interactive card failed (%s) — fallback to plain text; "
                'check app permission "Send message cards".',
                e,
            )
    _lark_send_text_auto(receive_id_type, rid, raw_reply, max_chars=3200)
    return False, False


# Playwright ``wait_for_function`` / ``evaluate``: true when dashboard body looks mounted (not only header).
_GRAFANA_JS_REACTROOT_HAS_CHARTS = """() => {
  const rr = document.getElementById('reactRoot');
  if (!rr) return false;
  const n = (sel) => rr.querySelectorAll(sel).length;
  const grid = n('.react-grid-item');
  const uplot = n('[data-testid="uplot-main-div"]');
  const canv = n('canvas');
  const gridCanv = rr.querySelectorAll('.react-grid-item canvas').length;
  const panels = n('[data-testid^="data-testid Panel"], [class*="PanelChrome"], [class*="panel-content"]');
  const main = document.querySelector('main');
  const mh = main ? main.getBoundingClientRect().height : 0;
  if (grid + uplot + canv >= 1) return true;
  if (gridCanv >= 1) return true;
  if (panels >= 1 && mh > 110) return true;
  if (canv >= 1 && mh > 90) return true;
  return false;
}"""


def _grafana_playwright_dock_nav_only(page: Any, timeout_ms: int) -> None:
    """
    Grafana 12+：收起左侧 mega-menu，让主 dashboard 占满宽度。
    ``#dock-menu-button``（aria-label Dock menu）常在 ``[data-testid='data-testid navigation mega-menu']``
    对话框内；使用 ``visible`` 等待 + ``force=True`` 避免被遮罩/动画挡住导致 silent 失败。
    若仍无按钮则尝试打开 #mega-menu-toggle 后再点 Dock。

    Set ``GRAFANA_SCREENSHOT_PAGE_RELOAD_INSTEAD_OF_DOCK=1`` to call ``page.reload()`` instead of Dock clicks.
    """
    if _lark_env_truthy("GRAFANA_SCREENSHOT_PAGE_RELOAD_INSTEAD_OF_DOCK"):
        t = min(25000, max(5000, int(timeout_ms)))
        try:
            page.reload(wait_until="domcontentloaded", timeout=t)
            page.wait_for_timeout(400)
            logger.info(
                "Grafana screenshot: page.reload() instead of Dock menu "
                "(GRAFANA_SCREENSHOT_PAGE_RELOAD_INSTEAD_OF_DOCK=1)"
            )
        except Exception as e:
            logger.warning("Grafana screenshot: page.reload failed: %s", e)
        return
    if not _lark_env_truthy("GRAFANA_SCREENSHOT_DOCK_NAV"):
        return
    t = min(25000, max(5000, int(timeout_ms)))
    try:
        page.locator("#reactRoot").wait_for(state="visible", timeout=t)
    except Exception:
        pass

    def _click_dock_js() -> bool:
        """Grafana/React 有时拦截 Playwright 合成点击；DOM 内连续两次完整 click（等同双击）。"""
        try:
            r = page.evaluate(
                """() => {
                  const mega = document.querySelector(
                    '[data-testid="data-testid navigation mega-menu"]'
                  );
                  let dock = mega ? mega.querySelector('#dock-menu-button') : null;
                  if (!dock) dock = document.querySelector('#dock-menu-button');
                  if (!dock) {
                    const all = Array.from(document.querySelectorAll('button[aria-label="Dock menu"]'));
                    dock = all[0] || null;
                  }
                  if (!dock) return 'missing';
                  try { dock.scrollIntoView({ block: 'center', inline: 'center' }); } catch (e) {}
                  try { dock.focus({ preventScroll: true }); } catch (e) {}
                  const v = window;
                  const o = { bubbles: true, cancelable: true, view: v };
                  const fireOnce = () => {
                    dock.dispatchEvent(new MouseEvent('pointerover', o));
                    dock.dispatchEvent(new MouseEvent('mouseover', o));
                    dock.dispatchEvent(new MouseEvent('pointerdown', o));
                    dock.dispatchEvent(new MouseEvent('mousedown', o));
                    dock.dispatchEvent(new MouseEvent('pointerup', o));
                    dock.dispatchEvent(new MouseEvent('mouseup', o));
                    dock.dispatchEvent(new MouseEvent('click', o));
                    if (typeof dock.click === 'function') dock.click();
                  };
                  fireOnce();
                  fireOnce();
                  try { dock.dispatchEvent(new MouseEvent('dblclick', o)); } catch (e2) {}
                  return 'ok';
                }"""
            )
            if r == "ok":
                logger.info(
                    "Grafana screenshot: Dock menu fired via in-page JS (double: two click cycles + dblclick)"
                )
                return True
        except Exception as ex:
            logger.info("Grafana screenshot: Dock JS click failed: %s", ex)
        return False

    def _click_dock() -> bool:
        # 侧栏处于「锁定」时会出现 Unlock menu，先点一次解除再 Dock（顺序因 Grafana 版本而异）
        for unlock_sel in (
            'button[aria-label="Unlock menu"]',
            '[aria-label="Unlock menu"]',
        ):
            ul = page.locator(unlock_sel).first
            try:
                if ul.count() > 0 and ul.is_visible():
                    ul.scroll_into_view_if_needed(timeout=3000)
                    ul.click(timeout=3000, force=True)
                    page.wait_for_timeout(200)
                    logger.info("Grafana screenshot: clicked %r before Dock", unlock_sel)
            except Exception:
                pass

        selectors = (
            '[data-testid="data-testid navigation mega-menu"] #dock-menu-button',
            '[data-testid="data-testid navigation mega-menu"] button[aria-label="Dock menu"]',
            "#dock-menu-button",
            'button[id="dock-menu-button"]',
            'button[aria-label="Dock menu"]',
        )
        for sel in selectors:
            loc = page.locator(sel).first
            try:
                if loc.count() == 0:
                    continue
                loc.wait_for(state="attached", timeout=5000)
                try:
                    loc.wait_for(state="visible", timeout=2500)
                except Exception:
                    pass
                loc.scroll_into_view_if_needed(timeout=5000)
                page.wait_for_timeout(140)
                try:
                    loc.hover(timeout=2500)
                    page.wait_for_timeout(80)
                except Exception:
                    pass
                try:
                    try:
                        loc.click(timeout=6000, force=True, delay=50, click_count=2)
                    except TypeError:
                        loc.dblclick(timeout=6000, force=True, delay=50)
                except Exception as e1:
                    logger.info(
                        "Grafana screenshot: Dock Playwright double-click failed %r (%s); trying dispatch_event",
                        sel,
                        e1,
                    )
                    try:
                        loc.dispatch_event("click")
                        page.wait_for_timeout(90)
                        loc.dispatch_event("click")
                    except Exception as e2:
                        logger.info("Grafana screenshot: Dock dispatch_event failed: %s", e2)
                        continue
                logger.info("Grafana screenshot: double-clicked Dock menu via %r", sel)
                return True
            except Exception:
                continue
        try:
            alt = page.get_by_role("button", name=re.compile(r"^\s*Dock menu\s*$", re.I)).first
            if alt.count() > 0:
                alt.wait_for(state="attached", timeout=4000)
                alt.scroll_into_view_if_needed(timeout=5000)
                try:
                    try:
                        alt.click(timeout=6000, force=True, delay=50, click_count=2)
                    except TypeError:
                        alt.dblclick(timeout=6000, force=True, delay=50)
                except Exception:
                    alt.dispatch_event("click")
                    page.wait_for_timeout(90)
                    alt.dispatch_event("click")
                logger.info("Grafana screenshot: double-clicked Dock menu (role=name)")
                return True
        except Exception:
            pass
        if _click_dock_js():
            return True
        return False

    try:
        if _click_dock():
            page.wait_for_timeout(320)
            return
        for open_sel in (
            "#mega-menu-toggle",
            '[data-testid="mega-menu-toggle"]',
            "button[aria-label*='Open menu']",
        ):
            mt = page.locator(open_sel).first
            try:
                if mt.count() == 0:
                    continue
                mt.click(timeout=2500, force=True)
                page.wait_for_timeout(450)
            except Exception:
                continue
            if _click_dock():
                page.wait_for_timeout(320)
                return
        logger.warning(
            "Grafana screenshot: could not click Dock menu — left nav may stay open "
            "(selectors tried: mega-menu #dock-menu-button, #dock-menu-button, aria-label)"
        )
    except Exception as e:
        logger.info("Grafana screenshot: dock nav optional step failed: %s", e)

    page.wait_for_timeout(200)


def _grafana_expand_collapsed_dashboard_rows(page: Any) -> None:
    """
    Grafana dashboards often collapse row groups (only the row title e.g. ``KPI`` is visible).
    Click collapsed row toggles so panels mount and queries run.
    """
    if not _lark_env_truthy("GRAFANA_SCREENSHOT_EXPAND_ROWS"):
        return
    selectors = (
        '[data-testid="dashboard-row-title"] [aria-expanded="false"]',
        '[data-testid="dashboard-row-title"] button[aria-expanded="false"]',
        'section[data-testid="dashboard-row"] button[aria-expanded="false"]',
    )
    for sel in selectors:
        loc = page.locator(sel)
        try:
            n = loc.count()
        except Exception:
            continue
        if n == 0:
            continue
        clicked = 0
        for i in range(min(int(n), 14)):
            try:
                loc.nth(i).click(timeout=900)
                clicked += 1
                page.wait_for_timeout(28)
            except Exception:
                pass
        if clicked:
            logger.info(
                "Grafana screenshot: expanded %s collapsed dashboard row(s) via %r",
                clicked,
                sel,
            )
            page.wait_for_timeout(110)
        return


def _grafana_close_open_menus(page: Any) -> None:
    """Escape stray overlays (e.g. auto-refresh interval picker opened by a mis-click)."""
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(100)
    except Exception:
        pass


def _grafana_click_dashboard_refresh(
    page: Any, timeout_ms: int, spinner_budget_ms: Optional[int] = None
) -> None:
    """
    Run **Refresh dashboard** (re-query). Order matters: Grafana often exposes a **refresh interval**
    control whose name also contains \"Refresh\" — clicking it only opens the **5s/10s/off** menu and
    **does not** load panels (blank main area + open dropdown in screenshots).
    """
    if not _lark_env_truthy("GRAFANA_SCREENSHOT_REFRESH"):
        return
    _grafana_close_open_menus(page)
    tclick = min(3500, max(1200, int(timeout_ms) // 35))
    spin_cap = (
        int(spinner_budget_ms)
        if spinner_budget_ms is not None
        else int(GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS)
    )
    spin_cap = max(0, min(25_000, spin_cap))
    # Exact \"Refresh dashboard\" first; avoid broad ``aria-label*=\"Refresh\"`` (interval picker).
    locators: List[Any] = [
        page.locator('button[aria-label="Refresh dashboard"]').first,
        page.locator('[aria-label="Refresh dashboard"]').first,
        page.get_by_role("button", name=re.compile(r"refresh\s+dashboard", re.I)).first,
        page.locator('[data-testid="refresh-dashboard-button"]').first,
        page.locator('[data-testid*="RefreshPicker"][data-testid*="run"]').first,
        page.locator('[data-testid*="refresh"][data-testid*="Run"]').first,
        page.get_by_role("button", name=re.compile(r"^run query$", re.I)).first,
    ]
    for idx, loc in enumerate(locators):
        try:
            if loc.count() == 0:
                continue
        except Exception:
            continue
        try:
            loc.click(timeout=tclick)
            logger.info("Grafana screenshot: clicked Refresh/run control (locator #%s)", idx)
            page.wait_for_timeout(120)
            _grafana_close_open_menus(page)
            _grafana_wait_loading_like_gone(page, spin_cap)
            return
        except Exception:
            _grafana_close_open_menus(page)
            continue
    if _grafana_click_refresh_dashboard_js(page):
        logger.info("Grafana screenshot: refresh via in-page JS (toolbar/testid)")
        page.wait_for_timeout(120)
        _grafana_close_open_menus(page)
        _grafana_wait_loading_like_gone(page, spin_cap)
        return
    try:
        logger.info("Grafana screenshot: no explicit Refresh control — using full page reload instead")
        page.reload(wait_until="load", timeout=timeout_ms)
        page.wait_for_timeout(260)
        _grafana_wait_loading_like_gone(page, spin_cap)
    except Exception as e:
        logger.info("Grafana screenshot: refresh fallback reload failed: %s", e)


def _grafana_loading_like_count(page: Any) -> int:
    """Rough count of visible Grafana-style loading elements (deduped by element)."""
    try:
        return int(
            page.evaluate(
                """() => {
                  const q = [
                    '[data-testid="Spinner"]',
                    '[data-testid="data-testid Panel loading bar"]',
                    '.panel-loading',
                    '[class*="PanelLoader"]',
                    '[class*="panel-loading"]',
                    '.fa-spin',
                    '.gf-spin',
                  ];
                  const seen = new Set();
                  for (const s of q) {
                    document.querySelectorAll(s).forEach((el) => {
                      const r = el.getBoundingClientRect();
                      const st = window.getComputedStyle(el);
                      if (r.width < 2 || r.height < 2) return;
                      if (st && st.visibility === "hidden") return;
                      if (st && st.display === "none") return;
                      seen.add(el);
                    });
                  }
                  return seen.size;
                }"""
            )
            or 0
        )
    except Exception:
        return 0


def _grafana_refresh_toolbar_busy(page: Any) -> bool:
    """
    Detect top-right query state: while loading, Grafana often shows a visible "Cancel" button;
    when idle it returns to "Refresh".
    """
    try:
        return bool(
            page.evaluate(
                """() => {
                  const visible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    if (r.width < 2 || r.height < 2) return false;
                    const st = window.getComputedStyle(el);
                    if (!st || st.display === "none" || st.visibility === "hidden") return false;
                    return true;
                  };
                  let hasCancel = false;
                  let hasRefresh = false;
                  for (const b of Array.from(document.querySelectorAll('button'))) {
                    if (!visible(b)) continue;
                    const t = (b.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    if (!t) continue;
                    if (t.includes('cancel') || t.includes('取消')) hasCancel = true;
                    if (t.includes('refresh') || t.includes('刷新')) hasRefresh = true;
                  }
                  return hasCancel && !hasRefresh;
                }"""
            )
        )
    except Exception:
        return False


def _grafana_wait_loading_like_gone(page: Any, budget_ms: int) -> None:
    """Poll until loading-like elements stay at 0 for a few ticks (queries + canvas paint)."""
    if budget_ms <= 0:
        return
    deadline = time.monotonic() + budget_ms / 1000.0
    stable = 0
    last_c = -1
    last_busy: Optional[bool] = None
    while time.monotonic() < deadline:
        c = _grafana_loading_like_count(page)
        busy = _grafana_refresh_toolbar_busy(page)
        if c != last_c or busy != last_busy:
            logger.debug("Grafana screenshot: loading-like count=%s toolbar_busy=%s", c, busy)
            last_c = c
            last_busy = busy
        if c == 0 and not busy:
            stable += 1
            if stable >= 2:
                return
        else:
            stable = 0
        page.wait_for_timeout(100)
    c = _grafana_loading_like_count(page)
    busy = _grafana_refresh_toolbar_busy(page)
    if c > 0 or busy:
        logger.warning(
            "Grafana screenshot: after %sms still busy (loading_count≈%s toolbar_busy=%s) — capture may be partial",
            budget_ms,
            c,
            busy,
        )


def _grafana_wait_min_react_grid_items(page: Any, min_items: int, budget_ms: int) -> None:
    """Classic dashboards use ``.react-grid-item``; scenes may skip (set MIN_GRID_ITEMS=0)."""
    if min_items <= 0 or budget_ms <= 0:
        return
    deadline = time.monotonic() + budget_ms / 1000.0
    while time.monotonic() < deadline:
        try:
            n = page.locator(".react-grid-item").count()
            if n >= min_items:
                logger.info("Grafana screenshot: react-grid-item count=%s (>= %s)", n, min_items)
                return
        except Exception:
            pass
        page.wait_for_timeout(280)
    try:
        n = page.locator(".react-grid-item").count()
    except Exception:
        n = -1
    logger.warning(
        "Grafana screenshot: react-grid-item count=%s did not reach %s within %sms",
        n,
        min_items,
        budget_ms,
    )


def _grafana_panel_ready_stats(page: Any) -> Tuple[int, int]:
    """
    Return (total_panels, ready_panels).
    A panel is "ready" when it shows chart canvas/uplot, or explicit "No data"/error text.
    Falls back to ``dashboard-panel-content`` / sized ``.react-grid-item`` when classic Panel headers
    are absent (Grafana Scenes and newer shells).
    """
    try:
        r = page.evaluate(
            """() => {
              let roots = Array.from(
                document.querySelectorAll(
                  'section[data-testid^="data-testid Panel header"], section[data-testid*="Panel header"]'
                )
              );
              if (!roots.length) {
                roots = Array.from(
                  document.querySelectorAll('[data-testid="dashboard-panel-content"]')
                );
              }
              if (!roots.length) {
                roots = Array.from(document.querySelectorAll('.react-grid-item')).filter((el) => {
                  const r = el.getBoundingClientRect();
                  return r.width > 80 && r.height > 48;
                });
              }
              roots = roots.slice(0, 48);
              let ready = 0;
              for (const p of roots) {
                const root = p.querySelector('[data-testid="data-testid panel content"]') || p;
                const hasChart = !!(
                  root.querySelector('[data-testid="uplot-main-div"]') ||
                  root.querySelector('canvas') ||
                  root.querySelector('[class*="timeseries"]')
                );
                const txt = (root.textContent || '').toLowerCase();
                const hasExplicitNoData =
                  txt.includes('no data') || txt.includes('n/a') || txt.includes('error');
                if (hasChart || hasExplicitNoData) ready += 1;
              }
              return { total: roots.length, ready };
            }"""
        )
        if isinstance(r, dict):
            return int(r.get("total") or 0), int(r.get("ready") or 0)
    except Exception:
        pass
    return 0, 0


def _grafana_click_refresh_dashboard_js(page: Any) -> bool:
    """
    Last-resort refresh before ``page.reload``: clicks a real dashboard refresh control in-page.
    Avoids Playwright locator misses on Grafana toolbar variants.
    """
    try:
        tag = page.evaluate(
            """() => {
              const clickVisible = (el) => {
                try {
                  el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                  if (typeof el.click === 'function') el.click();
                  return true;
                } catch (e) {
                  return false;
                }
              };
              const roots = [
                document.querySelector('[data-testid="topnav-toolbar-content"]'),
                document.querySelector('[class*="ToolbarButtonRow"]'),
                document.querySelector('header'),
                document.body,
              ].filter(Boolean);
              for (const root of roots) {
                for (const b of root.querySelectorAll('button')) {
                  const al = (b.getAttribute('aria-label') || '').trim().toLowerCase();
                  if (al === 'refresh dashboard' || al.endsWith('refresh dashboard')) {
                    if (clickVisible(b)) return 'aria';
                  }
                }
              }
              const byTest = document.querySelector('[data-testid="refresh-dashboard-button"]');
              if (byTest && clickVisible(byTest)) return 'testid';
              const insidePicker = document.querySelector(
                '[data-testid*="RefreshPicker"] [data-testid*="run"]'
              );
              if (insidePicker && clickVisible(insidePicker)) return 'picker_desc';
              for (const b of document.querySelectorAll('button[data-testid]')) {
                const tid = (b.getAttribute('data-testid') || '').toLowerCase();
                if (tid.includes('refresh') && tid.includes('run') && clickVisible(b)) return 'picker_btn';
              }
              return '';
            }"""
        )
        return bool(tag)
    except Exception:
        return False


def _grafana_wait_panels_fully_loaded(page: Any, budget_ms: int) -> None:
    """
    Wait until most dashboard panels are ready before screenshot.
    Uses panel ready ratio + loading-like elements stable at 0.
    When **no** panel roots match (Scenes DOM), do not burn the full ``budget_ms``: use chart
    heuristic + idle loading for a short capped window (``GRAFANA_SCREENSHOT_PANEL_READY_ZERO_TOTAL_MAX_MS``).
    """
    b = max(1000, int(budget_ms))
    deadline = time.monotonic() + b / 1000.0
    stable = 0
    zero_started: Optional[float] = None

    while time.monotonic() < deadline:
        total, ready = _grafana_panel_ready_stats(page)
        loading = _grafana_loading_like_count(page)

        if total == 0:
            if zero_started is None:
                zero_started = time.monotonic()
                stable = 0
            z_cap = zero_started + float(GRAFANA_SCREENSHOT_PANEL_READY_ZERO_TOTAL_MAX_MS) / 1000.0

            vis = _grafana_dashboard_has_visual_content(page)
            if vis and loading == 0:
                stable += 1
                if stable >= 2:
                    logger.info(
                        "Grafana screenshot: panel roots=0 (layout); chart heuristic + idle loading OK"
                    )
                    return
            else:
                stable = 0

            if time.monotonic() >= z_cap:
                logger.info(
                    "Grafana screenshot: panel roots=0 — stop ratio wait after %sms "
                    "(GRAFANA_SCREENSHOT_PANEL_READY_ZERO_TOTAL_MAX_MS)",
                    int(GRAFANA_SCREENSHOT_PANEL_READY_ZERO_TOTAL_MAX_MS),
                )
                return

            page.wait_for_timeout(110)
            continue

        if zero_started is not None:
            stable = 0
        zero_started = None

        need = max(
            int(math.ceil(total * GRAFANA_SCREENSHOT_PANEL_READY_RATIO)),
            int(GRAFANA_SCREENSHOT_PANEL_READY_MIN),
        )
        ok_panels = ready >= min(total, need)
        if ok_panels and loading == 0:
            stable += 1
            if stable >= 2:
                logger.info(
                    "Grafana screenshot: panel readiness reached ready=%s/%s (need=%s), loading=%s",
                    ready,
                    total,
                    need,
                    loading,
                )
                return
        else:
            stable = 0
        page.wait_for_timeout(150)

    total, ready = _grafana_panel_ready_stats(page)
    logger.warning(
        "Grafana screenshot: panel readiness timeout after %sms (ready=%s/%s ratio_target=%.2f min=%s)",
        b,
        ready,
        total,
        GRAFANA_SCREENSHOT_PANEL_READY_RATIO,
        GRAFANA_SCREENSHOT_PANEL_READY_MIN,
    )


def _grafana_scroll_paint_lazy_panels(page: Any) -> None:
    """Scroll by ~viewport steps so off-screen panels mount and uPlot/canvas paint."""
    pause = int(GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS)
    try:
        vh = int(page.evaluate("() => window.innerHeight || 900") or 900)
        vh = max(400, min(vh, 2400))
        step = max(220, int(vh * 0.72))
        h = page.evaluate(
            "() => Math.max(document.body.scrollHeight, (document.scrollingElement || document.documentElement).scrollHeight)"
        )
        h = int(h or 0)
        h = min(max(h, 800), 48000)
        y = 0
        while y <= h:
            page.evaluate("(yy) => window.scrollTo(0, yy)", y)
            page.wait_for_timeout(pause)
            y += step
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(pause)
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(max(90, int(pause * 0.45)))
    except Exception as e:
        logger.info("Grafana screenshot: scroll paint step skipped: %s", e)


def _grafana_stabilize_dashboard_render(
    page: Any, timeout_ms: int, rounds: Optional[int] = None
) -> None:
    """
    Multiple scroll passes + spinner polling so lower panels finish Prometheus queries before PNG.
    ``rounds`` overrides ``GRAFANA_SCREENSHOT_STABILIZE_ROUNDS`` (e.g. ``1`` on reload retry).
    """
    r = GRAFANA_SCREENSHOT_STABILIZE_ROUNDS if rounds is None else max(1, min(8, int(rounds)))
    sm = int(GRAFANA_SCREENSHOT_SPINNER_MAX_MS)
    per_round = max(450, min(2400, int(sm * 0.28)))
    final_spin = max(550, min(3200, int(sm * 0.42)))

    if GRAFANA_SCREENSHOT_MIN_GRID_ITEMS > 0:
        _grafana_wait_min_react_grid_items(
            page,
            GRAFANA_SCREENSHOT_MIN_GRID_ITEMS,
            min(12000, max(4000, timeout_ms // 3)),
        )

    for rnd in range(r):
        logger.info("Grafana screenshot: stabilize round %s/%s", rnd + 1, r)
        _grafana_scroll_paint_lazy_panels(page)
        _grafana_wait_loading_like_gone(page, per_round)

    _grafana_scroll_paint_lazy_panels(page)
    _grafana_wait_loading_like_gone(page, final_spin)


def _grafana_dashboard_has_visual_content(page: Any) -> bool:
    """True when #reactRoot looks like a loaded dashboard (see ``_GRAFANA_JS_REACTROOT_HAS_CHARTS``)."""
    try:
        return bool(page.evaluate(_GRAFANA_JS_REACTROOT_HAS_CHARTS))
    except Exception:
        return False


def _grafana_wait_dashboard_body_populated(page: Any, budget_ms: int) -> bool:
    """Short wait_for_function — budget capped by ``GRAFANA_SCREENSHOT_POPULATE_MAX_MS`` style callers."""
    b = max(1000, min(int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS), int(budget_ms)))
    try:
        page.wait_for_function(_GRAFANA_JS_REACTROOT_HAS_CHARTS, timeout=b)
        logger.info("Grafana screenshot: reactRoot looks populated")
        return True
    except Exception as e:
        logger.warning(
            "Grafana screenshot: populate wait stopped after %sms: %s",
            b,
            e,
        )
        return False


def _grafana_build_screenshot_dashboard_url(
    start_unix: int,
    end_unix: int,
    *,
    relative_from: Optional[str] = None,
    relative_to: Optional[str] = None,
    timezone_param: Optional[str] = None,
) -> str:
    params: List[Tuple[str, str]] = [("orgId", "1")]
    rf_ov = (relative_from or "").strip()
    rt_ov = (relative_to or "").strip()
    force_relative = bool(rf_ov or rt_ov)
    if GRAFANA_SCREENSHOT_RELATIVE_RANGE or force_relative:
        rf = rf_ov or (GRAFANA_DASHBOARD_FROM or "now-1h").strip()
        rt = rt_ov or (GRAFANA_DASHBOARD_TO or "now").strip()
        params.extend([("from", rf), ("to", rt)])
    else:
        params.extend(
            [
                ("from", str(int(start_unix) * 1000)),
                ("to", str(int(end_unix) * 1000)),
            ]
        )
    tz = (timezone_param or "").strip()
    if not tz:
        tz = (GRAFANA_SCREENSHOT_TIMEZONE or "").strip()
    if tz.lower() in ("none", "-", "off", "0", "false", "no"):
        tz = ""
    if tz:
        params.append(("timezone", tz))
    url_refresh = _cfg_str("GRAFANA_SCREENSHOT_URL_REFRESH", "1m").strip()
    if url_refresh.lower() not in ("none", "-", "off", "0", "false", "no", ""):
        params.append(("refresh", url_refresh))
    k = (GRAFANA_SCREENSHOT_KIOSK or "").strip().lower()
    if k and k not in ("0", "false", "no", "off"):
        if k in ("1", "true", "yes", "on"):
            params.append(("kiosk", "1"))
        else:
            params.append(("kiosk", k))
    q = urlencode(params)
    return f"{GRAFANA_BASE_URL}{GRAFANA_DASHBOARD_PATH}?{q}"


def _grafana_wait_dashboard_ready(page: Any, timeout_ms: int) -> None:
    """
    SPA 在 ``domcontentloaded`` 时往往还没有 panel；此处在 ``load`` 之后仍要等网格/画布出现。
    与 ``GRAFANA_SCREENSHOT_DOCK_NAV`` 无关：关 dock 时也必须执行，否则截到空白主区。
    """
    t = min(14000, max(4000, int(timeout_ms) // 3))
    try:
        page.locator("#reactRoot").wait_for(state="visible", timeout=min(9000, t))
    except Exception:
        pass

    selectors = (
        '[data-testid="uplot-main-div"]',
        ".react-grid-item",
        '[data-testid="dashboard-panel-content"]',
        '[data-testid="panel-content"]',
        "main canvas",
        '[class*="PanelChrome"]',
    )
    matched: Optional[str] = None
    slot = min(5000, max(1600, t // 2))
    for sel in selectors:
        try:
            page.locator(sel).first.wait_for(state="visible", timeout=slot)
            matched = sel
            break
        except Exception:
            continue

    if not matched:
        try:
            safe_title = (GRAFANA_PANEL_TITLE or "").replace('"', '\\"')
            if safe_title:
                page.locator(f'h2[title="{safe_title}"]').first.wait_for(
                    state="visible", timeout=slot
                )
                matched = f'h2[title="{safe_title}"]'
        except Exception:
            logger.warning(
                "Grafana screenshot: no known panel/grid selector matched — continuing "
                "(selectors tried: %s; panel title: %r)",
                selectors,
                GRAFANA_PANEL_TITLE,
            )
    else:
        logger.info("Grafana screenshot: dashboard content wait matched %r", matched)

    page.wait_for_timeout(160)


def _playwright_cookie_list(session: requests.Session) -> List[Dict[str, Any]]:
    """
    Use per-cookie ``url`` (Grafana origin) so ``add_cookies`` matches Playwright rules;
    ``domain``+``path`` alone often fails on Linux headless before first navigation.
    """
    base = str(GRAFANA_BASE_URL).rstrip("/")
    out: List[Dict[str, Any]] = []
    for c in session.cookies:
        out.append({"name": c.name, "value": c.value, "url": base})
    return out


def _grafana_persistent_browser_enabled() -> bool:
    return _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE") and _lark_env_truthy("GRAFANA_PERSISTENT_BROWSER")


_GRAFANA_LEGEND_ISOLATE_FIND_JS = r"""([panelTitle, seriesNeedle]) => {
  const norm = (s) => (s || '').replace(/[–—]/g, '-').replace(/\s+/g, ' ').trim().toLowerCase();
  const pt = norm(panelTitle);
  const sn = norm(seriesNeedle);
  if (!pt || !sn) return { status: 'missing' };
  const panelRoots = document.querySelectorAll('.react-grid-item, [data-testid="dashboard-panel"]');
  let panelRoot = null;
  for (const root of panelRoots) {
    const titleEl = root.querySelector('[data-testid="panel-title"], h2, [class*="panel-title"]');
    const titleTxt = norm(titleEl && (titleEl.textContent || titleEl.getAttribute('title')));
    if (titleTxt && (titleTxt === pt || titleTxt.includes(pt) || pt.includes(titleTxt))) {
      panelRoot = root;
      break;
    }
  }
  if (!panelRoot) return { status: 'panel_not_found' };
  try { panelRoot.scrollIntoView({ block: 'center', behavior: 'auto' }); } catch (e) {}
  // Grafana timeseries legend rows render as <button class="...LegendLabel...">.
  let btns = Array.from(panelRoot.querySelectorAll('button[class*="LegendLabel"]'));
  if (!btns.length) {
    btns = Array.from(panelRoot.querySelectorAll(
      '[data-testid="viz-legend-list-item"], [class*="LegendItem"], ' +
      '.uplot-legend .u-series, button[class*="legend"], div[class*="legendItem"]'
    ));
  }
  // A single-series panel already shows "only that series" — clicking would just hide it.
  if (btns.length < 2) return { status: 'already_solo' };
  let target = btns.find(b => norm(b.textContent) === sn);
  if (!target) target = btns.find(b => { const t = norm(b.textContent); return t && (t.includes(sn) || sn.includes(t)); });
  if (!target) return { status: 'legend_not_found' };
  try { target.scrollIntoView({ block: 'nearest', inline: 'nearest', behavior: 'auto' }); } catch (e) {}
  const r = target.getBoundingClientRect();
  if (r.width < 2 || r.height < 2) return { status: 'not_visible' };
  return { status: 'found', x: r.left + r.width / 2, y: r.top + r.height / 2 };
}"""

_GRAFANA_LEGEND_ISOLATE_VERIFY_JS = r"""([panelTitle, seriesNeedle]) => {
  const norm = (s) => (s || '').replace(/[–—]/g, '-').replace(/\s+/g, ' ').trim().toLowerCase();
  const pt = norm(panelTitle);
  const sn = norm(seriesNeedle);
  const panelRoots = document.querySelectorAll('.react-grid-item, [data-testid="dashboard-panel"]');
  let panelRoot = null;
  for (const root of panelRoots) {
    const titleEl = root.querySelector('[data-testid="panel-title"], h2, [class*="panel-title"]');
    const titleTxt = norm(titleEl && (titleEl.textContent || titleEl.getAttribute('title')));
    if (titleTxt && (titleTxt === pt || titleTxt.includes(pt) || pt.includes(titleTxt))) { panelRoot = root; break; }
  }
  if (!panelRoot) return false;
  const btns = Array.from(panelRoot.querySelectorAll('button[class*="LegendLabel"]'));
  if (btns.length < 2) return true;
  const isDisabled = (b) => /LegendLabelDisabled/.test(b.className || '');
  let target = btns.find(b => norm(b.textContent) === sn);
  if (!target) target = btns.find(b => { const t = norm(b.textContent); return t && (t.includes(sn) || sn.includes(t)); });
  if (!target) return false;
  if (isDisabled(target)) return false;               // target itself hidden → not isolated
  return btns.some(b => b !== target && isDisabled(b)); // others hidden → only target shown
}"""


def _grafana_playwright_click_alert_series_legends(
    page: Any,
    targets: List[Dict[str, str]],
) -> int:
    """
    On each alerting panel, isolate the alert series so the screenshot shows **only that series**.

    Uses Grafana's native "show only this series" gesture — a **single real mouse click** on the
    series' legend label (ported from the ``monitoring`` project), not a synthetic Ctrl+click
    (which only *toggles* one line on/off). Isolation is verified via the ``LegendLabelDisabled``
    class the other labels gain, and retried a few times because a stray click can toggle a prior
    isolate back off. Single-series panels are left untouched (already "only that series"). This
    only affects the throwaway screenshot render session — the dashboard/graph is not modified.
    Returns the count of panels isolated (or already solo).
    """
    if not targets or not _lark_env_truthy("GRAFANA_SCREENSHOT_ALERT_LEGEND_CLICK_ENABLE"):
        return 0

    # One isolate per panel: a second plain click in the same panel would flip the isolate to a
    # different series ("last wins"). Keep the first alerting series encountered for each panel.
    seen_panels: Set[str] = set()
    ordered: List[Tuple[str, str]] = []
    for tgt in targets:
        panel_title = str(tgt.get("panel") or "").strip()
        series_label = str(tgt.get("series") or "").strip()
        if not panel_title or not series_label:
            continue
        pkey = panel_title.casefold()
        if pkey in seen_panels:
            continue
        seen_panels.add(pkey)
        ordered.append((panel_title, series_label))

    ok = 0
    for panel_title, series_label in ordered:
        try:
            isolated = False
            for attempt in range(3):
                info = page.evaluate(_GRAFANA_LEGEND_ISOLATE_FIND_JS, [panel_title, series_label])
                status = info.get("status") if isinstance(info, dict) else None
                if status == "already_solo":
                    isolated = True
                    logger.info(
                        "Grafana screenshot: panel already single-series panel=%r", panel_title
                    )
                    break
                if status != "found":
                    logger.info(
                        "Grafana screenshot: legend isolate %s panel=%r series=%r attempt=%s",
                        status,
                        panel_title,
                        series_label,
                        attempt,
                    )
                    break
                page.mouse.click(float(info["x"]), float(info["y"]))
                try:
                    page.evaluate(
                        "() => { const s = window.getSelection && window.getSelection();"
                        " if (s && s.removeAllRanges) s.removeAllRanges(); }"
                    )
                except Exception:
                    pass
                page.wait_for_timeout(400)
                try:
                    _grafana_wait_loading_like_gone(
                        page, min(3000, int(GRAFANA_SCREENSHOT_SPINNER_MAX_MS))
                    )
                except Exception:
                    pass
                if page.evaluate(_GRAFANA_LEGEND_ISOLATE_VERIFY_JS, [panel_title, series_label]):
                    isolated = True
                    logger.info(
                        "Grafana screenshot: isolated legend panel=%r series=%r attempt=%s",
                        panel_title,
                        series_label,
                        attempt,
                    )
                    break
                # Not verified — a click may have toggled a prior isolate off; loop re-clicks.
            if isolated:
                ok += 1
            else:
                logger.warning(
                    "Grafana screenshot: legend isolate not verified panel=%r series=%r — "
                    "PNG may show all series",
                    panel_title,
                    series_label,
                )
        except Exception as ex:
            logger.info(
                "Grafana screenshot: legend isolate failed panel=%r series=%r: %s",
                panel_title,
                series_label,
                ex,
            )
    return ok


def _grafana_playwright_pre_screenshot_paint_flush(page: Any) -> None:
    """
    Headless Grafana 有时「面板 ready 统计够了」但 uPlot/canvas 尚未合成进位图；快门前强制置顶、
    等字体与双 rAF，减少全页截图只有侧栏/顶栏、主区纯底色的情况。
    """
    try:
        page.evaluate(
            """() => {
              window.scrollTo(0, 0);
              const s = document.scrollingElement || document.documentElement;
              if (s) s.scrollTop = 0;
            }"""
        )
    except Exception:
        pass
    try:
        page.evaluate(
            """async () => {
              try {
                if (document.fonts && document.fonts.ready) await document.fonts.ready;
              } catch (e) {}
            }"""
        )
    except Exception:
        pass
    extra = max(0, min(5000, _cfg_int("GRAFANA_SCREENSHOT_PRE_CAPTURE_MS", 800)))
    page.wait_for_timeout(extra)
    try:
        page.evaluate(
            "() => new Promise((resolve) => {"
            "  requestAnimationFrame(() => { requestAnimationFrame(() => resolve(undefined)); });"
            "})"
        )
    except Exception:
        pass
    if _lark_env_truthy("GRAFANA_SCREENSHOT_PRE_CAPTURE_RESCROLL"):
        try:
            _grafana_scroll_paint_lazy_panels(page)
            page.evaluate(
                "() => { window.scrollTo(0, 0); const s = document.scrollingElement || document.documentElement; if (s) s.scrollTop = 0; }"
            )
            page.wait_for_timeout(220)
        except Exception:
            pass


def _grafana_playwright_render_dashboard_and_png(
    page: Any,
    url: str,
    timeout_ms: int,
    *,
    skip_nav_and_refresh: bool = False,
    legend_targets: Optional[List[Dict[str, str]]] = None,
) -> bytes:
    """
    Navigate ``page`` to dashboard ``url`` and return a PNG after the same wait/stabilize path
    as ephemeral screenshots (shared with :class:`GrafanaPlaywrightKeeper`).
    Caller must have injected Grafana cookies (and optional boot-warm root ``/``) beforehand.

    ``skip_nav_and_refresh=True`` (persistent keeper jobs): ``goto`` already loads the target range —
    skip Dock + Refresh + second Dock to avoid redundant work and reload fallbacks.
    Blank-chart recovery paths below still run Refresh/Dock when needed.
    """
    page.goto(url, wait_until="load", timeout=timeout_ms)
    page.wait_for_timeout(120)
    if not skip_nav_and_refresh:
        _grafana_playwright_dock_nav_only(page, timeout_ms)
        _grafana_click_dashboard_refresh(page, timeout_ms)
        # Refresh 有时会重新弹出 mega-menu；再收一次侧栏
        _grafana_playwright_dock_nav_only(page, timeout_ms)
    _grafana_expand_collapsed_dashboard_rows(page)
    _grafana_wait_dashboard_ready(page, timeout_ms)
    _grafana_wait_dashboard_body_populated(page, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
    _grafana_stabilize_dashboard_render(page, timeout_ms)
    _grafana_wait_panels_fully_loaded(page, int(GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS))
    page.wait_for_timeout(90)

    if not _grafana_dashboard_has_visual_content(page):
        _grafana_expand_collapsed_dashboard_rows(page)
        _grafana_click_dashboard_refresh(
            page,
            timeout_ms,
            spinner_budget_ms=min(1400, int(GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS)),
        )
        _grafana_playwright_dock_nav_only(page, timeout_ms)
        _grafana_wait_dashboard_body_populated(
            page, min(3200, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
        )
        _grafana_scroll_paint_lazy_panels(page)
        _grafana_wait_panels_fully_loaded(page, min(8000, int(GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS)))
        page.wait_for_timeout(120)

    if not _grafana_dashboard_has_visual_content(page):
        logger.warning(
            "Grafana screenshot: chart DOM not detected — reload once (kiosk=%r)",
            GRAFANA_SCREENSHOT_KIOSK or "(off)",
        )
        try:
            page.reload(wait_until="load", timeout=timeout_ms)
            page.wait_for_timeout(450)
            _grafana_playwright_dock_nav_only(page, timeout_ms)
            _grafana_click_dashboard_refresh(page, timeout_ms)
            _grafana_playwright_dock_nav_only(page, timeout_ms)
            _grafana_expand_collapsed_dashboard_rows(page)
            _grafana_wait_dashboard_ready(page, min(20000, timeout_ms // 2))
            _grafana_wait_dashboard_body_populated(
                page, min(8000, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
            )
            _grafana_stabilize_dashboard_render(page, timeout_ms, rounds=1)
            _grafana_wait_panels_fully_loaded(page, min(9000, int(GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS)))
        except Exception as e:
            logger.warning("Grafana screenshot: reload retry failed: %s", e)

    if not _grafana_dashboard_has_visual_content(page):
        logger.error(
            "Grafana screenshot: still no chart-like DOM — PNG may be blank "
            "(session cookie / GRAFANA_DASHBOARD_PATH / try GRAFANA_SCREENSHOT_RELATIVE_RANGE=0 "
            "or GRAFANA_SCREENSHOT_KIOSK=tv)."
        )

    if GRAFANA_SCREENSHOT_SETTLE_MS > 0:
        page.wait_for_timeout(int(GRAFANA_SCREENSHOT_SETTLE_MS))
    _grafana_close_open_menus(page)
    if not skip_nav_and_refresh:
        _grafana_playwright_dock_nav_only(page, timeout_ms)
    if legend_targets:
        _grafana_playwright_click_alert_series_legends(page, legend_targets)
        page.wait_for_timeout(180)
    _grafana_playwright_pre_screenshot_paint_flush(page)
    full_page = _lark_env_truthy("GRAFANA_SCREENSHOT_FULL_PAGE")
    try:
        return page.screenshot(type="png", full_page=full_page, animations="disabled")
    except TypeError:
        return page.screenshot(type="png", full_page=full_page)


class GrafanaPlaywrightKeeper:
    """
    One daemon thread owns Playwright + Chromium + a single dashboard ``page``.
    Screenshots are serialized via a queue so Flask / watchdog threads never touch Playwright directly.
    """

    def __init__(self) -> None:
        self._q: queue.Queue = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._fatal: Optional[BaseException] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        t = threading.Thread(target=self._run, daemon=True, name="grafana-playwright-keeper")
        self._thread = t
        t.start()

    def wait_ready(self, timeout: float) -> bool:
        return self._ready.wait(timeout=timeout)

    def request_png(
        self,
        url: str,
        timeout_ms: int,
        *,
        legend_targets: Optional[List[Dict[str, str]]] = None,
    ) -> bytes:
        warm_wait = max(120.0, float(timeout_ms) / 1000.0 + 45.0)
        if not self._ready.wait(timeout=warm_wait):
            raise TimeoutError("GrafanaPlaywrightKeeper not ready (warm-up still running or failed)")
        if self._fatal is not None:
            raise RuntimeError("GrafanaPlaywrightKeeper failed during warm-up") from self._fatal
        job_timeout = max(30.0, _cfg_float("GRAFANA_PERSISTENT_BROWSER_JOB_TIMEOUT_SECONDS", 180.0))
        ev = threading.Event()
        box: Dict[str, Any] = {}
        self._q.put(
            {
                "op": "png",
                "url": url,
                "timeout_ms": int(timeout_ms),
                "legend_targets": legend_targets,
                "ev": ev,
                "box": box,
            }
        )
        if not ev.wait(timeout=job_timeout):
            raise TimeoutError("GrafanaPlaywrightKeeper screenshot job timed out")
        err = box.get("err")
        if err is not None:
            if isinstance(err, BaseException):
                raise err
            raise RuntimeError(str(err))
        png = box.get("png")
        if not isinstance(png, (bytes, bytearray)):
            raise RuntimeError("keeper returned no PNG bytes")
        return bytes(png)

    def _run(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as e:
            self._fatal = e
            self._ready.set()
            logger.exception("GrafanaPlaywrightKeeper: Playwright not installed")
            return

        p = None
        browser = None
        try:
            p = sync_playwright().start()
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = browser.new_context(
                viewport={
                    "width": max(400, int(GRAFANA_SCREENSHOT_WIDTH)),
                    "height": max(300, int(GRAFANA_SCREENSHOT_HEIGHT)),
                },
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )
            page = context.new_page()
            try:
                page.add_init_script(
                    "try{Object.defineProperty(navigator,'webdriver',{get:()=>undefined});}catch(e){}"
                )
            except Exception:
                pass

            timeout_ms = max(5000, int(GRAFANA_SCREENSHOT_TIMEOUT_MS))
            base = str(GRAFANA_BASE_URL).rstrip("/")
            sess0 = grafana_login_session()
            context.add_cookies(_playwright_cookie_list(sess0))
            if _lark_env_truthy("GRAFANA_SCREENSHOT_BOOT_WARM"):
                page.goto(f"{base}/", wait_until="domcontentloaded", timeout=min(20000, timeout_ms))
                page.wait_for_timeout(140)

            warm_url = _grafana_build_screenshot_dashboard_url(0, 0)
            logger.info("GrafanaPlaywrightKeeper: warm-up load url=%s…", warm_url[:220])
            _ = _grafana_playwright_render_dashboard_and_png(page, warm_url, timeout_ms)
            logger.info(
                "GrafanaPlaywrightKeeper: warm-up finished — persistent Chromium stays open; "
                "screenshot jobs reuse this browser (see log line 'using persistent Playwright keeper')."
            )
            self._ready.set()

            idle_sec = max(30.0, _cfg_float("GRAFANA_PERSISTENT_BROWSER_IDLE_REFRESH_SECONDS", 120.0))
            while True:
                try:
                    job = self._q.get(timeout=idle_sec)
                except queue.Empty:
                    try:
                        _grafana_click_dashboard_refresh(page, timeout_ms)
                    except Exception as ex:
                        logger.debug("GrafanaPlaywrightKeeper idle refresh: %s", ex)
                    continue
                if job.get("op") == "stop":
                    break
                if job.get("op") != "png":
                    continue
                ev: threading.Event = job["ev"]
                box: Dict[str, Any] = job["box"]
                jurl = str(job.get("url") or "")
                jto = int(job.get("timeout_ms") or timeout_ms)
                try:
                    sess = grafana_login_session()
                    ck = _playwright_cookie_list(sess)
                    if _lark_env_truthy("GRAFANA_PERSISTENT_BROWSER_SOFT_COOKIE"):
                        try:
                            context.add_cookies(ck)
                        except Exception:
                            context.clear_cookies()
                            context.add_cookies(ck)
                    else:
                        context.clear_cookies()
                        context.add_cookies(ck)
                    box["png"] = _grafana_playwright_render_dashboard_and_png(
                        page,
                        jurl,
                        max(5000, jto),
                        skip_nav_and_refresh=True,
                        legend_targets=job.get("legend_targets"),
                    )
                except Exception as ex:
                    box["err"] = ex
                finally:
                    ev.set()
        except Exception as ex:
            self._fatal = ex
            logger.exception("GrafanaPlaywrightKeeper: crashed")
            self._ready.set()
        finally:
            try:
                if browser is not None:
                    browser.close()
            except Exception:
                pass
            try:
                if p is not None:
                    p.stop()
            except Exception:
                pass


def _start_grafana_playwright_keeper_if_enabled() -> None:
    """Start background Chromium once (non-blocking); screenshots wait on warm-up inside ``request_png``."""
    global _grafana_pw_keeper, _grafana_pw_keeper_start_attempted
    if not _grafana_persistent_browser_enabled():
        logger.info("Grafana persistent browser: off (GRAFANA_SCREENSHOT_ENABLE or GRAFANA_PERSISTENT_BROWSER=0)")
        return
    with _grafana_pw_keeper_lock:
        if _grafana_pw_keeper_start_attempted:
            return
        _grafana_pw_keeper_start_attempted = True
        try:
            k = GrafanaPlaywrightKeeper()
            k.start()
            _grafana_pw_keeper = k
            logger.info("GrafanaPlaywrightKeeper thread started (warm-up runs in background)")
        except Exception:
            logger.exception("GrafanaPlaywrightKeeper failed to start — ephemeral screenshots only")
            _grafana_pw_keeper = None


def _grafana_headless_screenshot_png(
    session: requests.Session,
    start_unix: int,
    end_unix: int,
    *,
    relative_from: Optional[str] = None,
    relative_to: Optional[str] = None,
    timezone_param: Optional[str] = None,
    legend_targets: Optional[List[Dict[str, str]]] = None,
) -> bytes:
    """
    Headless Chromium (Playwright) opens the same dashboard URL as the UI, with session cookies.
    Requires: ``pip install playwright`` and ``playwright install chromium`` on the server.

    ``GRAFANA_SCREENSHOT_FULL_PAGE=1`` (default): ``page.screenshot(full_page=True)`` — full scroll height
    so KPI rows below the fold are included. ``0`` captures only the viewport (``WIDTH``×``HEIGHT``).

    Defaults favor **low latency** (short sleeps, tight spinner/populate caps). If captures go blank,
    raise ``GRAFANA_SCREENSHOT_POPULATE_MAX_MS`` and ``GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS`` first.

    Optional ``relative_from`` / ``relative_to`` / ``timezone_param`` override the URL query for this
    capture only (watchdog uses ``now-15m`` … ``now`` + ``timezone=browser`` while Prometheus uses a shorter eval window).

    When ``GRAFANA_PERSISTENT_BROWSER=1`` and the background :class:`GrafanaPlaywrightKeeper` is running,
    screenshots reuse one Chromium tab (warm at process start) instead of launching a new browser each time.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "Playwright not installed — pip install playwright && playwright install chromium"
        ) from e

    url = _grafana_build_screenshot_dashboard_url(
        start_unix,
        end_unix,
        relative_from=relative_from,
        relative_to=relative_to,
        timezone_param=timezone_param,
    )
    cookies = _playwright_cookie_list(session)
    timeout_ms = max(5000, int(GRAFANA_SCREENSHOT_TIMEOUT_MS))
    rel_eff = GRAFANA_SCREENSHOT_RELATIVE_RANGE or bool(
        (relative_from or "").strip() or (relative_to or "").strip()
    )
    logger.info(
        "Grafana screenshot: relative_range=%s url=%s",
        rel_eff,
        url[:300] + ("…" if len(url) > 300 else ""),
    )

    k = _grafana_pw_keeper
    if k is not None and _grafana_persistent_browser_enabled():
        try:
            logger.info("Grafana screenshot: using persistent Playwright keeper")
            return k.request_png(url, timeout_ms, legend_targets=legend_targets)
        except Exception as e:
            logger.warning(
                "Grafana persistent keeper screenshot failed (%s); falling back to ephemeral browser",
                e,
            )

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        try:
            context = browser.new_context(
                viewport={
                    "width": max(400, int(GRAFANA_SCREENSHOT_WIDTH)),
                    "height": max(300, int(GRAFANA_SCREENSHOT_HEIGHT)),
                },
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )
            if cookies:
                context.add_cookies(cookies)
            page = context.new_page()
            try:
                page.add_init_script(
                    "try{Object.defineProperty(navigator,'webdriver',{get:()=>undefined});}catch(e){}"
                )
            except Exception:
                pass

            base = str(GRAFANA_BASE_URL).rstrip("/")
            if _lark_env_truthy("GRAFANA_SCREENSHOT_BOOT_WARM"):
                page.goto(f"{base}/", wait_until="domcontentloaded", timeout=min(20000, timeout_ms))
                page.wait_for_timeout(140)

            return _grafana_playwright_render_dashboard_and_png(
                page, url, timeout_ms, legend_targets=legend_targets
            )
        finally:
            browser.close()


def _analysis_hit_alert_series_labels(analysis: Dict[str, Any]) -> List[str]:
    """Series labels that triggered ``hit_alert`` (per-series or aggregate)."""
    labels: List[str] = []
    per = analysis.get("per_series")
    if isinstance(per, list) and per:
        for sub in per:
            if isinstance(sub, dict) and sub.get("hit_alert"):
                lbl = str(sub.get("series_label") or "").strip()
                if lbl and lbl not in labels:
                    labels.append(lbl)
        return labels
    if analysis.get("hit_alert"):
        lbl = str(analysis.get("series_label") or "").strip()
        if lbl:
            labels.append(lbl)
    return labels


def _monitoring_collect_alert_legend_targets(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """``[{panel, series}, …]`` for panels/series that hit alert — used before alert screenshots."""
    _mute_purge_expired()
    out: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    def append(panel: str, series: str) -> None:
        p = (panel or "").strip()
        s = (series or "").strip()
        if not p or not s:
            return
        key = (p.casefold(), s.casefold())
        if key in seen:
            return
        seen.add(key)
        out.append({"panel": p, "series": s})

    if MONITORING_HTTP_PRIMARY_ENABLE and not _monitoring_alert_channel_muted("http"):
        for lbl in _analysis_hit_alert_series_labels(_http_analysis_for_payload(payload)):
            append(GRAFANA_PANEL_TITLE, lbl)

    for ex in payload.get("extraPanels") or []:
        if not isinstance(ex, dict):
            continue
        kind = (ex.get("kind") or "")
        logical = _extra_panel_logical_kind(kind)
        if logical not in (
            MONITORING_EXTRA_KIND_EGAME_ONLINE,
            MONITORING_EXTRA_KIND_EGAMES_BET,
            MONITORING_EXTRA_KIND_LIVESLOT_BET,
            MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT,
        ):
            continue
        if _monitoring_extra_channel_muted(kind):
            continue
        p2 = ex.get("payload") if isinstance(ex.get("payload"), dict) else {}
        if logical == MONITORING_EXTRA_KIND_EGAME_ONLINE:
            panel_title = GRAFANA_PANEL_TITLE_EGAME_ONLINE
            analysis = _analysis_for_egame_online_payload(p2)
            fallbacks: Tuple[str, ...] = (
                (MONITORING_EGAME_ONLINE_SERIES_KEYWORD,) if MONITORING_EGAME_ONLINE_SERIES_KEYWORD else ()
            )
        elif logical == MONITORING_EXTRA_KIND_EGAMES_BET:
            panel_title = GRAFANA_PANEL_TITLE_EGAMES_BET
            analysis = _analysis_for_egames_bet_payload(p2)
            inc = (MONITORING_EGAMES_BET_SERIES_INCLUDE or MONITORING_EGAMES_BET_SERIES_KEYWORD or "").strip()
            fallbacks = tuple(_parse_monitoring_series_keywords(inc))
        elif logical == MONITORING_EXTRA_KIND_LIVESLOT_BET:
            panel_title = GRAFANA_PANEL_TITLE_LIVESLOT_BET
            analysis = _analysis_for_liveslot_bet_payload(p2)
            fallbacks = (MONITORING_LIVESLOT_BET_SERIES_INCLUDE or "total spins",)
        else:
            panel_title = GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET
            analysis = _analysis_for_liveslot_spin_count_payload(p2)
            fallbacks = (MONITORING_LIVESLOT_SPIN_COUNT_SERIES_INCLUDE or "spin_count",)

        hit_labels = _analysis_hit_alert_series_labels(analysis)
        if hit_labels:
            for lbl in hit_labels:
                append(panel_title, lbl)
        elif _analysis_aggregate_hit_alert(analysis):
            for fb in fallbacks:
                append(panel_title, fb)

    return out


def _grafana_monitoring_screenshot_png(
    session: requests.Session,
    payload: Optional[Dict[str, Any]] = None,
    *,
    for_alert: bool = False,
    relative_from: Optional[str] = None,
    relative_to: Optional[str] = None,
) -> bytes:
    """Dashboard screenshot (default **1h** browser range). Alert shots isolate legend series."""
    su, eu = _monitoring_watch_eval_window_unix()
    rf = (relative_from or "").strip() or (
        _cfg_str("MONITORING_WATCH_SCREENSHOT_FROM", "now-1h").strip()
        if for_alert
        else _cfg_str("GRAFANA_DASHBOARD_FROM", "now-1h").strip()
    ) or "now-1h"
    rt = (relative_to or "").strip() or (
        _cfg_str("MONITORING_WATCH_SCREENSHOT_TO", "now").strip()
        if for_alert
        else _cfg_str("GRAFANA_DASHBOARD_TO", "now").strip()
    ) or "now"
    tz = _cfg_str("GRAFANA_SCREENSHOT_TIMEZONE", "browser").strip()
    targets = _monitoring_collect_alert_legend_targets(payload) if for_alert and payload else None
    return _grafana_headless_screenshot_png(
        session,
        su,
        eu,
        relative_from=rf,
        relative_to=rt,
        timezone_param=tz or None,
        legend_targets=targets,
    )


def _grafana_watchdog_alert_screenshot_png(
    session: requests.Session,
    payload: Optional[Dict[str, Any]] = None,
) -> bytes:
    """
    Watchdog alert image: Grafana **browser** range ``now-1h`` … ``now`` (plus optional ``timezone``),
    independent of the shorter Prometheus eval window on the payload.
    """
    return _grafana_monitoring_screenshot_png(session, payload, for_alert=True)


def _metric_series_is_http_leg(metric: Dict[str, Any]) -> bool:
    """Pick Prometheus rows that correspond to the HTTP series (legend ``http`` / label value ``http``)."""
    for k, v in metric.items():
        if k == "__name__":
            continue
        if str(v).strip().lower() == "http":
            return True
    return False


def _compact_http_legend(metric: Dict[str, Any], ref: str) -> str:
    """
    Prefer a ``callType=http``-style token when a label value is ``http``,
    but **append other labels** so multiple http streams (different instance/job/…)
    do not look like duplicate lines with mysteriously different values.
    """
    http_pair: Optional[str] = None
    other_bits: List[str] = []
    for k, v in sorted(metric.items()):
        if k == "__name__":
            continue
        if str(v).strip().lower() == "http":
            if http_pair is None:
                http_pair = f"{k}=http"
        else:
            other_bits.append(f"{k}={v}")
    if http_pair:
        if not other_bits:
            return http_pair
        tail = ", ".join(other_bits[:5])
        if len(other_bits) > 5:
            tail += ", …"
        return f"{http_pair} | {tail}"
    bits = [f"{k}={v}" for k, v in sorted(metric.items()) if k != "__name__"]
    return ", ".join(bits[:4]) or str(metric.get("__name__", ref))


def _merge_http_timeseries_points(payload: Dict[str, Any]) -> List[Tuple[float, float]]:
    """Sum all HTTP-leg series per Unix timestamp (ascending).

    For non-HTTP dashboards (e.g. Online Number / Infinity JSON panels), Prometheus rows may not
    carry ``http`` labels — fall back to the same keyword merge used elsewhere (empty keyword = all series).
    """
    by_ts: Dict[float, float] = {}
    for s in payload.get("series") or []:
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        for r in pdata.get("result") or []:
            m = r.get("metric") or {}
            if not _metric_series_is_http_leg(m):
                continue
            for pair in r.get("values") or []:
                if len(pair) < 2:
                    continue
                try:
                    ts = float(pair[0])
                    val = float(pair[1])
                except (TypeError, ValueError):
                    continue
                by_ts[ts] = by_ts.get(ts, 0.0) + val
    if not by_ts:
        return _merge_series_points_by_keyword(payload, "")
    return sorted(by_ts.items(), key=lambda x: x[0])


def _interpolate_grafana_legend_template(legend_format: str, metric: Dict[str, Any]) -> str:
    """
    Grafana ``legendFormat`` often contains ``{{project}}`` etc.; API snapshots keep the template
    while values live on Prometheus ``metric`` labels — substitute when keys match.
    """
    s = str(legend_format or "").strip()
    if not s or "{{" not in s:
        return s
    md = metric if isinstance(metric, dict) else {}

    def repl(m: re.Match[str]) -> str:
        key = (m.group(1) or "").strip()
        if not key:
            return m.group(0)
        v = md.get(key)
        if v is None:
            return m.group(0)
        return str(v)

    return re.sub(r"\{\{\s*([^}]+?)\s*\}\}", repl, s).strip()


def _series_group_key(legend_format: str, metric: Dict[str, Any]) -> str:
    """Stable key to merge duplicate Prometheus rows that belong to the same Grafana series."""
    md = metric if isinstance(metric, dict) else {}
    raw = str(legend_format or "").strip()
    interpolated = _interpolate_grafana_legend_template(raw, md)
    unresolved = "{{" in interpolated
    if interpolated and not unresolved:
        return f"leg:{interpolated.casefold()}"
    fp = "|".join(f"{k}={v}" for k, v in sorted(md.items()))
    return f"m:{fp}" if fp else "empty"


def _series_row_display_label(legend_format: str, metric: Dict[str, Any]) -> str:
    md = metric if isinstance(metric, dict) else {}
    raw = str(legend_format or "").strip()
    lg = _interpolate_grafana_legend_template(raw, md)
    if lg and "{{" not in lg:
        return lg
    lbl = str(md.get("series") or md.get("name") or md.get("project") or "").strip()
    if lbl:
        return lbl
    bits = [f"{k}={v}" for k, v in sorted(md.items()) if str(v).strip()]
    compact = ", ".join(bits[:6])
    return compact or lg or "series"


def _parse_monitoring_series_keywords(raw: str) -> List[str]:
    """Split ``EcallTW,Sinonet`` / space-separated include list for per-series filtering."""
    out: List[str] = []
    for part in re.split(r"[,;\s]+", (raw or "").strip()):
        p = part.strip()
        if p and p not in out:
            out.append(p)
    return out


def _series_label_matches_keywords(label: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    lb = (label or "").casefold()
    return any(kw.casefold() in lb for kw in keywords)


def _group_per_series_points_from_payload(
    payload: Dict[str, Any],
) -> List[Tuple[str, List[Tuple[float, float]]]]:
    """
    One merged point series per distinct Grafana legend / metric row (duplicate rows → max per timestamp).
    Used when ``MONITORING_PER_SERIES_ANALYSIS`` is on and no keyword merges everything.
    """
    buckets: Dict[str, Tuple[str, List[List[Tuple[float, float]]]]] = {}
    for s in payload.get("series") or []:
        lg = str(s.get("legendFormat") or "")
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        for r in pdata.get("result") or []:
            metric = r.get("metric") if isinstance(r.get("metric"), dict) else {}
            pts = _prometheus_result_value_pairs(r if isinstance(r, dict) else {})
            if not pts:
                continue
            gk = _series_group_key(lg, metric)
            disp = _series_row_display_label(lg, metric)
            if gk not in buckets:
                buckets[gk] = (disp, [])
            buckets[gk][1].append(pts)

    out: List[Tuple[str, List[Tuple[float, float]]]] = []
    for gk in sorted(buckets.keys()):
        disp, rows = buckets[gk]
        if len(rows) == 1:
            merged = rows[0]
        else:
            merged = _merge_result_rows_max_per_ts(rows)
        out.append((disp, merged))
    return out


def _analysis_aggregate_hit_alert(analysis: Dict[str, Any]) -> bool:
    ps = analysis.get("per_series")
    if isinstance(ps, list) and ps:
        return any(bool(x.get("hit_alert")) for x in ps if isinstance(x, dict))
    return bool(analysis.get("hit_alert"))


def _keyword_matches_series_labels(keyword: str, legend_format: str, metric: Dict[str, Any]) -> bool:
    """
    Match a panel ``keyword`` to a Prometheus-like series row.

    Purely **numeric** keywords (e.g. ``3201``, ``1492288``) use **token-boundary** matching so
    ``3201`` does **not** match ``13201`` / ``32012`` when those appear in legend or label text —
    substring-only matching caused bogus baselines (e.g. ~6096) vs Grafana's selected series (~21k).
    Non-numeric keywords keep substring behavior (legend / label substring).
    """
    kw_raw = (keyword or "").strip()
    if not kw_raw:
        return True
    md = metric if isinstance(metric, dict) else {}
    lg = _interpolate_grafana_legend_template(str(legend_format or "").strip(), md)
    metric_blob = " ".join(str(v) for v in md.values() if v is not None)
    kw_cf = kw_raw.casefold()
    lg_cf = lg.casefold()
    mb_cf = metric_blob.casefold()
    if lg_cf == kw_cf or mb_cf.strip() == kw_cf:
        return True
    if kw_raw.isdigit():
        boundary = re.compile(r"(^|[^0-9])" + re.escape(kw_raw) + r"([^0-9]|$)")
        return bool(boundary.search(lg_cf) or boundary.search(mb_cf))
    return kw_cf in lg_cf or kw_cf in mb_cf


def _series_row_exact_keyword_id(keyword: str, legend_format: str, metric: Dict[str, Any]) -> bool:
    """
    True when this row is unambiguously the single-series id (e.g. Grafana legend ``3201``),
    not merely ``keyword`` appearing somewhere in a long label blob (which can still wrong-merge).

    If any exact-id rows exist, :func:`_merge_series_points_by_keyword` uses **only** those rows so
    alert numbers match the highlighted series (~21k) instead of accidental mixes (~14k).
    """
    kw_raw = (keyword or "").strip()
    if not kw_raw:
        return False
    kw_cf = kw_raw.casefold()
    md = metric if isinstance(metric, dict) else {}
    lg = _interpolate_grafana_legend_template(str(legend_format or "").strip(), md).casefold()
    if lg == kw_cf:
        return True
    if not md:
        return False
    for mk in (
        "series",
        "name",
        "project",
        "provider",
        "providerid",
        "provider_id",
        "game",
        "gameid",
        "game_id",
        "label",
        "__series_id__",
    ):
        v = md.get(mk)
        if v is None:
            continue
        if str(v).strip().casefold() == kw_cf:
            return True
    return False


def _prometheus_result_value_pairs(result: Dict[str, Any]) -> List[Tuple[float, float]]:
    """``values`` pairs from one Prometheus ``result`` row as ``(timestamp_unix, value)``."""
    out: List[Tuple[float, float]] = []
    for pair in result.get("values") or []:
        if len(pair) < 2:
            continue
        try:
            ts = float(pair[0])
            val = float(pair[1])
        except (TypeError, ValueError):
            continue
        out.append((ts, val))
    return out


def _median_positive_abs(values: List[float]) -> float:
    """Median of ``abs(v)`` for finite ``v > 0``; ``0.0`` if none."""
    xs = sorted(abs(float(v)) for v in values if math.isfinite(float(v)) and float(v) > 0.0)
    if not xs:
        return 0.0
    mid = len(xs) // 2
    if len(xs) % 2 == 1:
        return float(xs[mid])
    return float(xs[mid - 1] + xs[mid]) / 2.0


def _pick_best_exact_keyword_series(candidates: List[List[Tuple[float, float]]]) -> List[Tuple[float, float]]:
    """
    When Grafana shows one ``3201`` line but the snapshot carries several duplicate ``result`` rows,
    picking **longest** series can select a stale/partial target (longer scrape history at wrong levels),
    producing bogus fast drops (e.g. ~18k -> ~8k) and fake spikes from ~11k baselines.
    Prefer the row whose magnitudes match the **main** curve: highest median ``|v|``, then length, then mass.
    """
    best_pl: Optional[List[Tuple[float, float]]] = None
    best_key: Optional[Tuple[float, int, float]] = None
    for pl in candidates:
        vs = [v for _, v in pl]
        med = _median_positive_abs(vs)
        mass = sum(abs(float(v)) for v in vs if math.isfinite(float(v)))
        key = (med, len(pl), mass)
        if best_key is None or key > best_key:
            best_key = key
            best_pl = pl
    return best_pl or []


def _merge_digit_keyword_rows_max_bucketed(
    rows: List[List[Tuple[float, float]]],
    *,
    tol_sec: float = 0.5,
) -> List[Tuple[float, float]]:
    """
    Duplicate numeric-id rows: for each minute, prefer samples near ``…:00`` (``max`` across those);
    if none, use the value at the timestamp **closest** to minute start (still one row per minute).
    """
    by_b: Dict[float, List[Tuple[float, float]]] = {}
    tol = float(tol_sec)
    for row in rows:
        for ts, val in row:
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(v):
                continue
            tsf = float(ts)
            b = _bucket_ts_monitoring_minute(tsf)
            by_b.setdefault(b, []).append((tsf, v))
    out: List[Tuple[float, float]] = []
    for b in sorted(by_b.keys()):
        cand = by_b[b]
        near = [v for t, v in cand if abs(t - b) <= tol]
        if near:
            out.append((b, max(near)))
        else:
            _, v_pick = min(cand, key=lambda x: abs(x[0] - b))
            out.append((b, v_pick))
    return out


def _merge_result_rows_max_per_ts(rows: List[List[Tuple[float, float]]]) -> List[Tuple[float, float]]:
    """Combine several Prometheus ``result`` rows that describe the same legend; ``max`` per timestamp."""
    by_ts: Dict[float, float] = {}
    for row in rows:
        row_ts: Dict[float, float] = {}
        for ts, val in row:
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(v):
                continue
            row_ts[float(ts)] = v
        for ts, v in row_ts.items():
            prev = by_ts.get(ts)
            if prev is None or v > prev:
                by_ts[ts] = v
    return sorted(by_ts.items(), key=lambda x: x[0])


def _merge_series_points_by_keyword(payload: Dict[str, Any], keyword: str) -> List[Tuple[float, float]]:
    kw = (keyword or "").strip()

    # Exact-id rows: duplicate Prometheus ``result`` rows (same legend ``3201``) must NOT be blindly
    # summed — sums like 5294+9483=14777 vs Grafana ~20k. Numeric ids: ``max`` per **minute** via
    # :func:`_bucket_ts_monitoring_minute` (``MONITORING_TIME_BUCKET_TZ``); even for a single
    # ``result`` row so sub-minute scrapes cannot leave a ghost low in the same minute as Grafana's
    # tooltip; non-numeric multi-row: pick one series by median magnitude.
    exact_candidates: List[List[Tuple[float, float]]] = []
    for s in payload.get("series") or []:
        lg = str(s.get("legendFormat") or "")
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        for r in pdata.get("result") or []:
            metric = r.get("metric") or {}
            md = metric if isinstance(metric, dict) else {}
            if kw and not _series_row_exact_keyword_id(kw, lg, md):
                continue
            pts = _prometheus_result_value_pairs(r if isinstance(r, dict) else {})
            if pts:
                exact_candidates.append(pts)
    if exact_candidates:
        if kw.isdigit():
            merged_pts = _merge_digit_keyword_rows_max_bucketed(exact_candidates)
        elif len(exact_candidates) == 1:
            merged_pts = exact_candidates[0]
        else:
            merged_pts = _pick_best_exact_keyword_series(exact_candidates)
        by_one: Dict[float, float] = {}
        for ts, val in merged_pts:
            by_one[ts] = val
        return sorted(by_one.items(), key=lambda x: x[0])

    def _accumulate_fuzzy() -> Dict[float, float]:
        digit_kw = bool(kw.isdigit())
        if digit_kw:
            digit_rows: List[List[Tuple[float, float]]] = []
            for s in payload.get("series") or []:
                lg = str(s.get("legendFormat") or "")
                prom = s.get("prometheus") or {}
                pdata = prom.get("data") or {}
                for r in pdata.get("result") or []:
                    metric = r.get("metric") or {}
                    md = metric if isinstance(metric, dict) else {}
                    if kw and not _keyword_matches_series_labels(kw, lg, md):
                        continue
                    pts = _prometheus_result_value_pairs(r if isinstance(r, dict) else {})
                    if pts:
                        digit_rows.append(pts)
            if digit_rows:
                return dict(_merge_digit_keyword_rows_max_bucketed(digit_rows))
            return {}

        by_ts_sum: Dict[float, float] = {}
        for s in payload.get("series") or []:
            lg = str(s.get("legendFormat") or "")
            prom = s.get("prometheus") or {}
            pdata = prom.get("data") or {}
            for r in pdata.get("result") or []:
                metric = r.get("metric") or {}
                md = metric if isinstance(metric, dict) else {}
                if kw and not _keyword_matches_series_labels(kw, lg, md):
                    continue
                row_ts: Dict[float, float] = {}
                for ts, val in _prometheus_result_value_pairs(r if isinstance(r, dict) else {}):
                    try:
                        v = float(val)
                    except (TypeError, ValueError):
                        continue
                    if not math.isfinite(v):
                        continue
                    row_ts[float(ts)] = v
                for ts, v in row_ts.items():
                    by_ts_sum[ts] = by_ts_sum.get(ts, 0.0) + v
        return by_ts_sum

    by_fuzzy = _accumulate_fuzzy()
    if not by_fuzzy:
        return []
    return sorted(by_fuzzy.items(), key=lambda x: x[0])


def _merge_deposit_points(payload: Dict[str, Any]) -> List[Tuple[float, float]]:
    return _merge_series_points_by_keyword(payload, MONITORING_DEPOSIT_SERIES_KEYWORD)


def _merge_withdraw_points(payload: Dict[str, Any]) -> List[Tuple[float, float]]:
    return _merge_series_points_by_keyword(payload, MONITORING_WITHDRAW_SERIES_KEYWORD)


def _filter_low_outlier_points(
    points: List[Tuple[float, float]],
    ratio_to_median: float = 0.10,
    min_abs_floor: float = 5.0,
) -> List[Tuple[float, float]]:
    """
    Remove tiny baseline outliers (e.g. 1/2/3/5) that can explode % change for
    high-volume series (30k~50k). This is used for Provider/Games keyword panels.
    """
    if len(points) < 4:
        return points
    vals = sorted(float(v) for _, v in points if float(v) > 0.0)
    if len(vals) < 3:
        return points
    mid = len(vals) // 2
    if len(vals) % 2 == 1:
        median_v = vals[mid]
    else:
        median_v = (vals[mid - 1] + vals[mid]) / 2.0
    floor = max(float(min_abs_floor), float(median_v) * float(ratio_to_median))
    filtered = [(t, v) for (t, v) in points if float(v) >= floor]
    # Keep original if filtering is too aggressive.
    if len(filtered) >= max(3, int(math.ceil(len(points) * 0.6))):
        return filtered
    return points


def _best_consecutive_drop_run(vals: List[float], ts: List[float]) -> Optional[Dict[str, Any]]:
    """
    Longest weakly-decreasing runs (each step ``vals[k+1] <= vals[k]``); score each run by
    ``(start - end) / start * 100`` over the whole span (not single-minute deltas).
    Returns the run with the largest such percentage (tie: more buckets wins).
    """
    L = len(vals)
    if L < 2:
        return None
    best: Optional[Dict[str, Any]] = None
    i = 0
    while i < L:
        j = i
        while j + 1 < L and vals[j + 1] <= vals[j]:
            j += 1
        if j > i and vals[i] > 0 and vals[j] < vals[i]:
            pct = (vals[i] - vals[j]) / vals[i] * 100.0
            buckets = j - i + 1
            cand = {
                "pct": round(pct, 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "buckets": buckets,
            }
            if best is None or pct > float(best["pct"]) or (
                pct == float(best["pct"]) and buckets > int(best["buckets"])
            ):
                best = cand
        i = j + 1
    return best


def _best_consecutive_spike_run(vals: List[float], ts: List[float]) -> Optional[Dict[str, Any]]:
    """Weakly-increasing runs; score ``(end - start) / start * 100`` over the span."""
    L = len(vals)
    if L < 2:
        return None
    best: Optional[Dict[str, Any]] = None
    i = 0
    while i < L:
        j = i
        while j + 1 < L and vals[j + 1] >= vals[j]:
            j += 1
        if j > i and vals[i] > 0 and vals[j] > vals[i]:
            pct = (vals[j] - vals[i]) / vals[i] * 100.0
            buckets = j - i + 1
            cand = {
                "pct": round(pct, 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "buckets": buckets,
            }
            if best is None or pct > float(best["pct"]) or (
                pct == float(best["pct"]) and buckets > int(best["buckets"])
            ):
                best = cand
        i = j + 1
    return best


def _http_drop_spike_analysis(
    points: List[Tuple[float, float]],
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    window_seconds: int = 120,
    *,
    fast_drop_threshold_pct: Optional[float] = None,
    fast_spike_threshold_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Alert rules:
    1) within ``window_seconds``, window drop vs ``fast_drop_threshold_pct`` (spike vs ``fast_spike_threshold_pct``)
    2) continuous monotonic run drop/spike >= ``continuous_threshold_pct``

    When ``fast_drop_threshold_pct`` / ``fast_spike_threshold_pct`` are omitted, both use ``fast_threshold_pct``.
    """
    fd = float(fast_drop_threshold_pct) if fast_drop_threshold_pct is not None else float(fast_threshold_pct)
    fs = float(fast_spike_threshold_pct) if fast_spike_threshold_pct is not None else float(fast_threshold_pct)
    out: Dict[str, Any] = {
        "pointCount": len(points),
        "hit_alert": False,
        "fast_threshold_pct": fast_threshold_pct,
        "fast_drop_threshold_pct": fd,
        "fast_spike_threshold_pct": fs,
        "continuous_threshold_pct": continuous_threshold_pct,
        "window_seconds": int(window_seconds),
        "consecutive_max_drop": None,
        "consecutive_max_spike": None,
        "window_max_drop": None,
        "window_max_spike": None,
    }
    if len(points) < 2:
        return out

    vals = [p[1] for p in points]
    ts = [p[0] for p in points]
    L = len(points)

    # Convert window_seconds to bucket span using median step.
    if L >= 2:
        diffs = [max(1.0, ts[i + 1] - ts[i]) for i in range(L - 1)]
        diffs.sort()
        step_sec = diffs[len(diffs) // 2]
    else:
        step_sec = 60.0
    span = max(1, int(round(float(window_seconds) / float(step_sec))))
    out["window_span_buckets"] = span + 1

    drop_run = _best_consecutive_drop_run(vals, ts)
    if drop_run is not None:
        out["consecutive_max_drop"] = {
            "pct": drop_run["pct"],
            "from_ts": drop_run["from_ts"],
            "to_ts": drop_run["to_ts"],
            "from_val": drop_run.get("from_val"),
            "to_val": drop_run.get("to_val"),
            "buckets": drop_run["buckets"],
        }
        if float(drop_run.get("pct") or 0.0) >= float(continuous_threshold_pct):
            out["hit_alert"] = True
    spike_run = _best_consecutive_spike_run(vals, ts)
    if spike_run is not None:
        out["consecutive_max_spike"] = {
            "pct": spike_run["pct"],
            "from_ts": spike_run["from_ts"],
            "to_ts": spike_run["to_ts"],
            "from_val": spike_run.get("from_val"),
            "to_val": spike_run.get("to_val"),
            "buckets": spike_run["buckets"],
        }
        if float(spike_run.get("pct") or 0.0) >= float(continuous_threshold_pct):
            out["hit_alert"] = True

    best_w_drop: Optional[Dict[str, Any]] = None
    best_w_spike: Optional[Dict[str, Any]] = None
    for i in range(0, L - span):
        j = i + span
        if vals[i] <= 0:
            continue
        pct = (vals[j] - vals[i]) / vals[i] * 100.0
        if pct < 0:
            cand_d = {
                "pct": round(abs(pct), 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "window_seconds": int(round(ts[j] - ts[i])),
            }
            if best_w_drop is None or float(cand_d["pct"]) > float(best_w_drop["pct"]):
                best_w_drop = cand_d
        elif pct > 0:
            cand_s = {
                "pct": round(abs(pct), 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "window_seconds": int(round(ts[j] - ts[i])),
            }
            if best_w_spike is None or float(cand_s["pct"]) > float(best_w_spike["pct"]):
                best_w_spike = cand_s
    out["window_max_drop"] = best_w_drop
    out["window_max_spike"] = best_w_spike
    if best_w_drop is not None and float(best_w_drop.get("pct") or 0.0) >= fd:
        out["hit_alert"] = True
    if best_w_spike is not None and float(best_w_spike.get("pct") or 0.0) >= fs:
        out["hit_alert"] = True
    return out


def _http_analysis_for_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    LiveSlots Online Number: per-series when ``MONITORING_PER_SERIES_ANALYSIS``;
    fast drop ``MONITORING_LIVESLOTS_FAST_DROP_ALERT_PCT`` / spike ``MONITORING_LIVESLOTS_FAST_SPIKE_ALERT_PCT``.
    """
    cont = MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT
    fd = MONITORING_LIVESLOTS_FAST_DROP_ALERT_PCT
    fs_fast = MONITORING_LIVESLOTS_FAST_SPIKE_ALERT_PCT

    def _one_series(pts_in: List[Tuple[float, float]], *, label: Optional[str] = None) -> Dict[str, Any]:
        pts2 = _snap_series_to_monitoring_minutes(pts_in, how="max")
        pts2 = _trim_trailing_minute_buckets(pts2, _analysis_drop_n())
        a = _http_drop_spike_analysis(
            pts2,
            fd,
            cont,
            MONITORING_ALERT_WINDOW_SECONDS,
            fast_drop_threshold_pct=fd,
            fast_spike_threshold_pct=fs_fast,
        )
        a["point_count"] = len(pts2)
        a["merged_points"] = [[t, v] for t, v in pts2]
        if label is not None:
            a["series_label"] = label
        return a

    if MONITORING_PER_SERIES_ANALYSIS:
        grouped = _group_per_series_points_from_payload(payload)
        if not grouped:
            empty = _one_series([], label=None)
            empty["hit_alert"] = False
            empty["per_series"] = []
            return empty
        subs: List[Dict[str, Any]] = []
        any_hit = False
        for lbl, pts in grouped:
            sub = _one_series(pts, label=lbl)
            subs.append(sub)
            any_hit = any_hit or bool(sub.get("hit_alert"))
        return {
            "hit_alert": any_hit,
            "per_series": subs,
            "point_count": sum(int(s.get("point_count") or 0) for s in subs),
            "merged_points": subs[0]["merged_points"] if len(subs) == 1 else [],
            "fast_threshold_pct": fd,
            "fast_drop_threshold_pct": fd,
            "fast_spike_threshold_pct": fs_fast,
            "continuous_threshold_pct": cont,
            "window_seconds": MONITORING_ALERT_WINDOW_SECONDS,
        }

    pts = _merge_http_timeseries_points(payload)
    pts = _snap_series_to_monitoring_minutes(pts, how="sum")
    pts = _trim_trailing_minute_buckets(pts, _analysis_drop_n())
    a = _http_drop_spike_analysis(
        pts,
        fd,
        cont,
        MONITORING_ALERT_WINDOW_SECONDS,
        fast_drop_threshold_pct=fd,
        fast_spike_threshold_pct=fs_fast,
    )
    a["point_count"] = len(pts)
    a["merged_points"] = [[t, v] for t, v in pts]
    return a


def _analysis_for_egame_online_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Egame Online Number — every series; fast drop/spike 25% (defaults)."""
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_EGAME_ONLINE_SERIES_KEYWORD,
        MONITORING_EGAME_FAST_DROP_ALERT_PCT,
        MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT,
        snap_how="max",
        apply_baseline_filter=False,
        fast_drop_threshold_pct=MONITORING_EGAME_FAST_DROP_ALERT_PCT,
        fast_spike_threshold_pct=MONITORING_EGAME_FAST_SPIKE_ALERT_PCT,
    )


def _analysis_for_deposit_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_DEPOSIT_SERIES_KEYWORD,
        MONITORING_DEPOSIT_ALERT_PCT,
        MONITORING_DEPOSIT_CONTINUOUS_ALERT_PCT,
        snap_how="sum",
        apply_baseline_filter=False,
    )


def _analysis_for_withdraw_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_WITHDRAW_SERIES_KEYWORD,
        MONITORING_WITHDRAW_ALERT_PCT,
        MONITORING_WITHDRAW_CONTINUOUS_ALERT_PCT,
        snap_how="sum",
        apply_baseline_filter=False,
    )


def _analysis_for_keyword_payload(
    payload: Dict[str, Any],
    keyword: str,
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    *,
    snap_how: str = "max",
    apply_baseline_filter: bool = True,
    fast_drop_threshold_pct: Optional[float] = None,
    fast_spike_threshold_pct: Optional[float] = None,
    series_include_keywords: Optional[List[str]] = None,
) -> Dict[str, Any]:
    kw = (keyword or "").strip()
    use_per_series = bool(MONITORING_PER_SERIES_ANALYSIS) and not kw
    include_kws = list(series_include_keywords or [])

    def _points_through_pipeline(
        pts_in: List[Tuple[float, float]],
        *,
        series_label: Optional[str] = None,
    ) -> Dict[str, Any]:
        pts_work = list(pts_in)
        if apply_baseline_filter:
            pts_f = _filter_low_outlier_points(pts_work, ratio_to_median=0.28)
            if len(pts_f) != len(pts_work):
                logger.info(
                    "keyword baseline filter applied keyword=%r label=%r points=%s->%s",
                    keyword,
                    (series_label or ""),
                    len(pts_work),
                    len(pts_f),
                )
            pts_work = pts_f
        pts_work = _snap_series_to_monitoring_minutes(pts_work, how=snap_how)
        pts_work = _trim_trailing_minute_buckets(pts_work, _analysis_drop_n())
        a = _http_drop_spike_analysis(
            pts_work,
            fast_threshold_pct,
            continuous_threshold_pct,
            MONITORING_ALERT_WINDOW_SECONDS,
            fast_drop_threshold_pct=fast_drop_threshold_pct,
            fast_spike_threshold_pct=fast_spike_threshold_pct,
        )
        a["point_count"] = len(pts_work)
        a["merged_points"] = [[t, v] for t, v in pts_work]
        if series_label is not None:
            a["series_label"] = series_label
        return a

    if use_per_series:
        grouped = _group_per_series_points_from_payload(payload)
        if include_kws:
            grouped = [
                (lbl, pts)
                for lbl, pts in grouped
                if _series_label_matches_keywords(lbl, include_kws)
            ]
        if not grouped:
            sample_labels: List[str] = []
            for s in payload.get("series") or []:
                prom = s.get("prometheus") if isinstance(s.get("prometheus"), dict) else {}
                pdata = prom.get("data") if isinstance(prom.get("data"), dict) else {}
                for r in pdata.get("result") or []:
                    metric = r.get("metric") if isinstance(r.get("metric"), dict) else {}
                    lbl = str(metric.get("series") or metric.get("name") or "").strip()
                    if not lbl:
                        lbl = " ".join(str(v) for v in metric.values() if str(v).strip()).strip()
                    if lbl and lbl not in sample_labels:
                        sample_labels.append(lbl)
                    if len(sample_labels) >= 20:
                        break
                if len(sample_labels) >= 20:
                    break
            logger.info(
                "per-series analysis: no points keyword=%r sample_series=%s",
                keyword,
                sample_labels[:20],
            )
            empty = _points_through_pipeline([])
            empty["hit_alert"] = False
            empty["per_series"] = []
            return empty

        subs: List[Dict[str, Any]] = []
        any_hit = False
        for lbl, pts in grouped:
            sub = _points_through_pipeline(pts, series_label=lbl)
            subs.append(sub)
            any_hit = any_hit or bool(sub.get("hit_alert"))
        return {
            "hit_alert": any_hit,
            "per_series": subs,
            "point_count": sum(int(s.get("point_count") or 0) for s in subs),
            "merged_points": subs[0]["merged_points"] if len(subs) == 1 else [],
            "fast_threshold_pct": fast_threshold_pct,
            "continuous_threshold_pct": continuous_threshold_pct,
            "window_seconds": MONITORING_ALERT_WINDOW_SECONDS,
        }

    pts = _merge_series_points_by_keyword(payload, keyword)
    pts_filtered = pts
    if apply_baseline_filter:
        pts_filtered = _filter_low_outlier_points(pts, ratio_to_median=0.28)
        if len(pts_filtered) != len(pts):
            logger.info(
                "keyword baseline filter applied keyword=%r points=%s->%s",
                keyword,
                len(pts),
                len(pts_filtered),
            )
    pts_filtered = _snap_series_to_monitoring_minutes(pts_filtered, how=snap_how)
    pts_filtered = _trim_trailing_minute_buckets(pts_filtered, _analysis_drop_n())
    a = _http_drop_spike_analysis(
        pts_filtered,
        fast_threshold_pct,
        continuous_threshold_pct,
        MONITORING_ALERT_WINDOW_SECONDS,
        fast_drop_threshold_pct=fast_drop_threshold_pct,
        fast_spike_threshold_pct=fast_spike_threshold_pct,
    )
    a["point_count"] = len(pts_filtered)
    a["merged_points"] = [[t, v] for t, v in pts_filtered]
    if not pts:
        sample_labels = []
        for s in payload.get("series") or []:
            prom = s.get("prometheus") if isinstance(s.get("prometheus"), dict) else {}
            pdata = prom.get("data") if isinstance(prom.get("data"), dict) else {}
            for r in pdata.get("result") or []:
                metric = r.get("metric") if isinstance(r.get("metric"), dict) else {}
                lbl = str(metric.get("series") or metric.get("name") or "").strip()
                if not lbl:
                    lbl = " ".join(str(v) for v in metric.values() if str(v).strip()).strip()
                if lbl and lbl not in sample_labels:
                    sample_labels.append(lbl)
                if len(sample_labels) >= 20:
                    break
            if len(sample_labels) >= 20:
                break
        logger.info(
            "keyword no match keyword=%r sample_series=%s",
            keyword,
            sample_labels[:20],
        )
    return a


def _analysis_for_egames_bet_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    include_raw = MONITORING_EGAMES_BET_SERIES_INCLUDE or MONITORING_EGAMES_BET_SERIES_KEYWORD
    include_kws = _parse_monitoring_series_keywords(include_raw)
    return _analysis_for_keyword_payload(
        payload,
        "",
        MONITORING_EGAMES_BET_FAST_DROP_ALERT_PCT,
        MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT,
        snap_how="sum",
        apply_baseline_filter=True,
        fast_drop_threshold_pct=MONITORING_EGAMES_BET_FAST_DROP_ALERT_PCT,
        fast_spike_threshold_pct=MONITORING_EGAMES_BET_FAST_SPIKE_ALERT_PCT,
        series_include_keywords=include_kws or None,
    )


def _merge_liveslot_bet_primary_series(payload: Dict[str, Any]) -> Tuple[List[Tuple[float, float]], str]:
    """
    One merged series for Liveslot 下注 — matches Grafana's aggregate line, not every hidden target.
    """
    include_raw = (MONITORING_LIVESLOT_BET_SERIES_INCLUDE or "").strip()
    include_kws = _parse_monitoring_series_keywords(include_raw)
    if include_kws:
        grouped = [
            (lbl, pts)
            for lbl, pts in _group_per_series_points_from_payload(payload)
            if _series_label_matches_keywords(lbl, include_kws)
        ]
        if len(grouped) == 1:
            return grouped[0][1], grouped[0][0]
        if len(grouped) > 1:
            merged = _merge_result_rows_max_per_ts([pts for _, pts in grouped])
            return merged, " | ".join(lbl for lbl, _ in grouped)
    grouped_all = _group_per_series_points_from_payload(payload)
    if not grouped_all:
        pts = _merge_series_points_by_keyword(payload, include_raw or "total spins")
        return pts, include_raw or "total spins"
    best_lbl, best_pts = max(
        grouped_all,
        key=lambda item: (_median_positive_abs([v for _, v in item[1]]), len(item[1])),
    )
    return best_pts, best_lbl


def _liveslot_bet_suppress_false_drop_alerts(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Drop false positives from baseline filtering, low-volume series, and scrape gaps.
    Keeps only fast window DROP when both endpoints sit near the series' typical level.
    """
    pts_raw = analysis.get("merged_points") or []
    vals_pos = [
        float(p[1])
        for p in pts_raw
        if isinstance(p, (list, tuple)) and len(p) >= 2 and float(p[1]) > 0.0
    ]
    if len(vals_pos) < 4:
        if analysis.get("hit_alert"):
            analysis["hit_alert"] = False
            analysis["false_alert_suppressed"] = "insufficient_points"
        return analysis

    med = _median_positive_abs(vals_pos)
    min_ep = max(
        MONITORING_LIVESLOT_BET_MIN_ABS_DROP * 0.05,
        med * MONITORING_LIVESLOT_BET_DROP_ENDPOINT_MIN_MEDIAN_RATIO,
    )
    fd_thr = float(
        analysis.get("fast_drop_threshold_pct") or MONITORING_LIVESLOT_BET_FAST_DROP_ALERT_PCT
    )
    wd = analysis.get("window_max_drop")
    if not isinstance(wd, dict):
        if analysis.get("hit_alert"):
            analysis["hit_alert"] = False
            analysis["false_alert_suppressed"] = "no_window_drop"
        return analysis

    pct = float(wd.get("pct") or 0.0)
    fv = float(wd.get("from_val") or 0.0)
    tv = float(wd.get("to_val") or 0.0)
    if pct < fd_thr:
        analysis["hit_alert"] = False
        return analysis

    abs_drop = fv - tv
    if fv < min_ep or tv < min_ep or abs_drop < MONITORING_LIVESLOT_BET_MIN_ABS_DROP:
        analysis["hit_alert"] = False
        analysis["false_alert_suppressed"] = "drop_endpoints_or_magnitude"
        logger.info(
            "liveslot_bet: suppress false drop pct=%.1f from=%s to=%s median=%.0f min_ep=%.0f min_abs=%.0f",
            pct,
            fv,
            tv,
            med,
            min_ep,
            MONITORING_LIVESLOT_BET_MIN_ABS_DROP,
        )
    return analysis


def _analysis_for_liveslot_bet_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Liveslot 下注 / Liveslots-Spin-Bet: single aggregate series, fast DROP only (default ≥50% / ~3m).
    Avoids per-series + baseline-filter false alerts.
    """
    pts_in, series_lbl = _merge_liveslot_bet_primary_series(payload)
    pts_work = list(pts_in)
    if MONITORING_LIVESLOT_BET_BASELINE_FILTER:
        pts_work = _filter_low_outlier_points(pts_work, ratio_to_median=0.28)
    pts_work = _snap_series_to_monitoring_minutes(pts_work, how="max")
    pts_work = _trim_trailing_minute_buckets(pts_work, _analysis_drop_n())
    a = _http_drop_spike_analysis(
        pts_work,
        MONITORING_LIVESLOT_BET_FAST_DROP_ALERT_PCT,
        MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT,
        MONITORING_ALERT_WINDOW_SECONDS,
        fast_drop_threshold_pct=MONITORING_LIVESLOT_BET_FAST_DROP_ALERT_PCT,
        fast_spike_threshold_pct=MONITORING_LIVESLOT_BET_FAST_SPIKE_ALERT_PCT,
    )
    a["point_count"] = len(pts_work)
    a["merged_points"] = [[t, v] for t, v in pts_work]
    a["series_label"] = series_lbl
    return _liveslot_bet_suppress_false_drop_alerts(a)


def _is_zero_metric_value(v: float, *, eps: float = 1e-9) -> bool:
    return math.isfinite(v) and abs(v) <= eps


def _trailing_zero_run_on_minute_buckets(
    points: List[Tuple[float, float]],
) -> Dict[str, Any]:
    """Trailing consecutive zero-valued minute buckets from the newest end."""
    if not points:
        return {"bucket_count": 0, "duration_seconds": 0.0}
    run = 0
    for _ts, val in reversed(points):
        try:
            v = float(val)
        except (TypeError, ValueError):
            break
        if not _is_zero_metric_value(v):
            break
        run += 1
    if run <= 0:
        return {"bucket_count": 0, "duration_seconds": 0.0}
    from_ts = points[-run][0]
    to_ts = points[-1][0]
    step = max(1, int(GRAFANA_QUERY_STEP))
    return {
        "bucket_count": run,
        "duration_seconds": float(run * step),
        "from_ts": from_ts,
        "to_ts": to_ts,
    }


def _select_liveslot_spin_count_series(payload: Dict[str, Any]) -> Tuple[List[Tuple[float, float]], str]:
    """Pick ``spin_count`` (or configured include keywords) from Liveslots-Spin-Bet panel."""
    include_raw = (MONITORING_LIVESLOT_SPIN_COUNT_SERIES_INCLUDE or "spin_count").strip()
    include_kws = _parse_monitoring_series_keywords(include_raw)
    grouped = [
        (lbl, pts)
        for lbl, pts in _group_per_series_points_from_payload(payload)
        if _series_label_matches_keywords(lbl, include_kws)
    ]
    if len(grouped) == 1:
        return grouped[0][1], grouped[0][0]
    if len(grouped) > 1:
        merged = _merge_result_rows_max_per_ts([pts for _, pts in grouped])
        return merged, " | ".join(lbl for lbl, _ in grouped)
    pts = _merge_series_points_by_keyword(payload, include_raw or "spin_count")
    return pts, include_raw or "spin_count"


def _analysis_for_liveslot_spin_count_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Liveslots-Spin-Bet / spin_count: alert when value stays 0 longer than configured seconds (default >2m).
    """
    pts_in, series_lbl = _select_liveslot_spin_count_series(payload)
    pts_work = _snap_series_to_monitoring_minutes(list(pts_in), how="max")
    pts_work = _trim_trailing_minute_buckets(pts_work, _analysis_drop_n())
    zero_run = _trailing_zero_run_on_minute_buckets(pts_work)
    dur = float(zero_run.get("duration_seconds") or 0.0)
    threshold = float(MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS)
    hit = dur > threshold and int(zero_run.get("bucket_count") or 0) > 0
    return {
        "alert_type": "zero_duration",
        "hit_alert": hit,
        "zero_run": zero_run,
        "zero_alert_seconds": threshold,
        "point_count": len(pts_work),
        "merged_points": [[t, v] for t, v in pts_work],
        "series_label": series_lbl,
    }


def _analysis_for_provider_general_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_PROVIDER_GENERAL_SERIES_KEYWORD,
        MONITORING_PROVIDER_GENERAL_ALERT_PCT,
        MONITORING_PROVIDER_GENERAL_CONTINUOUS_ALERT_PCT,
    )


def _analysis_for_provider_inhouse_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_PROVIDER_INHOUSE_SERIES_KEYWORD,
        MONITORING_PROVIDER_INHOUSE_ALERT_PCT,
        MONITORING_PROVIDER_INHOUSE_CONTINUOUS_ALERT_PCT,
    )


def _format_extra_analysis_lines(section_label: str, analysis: Dict[str, Any]) -> List[str]:
    if MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS:
        return []
    if analysis.get("alert_type") == "zero_duration":
        thr = int(
            analysis.get("zero_alert_seconds") or MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS
        )
        thr_m = max(1, (thr + 59) // 60)
        lines: List[str] = [
            "",
            f"[{section_label}] alert when value = 0 for > {thr_m}m ({thr}s)",
        ]
        zr = analysis.get("zero_run") if isinstance(analysis.get("zero_run"), dict) else {}
        if zr.get("bucket_count"):
            lines.append(
                f"current trailing zero run: {int(zr.get('bucket_count') or 0)}m "
                f"({_fmt_ts_short(zr.get('from_ts'))} → {_fmt_ts_short(zr.get('to_ts'))})"
            )
        return lines
    fd = float(analysis.get("fast_drop_threshold_pct") or analysis.get("fast_threshold_pct") or 15.0)
    fs = float(analysis.get("fast_spike_threshold_pct") or analysis.get("fast_threshold_pct") or 15.0)
    cont_thr = float(
        analysis.get("continuous_threshold_pct") or MONITORING_GAME_ALERT_CONTINUOUS_PCT
    )
    win_sec = int(analysis.get("window_seconds") or MONITORING_ALERT_WINDOW_SECONDS)
    win_m = max(1, win_sec // 60)
    if math.isfinite(fs) and abs(fd - fs) < 1e-9:
        rule_fast = f"drop/spike > {fd:g}% within {win_m}m"
    elif math.isfinite(fs):
        rule_fast = f"drop > {fd:g}% or spike > {fs:g}% within {win_m}m"
    else:
        rule_fast = f"drop > {fd:g}% within {win_m}m (spike fast off)"
    lines: List[str] = [
        "",
        f"[{section_label}] alert when {rule_fast} or continuous drop/spike > {cont_thr:g}%",
    ]
    wd = analysis.get("window_max_drop")
    ws = analysis.get("window_max_spike")
    cd = analysis.get("consecutive_max_drop")
    cs = analysis.get("consecutive_max_spike")
    lines.append(
        f"within {win_sec//60}m drop/spike: -{(wd or {}).get('pct', 'n/a')}% / +{(ws or {}).get('pct', 'n/a')}%"
    )
    lines.append(
        f"continuous drop/spike : -{(cd or {}).get('pct', 'n/a')}% / +{(cs or {}).get('pct', 'n/a')}%"
    )
    return lines


def _format_trigger_lines(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    window_seconds: int,
) -> List[str]:
    out: List[str] = []
    wd = analysis.get("window_max_drop")
    ws = analysis.get("window_max_spike")
    cd = analysis.get("consecutive_max_drop")
    cs = analysis.get("consecutive_max_spike")
    win_m = max(1, int(round(float(window_seconds) / 60.0)))
    ft_drop = float(analysis.get("fast_drop_threshold_pct", fast_threshold_pct))
    ft_spike = float(analysis.get("fast_spike_threshold_pct", fast_threshold_pct))

    def _pct_text(v: Any) -> str:
        try:
            f = float(v)
            if abs(f - round(f)) < 1e-6:
                return f"{int(round(f)):,}"
            return f"{f:,.2f}"
        except (TypeError, ValueError):
            return str(v)

    def _event_text(ev: Dict[str, Any], direction: str, threshold_pct: float) -> str:
        sign = "+" if direction == "SPIKE" else "-"
        pct = _pct_text(ev.get("pct"))
        return (
            f"{direction} {sign}{pct}% (>{threshold_pct:g}%) "
            f"{_fmt_num(ev.get('from_val'))} ({_fmt_ts_short(ev.get('from_ts'))}) -> "
            f"{_fmt_num(ev.get('to_val'))} ({_fmt_ts_short(ev.get('to_ts'))})"
        )

    fast_hits: List[str] = []
    if isinstance(wd, dict) and float(wd.get("pct") or 0.0) >= ft_drop:
        fast_hits.append(_event_text(wd, "DROP", ft_drop))
    if isinstance(ws, dict) and float(ws.get("pct") or 0.0) >= ft_spike and math.isfinite(ft_spike):
        fast_hits.append(_event_text(ws, "SPIKE", ft_spike))

    cont_hits: List[str] = []
    if isinstance(cd, dict) and float(cd.get("pct") or 0.0) >= float(continuous_threshold_pct):
        cont_hits.append(_event_text(cd, "DROP", continuous_threshold_pct))
    if isinstance(cs, dict) and float(cs.get("pct") or 0.0) >= float(continuous_threshold_pct):
        cont_hits.append(_event_text(cs, "SPIKE", continuous_threshold_pct))

    if fast_hits or cont_hits:
        block: List[str] = [f"[{graph_label}] {series_label}"]
        if fast_hits:
            block.append(f"Fast ({win_m}m): {' | '.join(fast_hits)}")
        if cont_hits:
            block.append(f"Continuous: {' | '.join(cont_hits)}")
        out.append("\n".join(block))
    if analysis.get("alert_type") == "zero_duration" and analysis.get("hit_alert"):
        zr = analysis.get("zero_run") if isinstance(analysis.get("zero_run"), dict) else {}
        dur_s = float(zr.get("duration_seconds") or 0.0)
        dur_m = max(1, int(round(dur_s / 60.0)))
        thr_s = int(
            analysis.get("zero_alert_seconds") or MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS
        )
        out.append(
            f"[{graph_label}] {series_label} ZERO — value 0 for {dur_m}m "
            f"(>{thr_s}s rule) "
            f"{_fmt_ts_short(zr.get('from_ts'))} → {_fmt_ts_short(zr.get('to_ts'))}"
        )
    return out


def _format_alert_series_table_footer(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    *,
    max_rows: Optional[int] = None,
) -> str:
    """English-only: compact table tail so alert lines can be checked against the same merged series."""
    pts = analysis.get("merged_points") or []
    title = f"Recent points — [{graph_label}] {series_label}:"
    if not pts:
        return f"{title}\n(no points)"
    cap = MONITORING_TABLE_TAIL_ROWS if max_rows is None else max(1, min(99, int(max_rows)))
    tail = pts[-cap:]
    rows = ["```text", "time           value"]
    for pair in tail:
        rows.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
    rows.append("```")
    return "\n".join([title, "\n".join(rows)])


def _format_simple_series_alert_block(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    *,
    max_rows: Optional[int] = None,
) -> str:
    """
    Grafana-style snippet: latest timestamps and values from the same merged series the bot analyzed.
    English-only user-visible lines (product convention).
    """
    pts = analysis.get("merged_points") or []
    head = [
        f"[{graph_label}] {series_label}",
        "Threshold exceeded. Recent points (bot merged series, oldest → newest):",
    ]
    if not pts:
        return "\n".join(head + ["(no points in window)"])
    cap = MONITORING_TABLE_TAIL_ROWS if max_rows is None else max(1, min(99, int(max_rows)))
    tail = pts[-cap:]
    rows = ["```text", "time           value"]
    for pair in tail:
        rows.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
    rows.append("```")
    return "\n".join(head + rows)


def _format_trigger_fallback_line(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    window_seconds: int,
) -> Optional[str]:
    """
    Fallback reason when concise consecutive drop/spike lines are empty but ``hit_alert`` is true.
    Uses avg-drop window details so alert messages are never reasonless.
    """
    win_m = max(1, int(round(float(window_seconds) / 60.0)))
    if bool(analysis.get("hit_alert")):
        fd = float(analysis.get("fast_drop_threshold_pct", fast_threshold_pct))
        fs = float(analysis.get("fast_spike_threshold_pct", fast_threshold_pct))
        if math.isfinite(fs) and abs(fd - fs) > 1e-9:
            return (
                f"[{graph_label}] {series_label} alert triggered "
                f"(rules: drop>{fd:g}% or spike>{fs:g}% within {win_m}m win, "
                f"or continuous >{continuous_threshold_pct:g}%)"
            )
        return (
            f"[{graph_label}] {series_label} alert triggered "
            f"(rules: >{fast_threshold_pct:g}% within {win_m}m or continuous >{continuous_threshold_pct:g}%)"
        )
    return None


def _format_alert_reason_chunks_for_analysis(
    graph_label: str,
    default_series_disp: str,
    analysis: Dict[str, Any],
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    window_seconds: int,
) -> List[str]:
    """Build one or more alert markdown chunks (per-game lines when ``per_series`` is set)."""
    chunks: List[str] = []
    per = analysis.get("per_series")
    if isinstance(per, list) and per:
        for sub in per:
            if not isinstance(sub, dict):
                continue
            s_one = str(sub.get("series_label") or "series")
            reasons = _format_trigger_lines(
                graph_label,
                s_one,
                sub,
                fast_threshold_pct,
                continuous_threshold_pct,
                window_seconds,
            )
            if not reasons:
                fb = _format_trigger_fallback_line(
                    graph_label,
                    s_one,
                    sub,
                    fast_threshold_pct,
                    continuous_threshold_pct,
                    window_seconds,
                )
                if fb:
                    reasons.append(fb)
            if MONITORING_SIMPLE_ALERT_TEXT and (reasons or bool(sub.get("hit_alert"))):
                reasons = [_format_simple_series_alert_block(graph_label, s_one, sub)]
            elif reasons and not MONITORING_SIMPLE_ALERT_TEXT:
                reasons.append(
                    _format_alert_series_table_footer(graph_label, s_one, sub)
                )
            if reasons:
                chunks.append("\n\n".join(reasons))
        return chunks

    s_disp = (default_series_disp or "").strip() or "all series merged"
    reasons2 = _format_trigger_lines(
        graph_label,
        s_disp,
        analysis,
        fast_threshold_pct,
        continuous_threshold_pct,
        window_seconds,
    )
    if not reasons2:
        fb2 = _format_trigger_fallback_line(
            graph_label,
            s_disp,
            analysis,
            fast_threshold_pct,
            continuous_threshold_pct,
            window_seconds,
        )
        if fb2:
            reasons2.append(fb2)
    if MONITORING_SIMPLE_ALERT_TEXT and (reasons2 or _analysis_aggregate_hit_alert(analysis)):
        reasons2 = [_format_simple_series_alert_block(graph_label, s_disp, analysis)]
    elif reasons2 and not MONITORING_SIMPLE_ALERT_TEXT:
        reasons2.append(_format_alert_series_table_footer(graph_label, s_disp, analysis))
    if reasons2:
        chunks.append("\n\n".join(reasons2))
    return chunks


def _append_monitoring_alert_target_user_mention(lines: List[str]) -> None:
    """Alert / threshold hits: optionally mention ``TARGET_USER_OPEN_ID``.

    Person tagging is DISABLED by default (the Qwen second-review replaces the human ping);
    set ``MONITORING_ALERT_AT_USER_ENABLE=1`` to restore the @mention.
    """
    if not _lark_env_truthy_or_default("MONITORING_ALERT_AT_USER_ENABLE", default=False):
        return
    if not TARGET_USER_OPEN_ID:
        return
    lines.append("")
    if MONITORING_ALERT_AT_USER_NOTE:
        lines.append(MONITORING_ALERT_AT_USER_NOTE)
    lines.append(f"<at id={TARGET_USER_OPEN_ID}></at>")


def _format_alert_trigger_reply(payload: Dict[str, Any]) -> str:
    """
    Alert-only concise content:
    which graph/series, spike or drop, from value/time -> to value/time.
    """
    _mute_purge_expired()
    lines: List[str] = [
        "[ALERT] Monitoring thresholds exceeded",
        "Fast = sharpest move within ~3 minutes; Continuous = longest steady climb or drop.",
        "",
    ]
    reason_blocks: List[str] = []
    if MONITORING_HTTP_PRIMARY_ENABLE and not _monitoring_alert_channel_muted("http"):
        a_http = _http_analysis_for_payload(payload)
        for chunk in _format_alert_reason_chunks_for_analysis(
            GRAFANA_PANEL_TITLE,
            "",
            a_http,
            MONITORING_LIVESLOTS_FAST_DROP_ALERT_PCT,
            MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT,
            MONITORING_ALERT_WINDOW_SECONDS,
        ):
            reason_blocks.append(chunk)
    for ex in payload.get("extraPanels") or []:
        if not isinstance(ex, dict):
            continue
        kind = (ex.get("kind") or "")
        logical = _extra_panel_logical_kind(kind)
        if logical not in (
            MONITORING_EXTRA_KIND_EGAME_ONLINE,
            MONITORING_EXTRA_KIND_EGAMES_BET,
            MONITORING_EXTRA_KIND_LIVESLOT_BET,
            MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT,
        ):
            continue
        if _monitoring_extra_channel_muted(kind):
            continue
        p2 = ex.get("payload") if isinstance(ex.get("payload"), dict) else {}
        if logical == MONITORING_EXTRA_KIND_EGAME_ONLINE:
            g_lbl = GRAFANA_PANEL_TITLE_EGAME_ONLINE
            s_lbl = MONITORING_EGAME_ONLINE_SERIES_KEYWORD
            a2 = _analysis_for_egame_online_payload(p2)
            fast2 = MONITORING_EGAME_FAST_DROP_ALERT_PCT
            cont2 = MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT
        elif logical == MONITORING_EXTRA_KIND_EGAMES_BET:
            g_lbl = GRAFANA_PANEL_TITLE_EGAMES_BET
            s_lbl = MONITORING_EGAMES_BET_SERIES_INCLUDE or MONITORING_EGAMES_BET_SERIES_KEYWORD
            a2 = _analysis_for_egames_bet_payload(p2)
            fast2 = MONITORING_EGAMES_BET_FAST_DROP_ALERT_PCT
            cont2 = MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT
        elif logical == MONITORING_EXTRA_KIND_LIVESLOT_BET:
            g_lbl = GRAFANA_PANEL_TITLE_LIVESLOT_BET
            s_lbl = ""
            a2 = _analysis_for_liveslot_bet_payload(p2)
            fast2 = MONITORING_LIVESLOT_BET_FAST_DROP_ALERT_PCT
            cont2 = MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT
        else:
            g_lbl = GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET
            s_lbl = MONITORING_LIVESLOT_SPIN_COUNT_SERIES_INCLUDE
            a2 = _analysis_for_liveslot_spin_count_payload(p2)
            fast2 = MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT
            cont2 = MONITORING_PANEL_FAST_ONLY_CONTINUOUS_PCT
        for chunk in _format_alert_reason_chunks_for_analysis(
            g_lbl,
            s_lbl,
            a2,
            fast2,
            cont2,
            MONITORING_ALERT_WINDOW_SECONDS,
        ):
            reason_blocks.append(chunk)
    if not reason_blocks:
        lines.append("Alert fired but no panel matched text details (no analyzable points).")
    else:
        lines.append("\n────────\n".join(reason_blocks))
    _append_monitoring_alert_target_user_mention(lines)
    return "\n".join(lines)


def _monitoring_payload_hit_alert(payload: Dict[str, Any]) -> bool:
    _mute_purge_expired()
    if (
        MONITORING_HTTP_PRIMARY_ENABLE
        and not _monitoring_alert_channel_muted("http")
        and _analysis_aggregate_hit_alert(_http_analysis_for_payload(payload))
    ):
        return True
    for ex in payload.get("extraPanels") or []:
        if not isinstance(ex, dict):
            continue
        k = (ex.get("kind") or "")
        logical = _extra_panel_logical_kind(k)
        if logical not in (
            MONITORING_EXTRA_KIND_EGAME_ONLINE,
            MONITORING_EXTRA_KIND_EGAMES_BET,
            MONITORING_EXTRA_KIND_LIVESLOT_BET,
            MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT,
        ):
            continue
        if _monitoring_extra_channel_muted(k):
            continue
        p2 = ex.get("payload") if isinstance(ex.get("payload"), dict) else {}
        if logical == MONITORING_EXTRA_KIND_EGAME_ONLINE and _analysis_aggregate_hit_alert(
            _analysis_for_egame_online_payload(p2)
        ):
            return True
        if logical == MONITORING_EXTRA_KIND_EGAMES_BET and _analysis_aggregate_hit_alert(
            _analysis_for_egames_bet_payload(p2)
        ):
            return True
        if logical == MONITORING_EXTRA_KIND_LIVESLOT_BET and _analysis_aggregate_hit_alert(
            _analysis_for_liveslot_bet_payload(p2)
        ):
            return True
        if logical == MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT and _analysis_aggregate_hit_alert(
            _analysis_for_liveslot_spin_count_payload(p2)
        ):
            return True
    return False


def _fmt_ts_short(ts: Any) -> str:
    try:
        ft = _bucket_ts_monitoring_minute(float(ts))
        return _monitoring_calendar_dt(ft).strftime("%m-%d %H:%M")
    except (TypeError, ValueError, OSError):
        return str(ts)


def _fmt_num(v: Any) -> str:
    try:
        f = float(v)
        if abs(f - round(f)) < 1e-6:
            return f"{int(round(f)):,}"
        return f"{f:,.2f}"
    except (TypeError, ValueError):
        return str(v)


def _format_http_analysis_lines(
    analysis: Dict[str, Any], *, section_label: Optional[str] = None
) -> List[str]:
    """
    Compact footer: max drop/spike from best consecutive monotonic run (first→last bucket %).
    Threshold line matches product copy; @mention is still driven by ``hit_alert`` (mean windows).
    """
    sec = (section_label or GRAFANA_PANEL_TITLE or "panel").strip() or "panel"
    fd = float(analysis.get("fast_drop_threshold_pct") or analysis.get("fast_threshold_pct") or 15.0)
    fs = float(analysis.get("fast_spike_threshold_pct") or analysis.get("fast_threshold_pct") or 15.0)
    cont_thr = float(
        analysis.get("continuous_threshold_pct") or MONITORING_GAME_ALERT_CONTINUOUS_PCT
    )
    win_sec = int(analysis.get("window_seconds") or MONITORING_ALERT_WINDOW_SECONDS)
    win_m = max(1, win_sec // 60)
    if math.isfinite(fs) and abs(fd - fs) < 1e-9:
        rule_fast = f"drop/spike > {fd:g}% within {win_m}m"
    elif math.isfinite(fs):
        rule_fast = f"drop > {fd:g}% or spike > {fs:g}% within {win_m}m"
    else:
        rule_fast = f"drop > {fd:g}% within {win_m}m (spike fast off)"
    lines: List[str] = [
        "",
        f"[{sec}] alert when {rule_fast} or continuous drop/spike > {cont_thr:g}%",
    ]

    wd = analysis.get("window_max_drop")
    ws = analysis.get("window_max_spike")
    cd = analysis.get("consecutive_max_drop")
    cs = analysis.get("consecutive_max_spike")
    lines.append(
        f"within {win_sec//60}m drop/spike: -{(wd or {}).get('pct', 'n/a')}% / +{(ws or {}).get('pct', 'n/a')}%"
    )
    lines.append(
        f"continuous drop/spike : -{(cd or {}).get('pct', 'n/a')}% / +{(cs or {}).get('pct', 'n/a')}%"
    )

    return lines


def _format_monitoring_reply(payload: Dict[str, Any], *, include_target_mention: bool = True) -> str:
    """
    Lark-friendly compact layout: ``[panel] graph`` + short ``Dashboard: …/d/{uid}`` + panel tables + rules.

    When the caller prepends ``_format_alert_trigger_reply`` (already contains ``<at>``), pass
    ``include_target_mention=False`` to avoid duplicate mentions.
    """
    max_rows = MONITORING_TABLE_TAIL_ROWS
    uid = str(payload.get("dashboardUid") or GRAFANA_DASHBOARD_UID)
    base = str(GRAFANA_BASE_URL).rstrip("/")
    http_ex = _http_analysis_for_payload(payload) if MONITORING_HTTP_PRIMARY_ENABLE else {}

    lines: List[str] = [
        f"[{payload.get('panelTitle')}] graph",
        f"Dashboard: {base}/d/{uid}",
    ]

    def append_analyzed_panel(title: str, series_keyword: str, a2: Dict[str, Any]) -> None:
        per_rows = a2.get("per_series")
        if isinstance(per_rows, list) and per_rows:
            for sub in per_rows:
                if not isinstance(sub, dict):
                    continue
                series_disp = str(sub.get("series_label") or "series")
                pts2 = sub.get("merged_points") or []
                lines.append("")
                lines.append(f"[{title}] series: {series_disp}")
                if pts2:
                    tail2 = pts2[-max_rows:]
                    rows2 = ["time           value"]
                    for pair in tail2:
                        rows2.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
                    lines.append("```text")
                    lines.extend(rows2)
                    lines.append("```")
                else:
                    lines.append(f"(no points matched for {series_disp})")
                lines.extend(_format_extra_analysis_lines(title, sub))
            return
        pts2 = a2.get("merged_points") or []
        lines.append("")
        series_disp = (series_keyword or "").strip() or "all series merged"
        lines.append(f"[{title}] series: {series_disp}")
        if pts2:
            tail2 = pts2[-max_rows:]
            rows2 = ["time           value"]
            for pair in tail2:
                rows2.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
            lines.append("```text")
            lines.extend(rows2)
            lines.append("```")
        else:
            lines.append(f"(no points matched for {series_disp})")
        lines.extend(_format_extra_analysis_lines(title, a2))

    if MONITORING_HTTP_PRIMARY_ENABLE:
        append_analyzed_panel(GRAFANA_PANEL_TITLE, "", http_ex)

    for ex in payload.get("extraPanels") or []:
        if not isinstance(ex, dict):
            continue
        k = (ex.get("kind") or "")
        p2 = ex.get("payload") if isinstance(ex.get("payload"), dict) else {}
        logical = _extra_panel_logical_kind(k)
        if logical == MONITORING_EXTRA_KIND_EGAME_ONLINE:
            append_analyzed_panel(
                GRAFANA_PANEL_TITLE_EGAME_ONLINE,
                MONITORING_EGAME_ONLINE_SERIES_KEYWORD,
                _analysis_for_egame_online_payload(p2),
            )
        elif logical == MONITORING_EXTRA_KIND_EGAMES_BET:
            append_analyzed_panel(
                GRAFANA_PANEL_TITLE_EGAMES_BET,
                MONITORING_EGAMES_BET_SERIES_INCLUDE or MONITORING_EGAMES_BET_SERIES_KEYWORD,
                _analysis_for_egames_bet_payload(p2),
            )
        elif logical == MONITORING_EXTRA_KIND_LIVESLOT_BET:
            append_analyzed_panel(
                GRAFANA_PANEL_TITLE_LIVESLOT_BET,
                "",
                _analysis_for_liveslot_bet_payload(p2),
            )
        elif logical == MONITORING_EXTRA_KIND_LIVESLOT_SPIN_COUNT:
            append_analyzed_panel(
                GRAFANA_PANEL_TITLE_LIVESLOT_SPIN_BET,
                MONITORING_LIVESLOT_SPIN_COUNT_SERIES_INCLUDE,
                _analysis_for_liveslot_spin_count_payload(p2),
            )

    if include_target_mention and _monitoring_payload_hit_alert(payload):
        _append_monitoring_alert_target_user_mention(lines)

    return "\n".join(lines)


def _lark_verify_event_token(data: Dict[str, Any]) -> bool:
    """True when ``_lark_extract_verification_token`` matches ``VERIFICATION_TOKEN`` (Chatbox pattern)."""
    if not VERIFICATION_TOKEN:
        return True
    et = _lark_header_event_type(data)
    if isinstance(et, str) and et.startswith("card.action"):
        raw_tok = data.get("token")
        if _lark_looks_like_lark_card_update_credential(raw_tok):
            # Card callback webhooks may use c-/d- credential token instead of app verification token.
            return True
    tok = _lark_extract_verification_token(data)
    return tok == VERIFICATION_TOKEN


def _lark_card_action_value(data: Dict[str, Any]) -> Dict[str, Any]:
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    act = ev.get("action")
    if isinstance(act, dict):
        val = act.get("value")
        if isinstance(val, dict):
            return val
    val2 = ev.get("value")
    if isinstance(val2, dict):
        return val2
    return {}


def _lark_card_action_target_ids(data: Dict[str, Any]) -> Tuple[str, str]:
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    chat_id = _lark_dict_pick_str(ev, "open_chat_id", "openChatId", "chat_id", "chatId")
    op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
    op_id = op.get("operator_id") if isinstance(op.get("operator_id"), dict) else {}
    open_id = _lark_dict_pick_str(op_id, "open_id", "openId", "user_id", "userId")
    if not open_id:
        open_id = _lark_dict_pick_str(op, "open_id", "openId", "user_id", "userId")
    return chat_id, open_id


def _monitoring_send_screenshot_on_card_click(chat_id: str, open_id: str) -> None:
    try:
        if not _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"):
            raise RuntimeError("GRAFANA_SCREENSHOT_ENABLE=0")
        sess = grafana_login_session()
        payload = fetch_monitoring_payload(session=sess)
        alert_hit = _monitoring_payload_hit_alert(payload)
        png = _grafana_monitoring_screenshot_png(sess, payload, for_alert=alert_hit)
        key = _lark_upload_png_image_key(png)
        if (chat_id or "").strip():
            _lark_send_image_message("chat_id", chat_id.strip(), key)
        elif (open_id or "").strip():
            _lark_send_image_message("open_id", open_id.strip(), key)
        else:
            raise RuntimeError("missing chat_id/open_id")
        logger.info(
            "monitoring card-action screenshot sent chat=%r open=%r bytes=%s",
            bool(chat_id),
            bool(open_id),
            len(png),
        )
    except Exception as e:
        logger.exception("monitoring card-action screenshot failed")
        msg = f"Screenshot refresh failed: {e}"
        try:
            if (chat_id or "").strip():
                _lark_send_text("chat_id", chat_id.strip(), msg)
            elif (open_id or "").strip():
                _lark_send_text("open_id", open_id.strip(), msg)
        except Exception:
            logger.exception("monitoring card-action error text send failed")


def _handle_monitoring_card_action(data: Dict[str, Any]) -> None:
    val = _lark_card_action_value(data)
    k = _lark_dict_pick_str(val, "k")
    v = _lark_dict_pick_str(val, "v")
    if not (k == "monitoring_btn" and v == "refresh"):
        return
    ev_id = _lark_im_payload_event_id(data)
    with _monitoring_reply_dispatch_lock:
        if ev_id and ev_id in _monitoring_card_action_event_ids:
            logger.info("duplicate card.action event_id=%r — skip", ev_id)
            return
        if ev_id:
            _monitoring_card_action_event_ids.add(ev_id)
            if len(_monitoring_card_action_event_ids) > 2000:
                _monitoring_card_action_event_ids.clear()
                _monitoring_card_action_event_ids.add(ev_id)
    chat_id, open_id = _lark_card_action_target_ids(data)
    # Prefer original card target from callback payload so group-card clicks reply in the same group
    # instead of falling back to operator open_id (private message).
    rid_t = _lark_dict_pick_str(val, "rid_t", "receive_id_type")
    rid = _lark_dict_pick_str(val, "rid", "receive_id")
    if rid_t == "chat_id" and rid:
        chat_id = rid
        open_id = ""
    elif rid_t == "open_id" and rid:
        open_id = rid
    logger.info("card.action refresh accepted chat=%r open=%r event_id=%r", bool(chat_id), bool(open_id), ev_id or None)
    threading.Thread(
        target=_monitoring_send_screenshot_on_card_click,
        args=(chat_id, open_id),
        daemon=True,
        name="monitoring-card-action",
    ).start()


def _monitoring_ai_extract_ollama_text(data: Any) -> str:
    """Collect assistant text from Ollama ``/api/chat`` (content + thinking fallbacks)."""
    chunks: List[str] = []
    if isinstance(data, dict):
        msg = data.get("message")
        if isinstance(msg, dict):
            for key in ("content", "thinking"):
                part = str(msg.get(key) or "").strip()
                if part:
                    chunks.append(part)
        resp = str(data.get("response") or "").strip()
        if resp:
            chunks.append(resp)
    return "\n\n".join(chunks).strip()


def _monitoring_ai_strip_model_reasoning(text: str) -> str:
    """Remove hidden reasoning blocks; keep the user-visible verdict + explanation."""
    out = (text or "").strip()
    for pat in (
        r"(?is)``.*?``",
        r"(?is)<think>.*?</think>",
        r"(?is)<thinking>.*?</thinking>",
    ):
        out = re.sub(pat, "", out)
    return out.strip()


def _monitoring_ai_parse_verdict(raw: str) -> Tuple[Optional[bool], str]:
    """
    Parse ``ABNORMAL`` / ``NORMAL`` and return ``(verdict, explanation_body)``.
    Explanation excludes the verdict line itself.
    """
    cleaned = _monitoring_ai_strip_model_reasoning(raw)
    if not cleaned:
        return None, ""

    lines = cleaned.splitlines()
    verdict: Optional[bool] = None
    verdict_idx = -1
    for i, line in enumerate(lines):
        u = line.strip().upper()
        if u == "ABNORMAL" or u.startswith("ABNORMAL"):
            verdict = True
            verdict_idx = i
            break
        if u == "NORMAL" or u.startswith("NORMAL"):
            verdict = False
            verdict_idx = i
            break

    if verdict is None:
        upper = cleaned.upper()
        if "ABNORMAL" in upper:
            verdict = True
        elif "NORMAL" in upper:
            verdict = False
        else:
            return None, cleaned

    explain_lines: List[str] = []
    if verdict_idx >= 0:
        for line in lines[verdict_idx + 1 :]:
            st = line.strip()
            if not st:
                continue
            if st.upper() in ("ABNORMAL", "NORMAL"):
                continue
            explain_lines.append(line.rstrip())
    explanation = "\n".join(explain_lines).strip()
    if verdict and not explanation:
        # Model returned only the verdict token — still show a visible AI block.
        explanation = "🤖 AI Assessment: ABNORMAL\n(模型未返回详细说明 / model returned no detail.)"
    return verdict, explanation


def _monitoring_ai_abnormal_verdict(
    png_bytes: bytes, alert_text: str
) -> Tuple[Optional[bool], str]:
    """
    Ask the local Ollama model whether a Grafana alert screenshot shows a genuine
    abnormality worth paging the group.

    Returns ``(is_abnormal, explanation)``. ``is_abnormal`` is ``None`` when the AI
    could not be reached / its answer could not be parsed (caller decides fail-open).
    """
    import base64

    url = _cfg_str("MONITORING_AI_OLLAMA_URL", "http://localhost:11434").strip().rstrip("/")
    model = _cfg_str("MONITORING_AI_MODEL", "qwen3.6:35b-a3b").strip()
    timeout = max(5.0, _cfg_float("MONITORING_AI_TIMEOUT_SECONDS", 120.0))
    prompt = _cfg_str("MONITORING_AI_PROMPT", "").strip() or (
        "You are an SRE assistant reviewing a Grafana monitoring screenshot. "
        "A threshold rule already detected this change:\n"
        "-----\n"
        "{alert}\n"
        "-----\n"
        "IMPORTANT rules:\n"
        "1) Base your decision ONLY on the alert lines above — do NOT invent numbers or "
        "times that are not in that text.\n"
        "2) If the listed changes are tiny (mostly single-digit or low-teens errors/min, "
        "e.g. 10→11, 2→1, 14→10), respond NORMAL — that is routine noise.\n"
        "3) Respond ABNORMAL only for a genuine incident worth paging on-call (large spike, "
        "sustained outage, or clearly abnormal pattern in the alert lines).\n"
        "4) **Main Site Deposit (createProposal)** and **Withdrawal (InitiateWithdrawal)** "
        "are naturally jagged every minute. A single-minute move similar to others on the "
        "chart (e.g. deposit 191→242, withdraw 53→32) is NORMAL business volatility — "
        "respond NORMAL unless the chart shows a sustained flatline, zero traffic, or an "
        "extreme outlier far outside the usual band.\n"
        "5) Your explanation must reference the specific series and values from the alert "
        "text, not unrelated peaks elsewhere on the chart.\n"
        "Reply with the FIRST line being exactly 'ABNORMAL' or 'NORMAL', then on the "
        "following lines a short explanation (1-3 sentences) in 中文 and English."
    )
    prompt = prompt.replace("{alert}", alert_text or "")
    b64 = base64.b64encode(png_bytes).decode("ascii")
    body: Dict[str, Any] = {
        "model": model,
        "stream": False,
        "messages": [{"role": "user", "content": prompt, "images": [b64]}],
        "options": {"temperature": 0},
    }
    # Qwen3 reasoning models: prefer direct answer in ``content`` (not only ``thinking``).
    if "qwen3" in model.casefold():
        body["think"] = False
    try:
        r = requests.post(f"{url}/api/chat", json=body, timeout=timeout)
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.exception(
            "monitoring AI gate: Ollama request failed (model=%s url=%s)", model, url
        )
        return None, ""

    raw = _monitoring_ai_extract_ollama_text(data)
    if not raw:
        logger.warning("monitoring AI gate: empty response from model=%s payload=%r", model, data)
        return None, ""

    verdict, explanation = _monitoring_ai_parse_verdict(raw)
    if verdict is None:
        logger.warning(
            "monitoring AI gate: could not parse verdict from response=%r",
            raw[:300],
        )
    else:
        logger.info(
            "monitoring AI gate: parsed verdict=%s explanation_len=%s",
            "ABNORMAL" if verdict else "NORMAL",
            len(explanation or ""),
        )
    return verdict, explanation


def _monitoring_ai_parse_alert_move(line: str) -> Tuple[Optional[float], Optional[float], str]:
    """Parse ``value of X increased/decreased to … value of Y`` from one alert block line."""
    m = re.search(
        r"value of ([\d,]+(?:\.\d+)?)\s+(increased|decreased)\s+to"
        r"(?:\s+\d{2}-\d{2}\s+\d{2}:\d{2}\s+value of)?\s+([\d,]+(?:\.\d+)?)",
        line,
        flags=re.IGNORECASE,
    )
    if not m:
        return None, None, ""
    try:
        fv = float(m.group(1).replace(",", ""))
        tv = float(m.group(3).replace(",", ""))
    except (TypeError, ValueError):
        return None, None, ""
    direction = "SPIKE" if m.group(2).casefold() == "increased" else "DROP"
    return fv, tv, direction


def _monitoring_ai_alert_block_pairs(alert_text: str) -> List[Tuple[str, str]]:
    """Each block: ``(emoji header line, following value/time line)``."""
    pairs: List[Tuple[str, str]] = []
    lines = [ln.rstrip() for ln in (alert_text or "").splitlines()]
    i = 0
    while i < len(lines):
        if lines[i].startswith(("📈", "📉")):
            detail = ""
            if i + 1 < len(lines):
                nxt = lines[i + 1].strip()
                if nxt and not nxt.startswith(("📈", "📉", "[", "🤖")):
                    detail = nxt
            pairs.append((lines[i], detail))
            i += 2
            continue
        i += 1
    return pairs


def _monitoring_ai_deposit_withdraw_routine_volatility(alert_text: str) -> Optional[str]:
    """
    Deposit/withdraw panels are naturally jagged. Single-minute moves like 191→242 or
    53→32 are routine — suppress before calling the vision model.
    """
    text = alert_text or ""
    pairs = _monitoring_ai_alert_block_pairs(text)
    if not pairs:
        return None
    other_panels = (
        "请求总数",
        "错误请求数",
        "9280",
        "IGO Distributions",
        "FPMS-NT",
        "Providers",
        "Games",
    )
    if any(tok in text for tok in other_panels):
        return None
    for header, detail in pairs:
        fv, tv, direction = _monitoring_ai_parse_alert_move(detail)
        if fv is None or tv is None:
            return None
        peak = max(fv, tv)
        delta = abs(tv - fv)
        if "主站充值" in header or "createProposal" in header:
            if direction != "SPIKE":
                return None
            if peak > 450 or peak < 80 or delta > 80:
                return None
        elif "提款" in header or "InitiateWithdrawal" in header:
            if direction != "DROP":
                return None
            if fv > 150 or fv < 20 or delta > 40:
                return None
        else:
            return None
    return (
        "🤖 AI Assessment: NORMAL\n"
        "主站充值/提款分钟级锯齿波动属于正常业务节奏，非持续故障。\n"
        "Main Site Deposit / Withdrawal minute-level jitter is normal business volatility, "
        "not a sustained outage."
    )


def _monitoring_ai_fail_open_note() -> str:
    return _cfg_str(
        "MONITORING_AI_FAIL_OPEN_NOTE",
        "🤖 AI review unavailable — alert sent without AI explanation.",
    ).strip()


def _monitoring_ai_gate_decide(alert_pngs: List[bytes], reply: str) -> Tuple[bool, str]:
    """
    Second gate after threshold detection: only let the alert through if the AI
    judges the screenshot abnormal. Returns ``(should_send, reply)`` where ``reply``
    has the AI explanation appended when the alert is allowed through.
    """
    if not _lark_env_truthy_or_default("MONITORING_AI_GATE_ENABLE", default=True):
        return True, reply
    fail_open = _lark_env_truthy_or_default("MONITORING_AI_GATE_FAIL_OPEN", default=True)
    fail_note = _monitoring_ai_fail_open_note()
    if not alert_pngs:
        logger.warning(
            "monitoring AI gate: no screenshot available — %s",
            "sending anyway (fail-open)" if fail_open else "suppressing (fail-closed)",
        )
        if fail_open and fail_note:
            reply = f"{reply}\n\n{fail_note}"
        return fail_open, reply
    routine = _monitoring_ai_deposit_withdraw_routine_volatility(reply)
    if routine:
        logger.info(
            "monitoring AI gate: deposit/withdraw routine volatility — alert suppressed"
        )
        return False, reply
    verdict, explanation = _monitoring_ai_abnormal_verdict(alert_pngs[0], reply)
    if verdict is None:
        logger.warning(
            "monitoring AI gate: undecided verdict — %s",
            "sending anyway (fail-open)" if fail_open else "suppressing (fail-closed)",
        )
        if fail_open and fail_note:
            reply = f"{reply}\n\n{fail_note}"
        return fail_open, reply
    if not verdict:
        logger.info(
            "monitoring AI gate: AI judged NORMAL — alert suppressed. explanation=%r",
            (explanation or "")[:300],
        )
        return False, reply
    logger.info("monitoring AI gate: AI judged ABNORMAL — alert will be sent")
    if explanation:
        reply = f"{reply}\n\n{explanation}"
    return True, reply


# ---------------------------------------------------------------------------
# p0bot — Lark wiki doc Q&A (local Ollama / Qwen)
#
# Reads a Lark wiki page (optionally its whole subtree), caches the plain text,
# and answers questions by passing that text to the local Ollama model as
# grounding context. Independent of the Grafana monitoring paths above.
# ---------------------------------------------------------------------------

_p0_doc_lock = threading.Lock()
_p0_doc_text: str = ""
_p0_doc_fetched_at: float = 0.0
_p0_doc_meta: str = ""

_p0_contacts_lock = threading.Lock()
_p0_contacts_entries_cache: List[Tuple[str, str, str]] = []  # [(name, team, phone), ...]
_p0_contacts_text_cache: str = ""
_p0_contacts_mtime: float = -1.0


def _p0_qa_enabled() -> bool:
    return _lark_env_truthy("P0_DOC_QA_ENABLE")


def _p0_contacts_file() -> str:
    """Path to the local contacts CSV (name,team,phone). Defaults to contacts.csv beside main.py."""
    p = _cfg_str("P0_CONTACTS_FILE", "").strip()
    if p:
        return p
    try:
        base = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        base = "."
    return os.path.join(base, "contacts.csv")


def _p0_contacts_load() -> Tuple[List[Tuple[str, str, str]], str]:
    """(entries, formatted_block) from contacts.csv, reloaded when the file changes.

    Returns ([], "") when disabled (P0_CONTACTS_ENABLE=0) or the file is absent/empty. The block
    is folded into the Q&A context; the entries also drive contact-lookup on answers.
    """
    global _p0_contacts_entries_cache, _p0_contacts_text_cache, _p0_contacts_mtime
    if not _lark_env_truthy_or_default("P0_CONTACTS_ENABLE", default=True):
        return [], ""
    path = _p0_contacts_file()
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return [], ""
    with _p0_contacts_lock:
        if _p0_contacts_text_cache and mtime == _p0_contacts_mtime:
            return _p0_contacts_entries_cache, _p0_contacts_text_cache
    entries: List[Tuple[str, str, str]] = []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                name = (row.get("name") or "").strip()
                team = (row.get("team") or "").strip()
                phone = (row.get("phone") or "").strip()
                if not name and not phone:
                    continue
                entries.append((name, team, phone))
    except Exception:
        logger.exception("p0 contacts load failed: %s", path)
        return [], ""
    if not entries:
        return [], ""
    block = (
        "CONTACT DIRECTORY — team members and phone numbers (format: name | team | phone). "
        "Use this to answer who to contact for a team, or a person's phone number.\n"
        + "\n".join(f"- {n} | {t or '-'} | {p or '-'}" for n, t, p in entries)
    )
    with _p0_contacts_lock:
        _p0_contacts_entries_cache = entries
        _p0_contacts_text_cache = block
        _p0_contacts_mtime = mtime
    logger.info("p0 contacts loaded: %d entries from %s", len(entries), path)
    return entries, block


def _p0_contacts_text() -> str:
    return _p0_contacts_load()[1]


def _p0_contacts_lookup(text: str, limit: int = 12) -> List[Tuple[str, str, str]]:
    """Directory people whose name appears as a whole word/phrase in ``text``.

    Longer names claim their span first, so a full name ("Kelvin Er") doesn't also drag in a
    shorter same-token contact ("Kelvin"). Single-token names under 4 chars are skipped to avoid
    false positives on ordinary words/acronyms (Bk, YC, AD, Don, Net, …).
    """
    entries, _ = _p0_contacts_load()
    work = text or ""
    if not entries or not work.strip():
        return []
    matched: List[Tuple[str, str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for name, team, phone in sorted(entries, key=lambda e: len(e[0]), reverse=True):
        if not name or (" " not in name and len(name) < 4):
            continue
        pat = r"(?<!\w)" + re.escape(name) + r"(?!\w)"
        if re.search(pat, work, flags=re.IGNORECASE):
            key = (name.lower(), phone)
            if key not in seen:
                seen.add(key)
                matched.append((name, team, phone))
            # blank out the matched span so shorter overlapping names don't re-match it
            work = re.sub(pat, lambda m: " " * len(m.group(0)), work, flags=re.IGNORECASE)
            if len(matched) >= limit:
                break
    return matched


def _p0_augment_answer_with_contacts(answer: str) -> str:
    """Append phone numbers for any directory people named in the answer (P0_CONTACTS_APPEND_ENABLE)."""
    if not _lark_env_truthy_or_default("P0_CONTACTS_APPEND_ENABLE", default=True):
        return answer
    try:
        hits = _p0_contacts_lookup(answer)
    except Exception:
        logger.exception("p0 contacts answer-lookup failed")
        return answer
    if not hits:
        return answer
    lines = [f"- **{n}** · {t or '-'} · `{p or '-'}`" for n, t, p in hits]
    return answer.rstrip() + "\n\n**📇 相关联系人 / Contacts**\n" + "\n".join(lines)


def _p0_wiki_node_token() -> str:
    """Wiki node token from P0_WIKI_NODE_TOKEN, else parsed from P0_WIKI_URL."""
    tok = _cfg_str("P0_WIKI_NODE_TOKEN", "").strip()
    if tok:
        return tok
    url = _cfg_str("P0_WIKI_URL", "").strip()
    if url:
        m = re.search(r"/wiki(?:/[a-z]{2}-[A-Z]{2})?/([A-Za-z0-9]+)", url)
        if m:
            return m.group(1)
    return ""


def _p0_qa_model() -> str:
    return (
        _cfg_str("P0_QA_MODEL", "").strip()
        or _cfg_str("MONITORING_AI_MODEL", "qwen3.6:35b-a3b").strip()
    )


def _p0_qa_ollama_url() -> str:
    return (
        (
            _cfg_str("P0_QA_OLLAMA_URL", "").strip()
            or _cfg_str("MONITORING_AI_OLLAMA_URL", "http://localhost:11434").strip()
        )
    ).rstrip("/")


def _p0_lark_get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET a Lark Open API endpoint with the tenant token; return parsed JSON."""
    tok = _lark_tenant_access_token_string()
    url = f"{_lark_api_domain()}{path}"
    r = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json; charset=utf-8",
        },
        params=params or {},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _p0_lark_add_reaction(message_id: str, emoji_type: str) -> Optional[str]:
    """Add an emoji reaction to a message; return its reaction_id (best-effort, never raises)."""
    mid = (message_id or "").strip()
    emoji = (emoji_type or "").strip()
    if not mid or not emoji:
        return None
    try:
        tok = _lark_tenant_access_token_string()
        url = f"{_lark_api_domain()}/open-apis/im/v1/messages/{mid}/reactions"
        r = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"reaction_type": {"emoji_type": emoji}},
            timeout=15,
        )
        j = r.json()
        if int(j.get("code", -1)) != 0:
            logger.info("p0 reaction add failed emoji=%s: %s", emoji, j)
            return None
        return _lark_dict_pick_str(j.get("data") or {}, "reaction_id", "reactionId") or None
    except Exception:
        logger.exception("p0 reaction add error (emoji=%s)", emoji)
        return None


def _p0_lark_remove_reaction(message_id: str, reaction_id: str) -> None:
    """Remove a previously-added reaction (best-effort, never raises)."""
    mid = (message_id or "").strip()
    rid = (reaction_id or "").strip()
    if not mid or not rid:
        return
    try:
        tok = _lark_tenant_access_token_string()
        url = f"{_lark_api_domain()}/open-apis/im/v1/messages/{mid}/reactions/{rid}"
        requests.delete(url, headers={"Authorization": f"Bearer {tok}"}, timeout=15)
    except Exception:
        logger.exception("p0 reaction remove error")


def _p0_wiki_get_node(node_token: str) -> Dict[str, Any]:
    """``wiki/v2/spaces/get_node`` → node dict (obj_type/obj_token/space_id/title/has_child)."""
    j = _p0_lark_get_json("/open-apis/wiki/v2/spaces/get_node", {"token": node_token})
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"wiki get_node failed: {j}")
    node = (j.get("data") or {}).get("node") or {}
    return node if isinstance(node, dict) else {}


def _p0_wiki_list_children(space_id: str, parent_node_token: str) -> List[Dict[str, Any]]:
    """List immediate child nodes of a wiki node (paginated)."""
    out: List[Dict[str, Any]] = []
    page_token = ""
    for _ in range(50):  # hard cap on pages
        params: Dict[str, Any] = {"parent_node_token": parent_node_token, "page_size": 50}
        if page_token:
            params["page_token"] = page_token
        j = _p0_lark_get_json(f"/open-apis/wiki/v2/spaces/{space_id}/nodes", params)
        if int(j.get("code", -1)) != 0:
            logger.warning(
                "p0 wiki list children failed space=%s parent=%s: %s",
                space_id,
                parent_node_token[:12],
                j,
            )
            break
        data = j.get("data") or {}
        items = data.get("items") or []
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    out.append(it)
        if not data.get("has_more"):
            break
        page_token = str(data.get("page_token") or "")
        if not page_token:
            break
    return out


def _p0_docx_raw_content(document_id: str) -> str:
    """Plain-text body of a docx document (``docx/v1/documents/{id}/raw_content``)."""
    j = _p0_lark_get_json(
        f"/open-apis/docx/v1/documents/{document_id}/raw_content", {"lang": 0}
    )
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"docx raw_content failed: {j}")
    return str((j.get("data") or {}).get("content") or "")


def _p0_node_plain_text(node: Dict[str, Any]) -> str:
    """Plain text for a wiki node's underlying object (docx; other types are skipped)."""
    obj_type = _lark_dict_pick_str(node, "obj_type", "objType").lower()
    obj_token = _lark_dict_pick_str(node, "obj_token", "objToken")
    if not obj_token:
        return ""
    if obj_type in ("docx", "doc"):
        try:
            return _p0_docx_raw_content(obj_token)
        except Exception:
            logger.exception(
                "p0 docx fetch failed obj_type=%s token=%s", obj_type, obj_token[:12]
            )
            return ""
    logger.info("p0 wiki node obj_type=%r not text-extractable — skipped", obj_type)
    return ""


def _p0_fetch_wiki_doc_text() -> Tuple[str, str]:
    """Fetch the configured wiki node (and, if enabled, its descendants). Returns (text, meta)."""
    node_token = _p0_wiki_node_token()
    if not node_token:
        raise RuntimeError("P0_WIKI_NODE_TOKEN / P0_WIKI_URL not configured")
    root = _p0_wiki_get_node(node_token)
    if not root:
        raise RuntimeError("wiki get_node returned no node (check wiki scopes + doc sharing)")
    space_id = _lark_dict_pick_str(root, "space_id", "spaceId")
    include_children = _lark_env_truthy_or_default("P0_WIKI_INCLUDE_CHILDREN", default=True)
    max_nodes = max(1, _cfg_int("P0_WIKI_MAX_NODES", 200))

    sections: List[str] = []
    counter = {"n": 0}
    seen_tokens: Set[str] = set()

    def _emit(node: Dict[str, Any], depth: int) -> None:
        if counter["n"] >= max_nodes:
            return
        tok = _lark_dict_pick_str(node, "node_token", "nodeToken")
        if tok and tok in seen_tokens:
            return
        if tok:
            seen_tokens.add(tok)
        title = _lark_dict_pick_str(node, "title") or "(untitled)"
        text = _p0_node_plain_text(node)
        counter["n"] += 1
        header = ("#" * min(6, depth + 1)) + " " + title
        sections.append(f"{header}\n{text}".strip())
        if not include_children:
            return
        has_child = bool(
            node.get("has_child") if "has_child" in node else node.get("hasChild")
        )
        if space_id and tok and has_child and counter["n"] < max_nodes:
            for child in _p0_wiki_list_children(space_id, tok):
                if counter["n"] >= max_nodes:
                    break
                _emit(child, depth + 1)

    _emit(root, 0)
    text = "\n\n".join(s for s in sections if s).strip()
    meta = f"nodes={counter['n']} chars={len(text)} space={space_id or '?'}"
    return text, meta


def _p0_doc_reload() -> Tuple[bool, str]:
    """(Re)fetch the wiki doc into the cache. Returns (ok, meta_or_error)."""
    global _p0_doc_text, _p0_doc_fetched_at, _p0_doc_meta
    try:
        text, meta = _p0_fetch_wiki_doc_text()
    except Exception as e:
        logger.exception("p0 doc reload failed")
        return False, str(e)
    with _p0_doc_lock:
        _p0_doc_text = text
        _p0_doc_fetched_at = time.time()
        _p0_doc_meta = meta
    logger.info("p0 doc loaded: %s", meta)
    return True, meta


def _p0_doc_get_cached() -> str:
    """Cached doc text; fetch once on first use if the cache is empty."""
    with _p0_doc_lock:
        if _p0_doc_text:
            return _p0_doc_text
    _p0_doc_reload()
    with _p0_doc_lock:
        return _p0_doc_text


def _p0_select_doc_context(doc_text: str, question: str, max_chars: int) -> str:
    """Whole doc when it fits, else the paragraphs most relevant to the question (doc order preserved)."""
    if len(doc_text) <= max_chars:
        return doc_text
    q_words = {
        w
        for w in re.findall(r"[\w一-鿿]+", (question or "").lower())
        if len(w) >= 2
    }
    paras = [p for p in re.split(r"\n\s*\n", doc_text) if p.strip()]
    scored: List[Tuple[int, int, str]] = []
    for idx, p in enumerate(paras):
        pl = p.lower()
        score = sum(1 for w in q_words if w in pl)
        scored.append((score, idx, p))
    scored.sort(key=lambda t: (-t[0], t[1]))
    note = (
        "（文档较长，仅提供与问题最相关的片段 / doc is long — showing the most relevant "
        "sections only）\n\n"
    )
    budget = max(1000, max_chars - len(note))
    chosen: List[Tuple[int, str]] = []
    used = 0
    for _score, idx, p in scored:
        if used + len(p) + 2 > budget:
            continue
        chosen.append((idx, p))
        used += len(p) + 2
        if used >= budget:
            break
    if not chosen:
        return note + doc_text[:budget]
    chosen.sort(key=lambda t: t[0])
    return note + "\n\n".join(p for _, p in chosen)


def _p0_clean_answer(text: str) -> str:
    """Strip qwen3 reasoning blocks; keep code/commands (unlike the monitoring-gate cleaner)."""
    out = (text or "").strip()
    for pat in (r"(?is)<think>.*?</think>", r"(?is)<thinking>.*?</thinking>"):
        out = re.sub(pat, "", out)
    return out.strip()


def _p0_ai_answer(question: str, doc_text: str) -> str:
    """Ask the local Ollama model the question, grounded on the documentation text."""
    url = _p0_qa_ollama_url()
    model = _p0_qa_model()
    timeout = max(10.0, _cfg_float("P0_QA_TIMEOUT_SECONDS", 180.0))
    num_ctx = max(2048, _cfg_int("P0_QA_NUM_CTX", 32768))
    temp = _cfg_float("P0_QA_TEMPERATURE", 0.2)
    max_chars = max(1000, _cfg_int("P0_DOC_MAX_CHARS", 24000))
    context = _p0_select_doc_context(doc_text, question, max_chars)
    # Always fold in the local contact directory (on top of the wiki budget) so contact
    # questions are answerable regardless of what the wiki contains or how it's truncated.
    contacts = _p0_contacts_text()
    if contacts:
        context = (
            f"----- CONTACTS -----\n{contacts}\n----- END CONTACTS -----\n\n"
            + (context or "(no wiki content)")
        )
    system = _cfg_str("P0_QA_SYSTEM_PROMPT", "").strip() or (
        "You are p0bot, a helpful assistant that answers questions using ONLY the "
        "documentation provided below. Rules:\n"
        "1) Base every answer strictly on the documentation. If the answer is not in "
        "the docs, say you could not find it in the documentation.\n"
        "2) Be concise and specific; quote exact steps, names, values or commands when relevant.\n"
        "3) Reply in the same language the user asked in (中文提问用中文回答, English → English).\n"
        "----- DOCUMENTATION -----\n"
        "{doc}\n"
        "----- END DOCUMENTATION -----"
    )
    if "{doc}" in system:
        system = system.replace("{doc}", context)
    else:
        system = (
            f"{system}\n\n----- DOCUMENTATION -----\n{context}\n----- END DOCUMENTATION -----"
        )
    body: Dict[str, Any] = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": (question or "").strip()},
        ],
        "options": {"temperature": temp, "num_ctx": num_ctx},
    }
    # Q&A never needs the model's chain-of-thought; disable it for every model so reasoning
    # text is never surfaced to users (Ollama ignores ``think`` for non-thinking models).
    body["think"] = False
    try:
        r = requests.post(f"{url}/api/chat", json=body, timeout=timeout)
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.exception("p0 QA Ollama request failed (model=%s url=%s)", model, url)
        return (
            "⚠️ 抱歉，本地 AI 模型暂时无法访问。/ Sorry — the local AI model is unreachable right now."
        )
    raw = _monitoring_ai_extract_ollama_text(data)
    ans = _p0_clean_answer(raw)
    if not ans:
        logger.warning("p0 QA empty answer payload=%r", data)
        return "⚠️ 模型没有返回内容，请重试。/ The model returned no content — please try again."
    return _p0_augment_answer_with_contacts(ans)


def _p0_bot_open_id() -> str:
    """This app's bot open_id: P0_BOT_OPEN_ID or LARK_BOT_OPEN_ID (raw env), else bot/v3/info.

    Deliberately does NOT use ``_lark_effective_bot_open_id()``: on this fork that value is
    force-normalized to the embedded Game bot open_id when unset, which is the wrong identity
    for a standalone p0bot. ``bot/v3/info`` (called with THIS app's creds) resolves p0bot's
    real open_id. Set P0_BOT_OPEN_ID in the env to pin it if bot/v3/info is flaky.
    """
    for key in ("P0_BOT_OPEN_ID", "LARK_BOT_OPEN_ID"):
        v = _cfg_str(key, "").strip()
        if v:
            return v
    try:
        return _lark_resolve_bot_open_id_via_api() or ""
    except Exception:
        return ""


def _p0_mentions_contain_bot(mentions: Any) -> bool:
    """True when the @-mentions include this bot's open_id or APP_ID."""
    oid = _p0_bot_open_id()
    app = str(APP_ID or "").strip()
    targets = {t for t in (oid, app) if t}
    if not targets:
        # Bot identity unknown — don't guess (avoids answering @other-bot). Use /ask instead.
        return False
    for s in _lark_iter_mention_scalar_strings(mentions):
        if s and s.strip() in targets:
            return True
    return False


def _p0_command_body(text_clean: str, trigger: str) -> Optional[str]:
    """If ``text_clean`` is ``<trigger>`` or ``<trigger> body``, return body (possibly ''); else None.

    Any whitespace may separate trigger and body — pasted link cards often put the body on a
    new line (``/p0docs\\n<card>``), which must still trigger."""
    c = (text_clean or "").strip()
    t = (trigger or "").strip()
    if not c or not t:
        return None
    cl = c.lower()
    tl = t.lower()
    if cl == tl:
        return ""
    if cl.startswith(tl) and len(c) > len(t) and c[len(t)].isspace():
        return c[len(t):].strip()
    return None


def _p0_answer_card(question: str, answer_chunk: str, part: int, total: int) -> Dict[str, Any]:
    """Clean schema-2.0 interactive card: header + echoed question + divider + answer (lark markdown)."""
    title = _cfg_str("P0_CARD_TITLE", "📖 p0bot").strip() or "📖 p0bot"
    template = _cfg_str("P0_CARD_TEMPLATE", "blue").strip() or "blue"
    subtitle = "文档解答 · Doc answer" + (f"  ({part}/{total})" if total > 1 else "")
    elements: List[Dict[str, Any]] = []
    q = (question or "").strip()
    if part == 1 and q:
        q_show = q if len(q) <= 300 else (q[:300] + "…")
        elements.append({"tag": "markdown", "content": f"**❓ 问题 / Question**\n{q_show}"})
        elements.append({"tag": "hr"})
    elements.append({"tag": "markdown", "content": answer_chunk})
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": title[:190]},
            "subtitle": {"tag": "plain_text", "content": subtitle[:190]},
        },
        "body": {"elements": elements},
    }


def _p0_send_answer(receive_id_type: str, receive_id: str, question: str, answer: str) -> None:
    """Send the doc answer as an interactive card (falls back to plain text on any failure)."""
    text = (answer or "").strip()
    if not text:
        return
    if not _lark_env_truthy_or_default("P0_ANSWER_CARD", default=True):
        _lark_send_text_auto(receive_id_type, receive_id, text)
        return
    max_card = max(500, _cfg_int("P0_CARD_MAX_CHARS", 8000))
    chunks = _split_text_for_lark(text, max_chars=max_card)
    total = len(chunks)
    for i, ch in enumerate(chunks, 1):
        try:
            _lark_send_interactive_card(
                receive_id_type, receive_id, _p0_answer_card(question, ch, i, total)
            )
        except Exception:
            logger.exception("p0 answer card send failed — falling back to text")
            try:
                _lark_send_text_auto(receive_id_type, receive_id, ch)
            except Exception:
                logger.exception("p0 answer text fallback failed")


def _p0_qa_worker(
    chat_id: str, open_id: str, kind: str, question: str, mid: str, debounce_key: str
) -> None:
    def _target() -> Tuple[str, str]:
        if (chat_id or "").strip():
            return "chat_id", chat_id
        if (open_id or "").strip():
            return "open_id", open_id
        return "", ""

    def _send(text: str) -> None:
        rt, rv = _target()
        if not rt:
            logger.warning("p0 qa: no chat_id/open_id to reply to")
            return
        try:
            _lark_send_text_auto(rt, rv, text)
        except Exception:
            logger.exception("p0 qa reply send failed")

    def _send_answer(q_text: str, answer: str) -> None:
        rt, rv = _target()
        if not rt:
            logger.warning("p0 qa: no chat_id/open_id to reply to")
            return
        _p0_send_answer(rt, rv, q_text, answer)

    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id: Optional[str] = None

    def _ack() -> None:
        nonlocal ack_id
        if react:
            ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK")

    def _done() -> None:
        if not react:
            return
        _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
        if ack_id and _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
            _p0_lark_remove_reaction(mid, ack_id)

    ask_trig = _cfg_str("P0_ASK_TRIGGER", "/ask").strip() or "/ask"
    did_ack = False
    try:
        if kind == "reload":
            _ack()
            did_ack = True
            ok, meta = _p0_doc_reload()
            _send(
                f"✅ 文档已重新读取 / Documentation reloaded.\n{meta}"
                if ok
                else f"⚠️ 重新读取失败 / Reload failed:\n{meta}"
            )
            return

        q = (question or "").strip()
        if not q:
            # Bare hello / empty @-mention — send usage, no reactions.
            _send(
                "👋 我是 p0bot。直接问我关于文档的问题即可"
                f"（群里请 @我 或用 `{ask_trig} 你的问题`）。\n"
                "I'm p0bot — ask me anything about the documentation "
                f"(in groups, @ me or use `{ask_trig} your question`)."
            )
            return

        _ack()
        did_ack = True
        doc = _p0_doc_get_cached()
        if not doc and not _p0_contacts_text():
            _send(
                "⚠️ 我还没能读取到文档。请确认机器人已获授权访问该 wiki"
                "（需要 wiki 与 docx 读取权限，且文档/知识库已共享给本应用），然后发送 /reload 重试。\n"
                "I couldn't read the documentation yet — grant the bot wiki + docx read scopes and "
                "share the page with the app, then send /reload."
            )
            return
        _send_answer(q, _p0_ai_answer(q, doc))
    finally:
        # Mark done for any path that acknowledged (success, no-doc, or error).
        if did_ack:
            try:
                _done()
            except Exception:
                logger.exception("p0 qa done-reaction failed")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_doc_qa(
    *,
    chat_id: str,
    open_id: str,
    raw_text: str,
    clean: str,
    mentions: Any,
    msg: Dict[str, Any],
    im_chat_type: str,
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """Handle a p0 doc Q&A / reload IM. Returns True when handled (skips monitoring)."""
    if not _p0_qa_enabled():
        return False
    ct = (im_chat_type or "").strip().lower()
    is_dm = ct in ("p2p", "private")
    ask_trigger = _cfg_str("P0_ASK_TRIGGER", "/ask").strip() or "/ask"
    reload_trigger = _cfg_str("P0_RELOAD_TRIGGER", "/reload").strip() or "/reload"
    text_clean = (clean or "").strip()

    # Never swallow other commands as doc questions: monitoring (/mo, /m, /c) must fall
    # through to their handlers, and /meeting is handled by _p0_try_handle_meeting.
    is_monitoring_cmd = (
        _im_command_matches(text_clean, MONITORING_TRIGGER)
        or _im_command_matches(text_clean, MONITORING_MUTE_TRIGGER)
        or _im_command_matches(text_clean, MONITORING_CANCELMUTE_TRIGGER)
        or _p0_command_body(text_clean, _p0_meeting_trigger()) is not None
        or _p0_command_body(text_clean, _p0_members_trigger()) is not None
        or _p0_command_body(text_clean, _p0_om_open_trigger()) is not None
        or _p0_command_body(text_clean, _p0_om_end_trigger()) is not None
        or _p0_command_body(text_clean, _p0_checkmeeting_trigger()) is not None
        or _p0_command_body(text_clean, _p0_whotalk_trigger()) is not None
        or _p0_command_body(text_clean, _p0_p0docs_trigger()) is not None
        or _p0_command_body(text_clean, _p0_ose_trigger()) is not None
        or _p0_command_body(text_clean, "/vcauth") is not None
        or _p0_command_body(text_clean, "/vccode") is not None
        or _p0_command_body(text_clean, "/whoami") is not None
        or _p0_command_body(text_clean, _p0_detect_confirm_trigger()) is not None
    )

    kind = ""
    question = ""
    if _p0_command_body(text_clean, reload_trigger) is not None:
        kind = "reload"
    else:
        asked = _p0_command_body(text_clean, ask_trigger)
        if asked is not None:
            kind, question = "ask", asked
        elif is_monitoring_cmd:
            # A monitoring command that isn't /ask or /reload — hand off to monitoring.
            return False
        elif not is_dm and _lark_env_truthy_or_default("P0_QA_AT_MENTION_ENABLE", default=True):
            if _p0_mentions_contain_bot(mentions):
                kind, question = "mention", text_clean
            else:
                # Group @-mention that didn't match this bot's identity — surface the actual ids
                # so P0_BOT_OPEN_ID can be pinned to the right value (common bot/v3/info mismatch).
                try:
                    _mids = [s for s in _lark_iter_mention_scalar_strings(mentions) if s]
                except Exception:
                    _mids = ["<parse-failed>"]
                logger.info(
                    "p0 doc-qa: group @-mention NOT matched to this bot — mention ids=%r, bot targets "
                    "open_id=%r app_id=%r. If one of the mention ids IS this bot, set P0_BOT_OPEN_ID to it.",
                    _mids, _p0_bot_open_id(), str(APP_ID or "").strip(),
                )
        elif is_dm and _lark_env_truthy_or_default("P0_QA_ANSWER_DM", default=True):
            kind, question = "dm", text_clean

    if not kind:
        return False

    processed_stick = _monitoring_processed_stick(
        mid, im_event_id, chat_id or "", sender_debounce, msg_time
    )
    # Key on the question text (not just chat+kind) so a second, distinct question in the
    # same chat is not silently dropped while the first is still being answered by the model.
    # (True duplicate deliveries are already collapsed by the event-id / stick guards below.)
    if kind == "reload":
        debounce_key = f"{(chat_id or '').strip()}\n__p0_reload__"
    else:
        _qn = re.sub(r"\s+", " ", (question or "").strip().lower())[:200]
        debounce_key = f"{(chat_id or '').strip()}\n__p0_{kind}__\n{_qn}"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            logger.info("duplicate IM event_id=%s — skip (p0 %s)", im_event_id, kind)
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            logger.info("duplicate p0 %s stick=%r — skip", kind, processed_stick[:96])
            return True
        if debounce_key in _monitoring_inflight_keys:
            logger.info("p0 %s skip — already in flight", kind)
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    logger.info(
        "p0 doc-qa accepted kind=%s chat=%s dm=%s q_len=%d",
        kind,
        bool(chat_id),
        is_dm,
        len(question or ""),
    )
    try:
        threading.Thread(
            target=_p0_qa_worker,
            args=(chat_id or "", open_id or "", kind, question, mid or "", debounce_key),
            daemon=True,
            name="p0-doc-qa",
        ).start()
    except Exception:
        # Worker never started → its finally never runs; release the in-flight key here
        # so this chat+question is not blocked forever (the set has no TTL/sweep).
        logger.exception("p0 doc-qa: worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


def _p0_start_doc_preload_if_enabled() -> None:
    """Warm the doc cache on startup (and optionally refresh it periodically)."""
    if not _p0_qa_enabled():
        return
    if not _p0_wiki_node_token():
        logger.warning(
            "P0_DOC_QA_ENABLE=1 but P0_WIKI_NODE_TOKEN/P0_WIKI_URL empty — Q&A will have no doc"
        )
        return
    logger.info(
        "p0 doc-qa enabled — model=%s ollama=%s node=%s include_children=%s",
        _p0_qa_model(),
        _p0_qa_ollama_url(),
        _p0_wiki_node_token(),
        _lark_env_truthy_or_default("P0_WIKI_INCLUDE_CHILDREN", default=True),
    )

    def _boot() -> None:
        ok, meta = _p0_doc_reload()
        if not ok:
            logger.warning(
                "p0 doc preload failed: %s (fix wiki/docx scopes + sharing, then /reload)", meta
            )
        try:
            oid = _p0_bot_open_id()
            if oid:
                logger.info(
                    "p0 bot open_id=%s — pin P0_BOT_OPEN_ID to this for reliable group @-mention",
                    oid,
                )
            else:
                logger.warning(
                    "p0 could not resolve this bot's open_id — group @-mention may miss; "
                    "DMs and %s still work. Set P0_BOT_OPEN_ID to fix.",
                    _cfg_str("P0_ASK_TRIGGER", "/ask").strip() or "/ask",
                )
        except Exception:
            logger.exception("p0 bot open_id resolve at startup failed")
        refresh = _cfg_int("P0_DOC_REFRESH_SECONDS", 0)
        if refresh > 0:
            while True:
                time.sleep(max(60, refresh))
                _p0_doc_reload()

    threading.Thread(target=_boot, daemon=True, name="p0-doc-preload").start()


# ---------------------------------------------------------------------------
# p0bot — meeting attendance (Mode C)
#
# A Lark app can only read meetings it OWNS, so there is no way to watch "who is
# inside" an arbitrary meeting live. Mode C is on-demand: "/meeting <link-or-no>"
# pulls the attendance report for that 9-digit meeting number via
# GET /open-apis/vc/v1/participant_list (supports ongoing + ended meetings, and
# returns participant_name directly) and posts a card. Needs the app to hold the
# VC meeting-management report permission (enterprise-admin granted); some tenants
# require a user_access_token for this endpoint.
# ---------------------------------------------------------------------------

_P0_MEETING_STATUS_LABEL = {
    "1": "进行中 / ongoing",
    "2": "已结束 / ended",
    "3": "已预约 / scheduled",
}


def _p0_meeting_enabled() -> bool:
    return _lark_env_truthy("P0_MEETING_ENABLE")


def _p0_meeting_trigger() -> str:
    return _cfg_str("P0_MEETING_TRIGGER", "/meeting").strip() or "/meeting"


def _p0_parse_meeting_no(text: str) -> str:
    """Extract a 9-digit Lark meeting number from raw text or a meeting link."""
    compact = re.sub(r"[\s\-]", "", text or "")
    m = re.search(r"\d{9}", compact)
    return m.group(0) if m else ""


def _p0_meeting_participant_rows(
    meeting_no: str, start: int, end: int, status: Optional[str]
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[Dict[str, Any]]]:
    """(rows, error). rows is None on API error; error carries the Lark code/msg for the user."""
    rows: List[Dict[str, Any]] = []
    page = ""
    cap = max(20, _cfg_int("P0_MEETING_MAX_ROWS", 200))
    base = f"{_lark_api_domain()}/open-apis/vc/v1/participant_list"
    # This report requires an admin's user_access_token (tenant token → 121005).
    at = _p0_vc_user_access_token()
    if not at:
        return None, {"code": "NO_AUTH", "msg": "no admin authorization yet — an admin must run /vcauth first"}
    for _ in range(40):
        params: Dict[str, Any] = {
            "meeting_no": meeting_no,
            "meeting_start_time": str(int(start)),
            "meeting_end_time": str(int(end)),
            "page_size": 100,
            "user_id_type": "open_id",
        }
        if status:
            params["meeting_status"] = status
        if page:
            params["page_token"] = page
        # Deliberately do NOT raise_for_status — read the JSON body on any status so the
        # user sees Lark's real code/msg (e.g. a permission error) instead of a generic one.
        try:
            r = requests.get(
                base,
                headers={
                    "Authorization": f"Bearer {at}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                params=params,
                timeout=20,
            )
        except Exception as e:
            logger.exception("p0 meeting participant_list transport error no=%s", meeting_no)
            return None, {"code": -1, "msg": f"network error: {e.__class__.__name__}: {e}"}
        try:
            j = r.json()
        except Exception:
            snippet = (r.text or "").strip().replace("\n", " ")[:200]
            logger.warning("p0 meeting participant_list HTTP %s non-JSON: %s", r.status_code, snippet)
            return None, {"code": f"HTTP {r.status_code}", "msg": snippet or "(empty body)"}
        try:
            code = int(j.get("code", -1))
        except (TypeError, ValueError):
            code = -1
        if code != 0:
            msg = j.get("msg") or j.get("message") or str(j)
            logger.warning(
                "p0 meeting participant_list code=%s http=%s msg=%s", j.get("code"), r.status_code, msg
            )
            return None, {"code": j.get("code", code), "msg": msg}
        data = j.get("data") or {}
        items = data.get("participants")
        if not isinstance(items, list):
            items = data.get("items") if isinstance(data.get("items"), list) else []
        rows.extend(items)
        if len(rows) >= cap or not data.get("has_more"):
            break
        page = str(data.get("page_token") or "")
        if not page:
            break
    return rows[:cap], None


def _p0_meeting_row_line(row: Dict[str, Any]) -> str:
    name = (
        _lark_dict_pick_str(row, "participant_name", "participantName")
        or _lark_dict_pick_str(row, "name")
        or "?"
    )
    dept = _lark_dict_pick_str(row, "department")
    join = _lark_dict_pick_str(row, "join_time", "joinTime")
    leave = _lark_dict_pick_str(row, "leave_time", "leaveTime")
    who = f"**{name}**"
    if dept:
        who += f" · {dept}"
    if join and not leave:
        who += " — 🟢 在会中 / in meeting"
    elif join and leave:
        who += f" — {join} → {leave}"
    elif join:
        who += f" — {join}"
    return f"• {who}"


def _p0_meeting_send_card(rt: str, rv: str, meeting_no: str, template: str, title: str, lines: List[str]) -> None:
    if not rt or not rv:
        return
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": title[:190]},
            "subtitle": {"tag": "plain_text", "content": (f"会议号 / No. {meeting_no}" if meeting_no else "")[:190]},
        },
        "body": {"elements": [{"tag": "markdown", "content": "\n".join(lines) if lines else "-"}]},
    }
    try:
        _lark_send_interactive_card(rt, rv, card)
    except Exception:
        logger.exception("p0 meeting card send failed; falling back to text")
        try:
            _lark_send_text_auto(rt, rv, f"{title}\n" + "\n".join(lines))
        except Exception:
            logger.exception("p0 meeting text fallback failed")


def _p0_meeting_worker(chat_id: str, open_id: str, meeting_no: str, mid: str, debounce_key: str) -> None:
    rt, rv = (
        ("chat_id", chat_id)
        if (chat_id or "").strip()
        else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    )
    template = _cfg_str("P0_MEETING_CARD_TEMPLATE", "turquoise").strip() or "turquoise"
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = (
        _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK")
        if react
        else None
    )
    try:
        now = int(time.time())
        lookback_h = max(1, min(24, _cfg_int("P0_MEETING_LOOKBACK_HOURS", 6)))
        start = now - lookback_h * 3600
        # Ongoing first (who is inside NOW); fall back to ended (who attended).
        r1, e1 = _p0_meeting_participant_rows(meeting_no, start, now, "1")
        if r1:
            rows, status_used, err = r1, "1", None
        else:
            r2, e2 = _p0_meeting_participant_rows(meeting_no, start, now, "2")
            if r2:
                rows, status_used, err = r2, "2", None
            elif r1 is None and r2 is None:
                rows, status_used, err = None, "", (e2 or e1)  # both attempts errored
            else:
                rows, status_used, err = [], "2", None  # succeeded but empty
        if rows is None:
            code = err.get("code") if isinstance(err, dict) else "?"
            msg = err.get("msg") if isinstance(err, dict) else str(err)
            if str(code) == "NO_AUTH":
                _p0_meeting_send_card(
                    rt, rv, meeting_no, "orange", "🔐 需要管理员授权 / Admin authorization needed",
                    [
                        "读取参会信息需要管理员先授权一次。请【管理员】运行 /vcauth 完成授权"
                        "（该管理员需拥有『视频会议 · 会议管理』后台权限）。",
                        "Reading attendance needs a one-time admin authorization. An admin runs "
                        "**/vcauth** (the admin must hold the VC 'Meeting Management' role).",
                    ],
                )
                return
            low = f"{code} {msg}".lower()
            hint = ""
            if any(k in low for k in ("permission", "access", "denied", "forbidden", "403", "99991")):
                hint = (
                    "\n\n提示 / Hint: 本应用需要在【管理后台 admin.larksuite.com】获得"
                    "「视频会议 · 会议管理」权限，并在开发者后台开通 vc 会议室明细权限；"
                    "此报表接口在多数租户仅支持 user_access_token（需管理员 OAuth 授权）。\n"
                    "The app needs the 'Video Conferencing · Meeting Management' permission in the "
                    "Admin Console, plus the vc room-detail scope; on most tenants this report API "
                    "only accepts a user_access_token (admin OAuth), not the bot's tenant token."
                )
            _p0_meeting_send_card(
                rt, rv, meeting_no, "red", "⚠️ 查询失败 / Lookup failed",
                [f"无法读取会议 {meeting_no} 的参会信息 / Could not read participants.", f"`code={code}  msg={msg}`{hint}"],
            )
            return
        if not rows:
            _p0_meeting_send_card(
                rt, rv, meeting_no, template, "🔍 无记录 / No participants",
                [
                    f"最近 {lookback_h}h 内未找到会议 {meeting_no} 的参会记录。",
                    f"No attendance for meeting {meeting_no} in the last {lookback_h}h.",
                    "（会议号是否正确？会议是否在时间窗内？/ correct number & within the window?）",
                ],
            )
            return
        lines = [_p0_meeting_row_line(r) for r in rows]
        head = f"**{_P0_MEETING_STATUS_LABEL.get(status_used, '')}** · 参会者 / Participants: **{len(rows)}**"
        _p0_meeting_send_card(
            rt, rv, meeting_no, template, "🎥 会议参会情况 / Meeting attendance", [head, ""] + lines
        )
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_meeting(
    *,
    chat_id: str,
    open_id: str,
    clean: str,
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """Handle "/meeting <link-or-number>". Returns True when handled."""
    if not _p0_meeting_enabled():
        return False
    body = _p0_command_body((clean or "").strip(), _p0_meeting_trigger())
    if body is None:
        return False
    meeting_no = _p0_parse_meeting_no(body)

    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_meeting__\n{meeting_no or 'help'}"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    if not meeting_no:
        def _usage() -> None:
            react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
            ack = (
                _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK")
                if react
                else None
            )
            try:
                rt, rv = (
                    ("chat_id", chat_id)
                    if (chat_id or "").strip()
                    else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
                )
                if rt and rv:
                    _lark_send_text_auto(
                        rt,
                        rv,
                        "请给我会议的 **9 位会议号** 或会议链接（不是会议名称）。\n"
                        "Please give the meeting's **9-digit number** or join link — not its name.\n"
                        f"例 / e.g. {_p0_meeting_trigger()} 123456789   或/or   "
                        f"{_p0_meeting_trigger()} https://vc.larksuite.com/j/123456789",
                    )
            finally:
                if react and ack:
                    _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
                    if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                        _p0_lark_remove_reaction(mid, ack)
                with _monitoring_reply_dispatch_lock:
                    _monitoring_inflight_keys.discard(debounce_key)

        threading.Thread(target=_usage, daemon=True, name="p0-meeting-usage").start()
        return True

    logger.info("p0 meeting lookup accepted no=%s chat=%s", meeting_no, bool(chat_id))
    try:
        threading.Thread(
            target=_p0_meeting_worker,
            args=(chat_id or "", open_id or "", meeting_no, mid or "", debounce_key),
            daemon=True,
            name="p0-meeting",
        ).start()
    except Exception:
        logger.exception("p0 meeting worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


# ---------------------------------------------------------------------------
# p0bot — "/whotalk": who-said-what transcript of a recorded meeting
#
# Lark Minutes does the ASR and knows the real speaker per line (each participant
# has their own mic stream) — that is the only way to get "Yang: ..." with real
# names. The RAW zh/en transcript is then cleaned + translated by the LOCAL Qwen
# (Lark's own translation is deliberately not used).
#
# Flow: /whotalk [minutes-link | meeting-link | 9-digit-no | (empty = last
# recorded bot meeting)] → minute_token → export transcript (need_speaker) →
# Qwen cleanup → answer cards.
#
# Console needs (add + PUBLISH a version): scope `minutes:minutes.transcript:export`
# (+ existing vc:record:readonly), and the tenant must have Minutes (妙记) enabled.
# If the tenant token can't access a host-owned minute, the stored /vcauth admin
# user token is tried as fallback (add the scope to P0_VC_OAUTH_SCOPES and re-auth).
# ---------------------------------------------------------------------------

def _p0_whotalk_enabled() -> bool:
    return _lark_env_truthy_or_default("P0_WHOTALK_ENABLE", default=True)


def _p0_whotalk_trigger() -> str:
    return _cfg_str("P0_WHOTALK_TRIGGER", "/whotalk").strip() or "/whotalk"


def _p0_minute_token_from_text(text: str) -> str:
    """Extract a Minutes token from a /minutes/<token> URL (24-char, tolerate variants)."""
    m = re.search(r"/minutes/([A-Za-z0-9]{16,64})", text or "")
    return m.group(1) if m else ""


def _p0_whotalk_last_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".p0_whotalk_last.json")


def _p0_whotalk_last_load() -> Dict[str, Any]:
    """Last-recording stash persisted across restarts (best-effort)."""
    try:
        with open(_p0_whotalk_last_path(), "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _p0_whotalk_resolve_minute_token(arg: str) -> Tuple[str, str]:
    """(minute_token, error). Resolve from a minutes link, meeting link/no, or the last recording."""
    a = (arg or "").strip()
    t = _p0_minute_token_from_text(a)
    if t:
        return t, ""
    mno = _p0_parse_meeting_no(a)
    mid_ = ""
    url = ""
    with _p0_om_lock:
        last = dict(_p0_whotalk_last)
    if not last.get("meeting_no"):
        last = _p0_whotalk_last_load()  # in-memory stash is wiped by restarts — fall back to disk
    if not mno and last.get("meeting_no"):
        mno = str(last.get("meeting_no") or "")
        mid_, url = str(last.get("meeting_id") or ""), str(last.get("url") or "")
    elif mno and str(last.get("meeting_no") or "") == mno:
        mid_, url = str(last.get("meeting_id") or ""), str(last.get("url") or "")
    if mno and not mid_:
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
        if rec:
            mid_ = str(rec.get("meeting_id") or "")
    if url:
        t = _p0_minute_token_from_text(url)
        if t:
            return t, ""
    if not mno and not mid_:
        return "", ("我还不知道最近的录制会议 — 请带上会议号/会议链接或妙记链接。\n"
                    "No recent recorded meeting known — pass a meeting number/link or a Minutes link.")
    if not mid_ and mno:
        # Resolve meeting_no → meeting_id via list_by_no over a lookback window.
        end = int(time.time())
        start = end - 3600 * max(1, _cfg_int("P0_WHOTALK_LOOKBACK_HOURS", 72))
        try:
            j = _p0_lark_get_json(
                "/open-apis/vc/v1/meetings/list_by_no",
                {"meeting_no": mno, "start_time": str(start), "end_time": str(end), "page_size": 20},
            )
            if int(j.get("code", -1)) == 0:
                briefs = (j.get("data") or {}).get("meeting_briefs") or []
                if isinstance(briefs, list) and briefs:
                    mid_ = _lark_dict_pick_str(briefs[-1], "id", "meeting_id", "meetingId")
            else:
                return "", f"list_by_no code={j.get('code')} msg={j.get('msg')}"
        except Exception as e:
            logger.exception("p0 whotalk list_by_no failed no=%s", mno)
            return "", f"list_by_no error: {e.__class__.__name__}"
    if not mid_:
        return "", (f"找不到会议 {mno} 的会议ID（超出回溯窗口？）— 请直接给我妙记链接。\n"
                    f"Could not resolve meeting id for No. {mno} — paste the Minutes link instead.")
    try:
        j = _p0_lark_get_json(f"/open-apis/vc/v1/meetings/{mid_}/recording")
    except Exception as e:
        return "", f"get-recording error: {e.__class__.__name__}"
    if int(j.get("code", -1)) != 0:
        return "", f"get-recording code={j.get('code')} msg={j.get('msg')}"
    u = _lark_dict_pick_str((j.get("data") or {}).get("recording") or {}, "url")
    t = _p0_minute_token_from_text(u)
    if t:
        return t, ""
    if u:
        return "", (f"录制链接不含妙记 token（{u[:80]}）— 租户是否开通了妙记/Minutes？\n"
                    "Recording url has no Minutes token — is Lark Minutes enabled for the tenant?")
    return "", ("录制还没生成（会议刚结束需等几分钟）。/ Recording not ready yet — try again a few "
                "minutes after the meeting ends.")


def _p0_minutes_tokens() -> List[Tuple[str, str]]:
    """Access tokens to try against the Minutes API: tenant first, then the /vcauth user token."""
    toks: List[Tuple[str, str]] = []
    try:
        toks.append(("tenant", _lark_tenant_access_token_string()))
    except Exception:
        logger.exception("p0 whotalk tenant token unavailable")
    ut = _p0_vc_user_access_token()
    if ut:
        toks.append(("user", ut))
    return toks


def _p0_minutes_export(minute_token: str, params: Dict[str, Any], what: str) -> Tuple[bytes, Dict[str, Any]]:
    """(file_bytes, error) from the transcript-export endpoint (binary stream on success)."""
    last_err: Dict[str, Any] = {"code": "NO_TOKEN", "msg": "no usable token"}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.get(
                f"{_lark_api_domain()}/open-apis/minutes/v1/minutes/{minute_token}/transcript",
                headers={"Authorization": f"Bearer {tok}"},
                params=params,
                timeout=60,
            )
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        ctype = (r.headers.get("Content-Type") or "").lower()
        if r.status_code == 200 and "json" not in ctype:
            if r.content.strip():
                logger.info("p0 whotalk %s ok via %s token: %d bytes", what, kind, len(r.content))
                return r.content, {}
            last_err = {"code": "EMPTY", "msg": f"{what} export returned an empty file"}
            continue
        try:
            j = r.json()
        except Exception:
            j = {"code": f"HTTP {r.status_code}", "msg": (r.text or "")[:200]}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg") or str(j)[:200]}
        logger.info("p0 whotalk %s via %s token failed: %s", what, kind, last_err)
    return b"", last_err


def _p0_minutes_transcript(minute_token: str) -> Tuple[str, Dict[str, Any]]:
    """(transcript_text, error). Lark's ASR text with speaker names (txt export)."""
    raw, err = _p0_minutes_export(
        minute_token,
        {"need_speaker": "true", "need_timestamp": "false", "file_format": "txt"},
        "transcript",
    )
    return raw.decode("utf-8", "replace").strip() if raw else "", err


def _p0_minutes_media_url(minute_token: str) -> Tuple[str, Dict[str, Any]]:
    """(download_url, error) for the recording's audio/video file (scope minutes:minutes.media:export)."""
    last_err: Dict[str, Any] = {"code": "NO_TOKEN", "msg": "no usable token"}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.get(
                f"{_lark_api_domain()}/open-apis/minutes/v1/minutes/{minute_token}/media",
                headers={"Authorization": f"Bearer {tok}"},
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        code = int(j.get("code", -1)) if isinstance(j, dict) and str(j.get("code", "")).lstrip("-").isdigit() else -1
        if code == 0:
            url = _lark_dict_pick_str((j.get("data") or {}), "download_url", "downloadUrl")
            if url:
                logger.info("p0 whotalk media url ok via %s token", kind)
                return url, {}
            last_err = {"code": "NO_URL", "msg": "media response had no download_url"}
            continue
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg") or str(j)[:200]}
        logger.info("p0 whotalk media via %s token failed: %s", kind, last_err)
    return "", last_err


_P0_SRT_TS = re.compile(
    r"(\d{1,2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*(\d{1,2}):(\d{2}):(\d{2})[,.](\d{3})"
)
_P0_SRT_SPEAKER = re.compile(r"^\s*([^:：\n]{1,40})[:：]\s*(.*)$")


def _p0_srt_turns(srt_text: str) -> List[Dict[str, Any]]:
    """Parse a Minutes SRT export into speaker turns [{speaker, start, end, text}] (seconds).

    Consecutive entries by the same speaker are merged when the gap is small, capped at
    P0_WHOTALK_ASR_MAX_TURN_SECONDS so segments stay inside SenseVoice's comfort zone.
    """
    max_turn = max(5.0, _cfg_float("P0_WHOTALK_ASR_MAX_TURN_SECONDS", 15.0))
    merge_gap = max(0.0, _cfg_float("P0_WHOTALK_ASR_MERGE_GAP_MS", 800.0)) / 1000.0
    entries: List[Dict[str, Any]] = []
    speaker = "?"
    for block in re.split(r"\n\s*\n", srt_text or ""):
        m = _P0_SRT_TS.search(block)
        if not m:
            continue
        g = [int(x) for x in m.groups()]
        start = g[0] * 3600 + g[1] * 60 + g[2] + g[3] / 1000.0
        end = g[4] * 3600 + g[5] * 60 + g[6] + g[7] / 1000.0
        text_lines = [ln.strip() for ln in block[m.end():].strip().splitlines() if ln.strip()]
        if not text_lines:
            continue
        sm = _P0_SRT_SPEAKER.match(text_lines[0])
        if sm:
            speaker = sm.group(1).strip() or speaker
            text_lines[0] = sm.group(2).strip()
        entries.append({"speaker": speaker, "start": start, "end": max(end, start),
                        "text": " ".join(t for t in text_lines if t)})
    turns: List[Dict[str, Any]] = []
    for e in entries:
        last = turns[-1] if turns else None
        if (last is not None and last["speaker"] == e["speaker"]
                and e["start"] - last["end"] <= merge_gap
                and e["end"] - last["start"] <= max_turn):
            last["end"] = e["end"]
            last["text"] = (last["text"] + " " + e["text"]).strip()
        else:
            turns.append(dict(e))
    return turns


def _p0_ffmpeg_bin() -> str:
    p = _cfg_str("P0_FFMPEG_BIN", "").strip()
    if p:
        return p
    try:
        local = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin", "ffmpeg")
        if os.path.isfile(local):
            return local
    except Exception:
        pass
    return "ffmpeg"


def _p0_whotalk_asr_enabled() -> bool:
    return _lark_env_truthy("P0_WHOTALK_ASR_ENABLE")


def _p0_whotalk_asr_model_paths() -> Tuple[str, str]:
    """(model_path, tokens_path) for the SenseVoice onnx model, or ("", "") if not installed."""
    d = _cfg_str("P0_WHOTALK_ASR_MODEL_DIR", "").strip()
    if not d:
        try:
            d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models", "sensevoice")
        except Exception:
            return "", ""
    tokens = os.path.join(d, "tokens.txt")
    for name in ("model.int8.onnx", "model.onnx"):
        model = os.path.join(d, name)
        if os.path.isfile(model) and os.path.isfile(tokens):
            return model, tokens
    return "", ""


_p0_asr_recognizer: Any = None
_p0_asr_lock = threading.Lock()


def _p0_whotalk_recognizer() -> Any:
    """Lazy-loaded, process-wide SenseVoice recognizer (~1 GB resident once loaded)."""
    global _p0_asr_recognizer
    with _p0_asr_lock:
        if _p0_asr_recognizer is not None:
            return _p0_asr_recognizer
        model, tokens = _p0_whotalk_asr_model_paths()
        if not model:
            raise RuntimeError(
                "SenseVoice model not found — run deploy/setup-whotalk-asr.sh (or set P0_WHOTALK_ASR_MODEL_DIR)"
            )
        import sherpa_onnx  # deferred: only needed when local ASR is enabled

        _p0_asr_recognizer = sherpa_onnx.OfflineRecognizer.from_sense_voice(
            model=model,
            tokens=tokens,
            num_threads=max(1, _cfg_int("P0_WHOTALK_ASR_THREADS", 4)),
            use_itn=True,
            language="auto",
        )
        logger.info("p0 whotalk ASR loaded: %s", model)
        return _p0_asr_recognizer


_p0_whisper_model: Any = None


def _p0_whotalk_whisper() -> Any:
    """Lazy-loaded faster-whisper model (engine=whisper). Downloads from HF hub on first use —
    on networks where huggingface.co is slow/blocked, set HF_ENDPOINT=https://hf-mirror.com in .env."""
    global _p0_whisper_model
    with _p0_asr_lock:
        if _p0_whisper_model is not None:
            return _p0_whisper_model
        from faster_whisper import WhisperModel  # deferred: only needed for engine=whisper

        name = _cfg_str("P0_WHOTALK_WHISPER_MODEL", "medium").strip() or "medium"
        _p0_whisper_model = WhisperModel(
            name, device="cpu", compute_type="int8",
            cpu_threads=max(1, _cfg_int("P0_WHOTALK_ASR_THREADS", 4)),
        )
        logger.info("p0 whotalk whisper loaded: %s (int8)", name)
        return _p0_whisper_model


def _p0_whotalk_asr_engine() -> str:
    e = _cfg_str("P0_WHOTALK_ASR_ENGINE", "sensevoice").strip().lower()
    return e if e in ("sensevoice", "whisper") else "sensevoice"


def _p0_whotalk_engine_warm() -> None:
    """Load the selected engine up-front so a broken install fails fast, before any download."""
    if _p0_whotalk_asr_engine() == "whisper":
        _p0_whotalk_whisper()
    else:
        _p0_whotalk_recognizer()


# Signature Whisper silence-hallucinations (YouTube outro boilerplate, subtitle credits) and
# echoes of our own initial prompt. A match means the segment was silence/noise — treat as empty
# so the caller falls back to Lark's text for that turn.
_P0_WHISPER_JUNK = re.compile(
    r"(?i)(please like,? subscribe|like, subscribe, share|明镜|點點|点点栏目|不吝点赞|请点赞|"
    r"订阅.{0,8}转发|thank you (so much )?for watching|字幕由|amara\.org|打赏支持|"
    r"transcribe this audio|record it accurately|按原话记录|中英混合的(工作会议)?对话)"
)


def _p0_whotalk_decode(seg: Any, sr: int) -> str:
    """Transcribe one audio segment (float32 numpy) with the configured engine."""
    if _p0_whotalk_asr_engine() == "whisper":
        m = _p0_whotalk_whisper()
        lang = _cfg_str("P0_WHOTALK_WHISPER_LANG", "zh").strip() or None
        prompt = _cfg_str(
            "P0_WHOTALK_WHISPER_PROMPT",
            "以下是一段中英混合的工作会议对话，请按原话记录：中文写汉字，英文单词保留英文。"
            "例如：我们现在 check 一下这个 server 的 status，然后 update 给大家。",
        ).strip() or None
        segments, _info = m.transcribe(
            seg,
            task="transcribe",
            language=lang,
            initial_prompt=prompt,
            beam_size=5,
            condition_on_previous_text=False,  # reduces hallucination loops on short segments
            vad_filter=True,  # skip silence — Whisper hallucinates YouTube boilerplate on it
        )
        text = "".join(s.text for s in segments).strip()
        if text and _P0_WHISPER_JUNK.search(text):
            logger.info("p0 whotalk whisper junk suppressed: %r", text[:100])
            return ""
        return text
    rec = _p0_whotalk_recognizer()
    stream = rec.create_stream()
    stream.accept_waveform(sr, seg)
    rec.decode_stream(stream)
    return (stream.result.text or "").strip()


def _p0_whotalk_local_transcribe(minute_token: str, with_times: bool = False,
                                 start_epoch: float = 0.0) -> Tuple[str, str]:
    """(transcript_text, error). Hybrid local ASR:

    speaker names + timestamps from the Minutes SRT export; the TEXT of each turn is
    re-transcribed from the actual recording audio by the local engine. With ``with_times``,
    each line is prefixed [HH:MM:SS] (wall clock when ``start_epoch`` is known, else relative).
    """

    def _tprefix(turn_start: float) -> str:
        if not with_times:
            return ""
        if start_epoch:
            return "[" + time.strftime("%H:%M:%S", time.localtime(start_epoch + turn_start)) + "] "
        s = int(turn_start)
        return f"[{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}] "
    import numpy as np  # deferred: only needed when local ASR is enabled

    srt_raw, err = _p0_minutes_export(
        minute_token,
        {"need_speaker": "true", "need_timestamp": "true", "file_format": "srt"},
        "srt",
    )
    if not srt_raw:
        return "", f"SRT export failed: code={err.get('code')} msg={err.get('msg')}"
    turns = _p0_srt_turns(srt_raw.decode("utf-8", "replace"))
    if not turns:
        return "", "SRT parsed to zero speaker turns"
    if not any(t.get("speaker") and t["speaker"] != "?" for t in turns):
        # SRT layout is undocumented; if this export carries no recognizable speaker labels,
        # a local transcript would lose all names — prefer the Lark text path (names known-good).
        return "", "SRT export carried no speaker labels — using Lark transcript to keep names"
    media_url, merr = _p0_minutes_media_url(minute_token)
    if not media_url:
        return "", (f"media download-url failed: code={merr.get('code')} msg={merr.get('msg')} "
                    "(scope minutes:minutes.media:export granted+published? re-/vcauth after adding it)")
    workdir = tempfile.mkdtemp(prefix="p0whotalk_")
    media_path = os.path.join(workdir, "media.bin")
    wav_path = os.path.join(workdir, "audio.wav")
    try:
        max_mb = _cfg_int("P0_WHOTALK_ASR_MAX_MEDIA_MB", 1024)
        # Try the URL as presigned first; if the CDN insists on auth, retry with our tokens.
        auth_headers: List[Dict[str, str]] = [{}]
        auth_headers += [{"Authorization": f"Bearer {tok}"} for _kind, tok in _p0_minutes_tokens()]
        written = 0
        last_status = "?"
        for hdrs in auth_headers:
            with requests.get(media_url, headers=hdrs, stream=True, timeout=120) as r:
                last_status = str(r.status_code)
                if r.status_code in (401, 403):
                    continue
                r.raise_for_status()
                clen = int(r.headers.get("Content-Length") or 0)
                if max_mb and clen and clen > max_mb * 1024 * 1024:
                    return "", f"recording too large ({clen // (1024 * 1024)} MB > {max_mb} MB limit)"
                written = 0
                with open(media_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        written += len(chunk)
                        if max_mb and written > max_mb * 1024 * 1024:
                            return "", f"recording exceeded the {max_mb} MB download limit"
            break
        if not written:
            return "", f"media download denied (HTTP {last_status}) with and without auth"
        logger.info("p0 whotalk media downloaded: %d bytes", written)
        proc = subprocess.run(
            [_p0_ffmpeg_bin(), "-y", "-i", media_path, "-vn", "-ac", "1", "-ar", "16000",
             "-c:a", "pcm_s16le", "-f", "wav", wav_path],
            capture_output=True, timeout=600,
        )
        if proc.returncode != 0 or not os.path.isfile(wav_path):
            tail = (proc.stderr or b"").decode("utf-8", "replace")[-300:]
            return "", f"ffmpeg failed (rc={proc.returncode}): {tail}"
        _p0_whotalk_engine_warm()
        with wave.open(wav_path, "rb") as wf:
            sr = wf.getframerate()
            nch = wf.getnchannels()
            pcm = wf.readframes(wf.getnframes())
        if nch != 1:
            return "", f"unexpected channel count {nch} after ffmpeg"
        samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
        total_s = len(samples) / float(sr)
        pad = max(0.0, _cfg_float("P0_WHOTALK_ASR_SEG_PAD_MS", 150.0)) / 1000.0
        lines: List[str] = []
        t0 = time.time()
        for i, turn in enumerate(turns):
            # Clamp the padded window so it never reaches into a neighboring turn — otherwise a
            # rapid exchange gets transcribed twice and one speaker's words leak into the other's line.
            prev_end = float(turns[i - 1]["end"]) if i > 0 else 0.0
            next_start = float(turns[i + 1]["start"]) if i + 1 < len(turns) else total_s
            a = max(0.0, float(turn["start"]) - pad, prev_end - 0.05)
            b = min(total_s, float(turn["end"]) + pad, next_start + 0.05)
            if b - a < 0.2:
                # window vanished (fully overlapped cross-talk) — keep Lark's text, don't drop the turn
                if turn.get("text"):
                    lines.append(f"{_tprefix(float(turn['start']))}{turn['speaker']}: {turn['text']}")
                continue
            seg = samples[int(a * sr):int(b * sr)]
            text = _p0_whotalk_decode(seg, sr)
            if text:
                lines.append(f"{_tprefix(float(turn['start']))}{turn['speaker']}: {text}")
            elif turn.get("text"):
                # local ASR heard nothing — keep Lark's text rather than dropping the turn
                lines.append(f"{_tprefix(float(turn['start']))}{turn['speaker']}: {turn['text']}")
        logger.info("p0 whotalk local ASR (%s) done: %d turns, %.1fs audio, %.1fs compute",
                    _p0_whotalk_asr_engine(), len(lines), total_s, time.time() - t0)
        if not lines:
            return "", "local ASR produced no text"
        return "\n".join(lines), ""
    except subprocess.TimeoutExpired:
        return "", "ffmpeg timed out"
    except Exception as e:
        logger.exception("p0 whotalk local ASR failed")
        return "", f"{e.__class__.__name__}: {e}"
    finally:
        if not _lark_env_truthy("P0_WHOTALK_ASR_KEEP_MEDIA"):
            shutil.rmtree(workdir, ignore_errors=True)
        else:
            logger.info("p0 whotalk media kept at %s (P0_WHOTALK_ASR_KEEP_MEDIA=1)", workdir)


def _p0_whotalk_ai(transcript: str) -> str:
    """Local-Qwen pass over the raw transcript: fix zh/en ASR errors, keep 'Name: text' turns,
    append an English translation to Chinese lines. Chunked to fit the context window."""
    url = _p0_qa_ollama_url()
    model = _p0_qa_model()
    timeout = max(
        10.0,
        _cfg_float("P0_WHOTALK_QA_TIMEOUT_SECONDS",
                   max(900.0, _cfg_float("P0_QA_TIMEOUT_SECONDS", 600.0))),
    )
    num_ctx = max(2048, _cfg_int("P0_QA_NUM_CTX", 16384))
    chunk_chars = max(2000, _cfg_int("P0_WHOTALK_CHUNK_CHARS", 6000))
    style = _cfg_str("P0_WHOTALK_PROMPT", "").strip() or (
        "You will receive a RAW speech-to-text meeting transcript with speaker names. "
        "It mixes Chinese and English and contains recognition errors.\n"
        "1) Fix obvious speech-recognition errors from context; NEVER invent content.\n"
        "2) For EACH speaker turn output EXACTLY this 3-line block, followed by ONE blank line:\n"
        "<Speaker Name>\n"
        "CN : <the utterance in Chinese>\n"
        "EN: <the utterance in English>\n"
        "3) The line matching the spoken language keeps the original wording (errors fixed); "
        "the other line is your faithful translation. If a sentence mixes both languages, the "
        "CN line keeps the inline English words as spoken.\n"
        "4) Preserve turn order and every speaker; do not merge or drop turns.\n"
        "5) Output ONLY these blocks — no preamble, no summary, no commentary."
    )
    lines = (transcript or "").splitlines()
    chunks: List[str] = []
    cur: List[str] = []
    size = 0
    for ln in lines:
        if size + len(ln) + 1 > chunk_chars and cur:
            chunks.append("\n".join(cur))
            cur, size = [], 0
        cur.append(ln)
        size += len(ln) + 1
    if cur:
        chunks.append("\n".join(cur))
    def _pass(text_in: str) -> str:
        body: Dict[str, Any] = {
            "model": model,
            "stream": False,
            "messages": [
                {"role": "system", "content": style},
                {"role": "user", "content": text_in},
            ],
            "options": {"temperature": 0.1, "num_ctx": num_ctx},
            "think": False,
        }
        r = requests.post(f"{url}/api/chat", json=body, timeout=timeout)
        r.raise_for_status()
        return _p0_clean_answer(_monitoring_ai_extract_ollama_text(r.json()))

    raw_banner = "⚠️（本段模型处理失败，以下为原文 / model failed on this section — raw text）\n"
    outs: List[str] = []
    for i, ch in enumerate(chunks, 1):
        t = ""
        try:
            t = _pass(ch)
        except Exception:
            logger.exception("p0 whotalk model pass %d/%d failed", i, len(chunks))
        if not t and len(ch) > 2500:
            # Usually a timeout on a long generation — halve the chunk and retry each part once.
            mid = ch.rfind("\n", 0, len(ch) // 2)
            if mid <= 0:
                mid = len(ch) // 2
            logger.info("p0 whotalk pass %d/%d: retrying as two halves (%d + %d chars)",
                        i, len(chunks), mid, len(ch) - mid)
            parts: List[str] = []
            for half in (ch[:mid], ch[mid:]):
                half = half.strip()
                if not half:
                    continue
                try:
                    parts.append(_pass(half) or half)
                except Exception:
                    logger.exception("p0 whotalk half-retry failed")
                    parts.append(raw_banner + half)
            t = "\n".join(parts)
        if not t:
            t = raw_banner + ch
        outs.append(t)
        if len(chunks) > 1:
            logger.info("p0 whotalk model pass %d/%d done", i, len(chunks))
    return "\n".join(outs).strip()


def _p0_whotalk_worker(chat_id: str, open_id: str, arg: str, mid: str, debounce_key: str) -> None:
    rt, rv = (
        ("chat_id", chat_id)
        if (chat_id or "").strip()
        else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    )
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None

    def _fail(lines: List[str]) -> None:
        if rt and rv:
            _p0_meeting_send_card(rt, rv, "", "red", "⚠️ /whotalk 失败 / failed", lines)

    try:
        token, err = _p0_whotalk_resolve_minute_token(arg)
        if not token:
            _fail([err,
                   "",
                   f"用法 / Usage: `{_p0_whotalk_trigger()} [妙记链接 | 会议链接 | 9位会议号]`（留空=最近一次机器人录制的会议 / empty = last bot-recorded meeting）"])
            return
        transcript = ""
        src = "Lark ASR"
        if _p0_whotalk_asr_enabled():
            transcript, aerr = _p0_whotalk_local_transcribe(token)
            if transcript:
                src = f"本地识别 local ASR ({_p0_whotalk_asr_engine()})"
            else:
                logger.warning("p0 whotalk local ASR failed — falling back to Lark transcript: %s", aerr)
                if rt and rv:
                    _lark_send_text_auto(rt, rv,
                                         f"⚠️ 本地语音识别失败，改用 Lark 转写 / local ASR failed, falling back to "
                                         f"Lark transcript:\n{aerr}")
        terr: Dict[str, Any] = {}
        if not transcript:
            transcript, terr = _p0_minutes_transcript(token)
        if not transcript:
            hint = ""
            low = f"{terr.get('code')} {terr.get('msg')}".lower()
            if "2091005" in low or "permission" in low or "forbidden" in low or "99991" in low or "1655" in low:
                hint = ("\n提示：妙记**不支持**把权限分享给应用/机器人（与云文档不同），租户 token 永远无法访问 — "
                        "必须由妙记**所有者（会议主持人）**完成 /vcauth → /vccode 授权。"
                        "若已授权仍报错：检查该篇妙记的「谁可以下载视频、导出妙记」设置（所有者不受限制）。\n"
                        "Minutes can NOT be shared to an app/bot (unlike cloud docs) — the tenant token can never access "
                        "them. The minute's **owner (the meeting host)** must authorize once via /vcauth → /vccode. "
                        "If already authorized and still denied: check the minute's \"who can download/export\" setting "
                        "(the owner is always allowed).")
            _fail([f"导出转写失败 / transcript export failed: `code={terr.get('code')}  msg={terr.get('msg')}`{hint}"])
            return
        if rt and rv:
            _lark_send_text_auto(rt, rv,
                                 f"🎧 转写已取得（{len(transcript)} 字符，来源/source: {src}），Qwen 正在整理，请稍候…\n"
                                 f"Transcript fetched ({len(transcript)} chars via {src}) — Qwen is cleaning it up, please wait…")
        cleaned = _p0_whotalk_ai(transcript)
        _p0_send_answer(rt, rv, f"{_p0_whotalk_trigger()} — 谁说了什么 / who said what", cleaned)
    except Exception:
        logger.exception("p0 whotalk worker failed")
        _fail(["内部错误，请查看服务器日志。/ Internal error — check the server logs."])
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_whotalk(
    *,
    chat_id: str,
    open_id: str,
    clean: str,
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """Handle "/whotalk [minutes-link|meeting-link|no]". Returns True when handled."""
    if not _p0_whotalk_enabled():
        return False
    body = _p0_command_body((clean or "").strip(), _p0_whotalk_trigger())
    if body is None:
        return False

    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_whotalk__\n{(body or 'last')[:80]}"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    logger.info("p0 whotalk accepted arg=%r chat=%s", (body or "")[:60], bool(chat_id))
    try:
        threading.Thread(
            target=_p0_whotalk_worker,
            args=(chat_id or "", open_id or "", body or "", mid or "", debounce_key),
            daemon=True,
            name="p0-whotalk",
        ).start()
    except Exception:
        logger.exception("p0 whotalk worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


# ---------------------------------------------------------------------------
# p0bot — "/p0docs": fill a P0 incident doc from a meeting transcript
#
# /p0docs <meeting link|9-digit no|minutes link> <wiki/docx link>
# Reads the target doc's blocks, gives Qwen the block list + the meeting
# transcript, and patches ONLY the blocks the model filled — fields the
# transcript doesn't answer are left exactly as they are ("don't know → ignore").
# Needs scope docx:document (edit) and the doc shared to the app as editable.
# ---------------------------------------------------------------------------

def _p0_p0docs_enabled() -> bool:
    return _lark_env_truthy_or_default("P0_P0DOCS_ENABLE", default=True)


def _p0_p0docs_trigger() -> str:
    return _cfg_str("P0_P0DOCS_TRIGGER", "/p0docs").strip() or "/p0docs"


def _p0_doc_link_to_document_id(link: str) -> Tuple[str, str]:
    """(document_id, error). Accepts a /docx/<token> or /wiki/<token> URL (wiki → obj_token)."""
    m = re.search(r"/docx/([A-Za-z0-9]{10,64})", link or "")
    if m:
        return m.group(1), ""
    m = re.search(r"/wiki(?:/[a-z]{2}-[A-Z]{2})?/([A-Za-z0-9]{10,64})", link or "")
    if not m:
        return "", "链接里没有 /wiki/ 或 /docx/ 文档 / no wiki or docx document in the link"
    try:
        j = _p0_lark_get_json("/open-apis/wiki/v2/spaces/get_node",
                              {"token": m.group(1), "obj_type": "wiki"})
    except Exception as e:
        return "", f"wiki get_node error: {e.__class__.__name__}"
    if int(j.get("code", -1)) != 0:
        return "", f"wiki get_node code={j.get('code')} msg={j.get('msg')}"
    n = (j.get("data") or {}).get("node") or {}
    if (n.get("obj_type") or "") != "docx":
        return "", f"wiki node is {n.get('obj_type')!r}, not a docx document"
    return _lark_dict_pick_str(n, "obj_token", "objToken"), ""


_P0_DOCX_TEXT_KEYS = ("text", "heading1", "heading2", "heading3", "heading4", "heading5",
                      "heading6", "heading7", "heading8", "heading9", "bullet", "ordered",
                      "todo", "quote", "page")


def _p0_docx_blocks_raw(document_id: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """(raw block items, error). Full block list of the doc, in document order."""
    last_err: Dict[str, Any] = {}
    for kind, tok in _p0_minutes_tokens():
        items: List[Dict[str, Any]] = []
        page, ok = "", True
        for _ in range(40):
            try:
                r = requests.get(
                    f"{_lark_api_domain()}/open-apis/docx/v1/documents/{document_id}/blocks",
                    headers={"Authorization": f"Bearer {tok}"},
                    params={"page_size": 500, **({"page_token": page} if page else {})},
                    timeout=30,
                )
                j = r.json()
            except Exception as e:
                last_err, ok = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}, False
                break
            if int(j.get("code", -1)) != 0:
                last_err, ok = {"token": kind, "code": j.get("code"), "msg": j.get("msg")}, False
                break
            data = j.get("data") or {}
            items += [b for b in (data.get("items") or []) if isinstance(b, dict)]
            if not data.get("has_more"):
                break
            page = str(data.get("page_token") or "")
            if not page:
                break
        if ok:
            return items, {}
        logger.info("p0 p0docs list blocks via %s token failed: %s", kind, last_err)
    return [], last_err or {"code": "NO_TOKEN", "msg": "no usable token"}


def _p0_docx_block_text(b: Dict[str, Any]) -> Tuple[str, str]:
    """(kind, text) for a text-bearing block, else ("", "")."""
    for key in _P0_DOCX_TEXT_KEYS:
        body = b.get(key)
        if isinstance(body, dict) and isinstance(body.get("elements"), list):
            return key, "".join(
                (el.get("text_run") or {}).get("content") or ""
                for el in body["elements"] if isinstance(el, dict)
            )
    return "", ""


def _p0_docx_extract_text(items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for b in items:
        kind, txt = _p0_docx_block_text(b)
        if kind:
            out.append({"id": str(b.get("block_id") or ""), "text": txt, "kind": kind})
    return out


def _p0_docx_insert_after(document_id: str, items: List[Dict[str, Any]],
                          heading_contains: str, lines: List[str]) -> Tuple[int, Dict[str, Any]]:
    """Insert plain-text blocks right after the first block whose text contains ``heading_contains``.
    Returns (inserted_count, error)."""
    lines = [ln.strip() for ln in lines if (ln or "").strip()][:20]
    if not lines:
        return 0, {}
    by_id = {str(b.get("block_id") or ""): b for b in items}
    target = None
    for b in items:
        _k, txt = _p0_docx_block_text(b)
        if txt and heading_contains.lower() in txt.lower():
            target = b
            break
    if target is None:
        return 0, {"code": "NO_ANCHOR", "msg": f"no block containing {heading_contains!r}"}
    pid = str(target.get("parent_id") or "")
    parent = by_id.get(pid)
    kids = (parent or {}).get("children") or []
    tid = str(target.get("block_id") or "")
    idx = (kids.index(tid) + 1) if tid in kids else len(kids)
    body = {
        "index": idx,
        "children": [
            {"block_type": 2, "text": {"elements": [{"text_run": {"content": ln}}]}}
            for ln in lines
        ],
    }
    last_err: Dict[str, Any] = {}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.post(
                f"{_lark_api_domain()}/open-apis/docx/v1/documents/{document_id}/blocks/{pid}/children",
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
                params={"document_revision_id": -1},
                json=body,
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            return len(lines), {}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg")}
    return 0, last_err


def _p0_minutes_meta(minute_token: str) -> Dict[str, str]:
    """Best-effort meeting metadata from the Minutes info API (scope minutes:minutes.basic:read
    or minutes:minutes:readonly). Empty dict when unavailable — callers treat everything optional."""
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.get(
                f"{_lark_api_domain()}/open-apis/minutes/v1/minutes/{minute_token}",
                headers={"Authorization": f"Bearer {tok}"},
                timeout=30,
            )
            j = r.json()
        except Exception:
            continue
        if int(j.get("code", -1)) != 0:
            logger.info("p0 p0docs minutes meta via %s token failed: code=%s msg=%s",
                        kind, j.get("code"), j.get("msg"))
            continue
        m = (j.get("data") or {}).get("minute") or {}
        out: Dict[str, str] = {}
        try:
            ct = int(m.get("create_time") or 0) / 1000.0
            dur = int(m.get("duration") or 0) / 1000.0
            if ct > 0:
                out["meeting date"] = time.strftime("%Y/%m/%d", time.localtime(ct))
                out["meeting start time"] = time.strftime("%Y/%m/%d %H:%M", time.localtime(ct))
                if dur > 0:
                    out["meeting end time"] = time.strftime("%Y/%m/%d %H:%M", time.localtime(ct + dur))
                    out["meeting duration"] = f"{int(dur // 60)} min"
        except (TypeError, ValueError):
            pass
        if m.get("title"):
            out["meeting title"] = str(m["title"])
        if m.get("url"):
            out["meeting minutes/recording link"] = str(m["url"])
        return out
    return {}


def _p0_transcript_speaker_teams(transcript: str) -> List[str]:
    """Map transcript speakers to their teams via contacts.csv, e.g. 'Karen CRD = CRD-CS'."""
    names: List[str] = []
    seen: Set[str] = set()
    for ln in (transcript or "").splitlines():
        # tolerate a leading [HH:MM:SS] time marker before the speaker name
        m = re.match(r"^\s*(?:\[[0-9:]{4,10}\]\s*)?([^:：\[\n]{1,40})[:：]", ln)
        if m:
            n = m.group(1).strip()
            if n and n.lower() not in seen:
                seen.add(n.lower())
                names.append(n)
    entries, _ = _p0_contacts_load()
    out: List[str] = []
    for n in names[:30]:
        nl = n.lower()
        team = ""
        for cn, ct, _ph in entries:  # pass 1: exact name
            if (cn or "").strip().lower() == nl:
                team = ct
                break
        if not team:
            # pass 2: whole-word containment either way; short names (<4 chars, e.g. "Kh",
            # "YC") excluded — substring matching on those produces wrong teams.
            for cn, ct, _ph in entries:
                cn_s = (cn or "").strip()
                if len(cn_s) < 4:
                    continue
                pat = r"(?<!\w)" + re.escape(cn_s) + r"(?!\w)"
                rpat = r"(?<!\w)" + re.escape(n) + r"(?!\w)"
                if re.search(pat, n, re.IGNORECASE) or (len(n) >= 4 and re.search(rpat, cn_s, re.IGNORECASE)):
                    team = ct
                    break
        out.append(f"{n} = {team or 'unknown team'}")
    return out


def _p0_srt_timed_transcript(minute_token: str, start_epoch: float) -> str:
    """Speaker transcript with wall-clock [HH:MM:SS] markers from the SRT export; "" if unavailable.
    Relative offsets are used when the meeting start time is unknown."""
    raw, _err = _p0_minutes_export(
        minute_token,
        {"need_speaker": "true", "need_timestamp": "true", "file_format": "srt"},
        "srt(p0docs)",
    )
    if not raw:
        return ""
    turns = _p0_srt_turns(raw.decode("utf-8", "replace"))
    if not turns:
        return ""
    lines: List[str] = []
    for t in turns:
        if start_epoch:
            ts = time.strftime("%H:%M:%S", time.localtime(start_epoch + float(t["start"])))
        else:
            s = int(t["start"])
            ts = f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"
        lines.append(f"[{ts}] {t['speaker']}: {t['text']}")
    return "\n".join(lines)


def _p0_docx_find_sheet_after(items: List[Dict[str, Any]], heading_contains: str) -> str:
    """Combined token ('<spreadsheet>_<sheetid>') of the first sheet block after the heading."""
    seen_heading = False
    for b in items:
        if not seen_heading:
            _k, txt = _p0_docx_block_text(b)
            if txt and heading_contains.lower() in txt.lower():
                seen_heading = True
            continue
        try:
            btype = int(b.get("block_type") or 0)
        except (TypeError, ValueError):
            btype = 0
        if btype == 30:  # embedded sheet
            tok = str((b.get("sheet") or {}).get("token") or "")
            if tok:
                return tok
    return ""


def _p0_sheet_read_range(combined_token: str, a1: str) -> List[List[str]]:
    """Computed cell values (FormattedValue — formulas resolved) of a sheet range."""
    if "_" not in combined_token:
        return []
    stoken, sid = combined_token.split("_", 1)
    for _kind, tok in _p0_minutes_tokens():
        try:
            r = requests.get(
                f"{_lark_api_domain()}/open-apis/sheets/v2/spreadsheets/{stoken}/values/{sid}!{a1}",
                headers={"Authorization": f"Bearer {tok}"},
                params={"valueRenderOption": "FormattedValue"},
                timeout=30,
            )
            j = r.json()
        except Exception:
            continue
        if int(j.get("code", -1)) != 0:
            logger.info("p0 sheet read %s failed: code=%s msg=%s", a1, j.get("code"), j.get("msg"))
            continue
        rows = ((j.get("data") or {}).get("valueRange") or {}).get("values") or []
        out: List[List[str]] = []
        for row in rows:
            cells: List[str] = []
            for v in (row or []):
                if isinstance(v, list):  # rich-text cells arrive as segment lists
                    v = "".join(str((seg or {}).get("text") or "") for seg in v if isinstance(seg, dict))
                cells.append(str(v).strip() if v is not None else "")
            out.append(cells)
        return out
    return []


def _p0_sheet_read_col_a(combined_token: str, max_row: int = 30) -> List[str]:
    """Column-A values of a sheet (row order, 1-based offsetting left to caller)."""
    return [(r[0] if r else "") for r in _p0_sheet_read_range(combined_token, f"A1:A{max_row}")]


def _p0_col_letter(n: int) -> str:
    """1-based column index → A1 letter (1→A, 27→AA)."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


_p0_duty_sheet_cache: Dict[str, str] = {}


def _p0_duty_sheet_token() -> str:
    """Resolve the duty-roster wiki node to '<spreadsheet>_<sheetid>' (cached)."""
    wiki = _cfg_str("P0_DUTY_WIKI_TOKEN", "O4Dfw4DVTiPpFukn801l5z3WgMd").strip()
    sid = _cfg_str("P0_DUTY_SHEET_ID", "AS33r7").strip()
    if not wiki or not sid:
        return ""
    key = f"{wiki}:{sid}"
    cached = _p0_duty_sheet_cache.get(key)
    if cached:
        return cached
    try:
        j = _p0_lark_get_json("/open-apis/wiki/v2/spaces/get_node", {"token": wiki, "obj_type": "wiki"})
        n = (j.get("data") or {}).get("node") or {}
        if int(j.get("code", -1)) == 0 and n.get("obj_type") == "sheet" and n.get("obj_token"):
            combined = f"{n['obj_token']}_{sid}"
            _p0_duty_sheet_cache[key] = combined
            return combined
        logger.info("p0 duty roster node unusable: code=%s obj_type=%s", j.get("code"), n.get("obj_type"))
    except Exception:
        logger.exception("p0 duty roster resolve failed")
    return ""


def _p0_duty_on(start_epoch: float) -> Tuple[str, List[str]]:
    """(shift 'D'|'N', names on duty) from the roster at the given moment; ("", []) when unknown.

    Roster layout: month labels in row 1 (first column of each month), day-of-month in row 2,
    names in column A, one column per day with D/N/* marks. Day shift = 07:00-19:00; a start
    before 07:00 belongs to the PREVIOUS day's N column (that shift began 19:00 the day before).
    """
    if not start_epoch:
        return "", []
    lt = time.localtime(start_epoch)
    if lt.tm_hour < 7:
        target, shift = time.localtime(start_epoch - 86400), "N"
    elif lt.tm_hour < 19:
        target, shift = lt, "D"
    else:
        target, shift = lt, "N"
    tok = _p0_duty_sheet_token()
    if not tok:
        return shift, []
    hdr = _p0_sheet_read_range(tok, f"A1:{_p0_col_letter(400)}2")
    if len(hdr) < 2:
        return shift, []
    months, days = hdr[0], hdr[1]
    want_month = time.strftime("%B %Y", target).lower()
    cur = ""
    col = 0
    for i in range(1, max(len(months), len(days))):
        if i < len(months) and months[i]:
            cur = months[i]
        d = days[i] if i < len(days) else ""
        if cur.strip().lower() == want_month and d == str(target.tm_mday):
            col = i + 1  # 1-based
            break
    if not col:
        logger.info("p0 duty: no roster column for %s day %s", want_month, target.tm_mday)
        return shift, []
    letter = _p0_col_letter(col)
    names_col = _p0_sheet_read_range(tok, "A1:A300")
    duty_col = _p0_sheet_read_range(tok, f"{letter}1:{letter}300")
    names: List[str] = []
    for r_ in range(len(duty_col)):
        v = (duty_col[r_][0] if duty_col[r_] else "").upper()
        nm = names_col[r_][0] if (r_ < len(names_col) and names_col[r_]) else ""
        if nm and v == shift:
            names.append(nm)
    logger.info("p0 duty %s col=%s shift=%s -> %d names",
                time.strftime("%Y/%m/%d", target), letter, shift, len(names))
    return shift, names


def _p0_sheet_batch_write(combined_token: str, ranges: List[Tuple[str, List[List[str]]]]) -> Tuple[bool, Dict[str, Any]]:
    """Write multiple cell ranges of an embedded sheet in one call."""
    if "_" not in combined_token or not ranges:
        return False, {"code": "BAD_TOKEN", "msg": "no sheet token or nothing to write"}
    stoken, sid = combined_token.split("_", 1)
    body = {"valueRanges": [{"range": f"{sid}!{a1}", "values": vals} for a1, vals in ranges]}
    last_err: Dict[str, Any] = {"code": "NO_TOKEN", "msg": "no usable token"}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.post(
                f"{_lark_api_domain()}/open-apis/sheets/v2/spreadsheets/{stoken}/values_batch_update",
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
                json=body,
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            return True, {}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg")}
        logger.info("p0 p0docs sheet batch write via %s token failed: %s", kind, last_err)
    return False, last_err


def _p0_sheet_append_rows(combined_token: str, rows: List[List[str]]) -> Tuple[bool, Dict[str, Any]]:
    """Append rows to an embedded sheet (scope sheets:spreadsheet; doc must be editable)."""
    if "_" not in combined_token:
        return False, {"code": "BAD_TOKEN", "msg": f"unexpected sheet token {combined_token!r}"}
    stoken, sid = combined_token.split("_", 1)
    body = {"valueRange": {"range": f"{sid}!A:D", "values": rows}}
    last_err: Dict[str, Any] = {"code": "NO_TOKEN", "msg": "no usable token"}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.post(
                f"{_lark_api_domain()}/open-apis/sheets/v2/spreadsheets/{stoken}/values_append",
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
                json=body,
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            return True, {}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg")}
        logger.info("p0 p0docs sheet append via %s token failed: %s", kind, last_err)
    return False, last_err


def _p0_docx_patch_block(document_id: str, block_id: str, new_text: str) -> Tuple[bool, Dict[str, Any]]:
    body = {"update_text_elements": {"elements": [{"text_run": {"content": new_text}}]}}
    last_err: Dict[str, Any] = {}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.patch(
                f"{_lark_api_domain()}/open-apis/docx/v1/documents/{document_id}/blocks/{block_id}",
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
                params={"document_revision_id": -1},
                json=body,
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            return True, {}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg"), "http": r.status_code}
        logger.info("p0 p0docs patch block=%s via %s token failed http=%s body=%r",
                    block_id, kind, r.status_code, (r.text or "")[:200])
    return False, last_err


def _p0_docx_tick_todo(document_id: str, block_id: str) -> Tuple[bool, Dict[str, Any]]:
    """Tick a todo checkbox block (empirically verified: style.done via fields=[2])."""
    body = {"update_text_style": {"style": {"done": True}, "fields": [2]}}
    last_err: Dict[str, Any] = {"code": "NO_TOKEN", "msg": "no usable token"}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.patch(
                f"{_lark_api_domain()}/open-apis/docx/v1/documents/{document_id}/blocks/{block_id}",
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
                params={"document_revision_id": -1},
                json=body,
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            return True, {}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg"), "http": r.status_code}
    return False, last_err


def _p0_strip_template_tail(text: str, aggressive: bool) -> str:
    """Remove trailing template markers ('Template Copy', '副本', …) from a filled value.

    aggressive=True (title/page block) also strips bare trailing 'Template'/'Copy'; other blocks
    only lose the unambiguous combinations so real content ending in 'copy' isn't damaged."""
    extra = r"|template|copy" if aggressive else r""
    pat = re.compile(r"\s*[-–—]?\s*(template\s+copy|template\s*副本|副本" + extra + r")\s*$",
                     re.IGNORECASE)
    t = text or ""
    while True:
        t2 = pat.sub("", t)
        if t2 == t:
            return t.strip()
        t = t2


def _p0_p0docs_ai_updates(
    blocks: List[Dict[str, str]], transcript: str, meta: Dict[str, str],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]], List[str], str]:
    """([{id, text}], [{time, stage, event}], [{metric, time, duration}], categories, error)."""
    url = _p0_qa_ollama_url()
    model = _p0_qa_model()
    timeout = max(10.0, _cfg_float("P0_WHOTALK_QA_TIMEOUT_SECONDS",
                                   max(900.0, _cfg_float("P0_QA_TIMEOUT_SECONDS", 600.0))))
    num_ctx = max(2048, _cfg_int("P0_QA_NUM_CTX", 16384))
    cap = max(3000, _cfg_int("P0_P0DOCS_TRANSCRIPT_CHARS", 12000))
    tr = transcript or ""
    if len(tr) > cap:
        head = int(cap * 0.75)
        tr = tr[:head] + "\n…(中间省略/omitted)…\n" + tr[-(cap - head):]
    doc_lines = "\n".join(
        f"[{b['id']}] {b['text']}" for b in blocks if (b.get("id") and (b.get("text") or "").strip())
    )
    meta_lines = "\n".join(f"- {k}: {v}" for k, v in meta.items() if v)
    style = _cfg_str("P0_P0DOCS_PROMPT", "").strip() or (
        "You fill a P0 incident report document from a meeting transcript.\n"
        "INPUT 1 is the document: one line per block as `[block_id] current text`. Lines containing "
        "[placeholders], 'e.g.', 'Insert', or blanks after a label are fields to fill.\n"
        "INPUT 2 is known metadata. INPUT 3 is the raw meeting transcript (mixed Chinese/English, "
        "has speech-recognition errors).\n"
        "Rules:\n"
        "1) Output ONLY JSON: {\"updates\": [{\"id\": \"<block_id>\", \"text\": \"<full new text for that "
        "block>\"}], \"timeline\": [\"<entry>\", ...]}. The id must be copied EXACTLY from between the "
        "square brackets of INPUT 1, e.g. {\"updates\": [{\"id\": \"doxlgwNC3X\", \"text\": \"🕐 Start "
        "Time: 2026/07/12 21:05\"}], \"timeline\": [...]}.\n"
        "2) Fill every field the transcript or metadata answers, even partially (e.g. teams that were "
        "clearly involved, a summary of the issue discussed, the fix that was applied). Only omit a "
        "field when you truly have no information for it. Never invent names, numbers or links.\n"
        "3) ALWAYS fill when present: the '📹 Meeting Recording' line (metadata has the link), Start/End/"
        "Duration lines when metadata has meeting times, any [YYYY/MM/DD] date placeholder (metadata has "
        "the meeting date), the '📝 Issue Summary' line (summarize the transcript in 1-2 sentences), and "
        "the example line under '✅ Fix Summary' (the one starting 'Brief summary of resolution' — REPLACE "
        "it with the actual resolution, e.g. 'Switched traffic to a new frontend domain, cleared COS "
        "cache'), and the heading '🚨 P0 Incident Meeting Minutes Template' — rewrite it as "
        "'🚨 P0 Incident Meeting Minutes for <meeting date>' (e.g. '... for 2026/07/12'). When filling "
        "the document title, REMOVE template words like 'Template', 'Copy', '副本' from it.\n"
        "4) 'Teams Involved' = the union of the speakers' teams from metadata (skip 'unknown team'). "
        "'OSE On-duty': fill EXACTLY with the metadata value 'OSE on-duty roster' when that metadata is "
        "present; when absent, leave the line untouched. NEVER fill the 'Message Link' line.\n"
        "4b) 💥 Impact Assessment lines (受影响服务 / 受影响用户数 / 业务影响): fill with the meeting's "
        "best grounded estimate even when no exact number was stated — e.g. if CS reported feedback "
        "from several players, write '受影响用户数 : More than 4 players (per CS reports)'. An "
        "approximation grounded in the discussion beats leaving the placeholder; only skip when the "
        "meeting gives no basis at all.\n"
        "5) `text` replaces the whole block line: keep the field's label/emoji prefix and replace the "
        "placeholder part.\n"
        "6) \"timeline\": a chronological incident log, each entry {\"time\": \"HH:MM:SS\", "
        "\"stage\": \"<Detection|Investigation|Mitigation|Recovery|Closed or empty>\", "
        "\"event\": \"<who: what>\"}. Set \"stage\" ONLY when the event clearly belongs to that stage "
        "(first alert/report = Detection; diagnosing, checking logs, discussing cause = Investigation; "
        "executing a change/restart/rollback = Mitigation; QA test or monitoring confirmation = Recovery; "
        "wrap-up / meeting end = Closed) — otherwise use \"\". Include EVERY substantive item: each "
        "question AND its answer (name both people, e.g. \"Zora War asked whether tonight's event is "
        "affected; Reynold answered it's fixed once YK's change lands\"), findings, decisions, fix "
        "actions, verifications. EXCLUDE pure filler — greetings, OK/嗯/好的/thanks, repeats. Take the "
        "time from the [HH:MM:SS] marker of the source transcript line. A full meeting typically yields "
        "10-40 entries.\n"
        "7) \"metrics\": response-time metrics you can determine from the timed transcript, each "
        "{\"metric\": \"<TTD|TTR|TTE|TTM|TTF|Impact Duration>\", \"time\": \"<HH:MM:SS when that phase "
        "happened, or N/A>\", \"duration\": \"<e.g. 12 min>\"}. Definitions: TTD = problem occurred → "
        "first detected (if detected by alert use the alert time; if a customer reported first, time is "
        "N/A); TTR = detected → OSE responded / war room created; TTE = war room → escalated to the tech "
        "team; TTM = escalation → fix action started (an actual change, not discussion); TTF = war room "
        "start → issue resolved; Impact Duration = users first affected → service recovered. Include ONLY "
        "metrics the transcript supports. If the issue was NOT resolved by the end of the meeting, set the "
        "resolution-dependent metrics (TTF, Impact Duration) to time='N/A', duration='N/A'.\n"
        "8) \"categories\": the option(s) under 🔸 Categorization that clearly match the incident's "
        "affected function, copied EXACTLY as written in the document (e.g. [\"Deposit\"] for a deposit/"
        "bank error, [\"Promotion / Voucher\"] for a reward/free-spin issue). Usually 1, at most 2; empty "
        "list when no option clearly fits. The system ticks the checkbox — you only NAME the options.\n"
        "9) Keep the document's language style (labels stay as-is; filled values may be Chinese or English "
        "as the transcript dictates).\n"
        "10) Do not modify instructional lines (填写指引, Tip, Stage 标签说明) and do not put category "
        "options into \"updates\" — categories go only in the \"categories\" key."
    )
    user = (f"INPUT 1 — DOCUMENT BLOCKS:\n{doc_lines}\n\n"
            f"INPUT 2 — METADATA:\n{meta_lines or '-'}\n\n"
            f"INPUT 3 — TRANSCRIPT:\n{tr}")
    body: Dict[str, Any] = {
        "model": model,
        "stream": False,
        "format": "json",
        "messages": [{"role": "system", "content": style}, {"role": "user", "content": user}],
        "options": {"temperature": 0.1, "num_ctx": num_ctx},
        "think": False,
    }
    try:
        r = requests.post(f"{url}/api/chat", json=body, timeout=timeout)
        r.raise_for_status()
        raw = _monitoring_ai_extract_ollama_text(r.json())
    except Exception:
        logger.exception("p0 p0docs model call failed")
        return [], [], [], [], "模型调用失败/超时 / model call failed or timed out — check the journal"
    try:
        parsed = json.loads(raw)
    except Exception:
        logger.warning("p0 p0docs model output not JSON: %r", (raw or "")[:400])
        return [], [], [], [], "模型输出不是有效 JSON / model output was not valid JSON — see journal"
    categories: List[str] = []
    if isinstance(parsed, dict) and isinstance(parsed.get("categories"), list):
        categories = [str(c).strip() for c in parsed["categories"][:4] if str(c or "").strip()]
    metrics: List[Dict[str, str]] = []
    if isinstance(parsed, dict) and isinstance(parsed.get("metrics"), list):
        for m_ in parsed["metrics"][:10]:
            if isinstance(m_, dict):
                name = str(m_.get("metric") or "").strip()
                if name:
                    metrics.append({"metric": name, "time": str(m_.get("time") or "").strip(),
                                    "duration": str(m_.get("duration") or "").strip()})
    timeline: List[Dict[str, str]] = []
    if isinstance(parsed, dict) and isinstance(parsed.get("timeline"), list):
        for t in parsed["timeline"][:40]:
            if isinstance(t, dict):
                ev = str(t.get("event") or t.get("text") or "").strip()
                if ev:
                    timeline.append({"time": str(t.get("time") or "").strip(),
                                     "stage": str(t.get("stage") or "").strip(), "event": ev})
            elif isinstance(t, str) and t.strip():
                timeline.append({"time": "", "stage": "", "event": t.strip()})
    if isinstance(parsed, dict) and isinstance(parsed.get("updates"), list):
        ups = parsed["updates"]
    elif isinstance(parsed, list):
        ups = parsed
    elif isinstance(parsed, dict):
        # tolerate a plain {"<block_id>": "<text>"} mapping
        ups = [{"id": k, "text": v} for k, v in parsed.items()
               if isinstance(v, str) and k not in ("timeline", "updates", "metrics", "categories")]
    else:
        ups = []
    valid_ids = {b["id"]: b for b in blocks if b.get("id")}
    out: List[Dict[str, str]] = []
    seen: Set[str] = set()
    bad_ids: List[str] = []
    for u in ups:
        if not isinstance(u, dict):
            continue
        bid = str(u.get("id") or u.get("block_id") or "").strip().strip("[]")
        txt = str(u.get("text") or u.get("new_text") or "").strip()
        if not bid or not txt or bid in seen:
            continue
        if bid not in valid_ids:
            bad_ids.append(bid)
            continue
        if txt == (valid_ids[bid].get("text") or "").strip():
            continue  # no-op
        seen.add(bid)
        out.append({"id": bid, "text": txt[:2000]})
    logger.info("p0 p0docs model: %d updates returned, %d valid, %d unknown ids%s, %d timeline, "
                "%d metrics, %d categories; raw head=%r",
                len(ups), len(out), len(bad_ids),
                (f" (e.g. {bad_ids[:3]})" if bad_ids else ""), len(timeline), len(metrics),
                len(categories), (raw or "")[:300])
    if ups and not out and bad_ids and not timeline and not metrics and not categories:
        return [], [], [], [], (f"模型返回了 {len(ups)} 个更新，但 block id 都不匹配（如 {bad_ids[:2]}）/ model "
                                f"returned {len(ups)} updates but no block ids matched — see journal")
    return out, timeline, metrics, categories, ""


def _p0_p0docs_worker(chat_id: str, open_id: str, arg: str, mid: str, debounce_key: str) -> None:
    rt, rv = (
        ("chat_id", chat_id)
        if (chat_id or "").strip()
        else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    )
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None

    def _card(title: str, template: str, lines: List[str]) -> None:
        if rt and rv:
            _p0_meeting_send_card(rt, rv, "", template, title, lines)

    try:
        a = (arg or "").strip()
        # The doc target is the /wiki/ or /docx/ link; everything else is the meeting reference.
        mdoc = re.search(r"https?://\S*/(?:wiki|docx)/[A-Za-z0-9]+\S*", a)
        if not mdoc:
            _card("⚠️ /p0docs 用法 / usage", "red",
                  [f"`{_p0_p0docs_trigger()} <会议链接|9位会议号|妙记链接> <文档链接(/wiki/ 或 /docx/)>`",
                   "会议部分留空 = 最近一次机器人录制的会议 / empty meeting part = last bot-recorded meeting"])
            return
        doc_link = mdoc.group(0)
        meeting_arg = (a[:mdoc.start()] + " " + a[mdoc.end():]).strip()

        document_id, derr = _p0_doc_link_to_document_id(doc_link)
        if not document_id:
            _card("⚠️ 文档无法访问 / doc not accessible", "red", [derr])
            return

        token, err = _p0_whotalk_resolve_minute_token(meeting_arg)
        if not token:
            _card("⚠️ 会议无法解析 / meeting not resolved", "red", [err])
            return

        items, berr = _p0_docx_blocks_raw(document_id)
        blocks = _p0_docx_extract_text(items)
        if not blocks:
            _card("⚠️ 读取文档失败 / could not read doc", "red",
                  [f"`code={berr.get('code')}  msg={berr.get('msg')}`",
                   "应用需 docx 读取权限且文档已共享给应用 / grant docx read + share the doc with the app"])
            return

        host = ""
        mm = re.search(r"https?://([^/]+)/", doc_link)
        if mm:
            host = mm.group(1)
        meta = {
            "meeting minutes/recording link": (f"https://{host}/minutes/{token}" if host else ""),
            "meeting number": _p0_parse_meeting_no(meeting_arg),
            "today's date": time.strftime("%Y/%m/%d"),
        }
        meta.update(_p0_minutes_meta(token))  # real start/end/duration/title when scope allows
        start_epoch = 0.0
        try:
            if meta.get("meeting start time"):
                start_epoch = time.mktime(time.strptime(meta["meeting start time"], "%Y/%m/%d %H:%M"))
        except (ValueError, OverflowError):
            pass
        if start_epoch:
            _shift, _duty = _p0_duty_on(start_epoch)
            if _duty:
                meta["OSE on-duty roster"] = f"{', '.join(_duty)}（{_shift} 班 / {_shift} shift）"
        # Transcript for the model — timed, and heard LOCALLY when P0_P0DOCS_USE_LOCAL_ASR=1
        # (times from the SRT, text from the local engine). Falls back Lark-timed → Lark-plain.
        transcript = ""
        src = "Lark ASR"
        if _lark_env_truthy("P0_P0DOCS_USE_LOCAL_ASR") and _p0_whotalk_asr_enabled():
            transcript, aerr = _p0_whotalk_local_transcribe(token, with_times=True, start_epoch=start_epoch)
            if transcript:
                src = f"本地识别 local ASR ({_p0_whotalk_asr_engine()})"
            else:
                logger.warning("p0 p0docs local ASR failed — falling back to Lark transcript: %s", aerr)
        if not transcript:
            transcript = _p0_srt_timed_transcript(token, start_epoch)
        terr: Dict[str, Any] = {}
        if not transcript:
            transcript, terr = _p0_minutes_transcript(token)
        if not transcript:
            _card("⚠️ 无法取得转写 / transcript unavailable", "red",
                  [f"`code={terr.get('code')}  msg={terr.get('msg')}`",
                   "参考 /whotalk 的权限要求 / see /whotalk permission requirements"])
            return
        teams = _p0_transcript_speaker_teams(transcript)
        if teams:
            meta["speakers and their teams (from contact directory)"] = "; ".join(teams)
        if rt and rv:
            _lark_send_text_auto(rt, rv,
                                 f"📝 转写已取得（{len(transcript)} 字符，来源/source: {src}），"
                                 f"Qwen 正在填写文档（{len(blocks)} 行），请稍候…\n"
                                 f"Transcript fetched ({len(transcript)} chars via {src}) — Qwen is filling "
                                 f"the doc ({len(blocks)} blocks), please wait…")
        updates, timeline, metrics, categories, uerr = _p0_p0docs_ai_updates(blocks, transcript, meta)
        if uerr:
            _card("⚠️ 填写失败 / fill failed", "red", [uerr])
            return
        if not updates and not timeline and not metrics and not categories:
            _card("ℹ️ 没有可填写的内容 / nothing fillable", "blue",
                  ["模型没有从会议中找到可确定的字段。/ The model found no fields it could fill with confidence.",
                   "journal 里有模型原始输出 / the model's raw output is in the journal (`p0 p0docs model:`)"])
            return
        # Deterministic template-marker cleanup — the model keeps 'Template Copy' despite prompt
        # rules, so enforce in code: clean every filled value, and if the model didn't rewrite the
        # title at all, synthesize the cleanup ourselves.
        page_ids = {b["id"] for b in blocks if b.get("kind") == "page"}
        for u in updates:
            u["text"] = _p0_strip_template_tail(u["text"], aggressive=(u["id"] in page_ids))
        updates = [u for u in updates if u["text"]]
        for b in blocks:
            if b.get("kind") != "page":
                continue
            cur = (b.get("text") or "").strip()
            cleaned = _p0_strip_template_tail(cur, aggressive=True)
            if cleaned and cleaned != cur and not any(u["id"] == b["id"] for u in updates):
                updates.append({"id": b["id"], "text": cleaned})
            break

        okc, failc = 0, 0
        first_err: Dict[str, Any] = {}
        for u in updates:
            ok, perr = _p0_docx_patch_block(document_id, u["id"], u["text"])
            if ok:
                okc += 1
            else:
                failc += 1
                first_err = first_err or perr
            time.sleep(0.34)  # docx write QPS limit — bursting many patches gets throttled
        tl_count = 0
        tl_dup = 0
        if timeline:
            # Stage values must match the sheet's data-validation options EXACTLY — read live from
            # the template: emoji prefix included, and "Detection" has a REAL trailing space.
            # Anything unmapped is written blank so the dropdown never gets an invalid value.
            stage_label = {
                "detection": "🔴 Detection(发现问题) ",
                "investigation": "🟡 Investigation(调查原因)",
                "mitigation": "🟠 Mitigation(执行修复)",
                "recovery": "🔵 Recovery(验证恢复)",
                "closed": "✅ Closed(事件关闭)",
            }
            # Preferred: append real rows into the Incident Log embedded sheet (Time|Stage|Event|Attachment).
            sheet_tok = _p0_docx_find_sheet_after(items, "Incident Log")
            handled_in_sheet = False
            if sheet_tok:
                # Idempotency: a re-run on an already-filled doc must not duplicate rows.
                existing_times = {r[0] for r in _p0_sheet_read_range(sheet_tok, "A1:A300") if r and r[0]}
                fresh = [t for t in timeline if not (t.get("time") and t["time"] in existing_times)]
                tl_dup = len(timeline) - len(fresh)
                if tl_dup:
                    logger.info("p0 p0docs timeline: %d rows already in the sheet — skipped", tl_dup)
                if fresh:
                    rows = [[t.get("time", ""), stage_label.get((t.get("stage") or "").strip().lower(), ""),
                             t.get("event", ""), ""] for t in fresh]
                    ok, serr = _p0_sheet_append_rows(sheet_tok, rows)
                    if ok:
                        tl_count = len(rows)
                        handled_in_sheet = True
                    else:
                        logger.info("p0 p0docs sheet append failed (falling back to text lines): %s", serr)
                else:
                    handled_in_sheet = True  # everything already present — nothing to append
            if not handled_in_sheet:
                # Fallback: text lines under the heading.
                lines = []
                for t in timeline:
                    parts = [p for p in (f"[{t['time']}]" if t.get("time") else "",
                                         stage_label.get((t.get("stage") or "").strip().lower(), ""),
                                         t.get("event", "")) if p]
                    lines.append(" ".join(parts))
                tl_count, tlerr = _p0_docx_insert_after(document_id, items, "Incident Log", lines)
                if not tl_count:
                    logger.info("p0 p0docs timeline insert failed: %s", tlerr)
        # Response Metrics sheet: Time (col B) + Duration (col C) per metric row. Cells the model
        # can't determine become "N/A" (e.g. not yet resolved at meeting end) — but a cell that
        # already holds a value (manual entry or earlier fill) is NEVER overwritten.
        mt_count = 0
        msheet = _p0_docx_find_sheet_after(items, "Response Metrics")
        if msheet:
            canon = ("impact duration", "ttd", "ttr", "tte", "ttm", "ttf")

            def _canon_of(s: str) -> str:
                sl = (s or "").strip().lower()
                for k in canon:
                    if k in sl:
                        return k
                return ""

            by_metric: Dict[str, Dict[str, str]] = {}
            for m_ in metrics:
                k = _canon_of(m_.get("metric", ""))
                if k:
                    by_metric[k] = m_
            col_a = _p0_sheet_read_col_a(msheet)
            bc = _p0_sheet_read_range(msheet, f"B1:C{max(len(col_a), 10)}")
            ranges: List[Tuple[str, List[List[str]]]] = []
            for i, cell in enumerate(col_a, 1):
                k = _canon_of(cell)
                if not k:
                    continue
                cur_b = bc[i - 1][0] if i - 1 < len(bc) and len(bc[i - 1]) > 0 else ""
                cur_c = bc[i - 1][1] if i - 1 < len(bc) and len(bc[i - 1]) > 1 else ""
                m_ = by_metric.get(k, {})
                new_b = cur_b or (m_.get("time") or "").strip() or "N/A"
                new_c = cur_c or (m_.get("duration") or "").strip() or "N/A"
                if new_b != cur_b or new_c != cur_c:
                    ranges.append((f"B{i}:C{i}", [[new_b, new_c]]))
            if ranges:
                ok, merr2 = _p0_sheet_batch_write(msheet, ranges)
                if ok:
                    mt_count = len(ranges)
                else:
                    logger.info("p0 p0docs metrics write failed: %s", merr2)
        else:
            logger.info("p0 p0docs: no Response Metrics sheet found")
        # Categorization checkboxes: tick todo blocks matching the model's confident categories.
        cat_count = 0
        if categories:
            raw_by_id = {str(b.get("block_id") or ""): b for b in items}
            wanted = {c.lower() for c in categories}
            for blk in blocks:
                if blk.get("kind") != "todo" or (blk.get("text") or "").strip().lower() not in wanted:
                    continue
                raw_item = raw_by_id.get(blk["id"]) or {}
                if (((raw_item.get("todo") or {}).get("style")) or {}).get("done"):
                    continue  # already ticked — idempotent re-runs
                ok, cerr = _p0_docx_tick_todo(document_id, blk["id"])
                if ok:
                    cat_count += 1
                else:
                    logger.info("p0 p0docs tick %r failed: %s", blk.get("text"), cerr)
                time.sleep(0.34)
        logger.info("p0 p0docs done doc=%s filled=%d failed=%d timeline=%d (dup-skipped=%d) metrics=%d "
                    "categories=%d",
                    document_id[:12], okc, failc, tl_count, tl_dup, mt_count, cat_count)
        notes: List[str] = []
        if okc:
            notes.append(f"字段 **{okc}** 个 / **{okc}** fields")
        if tl_count:
            notes.append(f"时间线 **{tl_count}** 条 / **{tl_count}** timeline rows")
        if tl_dup:
            notes.append(f"时间线 {tl_dup} 条已存在（跳过）/ {tl_dup} timeline rows already present (skipped)")
        if mt_count:
            notes.append(f"指标 **{mt_count}** 项 / **{mt_count}** metrics")
        if cat_count:
            notes.append(f"分类勾选 **{cat_count}** 项 / **{cat_count}** categories ticked")
        wrote_any = bool(okc or tl_count or mt_count or cat_count)
        if failc and not wrote_any:
            # every write genuinely failed — permissions are the usual cause
            hint = ("应用需要 `docx:document`（编辑）权限并发布版本，且文档/知识库需以 **可编辑** 共享给应用。\n"
                    "Grant + publish the `docx:document` (edit) scope and share the doc/wiki with the app as EDITABLE.")
            _card("⚠️ 无法写入文档 / could not write doc", "red",
                  [f"`code={first_err.get('code')}  msg={first_err.get('msg')}  http={first_err.get('http')}`",
                   hint, "以下是生成的内容，可手动粘贴 / generated content below for manual paste:"])
            filled = "\n".join(f"{u['text']}" for u in updates)
            _p0_send_answer(rt, rv, f"{_p0_p0docs_trigger()} — 填写内容 / filled fields", filled)
        elif wrote_any:
            _card("✅ 文档已填写 / doc filled" if not failc else "⚠️ 部分填写 / partially filled",
                  "green" if not failc else "orange",
                  ["已写入：" + "，".join(notes) + "（不确定的字段保持原样）。/ Written: " + ", ".join(notes)
                   + " (unknown fields left untouched)."]
                  + ([f"失败 {failc}：`code={first_err.get('code')} msg={first_err.get('msg')}`"] if failc else [])
                  + [f"[打开文档 / open the doc]({doc_link})"])
        else:
            _card("ℹ️ 文档已是最新 / already up to date", "blue",
                  ["模型给出的值与文档现有内容相同，没有需要写入的改动 — 是否在同一份文档上重复运行了？"
                   + (f"（时间线 {tl_dup} 条已存在）" if tl_dup else ""),
                   "The model's values match what's already in the doc — nothing to write. Did you re-run "
                   "on an already-filled doc?" + (f" ({tl_dup} timeline rows already present)" if tl_dup else ""),
                   f"[打开文档 / open the doc]({doc_link})"])
    except Exception:
        logger.exception("p0 p0docs worker failed")
        _card("⚠️ 内部错误 / internal error", "red", ["请查看服务器日志 / check the server logs."])
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_p0docs(
    *,
    chat_id: str,
    open_id: str,
    clean: str,
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """Handle "/p0docs <meeting> <doc link>". Returns True when handled."""
    if not _p0_p0docs_enabled():
        return False
    body = _p0_command_body((clean or "").strip(), _p0_p0docs_trigger())
    if body is None:
        return False
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_p0docs__\n{(body or '')[:80]}"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)
    logger.info("p0 p0docs accepted arg=%r chat=%s", (body or "")[:80], bool(chat_id))
    try:
        threading.Thread(
            target=_p0_p0docs_worker,
            args=(chat_id or "", open_id or "", body or "", mid or "", debounce_key),
            daemon=True,
            name="p0-p0docs",
        ).start()
    except Exception:
        logger.exception("p0 p0docs worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


# ---------------------------------------------------------------------------
# p0bot — "/osemeeting": write the bilingual OSE / weekly meeting minutes doc
#
#   /osemeeting
#   <meeting link|9-digit no|minutes link>
#   <wiki/docx doc link>
#
# The two links may be given in either order (the doc is recognised by its
# /wiki/ or /docx/ path; everything else on the line is the meeting reference).
#
# Three models, one pass over one download:
#   * OpenAI ASR (whisper-1) hears the audio and returns timed segments; speaker
#     names come from the Minutes SRT and are matched onto those segments by time
#     overlap, so every line ends up as "[HH:MM:SS] Name: text".
#   * qwen2.5vl:3b watches the video — frames are sampled, near-duplicates are
#     dropped before the model sees them, and it decides which of the survivors
#     actually carry information (shared screens, dashboards, errors, configs).
#   * qwen3.6:35b-a3b turns the transcript + the frame captions into the doc's own
#     layout: the Overview table, then numbered discussion topics written twice —
#     once under "English Version", once under "中文版" — with the kept frames
#     embedded under the topic they belong to.
#
# Everything degrades instead of failing: no OpenAI key -> local ASR -> Lark's own
# text; no ffmpeg/video -> text-only minutes; a block that will not patch is
# counted and reported rather than aborting the run.
# ---------------------------------------------------------------------------

_P0_OSE_NUM_EMOJI = ["1️⃣", "2️⃣", "3️⃣", "4️⃣",
                     "5️⃣", "6️⃣", "7️⃣", "8️⃣",
                     "9️⃣", "\U0001f51f"]

# A heading that is a topic slot: "1️⃣ …", "1. …", "1) …", or a bare number.
_P0_OSE_NUMBERED_RE = re.compile(
    r"^\s*(?:([1-9]️?⃣|\U0001f51f)|([1-9][0-9]?)\s*[.)、]?)\s*"
)

# Section splitters inside the minutes doc.
_P0_OSE_EN_SECTION_RE = re.compile(r"english\s*version|^\s*english\s*$", re.IGNORECASE)
_P0_OSE_ZH_SECTION_RE = re.compile(r"中文版|中文版本|chinese\s*version", re.IGNORECASE)

# Overview-table labels -> which value the bot writes into the cell after them.
_P0_OSE_OVERVIEW_LABELS = (
    ("date", ("date", "日期", "會議日期", "会议日期")),
    ("participants", ("participants", "participant", "attendees", "参与人", "參與人",
                      "出席人员", "出席人員", "参加人", "參加人")),
    ("prepared_by", ("prepared by", "prepared", "记录人", "記錄人", "撰写人", "撰寫人",
                     "整理人")),
)


def _p0_ose_enabled() -> bool:
    return _lark_env_truthy_or_default("P0_OSEMEETING_ENABLE", default=True)


def _p0_ose_trigger() -> str:
    return _cfg_str("P0_OSEMEETING_TRIGGER", "/osemeeting").strip() or "/osemeeting"


def _p0_ose_openai_key() -> str:
    return (_cfg_str("P0_OSEMEETING_OPENAI_API_KEY", "").strip()
            or os.environ.get("OPENAI_API_KEY", "").strip())


def _p0_ose_writer_model() -> str:
    return _cfg_str("P0_OSEMEETING_WRITER_MODEL", "").strip() or _p0_qa_model()


def _p0_ose_hhmmss(seconds: float, start_epoch: float) -> str:
    """HH:MM:SS wall clock when the meeting start is known, else relative to the recording."""
    if start_epoch:
        return time.strftime("%H:%M:%S", time.localtime(start_epoch + max(0.0, seconds)))
    s = int(max(0.0, seconds))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def _p0_ffprobe_bin() -> str:
    """ffprobe sitting next to the configured ffmpeg, else PATH."""
    fb = _p0_ffmpeg_bin()
    if fb and os.path.sep in fb:
        cand = os.path.join(os.path.dirname(fb), "ffprobe")
        for c in (cand, cand + ".exe"):
            if os.path.isfile(c):
                return c
    return "ffprobe"


def _p0_ose_media_duration(media_path: str) -> float:
    """Duration in seconds; 0.0 when it cannot be determined."""
    try:
        proc = subprocess.run(
            [_p0_ffprobe_bin(), "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", media_path],
            capture_output=True, timeout=120,
        )
        if proc.returncode == 0:
            return max(0.0, float((proc.stdout or b"").decode("utf-8", "replace").strip()))
    except (subprocess.SubprocessError, OSError, ValueError):
        pass
    # ffprobe does not ship with every ffmpeg build — read it off ffmpeg's own banner instead.
    try:
        proc = subprocess.run([_p0_ffmpeg_bin(), "-i", media_path],
                              capture_output=True, timeout=120)
        m = re.search(r"Duration:\s*(\d+):(\d\d):(\d\d(?:\.\d+)?)",
                      (proc.stderr or b"").decode("utf-8", "replace"))
        if m:
            return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
    except (subprocess.SubprocessError, OSError, ValueError):
        pass
    return 0.0


def _p0_ose_download_media(minute_token: str, workdir: str) -> Tuple[str, str]:
    """(path to the downloaded recording, error). Tries the URL presigned, then with our tokens."""
    media_url, merr = _p0_minutes_media_url(minute_token)
    if not media_url:
        return "", (f"media download-url failed: code={merr.get('code')} msg={merr.get('msg')} "
                    "(scope minutes:minutes.media:export granted+published? re-/vcauth after adding it)")
    media_path = os.path.join(workdir, "media.bin")
    max_mb = _cfg_int("P0_WHOTALK_ASR_MAX_MEDIA_MB", 1024)
    auth_headers: List[Dict[str, str]] = [{}]
    auth_headers += [{"Authorization": f"Bearer {tok}"} for _kind, tok in _p0_minutes_tokens()]
    written, last_status = 0, "?"
    for hdrs in auth_headers:
        try:
            with requests.get(media_url, headers=hdrs, stream=True, timeout=180) as r:
                last_status = str(r.status_code)
                if r.status_code in (401, 403):
                    continue
                r.raise_for_status()
                clen = int(r.headers.get("Content-Length") or 0)
                if max_mb and clen and clen > max_mb * 1024 * 1024:
                    return "", f"recording too large ({clen // (1024 * 1024)} MB > {max_mb} MB limit)"
                written = 0
                with open(media_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        written += len(chunk)
                        if max_mb and written > max_mb * 1024 * 1024:
                            return "", f"recording exceeded the {max_mb} MB download limit"
        except Exception as e:
            last_status = e.__class__.__name__
            continue
        break
    if not written:
        return "", f"media download denied (HTTP {last_status}) with and without auth"
    logger.info("p0 osemeeting media downloaded: %d bytes", written)
    return media_path, ""


# ---- audio -> timed segments via OpenAI ASR ------------------------------------------------

def _p0_ose_openai_segments(media_path: str, workdir: str) -> Tuple[List[Dict[str, Any]], str]:
    """([{start, end, text}] in seconds from the start of the recording, error).

    The audio is re-encoded to low-bitrate mono mp3 and cut into chunks (one OpenAI request is
    capped at 25 MB), each chunk transcribed with per-segment timestamps and shifted back onto
    the recording's own timeline.
    """
    key = _p0_ose_openai_key()
    if not key:
        return [], ("no OpenAI API key — set P0_OSEMEETING_OPENAI_API_KEY (or OPENAI_API_KEY "
                    "in the environment)")
    base = (_cfg_str("P0_OSEMEETING_OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
            or "https://api.openai.com/v1").rstrip("/")
    model = _cfg_str("P0_OSEMEETING_OPENAI_ASR_MODEL", "whisper-1").strip() or "whisper-1"
    chunk_s = max(60, _cfg_int("P0_OSEMEETING_ASR_CHUNK_SECONDS", 600))
    bitrate = _cfg_str("P0_OSEMEETING_ASR_BITRATE", "32k").strip() or "32k"
    timeout = max(30.0, _cfg_float("P0_OSEMEETING_ASR_TIMEOUT_SECONDS", 300.0))
    lang = _cfg_str("P0_OSEMEETING_ASR_LANG", "").strip()
    prompt = _cfg_str("P0_OSEMEETING_ASR_PROMPT", "").strip()

    adir = os.path.join(workdir, "asr")
    os.makedirs(adir, exist_ok=True)
    try:
        proc = subprocess.run(
            [_p0_ffmpeg_bin(), "-y", "-i", media_path, "-vn", "-ac", "1", "-ar", "16000",
             "-b:a", bitrate, "-f", "segment", "-segment_time", str(chunk_s),
             "-reset_timestamps", "1", os.path.join(adir, "chunk_%04d.mp3")],
            capture_output=True, timeout=1800,
        )
    except subprocess.TimeoutExpired:
        return [], "ffmpeg timed out splitting the audio"
    chunks = sorted(f for f in os.listdir(adir) if f.startswith("chunk_") and f.endswith(".mp3"))
    if not chunks:
        tail = (proc.stderr or b"").decode("utf-8", "replace")[-300:]
        return [], f"ffmpeg produced no audio chunks (rc={proc.returncode}): {tail}"

    segments: List[Dict[str, Any]] = []
    t0 = time.time()
    for i, name in enumerate(chunks):
        path = os.path.join(adir, name)
        offset = float(i * chunk_s)
        j: Dict[str, Any] = {}
        last_err = ""
        # verbose_json first (it carries the timestamps); a model that rejects it gets a plain retry.
        for attempt in (0, 1):
            data: Dict[str, str] = {"model": model}
            if attempt == 0:
                data["response_format"] = "verbose_json"
                data["timestamp_granularities[]"] = "segment"
            else:
                data["response_format"] = "json"
            if lang:
                data["language"] = lang
            if prompt:
                data["prompt"] = prompt
            try:
                with open(path, "rb") as fh:
                    r = requests.post(
                        f"{base}/audio/transcriptions",
                        headers={"Authorization": f"Bearer {key}"},
                        files={"file": (name, fh, "audio/mpeg")},
                        data=data,
                        timeout=timeout,
                    )
            except Exception as e:
                last_err = f"{e.__class__.__name__}: {e}"
                continue
            if r.status_code == 200:
                try:
                    j = r.json()
                except ValueError:
                    last_err = "response was not JSON"
                    continue
                break
            last_err = f"HTTP {r.status_code}: {(r.text or '')[:200]}"
            # 400 usually means "this model does not support verbose_json" — worth the retry.
            if r.status_code != 400:
                break
        if not j:
            if segments:
                logger.warning("p0 osemeeting ASR chunk %s failed, keeping earlier text: %s",
                               name, last_err)
                continue
            return [], f"OpenAI ASR failed on the first chunk — {last_err}"
        segs = j.get("segments")
        if isinstance(segs, list) and segs:
            for s in segs:
                if not isinstance(s, dict):
                    continue
                txt = str(s.get("text") or "").strip()
                if not txt:
                    continue
                try:
                    st = float(s.get("start") or 0.0)
                    en = float(s.get("end") or st)
                except (TypeError, ValueError):
                    st, en = 0.0, 0.0
                segments.append({"start": offset + st, "end": offset + max(st, en), "text": txt})
        else:
            # No timestamps available (the json fallback): one segment spanning the whole chunk.
            txt = str(j.get("text") or "").strip()
            if txt:
                segments.append({"start": offset, "end": offset + chunk_s, "text": txt})
    if not segments:
        return [], "OpenAI ASR returned no text"
    segments.sort(key=lambda s: (s["start"], s["end"]))
    logger.info("p0 osemeeting OpenAI ASR (%s): %d chunks, %d segments, %.1fs compute",
                model, len(chunks), len(segments), time.time() - t0)
    return segments, ""


def _p0_ose_speakered(segments: List[Dict[str, Any]], turns: List[Dict[str, Any]],
                      start_epoch: float) -> str:
    """Attach speaker names from the SRT turns onto the ASR segments by time overlap.

    Consecutive segments from the same speaker are merged, so one person's paragraph stays one
    line — which reads far better in the minutes than one line per breath.
    """
    lines: List[str] = []
    state = {"spk": "", "txt": "", "start": 0.0}

    def _flush() -> None:
        if state["txt"].strip():
            who = state["spk"] or "Speaker"
            lines.append(f"[{_p0_ose_hhmmss(state['start'], start_epoch)}] {who}: "
                         f"{state['txt'].strip()}")

    for s in segments:
        st, en = float(s["start"]), float(s["end"])
        best, best_ov = "", 0.0
        for t in turns:
            ov = min(en, float(t["end"])) - max(st, float(t["start"]))
            if ov > best_ov:
                best_ov, best = ov, str(t.get("speaker") or "")
        if best in ("", "?"):
            best = state["spk"]  # unattributable sliver — keep it with whoever was talking
        if best == state["spk"] and state["txt"]:
            sep = "" if state["txt"].endswith(("。", "，", "、", "…", " ")) else " "
            state["txt"] = f"{state['txt']}{sep}{s['text']}"
        else:
            _flush()
            state["spk"], state["txt"], state["start"] = best, s["text"], st
    _flush()
    return "\n".join(lines)
# ---- video -> the frames worth keeping, via qwen2.5vl ---------------------------------------

def _p0_ose_sample_frames(media_path: str, workdir: str,
                          duration: float) -> Tuple[List[Dict[str, Any]], str]:
    """([{path, t}] distinct sampled frames, error).

    One ffmpeg pass writes both the JPEGs and a 16x16 grey thumbnail stream of the very same
    frames; the thumbnails are compared in numpy so a screen share that sits still for five
    minutes costs the vision model one look instead of fifteen. Frame *i* of the ``fps=1/N``
    stream is at t = i*N, which is where each kept frame's timestamp comes from.
    """
    interval = max(2, _cfg_int("P0_OSEMEETING_FRAME_INTERVAL_SECONDS", 20))
    width = max(320, _cfg_int("P0_OSEMEETING_FRAME_WIDTH", 1280))
    thresh = max(0.0, _cfg_float("P0_OSEMEETING_FRAME_DEDUPE_THRESHOLD", 3.0))
    cap = max(1, _cfg_int("P0_OSEMEETING_MAX_FRAMES", 120))

    fdir = os.path.join(workdir, "frames")
    os.makedirs(fdir, exist_ok=True)
    raw_path = os.path.join(fdir, "hash.raw")
    try:
        proc = subprocess.run(
            [_p0_ffmpeg_bin(), "-y", "-i", media_path,
             "-vf", f"fps=1/{interval},scale={width}:-2", "-q:v", "4",
             os.path.join(fdir, "f_%05d.jpg"),
             "-vf", f"fps=1/{interval},scale=16:16,format=gray",
             "-f", "rawvideo", raw_path],
            capture_output=True, timeout=1800,
        )
    except subprocess.TimeoutExpired:
        return [], "ffmpeg timed out extracting frames"
    jpgs = sorted(f for f in os.listdir(fdir) if f.startswith("f_") and f.endswith(".jpg"))
    if not jpgs:
        tail = (proc.stderr or b"").decode("utf-8", "replace")[-300:]
        return [], f"no frames extracted (rc={proc.returncode}) — audio-only recording? {tail}"

    # Dedupe on the grey thumbnails when we got them; otherwise keep every sampled frame.
    keep_idx: List[int] = []
    try:
        import numpy as np  # deferred: only needed when the vision pass runs

        buf = np.frombuffer(open(raw_path, "rb").read(), dtype=np.uint8)
        thumbs = buf[: (len(buf) // 256) * 256].reshape(-1, 256).astype(np.int16)
        last = None
        for i in range(min(len(thumbs), len(jpgs))):
            if last is None or float(np.abs(thumbs[i] - last).mean()) >= thresh:
                keep_idx.append(i)
                last = thumbs[i]
    except Exception as e:
        logger.info("p0 osemeeting frame dedupe unavailable (%s) — keeping every sample",
                    e.__class__.__name__)
        keep_idx = list(range(len(jpgs)))

    frames = [{"path": os.path.join(fdir, jpgs[i]), "t": float(i * interval)} for i in keep_idx]
    if duration > 0:
        frames = [f for f in frames if f["t"] <= duration + interval]
    dropped = len(jpgs) - len(frames)
    if len(frames) > cap:
        # Thin evenly rather than truncating, so the late half of the meeting is still represented.
        step = len(frames) / float(cap)
        frames = [frames[int(i * step)] for i in range(cap)]
        logger.info("p0 osemeeting frames thinned to the P0_OSEMEETING_MAX_FRAMES cap of %d", cap)
    logger.info("p0 osemeeting frames: %d sampled every %ds, %d near-duplicates dropped, "
                "%d going to %s", len(jpgs), interval, dropped, len(frames),
                _cfg_str("P0_OSEMEETING_VISION_MODEL", "qwen2.5vl:3b"))
    return frames, ""


def _p0_ose_vision_pick(frames: List[Dict[str, Any]],
                        start_epoch: float) -> List[Dict[str, Any]]:
    """Ask the vision model which sampled frames carry information; return the keepers captioned."""
    import base64

    url = _p0_qa_ollama_url()
    model = _cfg_str("P0_OSEMEETING_VISION_MODEL", "qwen2.5vl:3b").strip() or "qwen2.5vl:3b"
    timeout = max(15.0, _cfg_float("P0_OSEMEETING_VISION_TIMEOUT_SECONDS", 120.0))
    max_imgs = max(0, _cfg_int("P0_OSEMEETING_MAX_IMAGES", 8))
    prompt = _cfg_str("P0_OSEMEETING_VISION_PROMPT", "").strip() or (
        "This is one frame from the recording of an operations (OSE) team meeting.\n"
        "Decide whether the frame is worth keeping as evidence in the written meeting minutes.\n"
        "KEEP it when the frame shows information someone reading the minutes would want to see: "
        "a shared screen, a dashboard or graph, a monitoring/alert page, a log or terminal output, "
        "an error message, a configuration or admin page, a spreadsheet, a ticket, a document, a "
        "diagram, or a slide.\n"
        "DO NOT keep it when the frame is only people on camera, avatars or initials, a waiting/"
        "lobby screen, a blank or nearly blank screen, or a duplicate-looking idle desktop.\n"
        "Reply with ONLY JSON: {\"keep\": true|false, \"caption\": \"<one short factual line "
        "naming what is on screen, in English>\"}. When keep is false the caption may be empty. "
        "Never guess at text you cannot actually read in the image."
    )
    kept: List[Dict[str, Any]] = []
    t0 = time.time()
    looked = 0
    for fr in frames:
        if max_imgs and len(kept) >= max_imgs:
            logger.info("p0 osemeeting vision: hit the %d-image cap, stopping early", max_imgs)
            break
        try:
            with open(fr["path"], "rb") as fh:
                b64 = base64.b64encode(fh.read()).decode("ascii")
        except OSError:
            continue
        body: Dict[str, Any] = {
            "model": model,
            "stream": False,
            "format": "json",
            "messages": [{"role": "user", "content": prompt, "images": [b64]}],
            "options": {"temperature": 0},
        }
        try:
            r = requests.post(f"{url}/api/chat", json=body, timeout=timeout)
            r.raise_for_status()
            raw = _monitoring_ai_extract_ollama_text(r.json())
            looked += 1
        except Exception as e:
            logger.info("p0 osemeeting vision call failed at t=%ss: %s: %s",
                        int(fr["t"]), e.__class__.__name__, e)
            continue
        try:
            parsed = json.loads(raw or "{}")
        except ValueError:
            logger.info("p0 osemeeting vision output not JSON at t=%ss: %r",
                        int(fr["t"]), (raw or "")[:160])
            continue
        if not isinstance(parsed, dict) or not parsed.get("keep"):
            continue
        cap_txt = str(parsed.get("caption") or "").strip()[:300]
        kept.append({"path": fr["path"], "t": fr["t"],
                     "clock": _p0_ose_hhmmss(fr["t"], start_epoch),
                     "caption": cap_txt or "Screen shared during the meeting"})
    logger.info("p0 osemeeting vision (%s): looked at %d frames, kept %d, %.1fs",
                model, looked, len(kept), time.time() - t0)
    return kept


# ---- transcript + frames -> the minutes, via the writer model -------------------------------

def _p0_ose_ai_minutes(transcript: str, frames: List[Dict[str, Any]], meta: Dict[str, str],
                       topic_slots: int) -> Tuple[Dict[str, Any], str]:
    """(structured minutes, error). One call: bilingual topics + the Overview values."""
    url = _p0_qa_ollama_url()
    model = _p0_ose_writer_model()
    timeout = max(30.0, _cfg_float("P0_WHOTALK_QA_TIMEOUT_SECONDS",
                                   max(900.0, _cfg_float("P0_QA_TIMEOUT_SECONDS", 600.0))))
    num_ctx = max(4096, _cfg_int("P0_QA_NUM_CTX", 16384))
    max_topics = max(1, _cfg_int("P0_OSEMEETING_MAX_TOPICS", 10))
    cap = max(4000, _cfg_int("P0_OSEMEETING_TRANSCRIPT_CHARS", 24000))
    tr = transcript or ""
    if len(tr) > cap:
        head = int(cap * 0.7)
        tr = tr[:head] + "\n…(中间省略/omitted)…\n" + tr[-(cap - head):]
    shots = "\n".join(f"[{i}] {f['clock']} — {f['caption']}" for i, f in enumerate(frames)) or "-"
    meta_lines = "\n".join(f"- {k}: {v}" for k, v in meta.items() if v) or "-"
    style = _cfg_str("P0_OSEMEETING_PROMPT", "").strip() or (
        "You write the minutes of an OSE (operations) team meeting into a bilingual template.\n"
        "INPUT 1 is metadata. INPUT 2 is the timed, speaker-labelled transcript (mixed Chinese and "
        "English, with speech-recognition errors — read through them, do not copy them). INPUT 3 "
        "lists screenshots already captured from the recording, each with an index and a caption.\n"
        "Output ONLY JSON of this shape:\n"
        "{\"overview\": {\"date\": \"YYYY/MM/DD\", \"participants\": \"<names, comma separated>\"},\n"
        " \"topics\": [{\"en_title\": \"<short topic title>\",\n"
        "              \"en_bullets\": [\"<what was said/decided>\", ...],\n"
        "              \"zh_title\": \"<same title in 中文>\",\n"
        "              \"zh_bullets\": [\"<same content in 中文>\", ...],\n"
        "              \"frames\": [<INPUT 3 indexes that belong to this topic>]}]}\n"
        "Rules:\n"
        f"1) Write between 1 and {max_topics} topics, ordered as the meeting covered them. Group the "
        "discussion by SUBJECT, not by speaker — one topic per thing the team actually talked about.\n"
        "2) en_bullets and zh_bullets must be the SAME content in the two languages, bullet for "
        "bullet and in the same order. Each list holds 1-6 bullets. A bullet is one complete, "
        "self-contained sentence a reader who missed the meeting can act on.\n"
        "3) Record what matters: the problem or topic raised, who raised it, findings, numbers and "
        "dates that were stated, decisions taken, action items with their owner, and anything left "
        "open. Name people when the transcript names them.\n"
        "4) Never invent anything. No names, numbers, dates, links or owners that are not in the "
        "inputs. If the meeting left something undecided, write that it is still open.\n"
        "5) Leave out pure filler — greetings, OK/嗯/好的, small talk, repeats, sound checks.\n"
        "6) Titles are short noun phrases (2-8 words), no numbering and no trailing punctuation — "
        "the numbering is added by the system.\n"
        "7) \"frames\": only the indexes whose caption clearly relates to that topic; the same index "
        "must not appear under two topics, and an index that fits nothing is simply left out.\n"
        "8) overview.date: the meeting date from the metadata, formatted YYYY/MM/DD. "
        "overview.participants: the speakers actually heard in the transcript, comma separated; "
        "leave it \"\" when the transcript carries no names.\n"
        "9) Chinese text uses 汉字; keep technical terms, product names and commands in English "
        "inside the Chinese bullets too (e.g. 「重启 gateway 服务」)."
    )
    user = (f"INPUT 1 — METADATA:\n{meta_lines}\n\n"
            f"INPUT 3 — SCREENSHOTS AVAILABLE:\n{shots}\n\n"
            f"INPUT 2 — TRANSCRIPT:\n{tr}")
    body: Dict[str, Any] = {
        "model": model,
        "stream": False,
        "format": "json",
        "messages": [{"role": "system", "content": style}, {"role": "user", "content": user}],
        "options": {"temperature": 0.2, "num_ctx": num_ctx},
        "think": False,
    }
    try:
        r = requests.post(f"{url}/api/chat", json=body, timeout=timeout)
        r.raise_for_status()
        raw = _monitoring_ai_extract_ollama_text(r.json())
    except Exception:
        logger.exception("p0 osemeeting writer call failed")
        return {}, "模型调用失败/超时 / writer model call failed or timed out — check the journal"
    try:
        parsed = json.loads(raw or "{}")
    except ValueError:
        logger.warning("p0 osemeeting writer output not JSON: %r", (raw or "")[:400])
        return {}, "模型输出不是有效 JSON / writer output was not valid JSON — see journal"
    if not isinstance(parsed, dict):
        return {}, "模型输出不是对象 / writer output was not a JSON object"

    used: Set[int] = set()
    topics: List[Dict[str, Any]] = []
    for t in (parsed.get("topics") or [])[:max_topics]:
        if not isinstance(t, dict):
            continue
        en_b = [str(b).strip() for b in (t.get("en_bullets") or []) if str(b or "").strip()][:6]
        zh_b = [str(b).strip() for b in (t.get("zh_bullets") or []) if str(b or "").strip()][:6]
        en_t = str(t.get("en_title") or "").strip()
        zh_t = str(t.get("zh_title") or "").strip()
        if not (en_t or zh_t) or not (en_b or zh_b):
            continue
        idx: List[int] = []
        for n in (t.get("frames") or [])[:6]:
            try:
                k = int(n)
            except (TypeError, ValueError):
                continue
            if 0 <= k < len(frames) and k not in used:
                used.add(k)
                idx.append(k)
        topics.append({
            "en_title": en_t or zh_t, "zh_title": zh_t or en_t,
            "en_bullets": en_b or zh_b, "zh_bullets": zh_b or en_b, "frames": idx,
        })
    if not topics:
        return {}, "模型没有写出任何议题 / the writer produced no topics — see journal"
    ov = parsed.get("overview") if isinstance(parsed.get("overview"), dict) else {}
    out = {
        "overview": {"date": str((ov or {}).get("date") or "").strip(),
                     "participants": str((ov or {}).get("participants") or "").strip()},
        "topics": topics,
        "unplaced_frames": [i for i in range(len(frames)) if i not in used],
    }
    logger.info("p0 osemeeting writer (%s): %d topics, %d/%d screenshots placed; raw head=%r",
                model, len(topics), len(used), len(frames), (raw or "")[:300])
    if topic_slots and len(topics) > topic_slots:
        logger.info("p0 osemeeting: %d topics vs %d template slots — the extras get new headings",
                    len(topics), topic_slots)
    return out, ""
# ---- writing into the docx ------------------------------------------------------------------

def _p0_docx_create_children(document_id: str, parent_id: str, index: int,
                             children: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
    """(ids of the created blocks, error). ``index`` is the position in the parent's child list."""
    if not children:
        return [], {}
    body = {"index": max(0, index), "children": children}
    last_err: Dict[str, Any] = {"code": "NO_TOKEN", "msg": "no usable token"}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.post(
                f"{_lark_api_domain()}/open-apis/docx/v1/documents/{document_id}/blocks/{parent_id}/children",
                headers={"Authorization": f"Bearer {tok}",
                         "Content-Type": "application/json; charset=utf-8"},
                params={"document_revision_id": -1},
                json=body,
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            made = (j.get("data") or {}).get("children") or []
            return [str(b.get("block_id") or "") for b in made if isinstance(b, dict)], {}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg"), "http": r.status_code}
        logger.info("p0 osemeeting create children under %s via %s token failed http=%s body=%r",
                    parent_id, kind, r.status_code, (r.text or "")[:200])
    return [], last_err


def _p0_ose_text_block(kind: str, text: str) -> Dict[str, Any]:
    """A new-block payload for the text-bearing block types this command writes."""
    types = {"heading3": 5, "heading4": 6, "text": 2, "bullet": 12}
    return {"block_type": types.get(kind, 2),
            kind: {"elements": [{"text_run": {"content": text}}]}}


def _p0_docx_insert_image(document_id: str, parent_id: str, index: int,
                          jpg_path: str) -> Tuple[bool, Dict[str, Any]]:
    """Put one local image into the doc: empty image block -> upload the bytes -> point the block
    at the uploaded file. Needs the drive:drive scope on top of docx:document."""
    ids, err = _p0_docx_create_children(document_id, parent_id, index,
                                        [{"block_type": 27, "image": {"token": ""}}])
    if not ids or not ids[0]:
        return False, err or {"code": "NO_BLOCK", "msg": "image block not created"}
    img_block = ids[0]
    try:
        size = os.path.getsize(jpg_path)
        blob = open(jpg_path, "rb").read()
    except OSError as e:
        return False, {"code": -1, "msg": f"cannot read {jpg_path}: {e.__class__.__name__}"}
    name = os.path.basename(jpg_path)
    file_token, last_err = "", {"code": "NO_TOKEN", "msg": "no usable token"}
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.post(
                f"{_lark_api_domain()}/open-apis/drive/v1/medias/upload_all",
                headers={"Authorization": f"Bearer {tok}"},
                files={"file": (name, blob, "image/jpeg")},
                data={"file_name": name, "parent_type": "docx_image",
                      "parent_node": img_block, "size": str(size)},
                timeout=180,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            file_token = str(((j.get("data") or {}).get("file_token")) or "")
            if file_token:
                break
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg"), "http": r.status_code}
        logger.info("p0 osemeeting media upload via %s token failed http=%s body=%r",
                    kind, r.status_code, (r.text or "")[:200])
    if not file_token:
        return False, last_err
    for kind, tok in _p0_minutes_tokens():
        try:
            r = requests.patch(
                f"{_lark_api_domain()}/open-apis/docx/v1/documents/{document_id}/blocks/{img_block}",
                headers={"Authorization": f"Bearer {tok}",
                         "Content-Type": "application/json; charset=utf-8"},
                params={"document_revision_id": -1},
                json={"replace_image": {"token": file_token}},
                timeout=30,
            )
            j = r.json()
        except Exception as e:
            last_err = {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
            continue
        if int(j.get("code", -1)) == 0:
            return True, {}
        last_err = {"token": kind, "code": j.get("code"), "msg": j.get("msg")}
    return False, last_err


def _p0_ose_norm_label(text: str) -> str:
    return re.sub(r"[\s:：*]+", " ", (text or "")).strip().lower()


def _p0_ose_doc_layout(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Map the minutes template: the page block, the two language sections and their topic slots.

    Only TOP-LEVEL blocks decide section boundaries, because a heading's bullets are nested
    inside it — so the insertion point for a brand-new topic is an index into the page's own
    child list, never into the flat block list.
    """
    page = items[0] if items else {}
    page_id = str(page.get("block_id") or "")
    kids = [str(k) for k in (page.get("children") or [])]
    by_id = {str(b.get("block_id") or ""): b for b in items}
    sections: Dict[str, Dict[str, Any]] = {
        "en": {"slots": [], "insert_index": len(kids), "found": False},
        "zh": {"slots": [], "insert_index": len(kids), "found": False},
    }
    cur = ""
    for pos, bid in enumerate(kids):
        b = by_id.get(bid) or {}
        _kind, txt = _p0_docx_block_text(b)
        t = (txt or "").strip()
        if t and _P0_OSE_ZH_SECTION_RE.search(t):
            cur = "zh"
            sections["zh"].update({"found": True, "insert_index": pos + 1})
            continue
        if t and _P0_OSE_EN_SECTION_RE.search(t):
            cur = "en"
            sections["en"].update({"found": True, "insert_index": pos + 1})
            continue
        if not cur:
            continue
        # heading3/heading4 that is (or should be) a numbered topic slot
        if b.get("block_type") in (5, 6) and (not t or _P0_OSE_NUMBERED_RE.match(t)):
            sections[cur]["slots"].append(bid)
        # Trailing dividers and blank paragraphs are section furniture, not content — an appended
        # topic belongs above them, not below the horizontal rule that closes the section.
        if t or b.get("block_type") not in (2, 22):
            sections[cur]["insert_index"] = pos + 1
    # A doc with no language headings at all: treat the whole thing as the English section.
    if not sections["en"]["found"] and not sections["zh"]["found"]:
        slots: List[str] = []
        for bid in kids:
            b = by_id.get(bid) or {}
            if b.get("block_type") not in (5, 6):
                continue
            t = (_p0_docx_block_text(b)[1] or "").strip()
            if not t or _P0_OSE_NUMBERED_RE.match(t):
                slots.append(bid)
        sections["en"].update({"found": True, "slots": slots, "insert_index": len(kids)})
    return {"page_id": page_id, "page_children": kids, "by_id": by_id, "sections": sections}


def _p0_ose_fill_overview(document_id: str, items: List[Dict[str, Any]],
                          values: Dict[str, str]) -> Tuple[int, int]:
    """Patch the Overview table: each label cell's value is the next text block after it."""
    label_of: Dict[int, str] = {}
    texts: List[Tuple[int, str, str]] = []  # (position, block_id, text)
    for i, b in enumerate(items):
        kind, txt = _p0_docx_block_text(b)
        if not kind or kind == "page":
            continue
        texts.append((i, str(b.get("block_id") or ""), (txt or "").strip()))
    all_labels = {lbl for _key, lbls in _P0_OSE_OVERVIEW_LABELS for lbl in lbls}
    for n, (_i, _bid, txt) in enumerate(texts):
        norm = _p0_ose_norm_label(txt)
        for key, labels in _P0_OSE_OVERVIEW_LABELS:
            if norm in labels:
                label_of[n] = key
                break
    ok = bad = 0
    for n, key in label_of.items():
        want = (values.get(key) or "").strip()
        if not want or n + 1 >= len(texts):
            continue
        _ni, nbid, ntxt = texts[n + 1]
        if _p0_ose_norm_label(ntxt) in all_labels:
            continue  # the next block is another label — this label has no value cell
        if ntxt == want:
            continue
        good, err = _p0_docx_patch_block(document_id, nbid, want)
        if good:
            ok += 1
        else:
            bad += 1
            logger.info("p0 osemeeting overview %s -> %r failed: %s", key, want[:40], err)
    return ok, bad


def _p0_ose_write_section(document_id: str, layout: Dict[str, Any], lang: str,
                          topics: List[Dict[str, Any]], frames: List[Dict[str, Any]],
                          with_images: bool) -> Dict[str, int]:
    """Write one language section. Existing numbered slots are filled first, extra topics get
    fresh headings appended at the end of that section."""
    sec = layout["sections"][lang]
    by_id = layout["by_id"]
    tkey, bkey = (f"{lang}_title", f"{lang}_bullets")
    stat = {"headings": 0, "bullets": 0, "images": 0, "failed": 0}
    slots = list(sec["slots"])
    insert_at = int(sec["insert_index"])
    for n, topic in enumerate(topics):
        emoji = _P0_OSE_NUM_EMOJI[n] if n < len(_P0_OSE_NUM_EMOJI) else f"{n + 1}."
        title = f"{emoji} {str(topic.get(tkey) or '').strip()}".strip()
        bullets = [str(b).strip() for b in (topic.get(bkey) or []) if str(b or "").strip()]
        if n < len(slots):
            head_id = slots[n]
            good, err = _p0_docx_patch_block(document_id, head_id, title)
            if good:
                stat["headings"] += 1
            else:
                stat["failed"] += 1
                logger.info("p0 osemeeting %s heading %d patch failed: %s", lang, n + 1, err)
            existing = [str(k) for k in ((by_id.get(head_id) or {}).get("children") or [])]
        else:
            made, err = _p0_docx_create_children(
                document_id, layout["page_id"], insert_at,
                [_p0_ose_text_block("heading3", title)])
            if not made:
                stat["failed"] += 1
                logger.info("p0 osemeeting %s extra heading %d create failed: %s", lang, n + 1, err)
                continue
            head_id, existing = made[0], []
            insert_at += 1
            stat["headings"] += 1
        # Fill the bullet slots that already exist, then append whatever is left over.
        for i, text in enumerate(bullets):
            if i < len(existing):
                good, err = _p0_docx_patch_block(document_id, existing[i], text)
                if good:
                    stat["bullets"] += 1
                else:
                    stat["failed"] += 1
                    logger.info("p0 osemeeting %s bullet patch failed: %s", lang, err)
        leftover = bullets[len(existing):]
        if leftover:
            made, err = _p0_docx_create_children(
                document_id, head_id, len(existing),
                [_p0_ose_text_block("bullet", t) for t in leftover])
            if made:
                stat["bullets"] += len(made)
            else:
                stat["failed"] += len(leftover)
                logger.info("p0 osemeeting %s bullet create failed: %s", lang, err)
        if with_images:
            at = max(len(existing), len(bullets))
            for k in topic.get("frames") or []:
                if not (0 <= k < len(frames)):
                    continue
                fr = frames[k]
                cap_line = f"🖼️ {fr['clock']} — {fr['caption']}"
                made, _err = _p0_docx_create_children(
                    document_id, head_id, at, [_p0_ose_text_block("text", cap_line)])
                at += 1 if made else 0
                good, ierr = _p0_docx_insert_image(document_id, head_id, at, fr["path"])
                if good:
                    stat["images"] += 1
                    at += 1
                else:
                    stat["failed"] += 1
                    logger.info("p0 osemeeting image insert failed at %s: %s", fr["clock"], ierr)
    return stat


def _p0_ose_retitle(document_id: str, items: List[Dict[str, Any]], date_str: str) -> bool:
    """Stamp the meeting date onto the doc title and drop template words from it."""
    if not items or not date_str:
        return False
    page = items[0]
    kind, txt = _p0_docx_block_text(page)
    if kind != "page":
        return False
    base = re.sub(r"^\s*[\[\(【]\s*[\d/.\-年月日\s]{6,20}\s*[\]\)】]\s*", "", (txt or "").strip())
    base = _p0_strip_template_tail(base, True).strip() or "Meeting Minutes"
    want = f"[{date_str}] {base}"
    if want == (txt or "").strip():
        return False
    good, err = _p0_docx_patch_block(document_id, str(page.get("block_id") or ""), want)
    if not good:
        logger.info("p0 osemeeting title patch failed: %s", err)
    return good
# ---- the worker ------------------------------------------------------------------------------

def _p0_ose_transcript(minute_token: str, workdir: str,
                       start_epoch: float) -> Tuple[str, str, str, float, List[str]]:
    """(transcript, source label, media path, duration, per-stage failure notes).

    OpenAI ASR first (audio heard fresh, speaker names borrowed from the Minutes SRT), then the
    local engine, then Lark's own text. The media is downloaded once and handed back so the
    vision pass can reuse it instead of pulling the recording twice.

    Every stage that declines or fails appends a note. When the whole chain comes up empty those
    notes ARE the diagnosis, so they are handed to the caller for the error card instead of only
    going to the journal — "everything failed" on its own is not something anyone can act on.
    """
    provider = (_cfg_str("P0_OSEMEETING_ASR_PROVIDER", "openai").strip().lower() or "openai")
    notes: List[str] = []
    media_path, duration = "", 0.0
    if provider == "openai" and _p0_ose_openai_key():
        media_path, derr = _p0_ose_download_media(minute_token, workdir)
        if not media_path:
            logger.warning("p0 osemeeting recording download failed: %s", derr)
            notes.append(f"录像下载 / recording download: {derr}")
        else:
            duration = _p0_ose_media_duration(media_path)
            segments, aerr = _p0_ose_openai_segments(media_path, workdir)
            if segments:
                turns: List[Dict[str, Any]] = []
                srt_raw, serr = _p0_minutes_export(
                    minute_token,
                    {"need_speaker": "true", "need_timestamp": "true", "file_format": "srt"}, "srt")
                if srt_raw:
                    turns = _p0_srt_turns(srt_raw.decode("utf-8", "replace"))
                else:
                    logger.info("p0 osemeeting SRT export unavailable (%s) — no speaker names",
                                serr.get("msg"))
                named = [t for t in turns if (t.get("speaker") or "?") != "?"]
                if named:
                    return (_p0_ose_speakered(segments, turns, start_epoch),
                            f"OpenAI ASR + Minutes speaker names "
                            f"({_cfg_str('P0_OSEMEETING_OPENAI_ASR_MODEL', 'whisper-1')})",
                            media_path, duration, notes)
                # No speaker labels to borrow — still far better than nothing, just unattributed.
                lines = [f"[{_p0_ose_hhmmss(s['start'], start_epoch)}] {s['text']}"
                         for s in segments]
                return ("\n".join(lines),
                        f"OpenAI ASR, no speaker labels "
                        f"({_cfg_str('P0_OSEMEETING_OPENAI_ASR_MODEL', 'whisper-1')})",
                        media_path, duration, notes)
            logger.warning("p0 osemeeting OpenAI ASR failed — falling back: %s", aerr)
            notes.append(f"OpenAI ASR: {aerr}")
    elif provider == "openai":
        logger.info("p0 osemeeting: no OpenAI key configured — falling back to the local/Lark path")
        notes.append("OpenAI ASR: 未配置 API key / no API key "
                     "(P0_OSEMEETING_OPENAI_API_KEY or OPENAI_API_KEY)")
    else:
        notes.append(f"OpenAI ASR: 已按 P0_OSEMEETING_ASR_PROVIDER={provider} 跳过 / skipped")

    if provider in ("openai", "local") and _p0_whotalk_asr_enabled():
        text, aerr = _p0_whotalk_local_transcribe(minute_token, with_times=True,
                                                  start_epoch=start_epoch)
        if text:
            return (text, f"本地识别 local ASR ({_p0_whotalk_asr_engine()})",
                    media_path, duration, notes)
        logger.warning("p0 osemeeting local ASR failed: %s", aerr)
        notes.append(f"本地识别 local ASR: {aerr}")
    else:
        notes.append("本地识别 local ASR: 未启用 / disabled (P0_WHOTALK_ASR_ENABLE=0)")

    text = _p0_srt_timed_transcript(minute_token, start_epoch)
    if text:
        return text, "Lark ASR (timed)", media_path, duration, notes
    notes.append("Lark SRT 导出 / SRT export: 空 / empty "
                 "(需 scope minutes:minutes.transcript + /vcauth)")
    text, terr = _p0_minutes_transcript(minute_token)
    if text:
        return text, "Lark ASR", media_path, duration, notes
    logger.info("p0 osemeeting: no transcript at all: %s", terr)
    notes.append(f"Lark 转写 / transcript: code={terr.get('code')} msg={terr.get('msg')}")
    return "", "", media_path, duration, notes


def _p0_ose_worker(chat_id: str, open_id: str, arg: str, mid: str, debounce_key: str) -> None:
    rt, rv = (
        ("chat_id", chat_id)
        if (chat_id or "").strip()
        else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    )
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None
    workdir = ""

    def _card(title: str, template: str, lines: List[str]) -> None:
        if rt and rv:
            _p0_meeting_send_card(rt, rv, "", template, title, lines)

    def _say(text: str) -> None:
        if rt and rv:
            _lark_send_text_auto(rt, rv, text)

    try:
        a = (arg or "").strip()
        # Either order: the doc is the /wiki/ or /docx/ link, the rest is the meeting reference.
        mdoc = re.search(r"https?://\S*/(?:wiki|docx)/[A-Za-z0-9]+\S*", a)
        if not mdoc:
            _card("⚠️ /osemeeting 用法 / usage", "red",
                  [f"`{_p0_ose_trigger()}`", "`<会议链接|9位会议号|妙记链接>`", "`<文档链接 /wiki/ 或 /docx/>`",
                   "两行顺序可以互换 / the two links may be given in either order",
                   "会议部分留空 = 最近一次机器人录制的会议 / empty meeting part = last bot-recorded meeting"])
            return
        doc_link = mdoc.group(0)
        meeting_arg = (a[:mdoc.start()] + " " + a[mdoc.end():]).strip()

        document_id, derr = _p0_doc_link_to_document_id(doc_link)
        if not document_id:
            _card("⚠️ 文档无法访问 / doc not accessible", "red", [derr])
            return
        token, err = _p0_whotalk_resolve_minute_token(meeting_arg)
        if not token:
            _card("⚠️ 会议无法解析 / meeting not resolved", "red", [err])
            return
        items, berr = _p0_docx_blocks_raw(document_id)
        if not items:
            _card("⚠️ 读取文档失败 / could not read doc", "red",
                  [f"`code={berr.get('code')}  msg={berr.get('msg')}`",
                   "应用需 docx 编辑权限且文档已共享给应用 / grant docx edit + share the doc with the app"])
            return
        layout = _p0_ose_doc_layout(items)
        slots_en = len(layout["sections"]["en"]["slots"])
        slots_zh = len(layout["sections"]["zh"]["slots"])

        meta = dict(_p0_minutes_meta(token))
        host_m = re.search(r"https?://([^/]+)/", doc_link)
        if host_m and not meta.get("meeting minutes/recording link"):
            meta["meeting minutes/recording link"] = f"https://{host_m.group(1)}/minutes/{token}"
        meta.setdefault("meeting date", time.strftime("%Y/%m/%d"))
        start_epoch = 0.0
        try:
            if meta.get("meeting start time"):
                start_epoch = time.mktime(time.strptime(meta["meeting start time"], "%Y/%m/%d %H:%M"))
        except (ValueError, OverflowError):
            pass
        if start_epoch:
            _shift, duty = _p0_duty_on(start_epoch)
            if duty:
                meta["OSE on-duty roster"] = f"{', '.join(duty)}（{_shift} 班 / {_shift} shift）"

        workdir = tempfile.mkdtemp(prefix="p0ose_")
        _say(f"🎬 开始整理会议记录 / writing the minutes…\n"
             f"文档 doc: `{document_id}` · 妙记 minutes: `{token}`\n"
             f"模板槽位 template slots: EN {slots_en} · 中文 {slots_zh}")

        transcript, src, media_path, duration, tnotes = _p0_ose_transcript(
            token, workdir, start_epoch)
        if not transcript:
            _card("⚠️ 无法取得转写 / transcript unavailable", "red",
                  [f"妙记 minutes token: `{token}`"]
                  + [f"• {n}" for n in tnotes]
                  + [f"用同一个会议试 `{_p0_whotalk_trigger()} {meeting_arg or token}`——"
                     f"它也失败就是权限/会议的问题，不是本命令的问题。/ Try "
                     f"`{_p0_whotalk_trigger()} {meeting_arg or token}` on the same meeting: if that "
                     f"fails too the problem is the scopes or the meeting, not this command."])
            return
        teams = _p0_transcript_speaker_teams(transcript)
        if teams:
            meta["speakers and their teams (from contact directory)"] = "; ".join(teams)

        frames: List[Dict[str, Any]] = []
        vision_note = "关闭 off"
        if _lark_env_truthy_or_default("P0_OSEMEETING_VISION_ENABLE", default=True):
            if not media_path:
                media_path, mderr = _p0_ose_download_media(token, workdir)
                if not media_path:
                    vision_note = f"录像下载失败 recording download failed ({mderr[:80]})"
            if media_path:
                if duration <= 0:
                    duration = _p0_ose_media_duration(media_path)
                sampled, ferr = _p0_ose_sample_frames(media_path, workdir, duration)
                if sampled:
                    frames = _p0_ose_vision_pick(sampled, start_epoch)
                    vision_note = (f"{_cfg_str('P0_OSEMEETING_VISION_MODEL', 'qwen2.5vl:3b')} "
                                   f"看了 {len(sampled)} 帧，留下 {len(frames)} 张 / "
                                   f"reviewed {len(sampled)} frames, kept {len(frames)}")
                else:
                    vision_note = f"无法抽帧 no frames ({ferr[:80]})"

        _say(f"📝 转写完成 / transcript ready — {len(transcript)} 字符 chars，来源 source: {src}\n"
             f"🖼️ 画面 vision: {vision_note}\n"
             f"✍️ {_p0_ose_writer_model()} 正在撰写双语议题 / writing the bilingual topics…")

        minutes, uerr = _p0_ose_ai_minutes(transcript, frames, meta, max(slots_en, slots_zh))
        if uerr:
            _card("⚠️ 撰写失败 / writing failed", "red",
                  [uerr, "journal 里有模型原始输出 / raw model output is in the journal "
                         "(`p0 osemeeting writer:`)"])
            return
        topics = minutes["topics"]

        date_str = (minutes["overview"].get("date") or meta.get("meeting date") or "").strip()
        participants = (minutes["overview"].get("participants")
                        or ", ".join(t.split(" (")[0] for t in teams[:12])
                        or _cfg_str("P0_OSEMEETING_PARTICIPANTS_FALLBACK", "MY OSE").strip())
        ov_ok, ov_bad = _p0_ose_fill_overview(document_id, items, {
            "date": date_str,
            "participants": participants,
            "prepared_by": _cfg_str("P0_OSEMEETING_PREPARED_BY", "").strip(),
        })
        retitled = _p0_ose_retitle(document_id, items, date_str)

        # Images go in the English section only — the same screenshot twice in one doc reads as noise.
        # The sections are written BACK TO FRONT: a topic appended past the template's slots inserts
        # a top-level block, which shifts the page-child index of everything after it, so the later
        # section has to be finished while its recorded insertion point is still valid.
        blank = {"headings": 0, "bullets": 0, "images": 0, "failed": 0}
        done: Dict[str, Dict[str, int]] = {"en": dict(blank), "zh": dict(blank)}
        order = sorted((l for l in ("en", "zh") if layout["sections"][l]["found"]),
                       key=lambda l: layout["sections"][l]["insert_index"], reverse=True)
        for lang in order:
            done[lang] = _p0_ose_write_section(document_id, layout, lang, topics, frames,
                                               with_images=(lang == "en"))
        en, zh = done["en"], done["zh"]

        logger.info("p0 osemeeting done doc=%s topics=%d en=%s zh=%s overview=%d/%d images=%d "
                    "title=%s src=%r", document_id, len(topics), en, zh, ov_ok, ov_bad,
                    en["images"], retitled, src)
        lines = [
            f"**议题 topics**: {len(topics)}",
            f"**English**: {en['headings']} 标题 headings · {en['bullets']} 条 bullets · "
            f"{en['images']} 张图 images",
            (f"**中文**: {zh['headings']} 标题 headings · {zh['bullets']} 条 bullets"
             if layout["sections"]["zh"]["found"] else "**中文**: 文档里没有中文版区块 / no 中文版 section"),
            f"**Overview 表**: 填了 {ov_ok} 格 cells" + (f"（{ov_bad} 失败 failed）" if ov_bad else ""),
            f"**转写 transcript**: {len(transcript)} 字符 chars · {src}",
            f"**画面 vision**: {vision_note}",
        ]
        if minutes.get("unplaced_frames"):
            lines.append(f"**未归类截图 unplaced screenshots**: {len(minutes['unplaced_frames'])}"
                         "（模型认为不属于任何议题 / the writer matched them to no topic）")
        failed = en["failed"] + zh["failed"] + ov_bad
        if failed:
            lines.append(f"⚠️ {failed} 处写入失败 / {failed} writes failed — journal 有详情 / see journal")
        if retitled:
            lines.append(f"**标题 title**: 已加上日期 / date stamped ({date_str})")
        lines.append(f"[打开文档 / open the doc]({doc_link})")
        _card("✅ /osemeeting — 会议记录已写入 / minutes written",
              "orange" if failed else "green", lines)
    except Exception:
        logger.exception("p0 osemeeting worker failed")
        _card("⚠️ /osemeeting 失败 / failed", "red",
              ["内部错误，详见 journal / internal error — see the journal"])
    finally:
        if workdir:
            if _lark_env_truthy("P0_OSEMEETING_KEEP_MEDIA"):
                logger.info("p0 osemeeting work dir kept at %s (P0_OSEMEETING_KEEP_MEDIA=1)", workdir)
            else:
                shutil.rmtree(workdir, ignore_errors=True)
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_osemeeting(
    *,
    chat_id: str,
    open_id: str,
    clean: str,
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """Handle "/osemeeting <meeting> <doc link>" (either order). Returns True when handled."""
    if not _p0_ose_enabled():
        return False
    body = _p0_command_body((clean or "").strip(), _p0_ose_trigger())
    if body is None:
        return False
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_osemeeting__\n{(body or '')[:80]}"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)
    logger.info("p0 osemeeting accepted arg=%r chat=%s", (body or "")[:80], bool(chat_id))
    try:
        threading.Thread(
            target=_p0_ose_worker,
            args=(chat_id or "", open_id or "", body or "", mid or "", debounce_key),
            daemon=True,
            name="p0-osemeeting",
        ).start()
    except Exception:
        logger.exception("p0 osemeeting worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


# ---------------------------------------------------------------------------
# p0bot — group members ("/members")
#
# Lists who is in the CHAT GROUP (not the video call). Works with the bot's own
# tenant token + im:chat:readonly; the bot only needs to be a MEMBER of the group
# (it need not have created it). This is the "can a bot see who's in the group"
# capability — distinct from VC meeting participants (admin-only report).
# ---------------------------------------------------------------------------

def _p0_members_enabled() -> bool:
    return _lark_env_truthy("P0_MEMBERS_ENABLE")


def _p0_members_trigger() -> str:
    return _cfg_str("P0_MEMBERS_TRIGGER", "/members").strip() or "/members"


def _p0_chat_members(chat_id: str) -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    """(members, meta). members is None on API error; meta carries code/msg or member_total."""
    members: List[Dict[str, Any]] = []
    page = ""
    total: Any = None
    cap = max(20, _cfg_int("P0_MEMBERS_MAX_ROWS", 500))
    base = f"{_lark_api_domain()}/open-apis/im/v1/chats/{chat_id}/members"
    try:
        tok = _lark_tenant_access_token_string()
    except Exception as e:
        return None, {"code": -1, "msg": f"token error: {e.__class__.__name__}"}
    for _ in range(50):
        params: Dict[str, Any] = {"member_id_type": "open_id", "page_size": 100}
        if page:
            params["page_token"] = page
        try:
            r = requests.get(
                base,
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
                params=params,
                timeout=20,
            )
            j = r.json()
        except Exception as e:
            logger.exception("p0 members request failed chat=%s", chat_id[:12])
            return None, {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
        if not isinstance(j, dict):
            return None, {"code": f"HTTP {getattr(r, 'status_code', '?')}", "msg": "unexpected response"}
        code = int(j.get("code", -1) if str(j.get("code", "")).lstrip("-").isdigit() else -1)
        if code != 0:
            return None, {"code": j.get("code", code), "msg": j.get("msg") or str(j)}
        data = j.get("data") or {}
        for it in data.get("items") or []:
            if isinstance(it, dict):
                members.append(it)
        if data.get("member_total") is not None:
            total = data.get("member_total")
        if len(members) >= cap or not data.get("has_more"):
            break
        page = str(data.get("page_token") or "")
        if not page:
            break
    return members[:cap], {"member_total": total}


def _p0_members_worker(chat_id: str, open_id: str, mid: str, debounce_key: str) -> None:
    rt, rv = (
        ("chat_id", chat_id)
        if (chat_id or "").strip()
        else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    )
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None
    template = _cfg_str("P0_MEMBERS_CARD_TEMPLATE", "blue").strip() or "blue"

    def _card(title: str, tmpl: str, lines: List[str]) -> None:
        if not rt:
            return
        card = {
            "schema": "2.0",
            "config": {"update_multi": True, "wide_screen_mode": True},
            "header": {"template": tmpl, "title": {"tag": "plain_text", "content": title[:190]}},
            "body": {"elements": [{"tag": "markdown", "content": "\n".join(lines) if lines else "-"}]},
        }
        try:
            _lark_send_interactive_card(rt, rv, card)
        except Exception:
            logger.exception("p0 members card failed; text fallback")
            try:
                _lark_send_text_auto(rt, rv, f"{title}\n" + "\n".join(lines))
            except Exception:
                logger.exception("p0 members text fallback failed")

    try:
        if not (chat_id or "").strip():
            if rt:
                try:
                    _lark_send_text_auto(rt, rv, "请在群里发送 /members / run /members inside a group chat.")
                except Exception:
                    logger.exception("p0 members dm-notice failed")
            return
        rows, meta = _p0_chat_members(chat_id)
        if rows is None:
            code = meta.get("code")
            msg = meta.get("msg")
            hint = ""
            if str(code) == "232011" or "out of the chat" in str(msg).lower() or "not in" in str(msg).lower():
                hint = "\n提示：请先把机器人拉进本群 / add the bot to this group first."
            elif "permission" in str(msg).lower() or "access" in str(msg).lower() or "forbidden" in str(msg).lower():
                hint = "\n提示：应用需开通 im:chat:readonly 权限 / grant the app the im:chat:readonly scope."
            _card("⚠️ 查询失败 / Lookup failed", "red", [f"`code={code}  msg={msg}`{hint}"])
            return
        names: List[str] = []
        for r in rows:
            nm = _lark_dict_pick_str(r, "name")
            if not nm:
                oid = _lark_dict_pick_str(r, "member_id", "open_id", "openId")
                nm = f"用户 / user …{oid[-6:]}" if oid else "?"
            names.append(nm)
        total = meta.get("member_total") or len(rows)
        lines = [f"**共 {total} 人（不含机器人）/ {total} members (bots excluded)**", ""] + [f"• {n}" for n in names]
        _card("👥 群成员 / Group members", template, lines)
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_members(
    *,
    chat_id: str,
    open_id: str,
    clean: str,
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """Handle "/members" — list the current group's chat members. Returns True when handled."""
    if not _p0_members_enabled():
        return False
    if _p0_command_body((clean or "").strip(), _p0_members_trigger()) is None:
        return False
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_members__"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)
    logger.info("p0 members lookup accepted chat=%s", bool(chat_id))
    try:
        threading.Thread(
            target=_p0_members_worker,
            args=(chat_id or "", open_id or "", mid or "", debounce_key),
            daemon=True,
            name="p0-members",
        ).start()
    except Exception:
        logger.exception("p0 members worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


# ---------------------------------------------------------------------------
# p0bot — VC admin OAuth (user_access_token) for the /meeting report
#
# participant_list requires the caller's identity to hold the admin "Meeting
# Management" role, which the bot's tenant token lacks (121005). So an admin
# authorizes once via OAuth (/vcauth → open link → /vccode <code>); we store +
# auto-refresh their user_access_token and use it for the report. The code is
# copied from the browser address bar, so NO public server is required.
# ---------------------------------------------------------------------------

_p0_vc_tok_lock = threading.Lock()
_p0_vc_refresh_lock = threading.Lock()  # serialize refresh (refresh_token is single-use / rotating)
_p0_vc_state_lock = threading.Lock()
_p0_vc_states: Dict[str, float] = {}  # issued OAuth state -> created (monotonic); for callback CSRF


def _p0_vc_state_issue() -> str:
    s = os.urandom(12).hex()
    with _p0_vc_state_lock:
        now = time.monotonic()
        for k in [k for k, t in _p0_vc_states.items() if now - t > 600]:
            _p0_vc_states.pop(k, None)
        _p0_vc_states[s] = now
    return s


def _p0_vc_state_check(s: str) -> bool:
    """Consume a previously-issued state (single-use, 10-min TTL). False if unknown/expired."""
    s = (s or "").strip()
    if not s:
        return False
    with _p0_vc_state_lock:
        t = _p0_vc_states.pop(s, None)
    return t is not None and (time.monotonic() - t) <= 600


def _p0_vc_token_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".p0_vc_token.json")


def _p0_vc_oauth_scopes() -> str:
    return _cfg_str(
        "P0_VC_OAUTH_SCOPES",
        "vc:rooms.room.detailinfo:read offline_access contact:contact.base:readonly "
        "contact:user.employee_id:readonly minutes:minutes.transcript:export "
        "minutes:minutes.media:export",
    ).strip()


def _p0_vc_redirect_uri() -> str:
    return _cfg_str("P0_VC_REDIRECT_URI", "http://localhost:5088/oauth/callback").strip()


def _p0_vc_token_load() -> Dict[str, Any]:
    with _p0_vc_tok_lock:
        try:
            with open(_p0_vc_token_path(), "r", encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except FileNotFoundError:
            return {}
        except Exception:
            logger.exception("p0 vc token load failed")
            return {}


def _p0_vc_token_save(d: Dict[str, Any]) -> None:
    with _p0_vc_tok_lock:
        try:
            path = _p0_vc_token_path()
            tmp = path + ".tmp"
            # Create owner-only from the outset (post-hoc chmod is racy on POSIX and a no-op
            # on Windows). Fall back to a plain open on platforms without O_CREAT mode support.
            try:
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(d, f)
            except Exception:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(d, f)
            os.replace(tmp, path)
            try:
                os.chmod(path, 0o600)
            except OSError as e:
                logger.debug("p0 vc token chmod skipped (%s)", e)
        except Exception:
            logger.exception("p0 vc token save failed")


def _p0_vc_oauth_accounts_host() -> str:
    return "https://accounts.larksuite.com" if "larksuite" in (LARK_HOST or "") else "https://accounts.feishu.cn"


def _p0_vc_oauth_token_endpoint() -> str:
    return f"{(LARK_HOST or 'https://open.larksuite.com').rstrip('/')}/open-apis/authen/v2/oauth/token"


def _p0_vc_oauth_authorize_url() -> str:
    from urllib.parse import quote

    params = {
        "client_id": str(APP_ID or "").strip(),
        "redirect_uri": _p0_vc_redirect_uri(),
        "scope": _p0_vc_oauth_scopes(),
        "state": _p0_vc_state_issue(),
    }
    return f"{_p0_vc_oauth_accounts_host()}/open-apis/authen/v1/authorize?{urlencode(params, quote_via=quote)}"


def _p0_vc_oauth_store_response(j: Dict[str, Any]) -> None:
    now = time.time()
    d = _p0_vc_token_load()
    d["access_token"] = j.get("access_token") or ""
    d["expires_at"] = now + float(j.get("expires_in") or 0)
    if j.get("refresh_token"):
        d["refresh_token"] = j.get("refresh_token")
        rexp = j.get("refresh_token_expires_in")
        if rexp:
            d["refresh_expires_at"] = now + float(rexp)
        else:
            d.pop("refresh_expires_at", None)  # unknown → don't mark the fresh token pre-expired
    d["scope"] = j.get("scope") or d.get("scope", "")
    d["obtained_at"] = now
    _p0_vc_token_save(d)


def _p0_vc_oauth_post(body: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        r = requests.post(
            _p0_vc_oauth_token_endpoint(),
            json=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=30,
        )
        j = r.json()
    except Exception as e:
        logger.exception("p0 vc oauth token request failed")
        return False, f"network error: {e.__class__.__name__}: {e}"
    if not isinstance(j, dict):
        return False, f"unexpected token response type: {type(j).__name__}"
    if int(j.get("code", -1) if str(j.get("code", "")).lstrip("-").isdigit() else -1) != 0 or not j.get("access_token"):
        return False, f"code={j.get('code')} error={j.get('error')} desc={j.get('error_description') or j.get('msg')}"
    _p0_vc_oauth_store_response(j)
    return True, ("ok" if j.get("refresh_token") else "ok (no refresh_token — add offline_access scope to keep it logged in)")


def _p0_vc_oauth_exchange(code: str) -> Tuple[bool, str]:
    code = (code or "").strip()
    if not code:
        return False, "empty code"
    return _p0_vc_oauth_post(
        {
            "grant_type": "authorization_code",
            "client_id": str(APP_ID or "").strip(),
            "client_secret": str(APP_SECRET or "").strip(),
            "code": code,
            "redirect_uri": _p0_vc_redirect_uri(),
        }
    )


def _p0_vc_oauth_refresh() -> Tuple[bool, str]:
    d = _p0_vc_token_load()
    rt = d.get("refresh_token")
    if not rt:
        return False, "no refresh_token stored"
    if d.get("refresh_expires_at") and time.time() > float(d["refresh_expires_at"]):
        return False, "refresh_token expired — re-run /vcauth"
    return _p0_vc_oauth_post(
        {
            "grant_type": "refresh_token",
            "client_id": str(APP_ID or "").strip(),
            "client_secret": str(APP_SECRET or "").strip(),
            "refresh_token": rt,
        }
    )


def _p0_vc_user_access_token() -> Optional[str]:
    d = _p0_vc_token_load()
    at = d.get("access_token")
    exp = float(d.get("expires_at") or 0)
    if at and time.time() < (exp - 120):
        return at
    # Serialize refresh so concurrent callers don't each spend the single-use refresh_token.
    with _p0_vc_refresh_lock:
        d = _p0_vc_token_load()
        at = d.get("access_token")
        exp = float(d.get("expires_at") or 0)
        if at and time.time() < (exp - 120):
            return at  # another thread refreshed while we waited
        ok, _msg = _p0_vc_oauth_refresh()
        if ok:
            return _p0_vc_token_load().get("access_token") or None
        return None


def _p0_vc_admin_allowed(open_id: str) -> bool:
    allow = {
        p.strip()
        for p in re.split(r"[\s,;]+", _cfg_str("P0_VC_ADMIN_OPEN_IDS", "").strip())
        if p.strip()
    }
    if not allow:
        # Fail closed: an unconfigured allowlist must not let anyone authorize, since a stored
        # token is shared and a non-admin's token would clobber a working admin token (DoS).
        return False
    if "*" in allow:
        # Explicit opt-in to "anyone may authorize". Note the stored token is bot-wide and
        # last-writer-wins: whoever ran /vcauth most recently is the identity /whotalk uses.
        return True
    return (open_id or "").strip() in allow


def _p0_vcauth_worker(kind: str, arg: str, rt: str, rv: str, debounce_key: str) -> None:
    def _reply(text: str) -> None:
        try:
            if rt and rv:
                _lark_send_text_auto(rt, rv, text)
        except Exception:
            logger.exception("p0 vcauth reply failed")

    try:
        if kind == "auth":
            url = _p0_vc_oauth_authorize_url()
            _reply(
                "① 请【管理员】在浏览器打开下面链接并登录授权"
                "（该管理员需拥有『视频会议 · 会议管理』后台权限）：\n"
                "Open this link in a browser and authorize as an ADMIN who holds the VC "
                "'Meeting Management' role:\n"
                f"{url}\n\n"
                "② 授权后浏览器会跳转到一个地址（可能打不开，没关系）。从地址栏复制其中的 "
                "code=… 值，然后发给我：\n"
                "After consent the browser lands on a redirect URL (it may fail to load — that's "
                "fine). Copy the code=… from the address bar and send it back:\n"
                "/vccode <粘贴 code 或整个跳转网址 / paste the code, or the whole redirected URL>"
            )
        else:  # kind == "code"
            code = (arg or "").strip()
            m = re.search(r"code=([^&\s]+)", code)
            if m:
                code = m.group(1)
            ok, msg = _p0_vc_oauth_exchange(code)
            if ok:
                _reply(
                    "✅ 授权成功，已保存管理员令牌。现在可用 /meeting <会议号> 查询参会情况。\n"
                    f"Authorized — admin token stored ({msg}). Now try /meeting <number>."
                )
            else:
                _reply(
                    "⚠️ 授权失败 / Authorization failed:\n"
                    f"{msg}\n"
                    "code 有效期仅 5 分钟且只能用一次；请重新 /vcauth 获取新链接后再试。\n"
                    "The code is valid 5 min and single-use — run /vcauth again for a fresh link."
                )
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_vcauth(
    *,
    chat_id: str,
    open_id: str,
    clean: str,
    mid: str,
    im_event_id: str,
    sender_debounce: str,
    msg_time: str,
) -> bool:
    """Handle /vcauth and /vccode <code>. Returns True when handled."""
    if not _p0_meeting_enabled():
        return False
    text = (clean or "").strip()
    is_auth = _p0_command_body(text, "/vcauth") is not None
    code_body = _p0_command_body(text, "/vccode")
    if not is_auth and code_body is None:
        return False

    rt, rv = (
        ("chat_id", chat_id)
        if (chat_id or "").strip()
        else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    )
    kind = "auth" if is_auth else "code"
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    # Key /vccode on the code so two distinct codes don't collide; /vcauth on chat only.
    if kind == "code":
        debounce_key = f"{(chat_id or '').strip()}\n__p0_vcauth_code__\n{(code_body or '').strip()[:120]}"
    else:
        debounce_key = f"{(chat_id or '').strip()}\n__p0_vcauth_auth__"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    if not _p0_vc_admin_allowed(open_id):
        def _deny() -> None:
            try:
                if rt and rv:
                    _lark_send_text_auto(
                        rt,
                        rv,
                        "仅授权管理员可执行此命令 / restricted to configured admins.\n"
                        f"你的 open_id / your open_id: {open_id or '(unknown)'}\n"
                        "把它加入 .env 的 P0_VC_ADMIN_OPEN_IDS 后重启即可 / add it to "
                        "P0_VC_ADMIN_OPEN_IDS in .env and restart.",
                    )
            except Exception:
                logger.exception("p0 vcauth deny reply failed")
            finally:
                with _monitoring_reply_dispatch_lock:
                    _monitoring_inflight_keys.discard(debounce_key)

        try:
            threading.Thread(target=_deny, daemon=True, name="p0-vcauth-deny").start()
        except Exception:
            logger.exception("p0 vcauth deny thread failed to start")
            with _monitoring_reply_dispatch_lock:
                _monitoring_inflight_keys.discard(debounce_key)
        return True

    arg = "" if is_auth else (code_body or "")
    try:
        threading.Thread(
            target=_p0_vcauth_worker,
            args=(kind, arg, rt, rv, debounce_key),
            daemon=True,
            name="p0-vcauth",
        ).start()
    except Exception:
        logger.exception("p0 vcauth worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


# ---------------------------------------------------------------------------
# p0bot — bot-hosted meeting (/openmeeting)
#
# The bot reserves a meeting it "owns" (owner_id = a real user), auto-records,
# assigns that user as host, posts the join link, and — because Open-API-reserved
# meetings emit participant events — announces joins/leaves live over the WS. When
# the meeting ends (host ends it in-client, or /endmeeting via the host's /vcauth
# token) the recording link is DM'd to the host (who owns it). Live events work
# with the bot's tenant token; only /endmeeting needs a host user token.
# ---------------------------------------------------------------------------

_p0_om_lock = threading.Lock()
_p0_om_active: Dict[str, Dict[str, Any]] = {}  # meeting_no -> {reserve_id, meeting_id, chat_id, topic, present:set}
# Last bot meeting whose recording became ready — lets a bare /whotalk target "the last meeting".
_p0_whotalk_last: Dict[str, Any] = {}  # {"meeting_no", "meeting_id", "url", "ts"}
_p0_name_cache: Dict[str, str] = {}
_p0_name_cache_lock = threading.Lock()
_p0_contact_warned: set = set()  # dedup keys so a scope/permission problem is logged once, not per-join
_p0_contact_warned_lock = threading.Lock()
_p0_chat_name_cache: Dict[str, Tuple[float, Dict[str, str]]] = {}  # chat_id -> (fetched_at, {open_id: name})
_p0_chat_name_cache_lock = threading.Lock()

_P0_OM_USER_TYPE_LABEL = {"2": "会议室/Room", "6": "电话/Phone", "7": "SIP"}


def _p0_contact_warn_once(key: str, msg: str, *args: Any) -> None:
    """Emit a WARNING at most once per distinct key (avoids one line per meeting join)."""
    with _p0_contact_warned_lock:
        if key in _p0_contact_warned:
            return
        if len(_p0_contact_warned) > 200:
            _p0_contact_warned.clear()
        _p0_contact_warned.add(key)
    logger.warning(msg, *args)


def _p0_ws_ignore_event(ce: Any) -> None:
    """No-op sink for subscribed events the bot intentionally doesn't act on.

    lark-oapi logs an ERROR ('handle message failed … processor not found') for every event
    type it receives without a registered processor. The bot adds its own ACK/DONE reactions
    (which echo back as im.message.reaction.* events) and may be subscribed to
    vc.meeting.recording_started_v1; registering this no-op keeps the journal clean. To stop
    receiving them entirely, unsubscribe the event in the Developer Console.
    """
    return None


def _p0_om_enabled() -> bool:
    return _lark_env_truthy("P0_OPENMEETING_ENABLE")


def _p0_om_open_trigger() -> str:
    return _cfg_str("P0_OPENMEETING_TRIGGER", "/openmeeting").strip() or "/openmeeting"


def _p0_om_end_trigger() -> str:
    return _cfg_str("P0_ENDMEETING_TRIGGER", "/endmeeting").strip() or "/endmeeting"


def _p0_checkmeeting_trigger() -> str:
    return _cfg_str("P0_CHECKMEETING_TRIGGER", "/checkmeeting").strip() or "/checkmeeting"


def _p0_om_host_open_id() -> str:
    return _cfg_str("P0_MEETING_HOST_OPEN_ID", "").strip()


def _p0_om_allowed(open_id: str) -> bool:
    allow = {
        p.strip()
        for p in re.split(r"[\s,;]+", _cfg_str("P0_OPENMEETING_ALLOWED_OPEN_IDS", "").strip())
        if p.strip()
    }
    if not allow:
        return True  # openmeeting just creates a meeting link; low-risk to leave open
    return (open_id or "").strip() in allow


def _p0_contact_name(user_id: str, id_type: str = "open_id") -> str:
    """Resolve a Lark user's display name from an id. ``id_type`` ∈ open_id|user_id|union_id.

    Returns "" on any failure; the *reason* (permission vs. out-of-scope) is logged once at
    WARNING so it is visible in the journal instead of being swallowed silently.
    """
    uid = (user_id or "").strip()
    if not uid:
        return ""
    it_type = (id_type or "open_id").strip() or "open_id"
    cache_key = f"{it_type}:{uid}"
    with _p0_name_cache_lock:
        if cache_key in _p0_name_cache:
            return _p0_name_cache[cache_key]
    name = ""
    try:
        tok = _lark_tenant_access_token_string()
        r = requests.get(
            f"{_lark_api_domain()}/open-apis/contact/v3/users/batch",
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
            params=[("user_ids", uid), ("user_id_type", it_type)],
            timeout=15,
        )
        j = r.json()
        code = int(j.get("code", -1)) if isinstance(j, dict) and str(j.get("code", "")).lstrip("-").isdigit() else -1
        if isinstance(j, dict) and code == 0:
            items = (j.get("data") or {}).get("items") or []
            for it in items:
                if isinstance(it, dict):
                    name = _lark_dict_pick_str(it, "name", "en_name", "enName", "nickname")
                    if name:
                        break
            if not name:
                _p0_contact_warn_once(
                    f"noname:{it_type}",
                    "p0 contact name: API returned ok (code=0) but no name for %s=%s (items=%d). The app can "
                    "call contact but cannot see this user — add scope 'contact:user.base:readonly', put the "
                    "user inside the app's availability scope (通讯录权限范围/可用范围), and PUBLISH a new version.",
                    it_type, uid[:14], len(items),
                )
        elif isinstance(j, dict):
            _p0_contact_warn_once(
                f"code:{j.get('code')}",
                "p0 contact name lookup failed code=%s msg=%s — grant 'contact:contact.base:readonly' + "
                "'contact:user.base:readonly' and PUBLISH a version to show real names instead of open_id.",
                j.get("code"), j.get("msg"),
            )
    except Exception:
        logger.debug("p0 contact name lookup failed for %s", uid[:10])
    if name:
        with _p0_name_cache_lock:
            _p0_name_cache[cache_key] = name
            if len(_p0_name_cache) > 2000:
                _p0_name_cache.clear()
    return name


def _p0_chat_name_map(chat_id: str) -> Dict[str, str]:
    """open_id -> display name for a chat's members (uses im:chat:readonly; cached ~5 min).

    This lets meeting join/leave announcements show real names WITHOUT any Contacts scope, as long
    as the participant is a member of the announce chat — the bot already has im:chat:readonly.
    """
    cid = (chat_id or "").strip()
    if not cid:
        return {}
    now = time.time()
    with _p0_chat_name_cache_lock:
        hit = _p0_chat_name_cache.get(cid)
        if hit and (now - hit[0]) < 300:
            return hit[1]
    mapping: Dict[str, str] = {}
    try:
        rows, _meta = _p0_chat_members(cid)
        for it in rows or []:
            oid = _lark_dict_pick_str(it, "member_id", "memberId", "open_id", "openId")
            nm = _lark_dict_pick_str(it, "name")
            if oid and nm:
                mapping[oid] = nm
    except Exception:
        logger.debug("p0 chat name map failed for %s", cid[:12])
    with _p0_chat_name_cache_lock:
        _p0_chat_name_cache[cid] = (now, mapping)
        if len(_p0_chat_name_cache) > 100:
            _p0_chat_name_cache.clear()
    return mapping


def _p0_om_display(op: Dict[str, Any], chat_id: str = "") -> str:
    idobj = op.get("id") if isinstance(op.get("id"), dict) else {}
    oid = _lark_dict_pick_str(idobj, "open_id", "openId") or _lark_dict_pick_str(op, "open_id", "openId")
    uuid_ = _lark_dict_pick_str(idobj, "user_id", "userId") or _lark_dict_pick_str(op, "user_id", "userId")
    unid = _lark_dict_pick_str(idobj, "union_id", "unionId") or _lark_dict_pick_str(op, "union_id", "unionId")
    # 1) Contacts API — needs a contact scope (contact:user.base:readonly) + the user inside the
    #    app's availability range + a PUBLISHED version. Try whichever id the event carries.
    for _id, _t in ((oid, "open_id"), (uuid_, "user_id"), (unid, "union_id")):
        if _id:
            nm = _p0_contact_name(_id, _t)
            if nm:
                return nm
    # 2) Fallback: announce-chat membership — only needs im:chat:readonly (already granted), no
    #    contact scope and no app re-publish. Works when the participant is in the announce chat.
    if oid and chat_id:
        nm = _p0_chat_name_map(chat_id).get(oid)
        if nm:
            return nm
    ut = str(op.get("user_type") if op.get("user_type") is not None else op.get("userType") or "").strip()
    tail = oid or uuid_ or unid
    return _P0_OM_USER_TYPE_LABEL.get(ut) or (f"用户/user …{tail[-6:]}" if tail else "someone")


def _p0_om_op_open_id(op: Dict[str, Any]) -> str:
    idobj = op.get("id") if isinstance(op.get("id"), dict) else {}
    return _lark_dict_pick_str(idobj, "open_id", "openId") or _lark_dict_pick_str(op, "open_id", "openId")


def _p0_om_op_key(op: Dict[str, Any]) -> str:
    """Stable dedup key for a participant, even rooms/phone/SIP that carry no open_id."""
    idobj = op.get("id") if isinstance(op.get("id"), dict) else {}
    for k in ("open_id", "openId", "union_id", "unionId", "user_id", "userId"):
        v = idobj.get(k) or op.get(k)
        if v:
            return str(v).strip()
    ut = str(op.get("user_type") if op.get("user_type") is not None else op.get("userType") or "").strip()
    if idobj:
        return f"ut{ut}:{json.dumps(idobj, sort_keys=True, ensure_ascii=False)}"
    return f"ut{ut}" if ut else ""


def _p0_om_card(chat_id: str, title: str, template: str, lines: List[str]) -> None:
    if not (chat_id or "").strip():
        return
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {"template": template, "title": {"tag": "plain_text", "content": title[:190]}},
        "body": {"elements": [{"tag": "markdown", "content": "\n".join(lines) if lines else "-"}]},
    }
    try:
        _lark_send_interactive_card("chat_id", chat_id, card)
    except Exception:
        logger.exception("p0 openmeeting card failed; text fallback")
        try:
            _lark_send_text_auto("chat_id", chat_id, f"{title}\n" + "\n".join(lines))
        except Exception:
            logger.exception("p0 openmeeting text fallback failed")


def _p0_om_reserve(topic: str, host_open_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Reserve a meeting owned by host_open_id, auto-record on. Returns (reserve, error)."""
    end_time = int(time.time()) + max(1, _cfg_int("P0_OPENMEETING_DURATION_HOURS", 4)) * 3600
    settings: Dict[str, Any] = {
        "topic": (topic or "p0bot meeting")[:200],
        "auto_record": _lark_env_truthy_or_default("P0_OPENMEETING_AUTO_RECORD", default=True),
    }
    if host_open_id:
        settings["assign_host_list"] = [{"user_type": 1, "id": host_open_id}]
    body = {"end_time": str(end_time), "owner_id": host_open_id, "meeting_settings": settings}
    try:
        tok = _lark_tenant_access_token_string()
        r = requests.post(
            f"{_lark_api_domain()}/open-apis/vc/v1/reserves/apply",
            params={"user_id_type": "open_id"},
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"},
            json=body,
            timeout=30,
        )
        j = r.json()
    except Exception as e:
        logger.exception("p0 openmeeting reserve failed")
        return None, {"code": -1, "msg": f"network error: {e.__class__.__name__}"}
    if not isinstance(j, dict) or int(j.get("code", -1) if str(j.get("code", "")).lstrip("-").isdigit() else -1) != 0:
        return None, {"code": (j.get("code") if isinstance(j, dict) else "?"), "msg": (j.get("msg") if isinstance(j, dict) else "unexpected response")}
    reserve = (j.get("data") or {}).get("reserve") or {}
    return (reserve if isinstance(reserve, dict) else {}), None


def _p0_om_announce_chat_default() -> str:
    return _cfg_str("P0_OPENMEETING_ANNOUNCE_CHAT_ID", "").strip()


def _p0_om_lookup_chat(meeting_no: str) -> str:
    with _p0_om_lock:
        rec = _p0_om_active.get(meeting_no)
        if rec and rec.get("chat_id"):
            return rec["chat_id"]
    return _p0_om_announce_chat_default()


def _p0_om_open_worker(chat_id: str, open_id: str, mid: str, debounce_key: str) -> None:
    rt, rv = ("chat_id", chat_id) if (chat_id or "").strip() else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None
    template = _cfg_str("P0_OPENMEETING_CARD_TEMPLATE", "turquoise").strip() or "turquoise"
    announce_chat = _p0_om_announce_chat_default() or chat_id
    try:
        host = _p0_om_host_open_id()
        if not host:
            if rt:
                _lark_send_text_auto(rt, rv, "未配置主持人 / set P0_MEETING_HOST_OPEN_ID to a real Lark user open_id first.")
            return
        topic = _cfg_str("P0_OPENMEETING_TOPIC", "p0bot meeting").strip() or "p0bot meeting"
        reserve, err = _p0_om_reserve(topic, host)
        if err:
            code, msg = err.get("code"), err.get("msg")
            hint = ""
            low = f"{code} {msg}".lower()
            if "permission" in low or "forbidden" in low or "403" in low or "99991" in low:
                hint = "\n提示：应用需开通 vc:reserve 权限 / grant the app the vc:reserve scope."
            elif "123004" in str(code) or "host" in low:
                hint = "\n提示：主持人/owner open_id 无效或非同租户用户 / host/owner open_id invalid or not a same-tenant user."
            _p0_om_card(announce_chat or chat_id, "⚠️ 开会失败 / Could not open meeting", "red", [f"`code={code}  msg={msg}`{hint}"])
            return
        meeting_no = _lark_dict_pick_str(reserve, "meeting_no", "meetingNo")
        url = _lark_dict_pick_str(reserve, "url")
        reserve_id = _lark_dict_pick_str(reserve, "id")
        with _p0_om_lock:
            _p0_om_active[meeting_no] = {
                "reserve_id": reserve_id,
                "meeting_id": "",
                "chat_id": announce_chat or chat_id,
                "topic": topic,
                "present": set(),
            }
            if len(_p0_om_active) > 50:
                for k in list(_p0_om_active)[:20]:
                    _p0_om_active.pop(k, None)
        host_name = _p0_contact_name(host) or "指定主持人 / assigned host"
        chat_target = announce_chat or chat_id
        elements: List[Dict[str, Any]] = [
            {
                "tag": "markdown",
                "content": "\n".join(
                    [
                        f"**主题 / Topic:** {topic}",
                        f"**会议号 / No.:** {meeting_no}",
                        f"**主持人 / Host:** {host_name}（自动录制已开 / auto-record on）",
                        "",
                        "点下方按钮加入，我会在这里播报谁加入/离开。/ Tap **Join** — I'll announce joins & leaves here.",
                        f"结束 / End: 主持人在客户端结束，或发送 {_p0_om_end_trigger()}。",
                    ]
                ),
            }
        ]
        if url:
            elements.append({"tag": "hr"})
            elements.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "🎥 加入会议 / Join meeting"},
                    "type": "primary",
                    "behaviors": [{"type": "open_url", "default_url": url}],
                }
            )
        card = {
            "schema": "2.0",
            "config": {"update_multi": True, "wide_screen_mode": True},
            "header": {"template": template, "title": {"tag": "plain_text", "content": "🎥 会议已开 / Meeting opened"}},
            "body": {"elements": elements},
        }
        if chat_target:
            try:
                _lark_send_interactive_card("chat_id", chat_target, card)
            except Exception:
                logger.exception("p0 openmeeting open card failed; text fallback")
                try:
                    _lark_send_text_auto("chat_id", chat_target, f"🎥 Meeting opened: {topic}\nNo. {meeting_no}\nJoin: {url}")
                except Exception:
                    logger.exception("p0 openmeeting open text fallback failed")
        logger.info("p0 openmeeting reserved no=%s host=%s chat=%s", meeting_no, host[:10], (chat_target or "")[:12])
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_om_event_parts(ce: Any) -> Tuple[Dict[str, Any], Dict[str, Any], str, str]:
    """Return (event, meeting, meeting_no, meeting_id) from a vc.meeting.* event."""
    data = _lark_ws_sdk_event_to_dict(ce)
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    meeting = ev.get("meeting") if isinstance(ev.get("meeting"), dict) else {}
    return ev, meeting, _lark_dict_pick_str(meeting, "meeting_no", "meetingNo"), _lark_dict_pick_str(meeting, "id", "meeting_id", "meetingId")


def _p0_om_on_started(ce: Any) -> None:
    if not _p0_om_enabled():
        return
    try:
        _ev, _m, mno, mid = _p0_om_event_parts(ce)
        if not mno:
            return
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
            if rec is not None and mid:
                rec["meeting_id"] = mid
        logger.info("p0 openmeeting started no=%s meeting_id=%s", mno, (mid or "")[:12])
    except Exception:
        logger.exception("p0 openmeeting started handler failed")


def _p0_om_on_join(ce: Any) -> None:
    if not _p0_om_enabled() or not _lark_env_truthy_or_default("P0_OPENMEETING_ANNOUNCE_JOINS", default=True):
        return
    try:
        ev, _m, mno, _mid = _p0_om_event_parts(ce)
        if not mno:
            return
        op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
        key = _p0_om_op_key(op)
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
            if rec is None:
                # Only meetings the bot reserved via /openmeeting are tracked; the map is in-memory
                # and cleared on restart, so a join in a pre-restart meeting lands here.
                logger.info("p0 om join no=%s ignored — not a bot-opened meeting (active=%s)",
                            mno, sorted(_p0_om_active.keys()))
                return
            if key and key in rec["present"]:
                return  # already announced this participant (WS redelivery)
            if key:
                rec["present"].add(key)
            chat = rec.get("chat_id") or _p0_om_announce_chat_default()
        name = _p0_om_display(op, chat)  # resolves name (network) — do outside the lock
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
            if rec is not None:
                roster = rec.setdefault("roster", {})
                roster[key or name] = {"name": name, "join_ts": time.time(), "leave_ts": 0.0}
        logger.info("p0 om join no=%s → announcing to chat=%s", mno, (chat or "")[:12])
        _p0_om_card(chat, "🟢 加入会议 / Joined", _cfg_str("P0_OPENMEETING_CARD_TEMPLATE", "turquoise").strip() or "turquoise",
                    [f"**{name}** 加入了会议 / joined the meeting"])
    except Exception:
        logger.exception("p0 openmeeting join handler failed")


def _p0_om_on_leave(ce: Any) -> None:
    if not _p0_om_enabled() or not _lark_env_truthy_or_default("P0_OPENMEETING_ANNOUNCE_LEAVES", default=True):
        return
    try:
        ev, _m, mno, _mid = _p0_om_event_parts(ce)
        if not mno:
            return
        op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
        key = _p0_om_op_key(op)
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
            if rec is None:
                logger.info("p0 om leave no=%s ignored — not a bot-opened meeting (active=%s)",
                            mno, sorted(_p0_om_active.keys()))
                return
            if key:
                rec["present"].discard(key)
            chat = rec.get("chat_id") or _p0_om_announce_chat_default()
        name = _p0_om_display(op, chat)  # resolves name (network) — do outside the lock
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
            if rec is not None:
                roster = rec.setdefault("roster", {})
                entry = roster.get(key)
                if entry is not None:
                    entry["leave_ts"] = time.time()
                else:
                    roster[key or name] = {"name": name, "join_ts": 0.0, "leave_ts": time.time()}
        reason = str(ev.get("leave_reason") or ev.get("leaveReason") or "").strip()
        why = {"1": "", "2": "（会议结束/meeting ended）", "3": "（被移出/removed）"}.get(reason, "")
        logger.info("p0 om leave no=%s reason=%s → announcing to chat=%s", mno, reason or "?", (chat or "")[:12])
        _p0_om_card(chat, "🔴 离开会议 / Left", _cfg_str("P0_OPENMEETING_CARD_TEMPLATE", "turquoise").strip() or "turquoise",
                    [f"**{name}** 离开了会议 / left the meeting {why}"])
    except Exception:
        logger.exception("p0 openmeeting leave handler failed")


def _p0_om_on_ended(ce: Any) -> None:
    if not _p0_om_enabled():
        return
    try:
        _ev, _m, mno, _mid = _p0_om_event_parts(ce)
        if not mno:
            return
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
            if rec is None:
                return  # not a meeting this bot opened
            # Ended → clear meeting_id so /endmeeting can't re-target it; keep the record so a
            # later recording_ready can still resolve the chat + deliver, then pop it.
            rec["meeting_id"] = ""
            rec["ended"] = True
            chat = rec.get("chat_id") or _p0_om_announce_chat_default()
        _p0_om_card(chat, "🏁 会议结束 / Meeting ended",
                    _cfg_str("P0_OPENMEETING_CARD_TEMPLATE", "turquoise").strip() or "turquoise",
                    ["会议已结束。若开启了录制，录制生成后会发给主持人。",
                     "Meeting ended. If it was recorded, the recording will be sent to the host once ready."])
        logger.info("p0 openmeeting ended no=%s", mno)
    except Exception:
        logger.exception("p0 openmeeting ended handler failed")


def _p0_om_on_recording_ready(ce: Any) -> None:
    if not _p0_om_enabled():
        return
    try:
        ev, _m, mno, mid = _p0_om_event_parts(ce)
        with _p0_om_lock:
            rec = _p0_om_active.get(mno)
            if rec is None:
                return  # not a meeting this bot opened — don't leak someone else's recording
            chat = rec.get("chat_id") or _p0_om_announce_chat_default()
        url = _lark_dict_pick_str(ev, "url")
        if not url and mid:
            try:
                j = _p0_lark_get_json(f"/open-apis/vc/v1/meetings/{mid}/recording")
                if int(j.get("code", -1)) == 0:
                    url = _lark_dict_pick_str((j.get("data") or {}).get("recording") or {}, "url")
            except Exception:
                logger.exception("p0 openmeeting get-recording fallback failed")
        # Remember the last ready recording so a bare /whotalk can target "the last meeting".
        with _p0_om_lock:
            _p0_whotalk_last.update({"meeting_no": mno, "meeting_id": mid, "url": url, "ts": time.time()})
            _last_snap = dict(_p0_whotalk_last)
        try:  # persist across restarts (best-effort)
            with open(_p0_whotalk_last_path(), "w", encoding="utf-8") as f:
                json.dump(_last_snap, f)
        except Exception:
            logger.debug("p0 whotalk last-recording persist failed")
        host = _p0_om_host_open_id()
        if url and host:
            try:
                _lark_send_text_auto("open_id", host, f"🎬 会议录制已生成 / Meeting recording ready:\n{url}")
            except Exception:
                logger.exception("p0 openmeeting recording DM to host failed")
        _p0_om_card(chat, "🎬 录制完成 / Recording ready",
                    _cfg_str("P0_OPENMEETING_CARD_TEMPLATE", "turquoise").strip() or "turquoise",
                    ["录制已生成并发送给主持人。/ Recording is ready and sent to the host."
                     if (url and host) else "录制已生成（未能获取链接）。/ Recording ready (link unavailable)."])
        with _p0_om_lock:
            _p0_om_active.pop(mno, None)
    except Exception:
        logger.exception("p0 openmeeting recording-ready handler failed")


def _p0_om_end_worker(chat_id: str, open_id: str, mid: str, debounce_key: str) -> None:
    rt, rv = ("chat_id", chat_id) if (chat_id or "").strip() else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None

    def _reply(text: str) -> None:
        try:
            if rt:
                _lark_send_text_auto(rt, rv, text)
        except Exception:
            logger.exception("p0 endmeeting reply failed")

    try:
        # Pick the active meeting for this chat (else the most recent tracked one).
        target_id = ""
        with _p0_om_lock:
            for rec in _p0_om_active.values():
                if rec.get("meeting_id") and rec.get("chat_id") == (chat_id or ""):
                    target_id = rec["meeting_id"]
            if not target_id:
                for rec in _p0_om_active.values():
                    if rec.get("meeting_id"):
                        target_id = rec["meeting_id"]
        if not target_id:
            _reply("没有正在进行的、由我开启的会议（或会议尚未开始）。/ No active bot-opened meeting to end (or it hasn't started yet).")
            return
        at = _p0_vc_user_access_token()
        if not at:
            _reply(
                "无法通过 API 结束会议：需要主持人的用户令牌。请主持人在 Lark 客户端里直接结束会议，"
                "或先用 /vcauth 授权主持人账号后再试。\n"
                "Can't end via API (needs the host's user token). The host can end it in the Lark client, "
                "or authorize once with /vcauth (as the host) then retry."
            )
            return
        try:
            r = requests.patch(
                f"{_lark_api_domain()}/open-apis/vc/v1/meetings/{target_id}/end",
                headers={"Authorization": f"Bearer {at}", "Content-Type": "application/json; charset=utf-8"},
                timeout=20,
            )
            j = r.json()
        except Exception as e:
            _reply(f"结束会议请求失败 / end request failed: {e.__class__.__name__}")
            return
        if isinstance(j, dict) and int(j.get("code", -1) if str(j.get("code", "")).lstrip("-").isdigit() else -1) == 0:
            with _p0_om_lock:
                for _rec in _p0_om_active.values():
                    if _rec.get("meeting_id") == target_id:
                        _rec["meeting_id"] = ""
                        _rec["ended"] = True
            _reply("✅ 已结束会议 / meeting ended.")
        else:
            code = j.get("code") if isinstance(j, dict) else "?"
            msg = j.get("msg") if isinstance(j, dict) else str(j)
            extra = ""
            if str(code) == "122003":
                extra = "\n（结束会议的用户必须是会中的当前主持人 / the user must be the current host, present in the meeting.）"
            _reply(f"⚠️ 结束失败 / end failed: code={code} msg={msg}{extra}")
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_om_dispatch(kind: str, chat_id: str, open_id: str, clean: str, mid: str, im_event_id: str, sender_debounce: str, msg_time: str) -> bool:
    trigger = _p0_om_open_trigger() if kind == "open" else _p0_om_end_trigger()
    if _p0_command_body((clean or "").strip(), trigger) is None:
        return False
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_om_{kind}__"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)
    if not _p0_om_allowed(open_id):
        def _deny() -> None:
            try:
                rt, rv = ("chat_id", chat_id) if (chat_id or "").strip() else ("open_id", open_id)
                if rt and rv:
                    _lark_send_text_auto(rt, rv, "仅授权用户可执行 / restricted to configured users (P0_OPENMEETING_ALLOWED_OPEN_IDS).")
            except Exception:
                logger.exception("p0 openmeeting deny reply failed")
            finally:
                with _monitoring_reply_dispatch_lock:
                    _monitoring_inflight_keys.discard(debounce_key)
        try:
            threading.Thread(target=_deny, daemon=True, name="p0-om-deny").start()
        except Exception:
            with _monitoring_reply_dispatch_lock:
                _monitoring_inflight_keys.discard(debounce_key)
        return True
    worker = _p0_om_open_worker if kind == "open" else _p0_om_end_worker
    try:
        threading.Thread(target=worker, args=(chat_id or "", open_id or "", mid or "", debounce_key), daemon=True, name=f"p0-om-{kind}").start()
    except Exception:
        logger.exception("p0 openmeeting %s worker thread failed to start", kind)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


def _p0_any_feature_enabled() -> bool:
    return _p0_qa_enabled() or _p0_meeting_enabled() or _p0_members_enabled() or _p0_om_enabled()


# ---------------------------------------------------------------------------
# p0bot — "p0" keyword detection: ask "is this a P0?" via a card, confirm by reply
#
# See the P0_DETECT_* config comment for why this is reply-based rather than
# clickable buttons: card callback clicks need a round-trip to the bot (HTTP
# webhook, disabled here, or a CARD frame over the long connection, which the
# pinned lark-oapi WS client discards before any handler sees it). This uses
# the same im.message.receive_v1 pipeline every other command already runs on.
# ---------------------------------------------------------------------------

_P0_DETECT_WORD_RE = re.compile(r"(?<![0-9A-Za-z])p0(?![0-9A-Za-z])", re.IGNORECASE)

_p0_detect_lock = threading.Lock()
_p0_detect_last_fired: Dict[str, float] = {}    # chat_id -> ts of the last card shown (cooldown)
_p0_detect_pending: Dict[str, float] = {}       # chat_id -> expiry ts for a valid /confirmp0
_p0_detect_seen_event_ids: Set[str] = set()     # dedup WS redelivery of the same detected message


def _p0_detect_enabled() -> bool:
    return _lark_env_truthy_or_default("P0_DETECT_ENABLE", default=True)


def _p0_detect_chat_ids() -> Set[str]:
    raw = _cfg_str("P0_DETECT_CHAT_IDS", "").strip()
    ids = {c.strip() for c in re.split(r"[\s,;]+", raw) if c.strip()}
    if not ids:
        default = _p0_om_announce_chat_default()
        if default:
            ids = {default}
    return ids


def _p0_detect_confirm_trigger() -> str:
    return _cfg_str("P0_DETECT_CONFIRM_TRIGGER", "/confirmp0").strip() or "/confirmp0"


def _p0_detect_confirm_window_seconds() -> float:
    return max(60.0, _cfg_float("P0_DETECT_CONFIRM_WINDOW_SECONDS", 900.0))


def _p0_detect_cooldown_and_arm(chat_id: str) -> bool:
    """True (and arms a pending /confirmp0 window) if this chat isn't in cooldown."""
    cool = max(0.0, _cfg_float("P0_DETECT_COOLDOWN_SECONDS", 2700.0))
    now = time.time()
    with _p0_detect_lock:
        last = _p0_detect_last_fired.get(chat_id, 0.0)
        if now - last < cool:
            return False
        _p0_detect_last_fired[chat_id] = now
        _p0_detect_pending[chat_id] = now + _p0_detect_confirm_window_seconds()
    return True


def _p0_detect_consume_pending(chat_id: str) -> bool:
    """True (and clears it) if there's a live, unexpired pending confirmation for this chat."""
    now = time.time()
    with _p0_detect_lock:
        exp = _p0_detect_pending.pop(chat_id, 0.0)
    return now < exp


def _p0_detect_maybe_fire(*, chat_id: str, clean: str, mid: str, im_event_id: str,
                          sender_debounce: str, msg_time: str) -> None:
    """Passive observer: if the message contains the whole word 'p0' in a watched chat, post a
    confirm-via-reply card (cooldown-gated). Never consumes the message — normal command
    dispatch below still runs regardless of what this does."""
    if not _p0_detect_enabled():
        return
    cid = (chat_id or "").strip()
    if not cid or cid not in _p0_detect_chat_ids():
        logger.info("p0 detect: skip — chat=%r not in watched set %r", cid, _p0_detect_chat_ids())
        return
    text = (clean or "").strip()
    if not text or not _P0_DETECT_WORD_RE.search(text):
        return  # not a "p0" mention — normal traffic, no need to log every message
    ev_key = (im_event_id or "").strip() or _monitoring_processed_stick(mid, im_event_id, cid, sender_debounce, msg_time)
    if ev_key:
        with _p0_detect_lock:
            if ev_key in _p0_detect_seen_event_ids:
                logger.info("p0 detect: skip — duplicate event_id=%r (WS redelivery)", ev_key)
                return
            _p0_detect_seen_event_ids.add(ev_key)
            if len(_p0_detect_seen_event_ids) > 2000:
                _p0_detect_seen_event_ids.clear()
                _p0_detect_seen_event_ids.add(ev_key)
    if not _p0_detect_cooldown_and_arm(cid):
        with _p0_detect_lock:
            last = _p0_detect_last_fired.get(cid, 0.0)
        cool = max(0.0, _cfg_float("P0_DETECT_COOLDOWN_SECONDS", 2700.0))
        remain = max(0.0, cool - (time.time() - last))
        logger.info("p0 detect: 'p0' seen in chat=%s but COOLDOWN active — %.0fs remaining "
                    "(P0_DETECT_COOLDOWN_SECONDS=%.0f); card suppressed", cid[:16], remain, cool)
        return
    logger.info("p0 detect: 'p0' matched in chat=%s — sending confirm card", cid[:16])
    trigger = _p0_detect_confirm_trigger()
    card = _p0_detect_prompt_card(cid)

    def _send() -> None:
        try:
            _lark_send_interactive_card("chat_id", cid, card)
            logger.info("p0 detect: confirm card sent to chat=%s", cid[:16])
        except Exception:
            logger.exception("p0 detect card send failed; text fallback")
            try:
                _lark_send_text_auto("chat_id", cid,
                                     f"检测到消息中提到 P0。若确为 P0，请回复 {trigger}。/ "
                                     f"Detected a P0 mention — reply {trigger} if it's real.")
            except Exception:
                logger.exception("p0 detect text fallback failed")

    try:
        threading.Thread(target=_send, daemon=True, name="p0-detect-card").start()
    except Exception:
        logger.exception("p0 detect: card send thread failed to start")


def _p0_detect_do_confirm(chat_id: str, open_id: str) -> None:
    """Shared confirm action: tag current on-duty + auto-open a meeting. Used by both /confirmp0
    and the Confirm button."""
    cid = (chat_id or "").strip()
    rt, rv = ("chat_id", cid) if cid else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
    shift, duty_names = _p0_duty_on(time.time())
    tag_line = (f"值班 / on-duty（{shift} 班/shift）：" + ", ".join(duty_names)) if duty_names else \
               "（未能读取值班表 / could not read the duty roster）"
    if rt and rv:
        _lark_send_text_auto(rt, rv, f"✅ 已确认为 P0 / Confirmed as P0.\n{tag_line}")
    if _p0_om_enabled():
        om_debounce = f"{cid}\n__p0_om_open__"
        with _monitoring_reply_dispatch_lock:
            if om_debounce in _monitoring_inflight_keys:
                return
            _monitoring_inflight_keys.add(om_debounce)
        try:
            _p0_om_open_worker(cid, open_id or "", "", om_debounce)
        except Exception:
            logger.exception("p0 detect: auto /openmeeting failed")
            with _monitoring_reply_dispatch_lock:
                _monitoring_inflight_keys.discard(om_debounce)
    elif rt and rv:
        _lark_send_text_auto(
            rt, rv,
            f"提示：{_p0_om_open_trigger()} 未启用（P0_OPENMEETING_ENABLE=0）— 未自动开会。/ "
            f"{_p0_om_open_trigger()} is disabled — meeting not auto-created.",
        )


def _p0_detect_confirm_worker(chat_id: str, open_id: str, mid: str, debounce_key: str) -> None:
    """Text `/confirmp0` path: requires a live pending detection (so it can't fire out of nowhere)."""
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None
    try:
        cid = (chat_id or "").strip()
        rt, rv = ("chat_id", cid) if cid else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
        if not _p0_detect_consume_pending(cid):
            if rt and rv:
                _lark_send_text_auto(
                    rt, rv,
                    "没有待确认的 P0 提示，或已超时。/ No pending P0 prompt to confirm (or it expired).\n"
                    f"请先在消息中提到 p0，或直接运行 {_p0_om_open_trigger()}。/ Mention p0 first, or run "
                    f"{_p0_om_open_trigger()} directly.",
                )
            return
        _p0_detect_do_confirm(cid, open_id)
    except Exception:
        logger.exception("p0 detect confirm worker failed")
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


# ---- Real Confirm/Cancel buttons on the P0 card (card.action.trigger over the long connection) ----

def _p0_card_buttons_enabled() -> bool:
    return _lark_env_truthy_or_default("P0_CARD_BUTTONS_ENABLE", default=True)


_p0_card_seen_lock = threading.Lock()
_p0_card_seen_msgs: Dict[str, float] = {}   # open_message_id -> ts, to dedup redelivery / double-click


def _p0_card_action_dedup(msg_id: str) -> bool:
    """True the first time this card message's action is seen within a few seconds (dedup)."""
    if not msg_id:
        return True
    now = time.time()
    with _p0_card_seen_lock:
        if len(_p0_card_seen_msgs) > 2000:
            _p0_card_seen_msgs.clear()
        if now - _p0_card_seen_msgs.get(msg_id, 0.0) < 8.0:
            return False
        _p0_card_seen_msgs[msg_id] = now
    return True


def _p0_detect_result_card(state: str, note: str) -> Dict[str, Any]:
    """Buttonless replacement card shown after Confirm/Cancel."""
    tmpl = "green" if state == "confirmed" else "grey"
    title = "✅ 已确认 P0 / Confirmed P0" if state == "confirmed" else "⏹️ 已取消 / Cancelled"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {"template": tmpl, "title": {"tag": "plain_text", "content": title}},
        "body": {"elements": [{"tag": "markdown", "content": note}]},
    }


def _p0_detect_prompt_card(cid: str) -> Dict[str, Any]:
    """The P0 prompt card — with real Confirm/Cancel buttons when P0_CARD_BUTTONS_ENABLE."""
    trigger = _p0_detect_confirm_trigger()
    window_min = int(_p0_detect_confirm_window_seconds() // 60)
    body_md = (
        "检测到消息中提到 **P0**。这是一次 P0 事件吗？\n"
        "Detected a mention of **P0**. Is this a real P0 incident?\n\n"
        f"（也可回复 `{trigger}` 确认；{window_min} 分钟内有效 / or reply `{trigger}` within {window_min} min）"
    )
    elements: List[Dict[str, Any]] = [{"tag": "markdown", "content": body_md}]
    if _p0_card_buttons_enabled():
        def _btn(text: str, typ: str, v: str) -> Dict[str, Any]:
            return {
                "tag": "button",
                "text": {"tag": "plain_text", "content": text},
                "type": typ,
                "behaviors": [{"type": "callback", "value": {"k": "p0det", "v": v, "cid": cid}}],
            }
        elements.append({
            "tag": "column_set", "flex_mode": "none", "horizontal_spacing": "default",
            "columns": [
                {"tag": "column", "width": "weighted", "weight": 1,
                 "elements": [_btn("✅ 确认 P0 / Confirm", "primary", "confirm")]},
                {"tag": "column", "width": "weighted", "weight": 1,
                 "elements": [_btn("✖️ 取消 / Cancel", "default", "cancel")]},
            ],
        })
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {"template": _cfg_str("P0_DETECT_CARD_TEMPLATE", "red").strip() or "red",
                   "title": {"tag": "plain_text", "content": "🚨 P0? 确认 / Confirm"}},
        "body": {"elements": elements},
    }


def _p0_card_action_handler(data: Any) -> Any:
    """card.action.trigger handler (reached over WS via the card→event frame patch). Handles the P0
    Confirm/Cancel buttons: returns a toast + a buttonless updated card, and does the heavy work
    (roster read + /openmeeting) in a background thread to stay within the ~3s callback window."""
    from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse

    def _resp(toast_type: str, toast_content: str, new_card: Optional[Dict[str, Any]] = None) -> Any:
        d: Dict[str, Any] = {"toast": {"type": toast_type, "content": (toast_content or "")[:200]}}
        if new_card is not None:
            d["card"] = {"type": "raw", "data": new_card}
        return P2CardActionTriggerResponse(d)

    try:
        ev = getattr(data, "event", None)
        action = getattr(ev, "action", None)
        val = (getattr(action, "value", None) or {}) if action is not None else {}
        k = str(val.get("k") or "")
        v = str(val.get("v") or "")
        ctx = getattr(ev, "context", None)
        operator = getattr(ev, "operator", None)
        chat_id = (getattr(ctx, "open_chat_id", "") or "") if ctx is not None else ""
        msg_id = (getattr(ctx, "open_message_id", "") or "") if ctx is not None else ""
        open_id = (getattr(operator, "open_id", "") or "") if operator is not None else ""
        cid = (chat_id or str(val.get("cid") or "")).strip()
        if k != "p0det":
            return _resp("info", "")
        if not _p0_card_action_dedup(msg_id):
            return _resp("info", "已处理 / already handled")
        if v == "cancel":
            with _p0_detect_lock:
                _p0_detect_pending.pop(cid, None)
            return _resp("info", "已取消 / Cancelled",
                         _p0_detect_result_card("cancelled", "已取消，不作为 P0 处理。/ Cancelled — not a P0."))
        if v == "confirm":
            with _p0_detect_lock:
                _p0_detect_pending.pop(cid, None)  # button IS the confirmation; no separate window check
            debounce_key = f"{cid}\n__p0_confirmp0_btn__"
            go = False
            with _monitoring_reply_dispatch_lock:
                if debounce_key not in _monitoring_inflight_keys:
                    _monitoring_inflight_keys.add(debounce_key)
                    go = True
            if go:
                def _bg() -> None:
                    try:
                        _p0_detect_do_confirm(cid, open_id)
                    except Exception:
                        logger.exception("p0 card confirm background failed")
                    finally:
                        with _monitoring_reply_dispatch_lock:
                            _monitoring_inflight_keys.discard(debounce_key)
                try:
                    threading.Thread(target=_bg, daemon=True, name="p0-confirmp0-btn").start()
                except Exception:
                    logger.exception("p0 card confirm: thread start failed")
                    with _monitoring_reply_dispatch_lock:
                        _monitoring_inflight_keys.discard(debounce_key)
            return _resp("success", "已确认 P0 / Confirmed",
                         _p0_detect_result_card("confirmed",
                                                "已确认为 P0，正在开会并 @ 值班人员…\n"
                                                "Confirmed — opening a meeting and tagging on-duty…"))
        return _resp("info", "")
    except Exception:
        logger.exception("p0 card action handler failed")
        try:
            return _resp("error", "内部错误 / internal error")
        except Exception:
            return None


def _p0_try_handle_confirmp0(*, chat_id, open_id, clean, mid, im_event_id, sender_debounce, msg_time) -> bool:
    """`/confirmp0` → consume a pending P0 detection: tag on-duty + auto-run /openmeeting."""
    if not _p0_detect_enabled():
        return False
    if _p0_command_body((clean or "").strip(), _p0_detect_confirm_trigger()) is None:
        return False
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_confirmp0__"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)
    try:
        threading.Thread(
            target=_p0_detect_confirm_worker,
            args=(chat_id or "", open_id or "", mid or "", debounce_key),
            daemon=True,
            name="p0-confirmp0",
        ).start()
    except Exception:
        logger.exception("p0 confirmp0 worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


def _p0_try_handle_whoami(*, chat_id, open_id, clean, mid, im_event_id, sender_debounce, msg_time, im_chat_type) -> bool:
    """`/whoami` → reply the sender's p0bot-namespace open_id + chat_id (open_id is per-app)."""
    if not _p0_any_feature_enabled():
        return False
    if _p0_command_body((clean or "").strip(), "/whoami") is None:
        return False
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_whoami__\n{(open_id or '')[:20]}"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    def _w() -> None:
        try:
            rt, rv = ("chat_id", chat_id) if (chat_id or "").strip() else (("open_id", open_id) if (open_id or "").strip() else ("", ""))
            if rt and rv:
                _lark_send_text_auto(
                    rt,
                    rv,
                    "🪪 p0bot 看到的你 / as p0bot sees you:\n"
                    f"open_id: {open_id or '(unknown)'}\n"
                    f"chat_id: {chat_id or '(none)'}\n"
                    f"chat_type: {im_chat_type or '(?)'}\n"
                    "open_id 因应用而异——配置 P0_MEETING_HOST_OPEN_ID 请用上面这个值。/ "
                    "open_id is per-app — use THIS value for P0_MEETING_HOST_OPEN_ID.",
                )
        except Exception:
            logger.exception("p0 whoami reply failed")
        finally:
            with _monitoring_reply_dispatch_lock:
                _monitoring_inflight_keys.discard(debounce_key)

    try:
        threading.Thread(target=_w, daemon=True, name="p0-whoami").start()
    except Exception:
        logger.exception("p0 whoami thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


def _p0_try_handle_openmeeting(*, chat_id, open_id, clean, mid, im_event_id, sender_debounce, msg_time) -> bool:
    if not _p0_om_enabled():
        return False
    if _p0_command_body((clean or "").strip(), _p0_om_end_trigger()) is not None:
        return _p0_om_dispatch("end", chat_id, open_id, clean, mid, im_event_id, sender_debounce, msg_time)
    if _p0_command_body((clean or "").strip(), _p0_om_open_trigger()) is not None:
        return _p0_om_dispatch("open", chat_id, open_id, clean, mid, im_event_id, sender_debounce, msg_time)
    return False


def _p0_checkmeeting_worker(chat_id: str, open_id: str, query: str, mid: str, debounce_key: str) -> None:
    react = _lark_env_truthy_or_default("P0_REACT_ENABLE", default=True) and bool((mid or "").strip())
    ack_id = _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_ACK_EMOJI", "OK").strip() or "OK") if react else None
    rt, rv = ("chat_id", chat_id) if (chat_id or "").strip() else (("open_id", open_id) if (open_id or "").strip() else ("", ""))

    def _out(title: str, template: str, lines: List[str]) -> None:
        if rt == "chat_id" and rv:
            _p0_om_card(rv, title, template, lines)   # _p0_om_card falls back to text on failure
        elif rt:
            _lark_send_text_auto(rt, rv, f"{title}\n\n" + "\n\n".join(lines))

    try:
        q = (query or "").strip().lower()
        # Snapshot the tracked rosters under the lock; match names against the query.
        matches: List[Tuple[str, str, float, float]] = []  # (name, meeting_no, join_ts, leave_ts)
        with _p0_om_lock:
            for mno, rec in _p0_om_active.items():
                for entry in (rec.get("roster") or {}).values():
                    nm = str(entry.get("name") or "")
                    if q and q not in nm.lower():
                        continue
                    matches.append((nm, mno, float(entry.get("join_ts") or 0.0), float(entry.get("leave_ts") or 0.0)))
        title = f"🔎 会中查找 / In meeting: “{query.strip()}”" if q else "🔎 会中人员 / Meeting roster"
        if not matches:
            hint = ((f"当前没有名字含 “{query.strip()}” 的与会者。" if q else "当前没有被跟踪的与会者。")
                    + "（仅跟踪由 /openmeeting 创建的会议，重启后清空）\n"
                    + (f"No participant matching “{query.strip()}” right now." if q else "No tracked participants right now.")
                    + " (Only /openmeeting-created meetings are tracked; cleared on restart.)")
            _out(title, "orange", [hint])
            return
        # In-meeting first, then most recent join; format times in local tz.
        matches.sort(key=lambda t: (t[3] != 0.0, -(t[2] or 0.0)))
        lines: List[str] = []
        for nm, mno, jt, lt in matches:
            j = time.strftime("%H:%M:%S", time.localtime(jt)) if jt else "—"
            if lt:
                status = f"🔴 {j} → {time.strftime('%H:%M:%S', time.localtime(lt))} 离开/left"
            else:
                status = f"🟢 {j} 加入，仍在会中 / joined, still in meeting"
            lines.append(f"**{nm}**（会议号/No. {mno}）\n{status}")
        _out(title, _cfg_str("P0_OPENMEETING_CARD_TEMPLATE", "turquoise").strip() or "turquoise", lines)
    except Exception:
        logger.exception("p0 checkmeeting worker failed")
        if rt:
            try:
                _lark_send_text_auto(rt, rv, "查询失败，请查看日志 / lookup failed — check the server logs.")
            except Exception:
                pass
    finally:
        if react and ack_id:
            _p0_lark_add_reaction(mid, _cfg_str("P0_REACT_DONE_EMOJI", "DONE").strip() or "DONE")
            if _lark_env_truthy_or_default("P0_REACT_REMOVE_ACK", default=True):
                _p0_lark_remove_reaction(mid, ack_id)
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _p0_try_handle_checkmeeting(*, chat_id, open_id, clean, mid, im_event_id, sender_debounce, msg_time) -> bool:
    """`/checkmeeting <name>` → find matching participants in bot-hosted meetings + join/leave times."""
    if not _p0_om_enabled():
        return False
    body = _p0_command_body((clean or "").strip(), _p0_checkmeeting_trigger())
    if body is None:
        return False
    processed_stick = _monitoring_processed_stick(mid, im_event_id, chat_id or "", sender_debounce, msg_time)
    debounce_key = f"{(chat_id or '').strip()}\n__p0_checkmeeting__\n{(body or '')[:40].lower()}"
    with _monitoring_reply_dispatch_lock:
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            return True
        if processed_stick and processed_stick in _processed_lark_message_ids:
            return True
        if debounce_key in _monitoring_inflight_keys:
            return True
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)
    try:
        threading.Thread(
            target=_p0_checkmeeting_worker,
            args=(chat_id or "", open_id or "", body or "", mid or "", debounce_key),
            daemon=True,
            name="p0-checkmeeting",
        ).start()
    except Exception:
        logger.exception("p0 checkmeeting worker thread failed to start")
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)
    return True


def _monitoring_watchdog_loop() -> None:
    """Periodic Grafana check; alert chat on >= threshold drop/spike."""
    global _monitoring_watch_last_alert_at, _monitoring_watch_pending_confirm
    sec = max(15.0, _cfg_float("MONITORING_WATCH_INTERVAL_SECONDS", 20.0))
    cool = max(0.0, _cfg_float("MONITORING_WATCH_ALERT_COOLDOWN_SECONDS", 300.0))
    confirm_s = MONITORING_WATCH_CONFIRM_SECONDS
    match_rp = _lark_env_truthy("MONITORING_WATCH_MATCH_REPORT_WINDOW")
    quiet_wins = _monitoring_watch_daily_quiet_windows()
    if not quiet_wins:
        q_note = "daily_quiet=disabled"
    else:
        parts: List[str] = []
        for qs, qe in quiet_wins:
            sh, sm = qs // 3600, (qs // 60) % 60
            eh, em = qe // 3600, (qe // 60) % 60
            parts.append(f"{sh:02d}:{sm:02d}–{eh:02d}:{em:02d}")
        q_note = f"daily_quiet=on {' + '.join(parts)} (end exclusive; no fetch/alert)"
    logger.info(
        "monitoring watchdog started interval=%.0fs cooldown=%.0fs confirm=%.0fs match_report_window=%s "
        "alert_chat=%r target_user=%r %s",
        sec,
        cool,
        confirm_s,
        match_rp,
        bool((MONITORING_ALERT_CHAT_ID or "").strip()),
        bool((TARGET_USER_OPEN_ID or "").strip()),
        q_note,
    )
    while True:
        try:
            alert_chat = (MONITORING_ALERT_CHAT_ID or "").strip()
            if not alert_chat:
                logger.warning("monitoring watchdog: MONITORING_ALERT_CHAT_ID empty — skip this cycle")
                time.sleep(sec)
                continue

            if _monitoring_watch_in_daily_quiet_local():
                _monitoring_watch_pending_confirm = None
                logger.debug(
                    "monitoring watchdog: skip — daily quiet window (MONITORING_WATCH_QUIET_WINDOW_ENABLE=0 to disable)"
                )
                time.sleep(sec)
                continue

            pc = _monitoring_watch_pending_confirm
            if pc is not None and confirm_s > 0 and not match_rp:
                ps, pe, deadline = pc
                now_m = time.monotonic()
                if now_m < deadline:
                    time.sleep(min(sec, max(1.0, deadline - now_m)))
                    continue
                _tls_analysis_drop.watchdog = True
                try:
                    min_age_c = MONITORING_WATCH_MIN_LAST_BUCKET_AGE_SECONDS
                    if min_age_c > 0:
                        age_c = time.time() - float(pe)
                        if age_c < min_age_c:
                            logger.debug(
                                "monitoring watchdog: skip confirm fetch — bucket end=%s age=%.1fs < min=%.1fs",
                                pe,
                                age_c,
                                min_age_c,
                            )
                            time.sleep(sec)
                            continue
                    sess_c = grafana_login_session()
                    payload_c = fetch_monitoring_payload(
                        session=sess_c,
                        for_watchdog=True,
                        start_unix=ps,
                        end_unix=pe,
                    )
                except Exception:
                    logger.exception("monitoring watchdog confirm fetch failed")
                    time.sleep(sec)
                    continue
                finally:
                    _tls_analysis_drop.watchdog = False

                _monitoring_watch_pending_confirm = None
                if not _monitoring_payload_hit_alert(payload_c):
                    logger.info(
                        "monitoring watchdog: pending alert CLEARED — false alert avoided "
                        "(frozen window unix %s..%s)",
                        ps,
                        pe,
                    )
                    time.sleep(sec)
                    continue

                now_m = time.monotonic()
                with _monitoring_reply_dispatch_lock:
                    prev = _monitoring_watch_last_alert_at
                    if cool > 0 and prev > 0 and (now_m - prev) < cool:
                        logger.info(
                            "monitoring watchdog alert skipped by cooldown after confirm (%.0fs left)",
                            cool - (now_m - prev),
                        )
                        time.sleep(sec)
                        continue
                    _monitoring_watch_last_alert_at = now_m

                reply = _format_alert_trigger_reply(payload_c)
                pre_key: Optional[str] = None
                # Screenshot up-front (series-isolated) so the AI can review it before we post.
                alert_png: Optional[bytes] = None
                if _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"):
                    try:
                        alert_png = _grafana_watchdog_alert_screenshot_png(sess_c, payload_c)
                    except Exception:
                        logger.exception("monitoring watchdog pre-screenshot failed")

                # Second review: only page the group if the local AI (Qwen) judges the isolated
                # screenshot abnormal; the AI explanation is appended to ``reply``.
                ai_ok, reply = _monitoring_ai_gate_decide(
                    [alert_png] if alert_png is not None else [], reply
                )
                if not ai_ok:
                    logger.info(
                        "monitoring watchdog: alert suppressed by AI gate (after confirm) chat_prefix=%s...",
                        alert_chat[:16],
                    )
                    time.sleep(sec)
                    continue

                if alert_png is not None and _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT"):
                    try:
                        pre_key = _lark_upload_png_image_key(alert_png)
                    except Exception:
                        logger.exception("monitoring watchdog pre-screenshot upload failed")

                used_card, embedded = _lark_send_monitoring_user_message(
                    "chat_id",
                    alert_chat,
                    reply,
                    pre_key if _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT") else None,
                )
                logger.info(
                    "monitoring watchdog alert sent (after confirm) chat_prefix=%s... card=%s embedded_png=%s",
                    alert_chat[:16],
                    used_card,
                    embedded,
                )

                if _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE") and not embedded:
                    if pre_key:
                        try:
                            _lark_send_image_message("chat_id", alert_chat, pre_key)
                            logger.info("monitoring watchdog screenshot sent via pre_key")
                        except Exception:
                            logger.exception("monitoring watchdog pre_key image send failed")
                    else:
                        try:
                            png = (
                                alert_png
                                if alert_png is not None
                                else _grafana_watchdog_alert_screenshot_png(sess_c, payload_c)
                            )
                            key = _lark_upload_png_image_key(png)
                            _lark_send_image_message("chat_id", alert_chat, key)
                            logger.info("monitoring watchdog screenshot sent bytes=%s", len(png))
                        except Exception:
                            logger.exception("monitoring watchdog screenshot send failed")
                time.sleep(sec)
                continue

            _tls_analysis_drop.watchdog = True
            try:
                if not match_rp:
                    min_age = MONITORING_WATCH_MIN_LAST_BUCKET_AGE_SECONDS
                    if min_age > 0:
                        _, w_end = _monitoring_watch_eval_window_unix()
                        age = time.time() - float(w_end)
                        if age < min_age:
                            logger.debug(
                                "monitoring watchdog: skip eval — newest bucket end=%s age=%.1fs < min=%.1fs",
                                w_end,
                                age,
                                min_age,
                            )
                            time.sleep(sec)
                            continue

                sess = grafana_login_session()
                payload = fetch_monitoring_payload(session=sess, for_watchdog=True)
                if not _monitoring_payload_hit_alert(payload):
                    time.sleep(sec)
                    continue

                now_m = time.monotonic()
                with _monitoring_reply_dispatch_lock:
                    prev = _monitoring_watch_last_alert_at
                    if cool > 0 and prev > 0 and (now_m - prev) < cool:
                        logger.info(
                            "monitoring watchdog alert skipped by cooldown (%.0fs left)",
                            cool - (now_m - prev),
                        )
                        time.sleep(sec)
                        continue

                if confirm_s > 0 and not match_rp:
                    w_s, w_e = _monitoring_watch_eval_window_unix()
                    _monitoring_watch_pending_confirm = (w_s, w_e, time.monotonic() + confirm_s)
                    logger.info(
                        "monitoring watchdog: threshold breach pending confirm in %.0fs "
                        "(frozen window unix %s..%s)",
                        confirm_s,
                        w_s,
                        w_e,
                    )
                    time.sleep(sec)
                    continue

                with _monitoring_reply_dispatch_lock:
                    _monitoring_watch_last_alert_at = now_m

                reply = _format_alert_trigger_reply(payload)
                pre_key = None
                # Screenshot up-front (series-isolated) so the AI can review it before we post.
                alert_png = None
                if _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"):
                    try:
                        alert_png = _grafana_watchdog_alert_screenshot_png(sess, payload)
                    except Exception:
                        logger.exception("monitoring watchdog pre-screenshot failed")

                # Second review: only page the group if the local AI (Qwen) judges the isolated
                # screenshot abnormal; the AI explanation is appended to ``reply``.
                ai_ok, reply = _monitoring_ai_gate_decide(
                    [alert_png] if alert_png is not None else [], reply
                )
                if not ai_ok:
                    logger.info(
                        "monitoring watchdog: alert suppressed by AI gate chat_prefix=%s...",
                        alert_chat[:16],
                    )
                    time.sleep(sec)
                    continue

                if alert_png is not None and _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT"):
                    try:
                        pre_key = _lark_upload_png_image_key(alert_png)
                    except Exception:
                        logger.exception("monitoring watchdog pre-screenshot upload failed")

                used_card, embedded = _lark_send_monitoring_user_message(
                    "chat_id",
                    alert_chat,
                    reply,
                    pre_key if _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT") else None,
                )
                logger.info(
                    "monitoring watchdog alert sent chat_prefix=%s... card=%s embedded_png=%s",
                    alert_chat[:16],
                    used_card,
                    embedded,
                )

                if _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE") and not embedded:
                    if pre_key:
                        try:
                            _lark_send_image_message("chat_id", alert_chat, pre_key)
                            logger.info("monitoring watchdog screenshot sent via pre_key")
                        except Exception:
                            logger.exception("monitoring watchdog pre_key image send failed")
                    else:
                        try:
                            png = (
                                alert_png
                                if alert_png is not None
                                else _grafana_watchdog_alert_screenshot_png(sess, payload)
                            )
                            key = _lark_upload_png_image_key(png)
                            _lark_send_image_message("chat_id", alert_chat, key)
                            logger.info("monitoring watchdog screenshot sent bytes=%s", len(png))
                        except Exception:
                            logger.exception("monitoring watchdog screenshot send failed")
            finally:
                _tls_analysis_drop.watchdog = False
        except Exception:
            logger.exception("monitoring watchdog cycle failed")
        time.sleep(sec)


def _start_monitoring_watchdog_if_enabled() -> None:
    global _monitoring_watch_started
    if not _lark_env_truthy("MONITORING_WATCH_ENABLE"):
        logger.info("monitoring watchdog disabled (MONITORING_WATCH_ENABLE=0)")
        return
    with _monitoring_reply_dispatch_lock:
        if _monitoring_watch_started:
            return
        _monitoring_watch_started = True
    threading.Thread(target=_monitoring_watchdog_loop, daemon=True, name="monitoring-watchdog").start()


def _run_monitoring_background_job(
    chat_id: str,
    open_id: str,
    mid: str,
    dispatch_key: str,
    source_chat_aliases: Optional[List[str]] = None,
) -> None:
    try:
        _monitoring_background_worker(chat_id, open_id, mid, dispatch_key, source_chat_aliases)
    finally:
        if dispatch_key:
            with _monitoring_reply_dispatch_lock:
                _monitoring_inflight_keys.discard(dispatch_key)


def _monitoring_background_worker(
    chat_id: str,
    open_id: str,
    mid: str,
    dispatch_key: str = "",
    source_chat_aliases: Optional[List[str]] = None,
) -> None:
    """
    Grafana + Lark send can exceed Feishu's ~3s webhook limit — run off the request thread.
    """
    logger.info("monitoring background job start mid=%r chat=%r open=%r", mid, bool(chat_id), bool(open_id))
    conv_key = (chat_id or "").strip() or (f"open:{(open_id or '').strip()}" if (open_id or "").strip() else "")
    if conv_key and not _monitoring_try_begin_chat_send(conv_key):
        logger.warning(
            "monitoring: blocked duplicate by conversation gate key=%r (MONITORING_CHAT_COALESCE_SECONDS)",
            conv_key[:96],
        )
        return
    if dispatch_key and not _monitoring_try_begin_user_send(dispatch_key):
        logger.warning(
            "monitoring: blocked duplicate **user-visible** send (MONITORING_SEND_COALESCE_SECONDS or concurrent send)"
        )
        _monitoring_end_chat_send(conv_key, False)
        return

    user_visible_send_ok = False
    try:
        grafana_session: Optional[requests.Session] = None
        payload: Optional[Dict[str, Any]] = None
        alert_hit = False
        try:
            grafana_session = grafana_login_session()
            payload = fetch_monitoring_payload(session=grafana_session)
            alert_hit = _monitoring_payload_hit_alert(payload)
            reply = _format_monitoring_reply(payload, include_target_mention=not alert_hit)
            if alert_hit and payload is not None:
                reply = _format_alert_trigger_reply(payload) + "\n\n---\n\n" + reply
        except Exception as e:
            logger.exception("monitoring fetch failed (background)")
            reply = f"Failed to fetch monitoring data: {e}"
            grafana_session = None
            payload = None
            alert_hit = False

        sent = False
        used_interactive_card = False
        embedded_png_in_card = False
        try:
            # Never block the Lark reply on Playwright: pre-screenshot-before-send left users with **no**
            # message until Grafana finished (often minutes). Send card/text first; screenshot follows below.
            pre_key: Optional[str] = None

            if chat_id:
                used_interactive_card, embedded_png_in_card = _lark_send_monitoring_user_message(
                    "chat_id", chat_id, reply, None
                )
                sent = True
                user_visible_send_ok = True
                logger.info(
                    "monitoring reply sent (background) chat_id_prefix=%s... len=%s card=%s embedded_png=%s",
                    chat_id[:16],
                    len(reply),
                    used_interactive_card,
                    embedded_png_in_card,
                )
            elif open_id:
                used_interactive_card, embedded_png_in_card = _lark_send_monitoring_user_message(
                    "open_id", open_id, reply, None
                )
                sent = True
                user_visible_send_ok = True
                logger.info(
                    "monitoring reply sent (background) open_id len=%s card=%s embedded_png=%s",
                    len(reply),
                    used_interactive_card,
                    embedded_png_in_card,
                )
            else:
                logger.warning(
                    "monitoring background: no chat_id/open_chat_id or sender open_id; msg cannot be sent"
                )

            alert_chat_id = (MONITORING_ALERT_CHAT_ID or "").strip()
            alert_reply = _format_alert_trigger_reply(payload) if alert_hit and payload is not None else reply
            src_alias = {str(x).strip() for x in (source_chat_aliases or []) if str(x).strip()}
            if (chat_id or "").strip():
                src_alias.add((chat_id or "").strip())
            suppress_alert_copy = alert_chat_id in src_alias
            if alert_hit and alert_chat_id and not suppress_alert_copy:
                try:
                    _lark_send_text_auto("chat_id", alert_chat_id, alert_reply, max_chars=3200)
                    logger.info(
                        "monitoring alert copy sent (background) alert_chat_id_prefix=%s... len=%s",
                        alert_chat_id[:16],
                        len(alert_reply),
                    )
                except Exception:
                    logger.exception(
                        "monitoring alert forward failed (background) alert_chat_id=%r",
                        alert_chat_id[:24],
                    )
            elif alert_hit and alert_chat_id and suppress_alert_copy:
                logger.info(
                    "monitoring alert copy skipped: source chat matches MONITORING_ALERT_CHAT_ID alias"
                )

            _raw_ss = _cfg_raw("GRAFANA_SCREENSHOT_ENABLE")
            logger.info(
                "monitoring screenshot gate sent=%s session=%s payload=%s ENABLE_raw=%r ENABLE_truthy=%s",
                sent,
                grafana_session is not None,
                payload is not None,
                _raw_ss,
                _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"),
            )

            if sent and grafana_session is not None and payload is not None:
                if not _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"):
                    logger.info(
                        "monitoring screenshot skipped: set GRAFANA_SCREENSHOT_ENABLE=1 (and install playwright + chromium)"
                    )
                elif used_interactive_card and embedded_png_in_card:
                    logger.info(
                        "monitoring: Grafana PNG embedded in interactive card — no separate image message"
                    )
                elif pre_key:
                    try:
                        if chat_id:
                            _lark_send_image_message("chat_id", chat_id, pre_key)
                        else:
                            _lark_send_image_message("open_id", open_id, pre_key)
                        logger.info(
                            "monitoring Grafana screenshot sent (fallback image after text/card) pre_key set"
                        )
                    except Exception:
                        logger.exception(
                            "monitoring follow-up image send failed (card may have been plain text)"
                        )
                else:
                    try:
                        jar = grafana_session.cookies.get_dict()
                        if "grafana_session" not in jar:
                            logger.warning(
                                "monitoring screenshot: no grafana_session cookie — expect login wall in PNG"
                            )
                        n_cookies = len(_playwright_cookie_list(grafana_session))
                        logger.info(
                            "monitoring screenshot start cookies=%s alert_hit=%s",
                            n_cookies,
                            alert_hit,
                        )
                        png = _grafana_monitoring_screenshot_png(
                            grafana_session, payload, for_alert=alert_hit
                        )
                        key = _lark_upload_png_image_key(png)
                        if chat_id:
                            _lark_send_image_message("chat_id", chat_id, key)
                        else:
                            _lark_send_image_message("open_id", open_id, key)
                        logger.info(
                            "monitoring Grafana screenshot sent (background) bytes=%s",
                            len(png),
                        )
                    except Exception:
                        logger.exception(
                            "monitoring Grafana screenshot or Lark image upload failed (text was already sent)"
                        )
            elif sent:
                logger.warning(
                    "monitoring screenshot skipped: sent text but grafana_session or payload is missing (unexpected)"
                )
        except Exception as e:
            logger.exception("monitoring lark text/image failed (background): %s", e)

        if sent and mid and len(_processed_lark_message_ids) > _PROCESSED_LARK_IDS_CAP:
            for _ in range(500):
                if len(_processed_lark_message_ids) <= _PROCESSED_LARK_IDS_CAP - 200:
                    break
                try:
                    _processed_lark_message_ids.pop()
                except KeyError:
                    break
    finally:
        if dispatch_key:
            _monitoring_end_user_send(dispatch_key, user_visible_send_ok)
        if conv_key:
            _monitoring_end_chat_send(conv_key, user_visible_send_ok)


def _serialize_lark_user_id(uid: Any) -> Dict[str, Any]:
    if uid is None:
        return {}
    out: Dict[str, Any] = {}
    for k in ("user_id", "open_id", "union_id"):
        v = getattr(uid, k, None)
        if v:
            out[k] = v
    return out


def _lark_ws_sdk_event_to_dict(model: Any) -> Dict[str, Any]:
    """
    Normalize WebSocket handler payloads to plain dict (same shape as HTTP webhook).

    Feishu docs recommend ``register_p2_im_message_receive_v1`` for long connection; that passes
    typed SDK models. ``JSON.marshal`` converts nested objects reliably; ``CustomizedEvent`` works too.
    """
    from lark_oapi.core.json import JSON

    if isinstance(model, dict):
        out = dict(model)
        _lark_coerce_event_dict(out)
        return out if isinstance(out, dict) else {}
    try:
        s = JSON.marshal(model)
        if not s:
            return {}
        obj = json.loads(s)
        if isinstance(obj, dict):
            _lark_coerce_event_dict(obj)
            return obj
    except Exception as e:
        logger.warning("Lark WS SDK event JSON marshal failed: %s", e)
    return {}


def _lark_customized_event_to_schema2_dict(ce: Any) -> Dict[str, Any]:
    """Backward-compatible path for customized handlers; prefer :func:`_lark_ws_sdk_event_to_dict`."""
    return _lark_ws_sdk_event_to_dict(ce)


def _process_im_message_event(data: Dict[str, Any]) -> None:
    """
    Shared handler for ``im.message`` from HTTP webhook or WebSocket (``CustomizedEvent`` v1/v2).
    HTTP path verifies token before calling; WS path uses ``EventDispatcherHandler.builder('', '')``.
    """
    try:
        _process_im_message_event_impl(data)
    except Exception:
        logger.exception("im.message handler crashed (swallowed so WS / HTTP worker stays up)")


def _process_im_message_event_impl(data: Dict[str, Any]) -> None:
    if isinstance(data, dict):
        data = _lark_coerce_event_dict(data)
    event = data.get("event") if isinstance(data.get("event"), dict) else {}
    raw_msg = event.get("message")
    msg = raw_msg if isinstance(raw_msg, dict) else {}
    mid = _lark_im_message_dedupe_id(msg)
    mtype = (_lark_dict_pick_str(msg, "message_type", "messageType") or "").lower()
    chat_resolved = _lark_message_chat_id(msg)
    im_chat_type_log = _lark_dict_pick_str(msg, "chat_type", "chatType") or ""
    raw_preview = ""
    if isinstance(msg, dict):
        raw_preview = (_lark_extract_plain_text_from_message(msg) or "")[:100]
    logger.info(
        "im.message mid=%r mtype=%r chat_type=%r chat_prefix=%r raw_preview=%r deploy_payload=%s",
        mid or None,
        mtype or None,
        im_chat_type_log or None,
        (chat_resolved[:12] + "…") if len(chat_resolved) > 12 else (chat_resolved or None),
        raw_preview or None,
        _lark_payload_looks_deploy_like(data),
    )
    logger.debug("im.message msg_keys=%s", list(msg.keys())[:24] if isinstance(msg, dict) else [])
    if mtype and mtype in _SKIP_IM_MESSAGE_TYPES:
        logger.info("im.message ignored (non-textual): message_type=%r", mtype)
        return

    send_wrap = event.get("sender")
    if not isinstance(send_wrap, dict):
        send_wrap = {}
    sid = send_wrap.get("sender_id") or send_wrap.get("senderId")
    if isinstance(sid, dict):
        sender = sid
    elif sid is not None and hasattr(sid, "open_id"):
        sender = _serialize_lark_user_id(sid)
    else:
        sender = {}
    sender_open = _lark_dict_pick_str(sender, "open_id", "openId", "user_id", "userId")
    _bot_self = _lark_effective_bot_open_id()
    if _bot_self and sender_open == _bot_self:
        return

    raw_text = _lark_extract_plain_text_from_message(msg)
    if not (raw_text or "").strip():
        fb = _lark_dict_pick_str(event, "text_without_at_bot", "textWithoutAtBot", "text")
        if fb:
            raw_text = fb
    mentions = _lark_collect_im_message_mentions(msg, event)
    clean = _lark_clean_command_text(raw_text, mentions)
    content_at_entity_ids = _lark_extract_at_entity_ids_from_im_message(msg, mentions_list=mentions)
    im_chat_type = im_chat_type_log

    chat_id = chat_resolved
    open_id = sender_open
    chat_aliases = _lark_message_chat_id_aliases(msg)
    sender_debounce = _lark_im_sender_debounce_token(sender, open_id)
    im_event_id = _lark_im_payload_event_id(data)
    msg_time = _lark_im_message_time_token(msg)
    deploy_blobs = _deploy_message_text_blobs(msg, event, raw_text or "", clean or "")
    deploy_like = _im_text_matches_deploy_request(*deploy_blobs)

    if _deploy_try_handle_im_message(
        msg=msg,
        event=event,
        data=data,
        raw_text=raw_text or "",
        clean=clean or "",
        mentions=mentions,
        content_at_entity_ids=content_at_entity_ids,
        im_chat_type=im_chat_type,
        chat_id=chat_id,
        open_id=open_id or "",
        sender=sender,
        send_wrap=send_wrap,
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: "/whoami" → tell the sender their p0bot-namespace open_id (+ chat_id).
    if _p0_try_handle_whoami(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
        im_chat_type=im_chat_type,
    ):
        return

    # p0bot: "/confirmp0" → consume a pending "is this P0?" detection.
    if _p0_try_handle_confirmp0(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: passive "p0" keyword watch — never blocks the rest of the dispatch chain.
    try:
        _p0_detect_maybe_fire(
            chat_id=chat_id,
            clean=clean or "",
            mid=mid,
            im_event_id=im_event_id,
            sender_debounce=sender_debounce,
            msg_time=msg_time,
        )
    except Exception:
        logger.exception("p0 detect: observer failed")

    # p0bot: bot-hosted meeting (/openmeeting, /endmeeting) — before other commands.
    if _p0_try_handle_openmeeting(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: "/checkmeeting <name>" → find matching participants + join/leave times.
    if _p0_try_handle_checkmeeting(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: "/members" → who is in this CHAT GROUP (bot's own token; needs to be in the group).
    if _p0_try_handle_members(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: admin OAuth for the meeting report (/vcauth, /vccode) — before /meeting.
    if _p0_try_handle_vcauth(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: "/osemeeting <meeting> <doc link>" → write the bilingual OSE meeting minutes.
    if _p0_try_handle_osemeeting(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: "/p0docs <meeting> <doc link>" → fill the P0 incident doc from the meeting.
    if _p0_try_handle_p0docs(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: "/whotalk [minutes-link|meeting-link|no]" → speaker transcript via Minutes + Qwen.
    if _p0_try_handle_whotalk(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: "/meeting <link-or-number>" → attendance report (before doc Q&A).
    if _p0_try_handle_meeting(
        chat_id=chat_id,
        open_id=open_id or "",
        clean=clean or "",
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    # p0bot: doc Q&A takes precedence (answers questions from the cached wiki doc).
    if _p0_try_handle_doc_qa(
        chat_id=chat_id,
        open_id=open_id or "",
        raw_text=raw_text or "",
        clean=clean or "",
        mentions=mentions,
        msg=msg,
        im_chat_type=im_chat_type,
        mid=mid,
        im_event_id=im_event_id,
        sender_debounce=sender_debounce,
        msg_time=msg_time,
    ):
        return

    sp_cmd: Optional[str] = None
    req_at_bot = MONITORING_TRIGGER_REQUIRES_AT_BOT
    mute_or_cancel = _im_command_matches(clean or "", MONITORING_MUTE_TRIGGER) or _im_command_matches(
        clean or "", MONITORING_CANCELMUTE_TRIGGER
    )
    # Only evaluate @-routing when /m|/c present — avoids ``monitoring: skip`` spam + wasted work on normal chat.
    monitoring_addressed_ok = True
    if req_at_bot and mute_or_cancel:
        monitoring_addressed_ok = _monitoring_at_bot_requirement_satisfied(
            raw_text,
            mentions,
            content_at_entity_ids=content_at_entity_ids,
            msg=msg,
            chat_type=im_chat_type,
        )
    # Loose OR strict: ``_lark_im_bot_addressed_in_mentions_or_body`` alone misses ``@_user_N`` + skewed
    # ``mentions[]`` where primary @ resolution still picks this bot (same rules as ``/mo``).
    _mute_cancel_allowed = (
        not req_at_bot
        or _lark_im_bot_addressed_in_mentions_or_body(mentions, content_at_entity_ids)
        or monitoring_addressed_ok
    )
    if mute_or_cancel and req_at_bot and not _mute_cancel_allowed:
        logger.info(
            "im.command @gate reject: %s raw=%r clean=%r (same primary rules as /mo — payload targets another bot)",
            ("mute /m" if _im_command_matches(clean or "", MONITORING_MUTE_TRIGGER) else "cancelmute /c"),
            (raw_text or "")[:160],
            (clean or "")[:160],
        )
    if _im_command_matches(clean or "", MONITORING_MUTE_TRIGGER):
        if _mute_cancel_allowed:
            sp_cmd = "mute"
    elif _im_command_matches(clean or "", MONITORING_CANCELMUTE_TRIGGER):
        if _mute_cancel_allowed:
            sp_cmd = "cancelmute"

    if sp_cmd:
        if req_at_bot and not monitoring_addressed_ok:
            logger.info(
                "%s skip — not addressed to this bot (MONITORING_TRIGGER_REQUIRES_AT_BOT=1)",
                sp_cmd,
            )
            return
        processed_stick_m = _monitoring_processed_stick(
            mid, im_event_id, chat_id or "", sender_debounce, msg_time
        )
        body_key_m = "__mute_cmd__" if sp_cmd == "mute" else "__cancelmute_cmd__"
        debounce_key_m = f"{(chat_id or '').strip()}\n{body_key_m}"
        now_mm = time.monotonic()
        with _monitoring_reply_dispatch_lock:
            if im_event_id and im_event_id in _processed_lark_im_event_ids:
                logger.info("duplicate IM event_id=%s — skip (%s)", im_event_id, sp_cmd)
                return
            if processed_stick_m and processed_stick_m in _processed_lark_message_ids:
                logger.info("duplicate %s dispatch stick=%r — skip", sp_cmd, processed_stick_m[:96])
                return
            if debounce_key_m in _monitoring_inflight_keys:
                logger.info("%s skip — already in flight", sp_cmd)
                return
            _monitoring_inflight_keys.add(debounce_key_m)
            if processed_stick_m:
                _processed_lark_message_ids.add(processed_stick_m)
            if im_event_id:
                _processed_lark_im_event_ids.add(im_event_id)
                if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                    _processed_lark_im_event_ids.clear()
                    _processed_lark_im_event_ids.add(im_event_id)
        logger.info("%s command accepted chat=%r open=%r", sp_cmd, bool(chat_id), bool(open_id))
        if sp_cmd == "mute":
            threading.Thread(
                target=_mute_send_selection_card_worker,
                args=(chat_id, open_id, debounce_key_m),
                daemon=True,
                name="mute-selection-card",
            ).start()
        else:
            threading.Thread(
                target=_cancelmute_worker,
                args=(chat_id, open_id, debounce_key_m),
                daemon=True,
                name="cancelmute",
            ).start()
        return

    cn = re.sub(r"\s+", " ", (clean or "").strip().lower())
    if (
        _lark_effective_bot_open_id()
        and _lark_message_mentions_bot(mentions)
        and cn
        and not _im_command_matches(clean or "", MONITORING_TRIGGER)
        and not _im_command_matches(clean or "", MONITORING_MUTE_TRIGGER)
        and not _im_command_matches(clean or "", MONITORING_CANCELMUTE_TRIGGER)
        and not deploy_like
    ):
        if MONITORING_TRIGGER_REQUIRES_AT_BOT and not _monitoring_at_bot_requirement_satisfied(
            raw_text,
            mentions,
            content_at_entity_ids=content_at_entity_ids,
            msg=msg,
            chat_type=im_chat_type,
        ):
            logger.info("cmd-help skip — @ not addressed to this bot")
            return
        processed_h = _monitoring_processed_stick(
            mid, im_event_id, chat_id or "", sender_debounce, msg_time
        )
        debounce_key_h = f"{(chat_id or '').strip()}\n__cmd_help__"
        with _monitoring_reply_dispatch_lock:
            if im_event_id and im_event_id in _processed_lark_im_event_ids:
                logger.info("duplicate IM event_id=%s — skip (cmd help)", im_event_id)
                return
            if processed_h and processed_h in _processed_lark_message_ids:
                logger.info("duplicate cmd-help stick=%r — skip", processed_h[:96])
                return
            if debounce_key_h in _monitoring_inflight_keys:
                logger.info("cmd help skip — already in flight")
                return
            _monitoring_inflight_keys.add(debounce_key_h)
            if processed_h:
                _processed_lark_message_ids.add(processed_h)
            if im_event_id:
                _processed_lark_im_event_ids.add(im_event_id)
                if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                    _processed_lark_im_event_ids.clear()
                    _processed_lark_im_event_ids.add(im_event_id)
        logger.info("at-mention non-command — sending cmd help chat=%r", bool(chat_id))
        threading.Thread(
            target=_monitoring_at_mention_help_worker,
            args=(chat_id, open_id, debounce_key_h),
            daemon=True,
            name="cmd-help",
        ).start()
        return

    if not _text_should_run_monitoring(
        raw_text,
        clean,
        mentions,
        content_at_entity_ids=content_at_entity_ids,
        msg=msg,
        chat_type=im_chat_type,
    ):
        ml = mentions if isinstance(mentions, list) else []
        body_ph = _lark_raw_text_has_feishu_at_placeholder(raw_text)
        mo_ph_blocked_by_other = (
            _lark_mentions_carry_strong_identity_other_than_bot(
                _lark_effective_bot_open_id(),
                str(APP_ID).strip() if APP_ID else "",
                ml,
            )
            if ml
            else False
        )
        mute_like = _im_command_matches(clean or "", MONITORING_MUTE_TRIGGER) or _im_command_matches(
            clean or "", MONITORING_CANCELMUTE_TRIGGER
        )
        log_fn = logger.warning if _text_has_monitoring_trigger(raw_text, clean) else logger.info
        log_fn(
            "im.message no trigger raw=%r clean=%r chat_type=%r mentions=%s mo/mute/cancel=%r/%r/%r "
            "require_at_bot_for_mo=%s mo_placeholder_cfg=%s mo_weak_nonempty_allow=%s body_has_@_user_N=%s "
            "mentions_other_ou_cli=%s bot_open_id_known=%s mute_cancel_cmd=%s",
            (raw_text or "")[:160],
            (clean or "")[:160],
            im_chat_type or None,
            len(mentions),
            MONITORING_TRIGGER,
            MONITORING_MUTE_TRIGGER,
            MONITORING_CANCELMUTE_TRIGGER,
            MONITORING_TRIGGER_REQUIRES_AT_BOT,
            MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER,
            MONITORING_MO_WEAK_NONEMPTY_MENTIONS_ALLOW,
            body_ph,
            mo_ph_blocked_by_other,
            bool((_lark_effective_bot_open_id() or "").strip()),
            mute_like,
        )
        if _text_has_monitoring_trigger(raw_text, clean):
            _monitoring_maybe_send_at_gate_feedback(
                chat_id=chat_id,
                open_id=open_id,
                clean=clean,
                raw_text=raw_text,
                chat_type=im_chat_type,
            )
        elif deploy_like and _deploy_sender_authorized(sender, open_id or "", send_wrap):
            logger.warning(
                "deploy-like IM reached no-trigger exit — open_id=%r (unexpected)",
                (open_id or "")[:24],
            )
            try:
                _deploy_reply(
                    chat_id,
                    open_id,
                    "Deploy (Grafana Game Bot): message seen but not handled — server may need restart with latest code.",
                )
            except Exception:
                logger.exception("deploy no-trigger fallback failed")
        return

    body_key = _monitoring_dispatch_body_key(clean, raw_text, mentions)
    processed_stick = _monitoring_processed_stick(
        mid, im_event_id, chat_id or "", sender_debounce, msg_time
    )

    logger.info(
        "monitoring trigger matched — background job mid=%r event_id=%r msg_time=%r stick=%r chat_id=%r open_id_prefix=%r",
        mid,
        im_event_id or None,
        msg_time or None,
        (processed_stick[:72] + "…") if len(processed_stick) > 72 else (processed_stick or None),
        bool(chat_id),
        (open_id[:12] + "…") if len(open_id) > 12 else open_id,
    )

    debounce_sec = 0.0
    raw_db = _cfg_raw("MONITORING_IM_DEBOUNCE_SECONDS")
    if raw_db is not None and str(raw_db).strip() != "":
        try:
            debounce_sec = float(raw_db)
        except (TypeError, ValueError):
            debounce_sec = 5.0
    # Some duplicated Feishu deliveries for the same human message can differ in sender/message envelope fields.
    # Keep debounce/send key stable on chat + normalized command body only, so variants collapse into one worker.
    debounce_key = f"{(chat_id or '').strip()}\n{body_key}"
    chat_gate_key = (chat_id or "").strip() or (f"open:{(open_id or '').strip()}" if (open_id or "").strip() else "")
    chat_gate_sec = _cfg_float("MONITORING_CHAT_TRIGGER_DEBOUNCE_SECONDS", 8.0)
    now_m = time.monotonic()
    with _monitoring_reply_dispatch_lock:
        if chat_gate_key and chat_gate_sec > 0:
            prev_chat = _monitoring_chat_trigger_last.get(chat_gate_key, 0.0)
            if prev_chat > 0.0 and (now_m - prev_chat) < chat_gate_sec:
                logger.info(
                    "monitoring chat-trigger debounce skip (%.2fs) key=%r",
                    chat_gate_sec,
                    chat_gate_key[:96],
                )
                return
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            logger.info("duplicate IM event_id=%s — skip", im_event_id)
            return
        if processed_stick and processed_stick in _processed_lark_message_ids:
            logger.info("duplicate monitoring dispatch stick=%r — skip", processed_stick[:96])
            return
        if debounce_key in _monitoring_inflight_keys:
            logger.info("monitoring skip — same trigger already **in flight** (wait for job to finish)")
            return
        if debounce_sec > 0:
            prev_t = _monitoring_im_trigger_last.get(debounce_key, 0.0)
            if now_m - prev_t < debounce_sec:
                logger.info(
                    "monitoring debounce skip (%.2fs) chat=%r",
                    debounce_sec,
                    bool(chat_id),
                )
                return
            _monitoring_im_trigger_last[debounce_key] = now_m
            if len(_monitoring_im_trigger_last) > 600:
                for k, _ in sorted(_monitoring_im_trigger_last.items(), key=lambda kv: kv[1])[:220]:
                    try:
                        del _monitoring_im_trigger_last[k]
                    except KeyError:
                        pass
                _monitoring_im_trigger_last[debounce_key] = now_m
        if chat_gate_key and chat_gate_sec > 0:
            _monitoring_chat_trigger_last[chat_gate_key] = now_m
            if len(_monitoring_chat_trigger_last) > 600:
                for k, _ in sorted(_monitoring_chat_trigger_last.items(), key=lambda kv: kv[1])[:220]:
                    try:
                        del _monitoring_chat_trigger_last[k]
                    except KeyError:
                        pass
                _monitoring_chat_trigger_last[chat_gate_key] = now_m
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    threading.Thread(
        target=_run_monitoring_background_job,
        args=(chat_id, open_id, mid, debounce_key, chat_aliases),
        daemon=True,
        name="monitoring-reply",
    ).start()


def _ws_log_message_snip(data: Dict[str, Any]) -> Tuple[Any, Any, str]:
    """Safe for ``event.message`` missing or null (``dict.get('message', {})`` returns None if key exists)."""
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    msg = ev.get("message") or {}
    if not isinstance(msg, dict):
        msg = {}
    mid = _lark_im_message_dedupe_id(msg) or None
    mtype = _lark_dict_pick_str(msg, "message_type", "messageType") or None
    chat = (_lark_message_chat_id(msg) or "")[:12]
    return mid, mtype, chat


def _handle_im_message_receive(data: Dict[str, Any]) -> Response:
    """
    HTTP path: Feishu ~3s deadline — return ``{}`` immediately (no deepcopy on request thread).
    WebSocket path still calls :func:`_process_im_message_event` synchronously (no HTTP timeout).
    """

    def _worker(ref: Dict[str, Any]) -> None:
        try:
            payload = copy.deepcopy(ref)
            et = _lark_header_event_type(payload)
            if _lark_payload_looks_deploy_like(payload):
                logger.warning(
                    "im ingress HTTP %s — deploy-like payload detected mid=%r",
                    et,
                    ((payload.get("event") or {}).get("message") or {}).get("message_id"),
                )
            logger.info(
                "handling %s (async) message_id=%r chat_id_prefix=%s",
                et,
                ((payload.get("event") or {}).get("message") or {}).get("message_id"),
                str(((payload.get("event") or {}).get("message") or {}).get("chat_id") or "")[:12],
            )
            _process_im_message_event(payload)
        except Exception:
            logger.exception("lark im.message webhook worker failed")

    threading.Thread(target=_worker, args=(data,), daemon=True, name="lark-im-webhook").start()
    return _lark_feishu_webhook_ack_immediate()


def _on_ws_p2_im_message_receive_v1(data: Any) -> None:
    """Official WS handler for ``im.message.receive_v1`` (Feishu long-connection sample code)."""
    try:
        payload = _lark_ws_sdk_event_to_dict(data)
        if _lark_payload_looks_deploy_like(payload):
            logger.warning("im ingress WS im.message.receive_v1 — deploy-like payload detected")
        mid, mtype, chat = _ws_log_message_snip(payload)
        logger.info("ws im.message.receive_v1 mid=%r mtype=%r chat=%r", mid, mtype, chat)
        _lark_ws_mark_im_received()
        _process_im_message_event(payload)
    except Exception:
        logger.exception("WebSocket P2ImMessageReceiveV1 handler failed")


def _on_ws_im_message_p2_customized(ce: Any) -> None:
    """
    Fallback for ``im.message.receive_v2`` or extra types (``LARK_WS_EXTRA_IM_TYPES``).
    ``receive_v1`` is handled by :func:`_on_ws_p2_im_message_receive_v1` per Feishu SDK guidance.
    """
    try:
        et = getattr(getattr(ce, "header", None), "event_type", None) or "?"
        data = _lark_ws_sdk_event_to_dict(ce)
        mid, mtype, chat = _ws_log_message_snip(data)
        logger.info("ws im.message %s mid=%r mtype=%r chat=%r", et, mid, mtype, chat)
        _lark_ws_mark_im_received()
        _process_im_message_event(data)
    except Exception:
        logger.exception("WebSocket im.message customized handler failed")


def _lark_ws_patch_dispatcher_inbound_log(handler: Any) -> None:
    """
    Wrap ``do_without_validation`` so we always see ``header.event_type`` for DATA/EVENT frames.
    Catches ``processor not found`` and logs the missing type (SDK default log may go to another logger).
    """
    orig = handler.do_without_validation

    def _wrapped(payload: bytes) -> Any:
        et_log: Any = None
        try:
            obj = json.loads(payload.decode("utf-8", errors="replace"))
            h = obj.get("header") if isinstance(obj.get("header"), dict) else {}
            et_log = h.get("event_type")
            if et_log:
                logger.info(
                    "Lark WS inbound event_type=%r schema=%r",
                    et_log,
                    obj.get("schema"),
                )
            else:
                logger.info(
                    "Lark WS inbound (no header.event_type) top_keys=%r event_keys=%r",
                    list(obj.keys())[:14],
                    list((obj.get("event") or {}).keys())[:14] if isinstance(obj.get("event"), dict) else None,
                )
        except Exception as ex:
            logger.warning(
                "Lark WS payload not JSON (%s) len=%s head=%r",
                ex,
                len(payload),
                payload[:80],
            )
        try:
            return orig(payload)
        except Exception as e:
            es = str(e).lower()
            if et_log is not None and "processor" in es and "not found" in es:
                logger.error(
                    "Lark WS no handler for event_type=%r — add to LARK_WS_EXTRA_IM_TYPES in .env (comma-separated) "
                    "or upgrade lark-oapi. err=%s",
                    et_log,
                    e,
                )
            raise

    handler.do_without_validation = _wrapped  # type: ignore[method-assign]


def _lark_ws_reset_bootstrap_frame_budget() -> int:
    """How many inbound WS protobuf frames to log at INFO on this connection (0 = off)."""
    global _lark_ws_bootstrap_frames_left
    raw = str(_cfg_int("LARK_WS_BOOTSTRAP_FRAMES", _LARK_WS_BOOTSTRAP_FRAMES_DEFAULT))
    try:
        n = int(raw)
    except ValueError:
        n = _LARK_WS_BOOTSTRAP_FRAMES_DEFAULT
    _lark_ws_bootstrap_frames_left = max(0, min(n, 500))
    return _lark_ws_bootstrap_frames_left


def _lark_ws_install_recv_frame_method_log(client_cls: Any) -> None:
    """
    Always patch inbound ``Frame.method`` logging:

    - By default, first ``LARK_WS_BOOTSTRAP_FRAMES`` frames at INFO (CONTROL vs DATA).
    - Set ``LARK_WS_LOG_FRAME_METHOD=1`` to log **every** frame.

    DATA frames carry Feishu business payloads (often IM events). CONTROL is ping/config.
    If you only ever see CONTROL after @mentioning the bot, Feishu is not pushing IM events to this connection
    (subscription, scopes, duplicate WS consumer, etc.).
    """
    global _lark_ws_recv_method_log_installed
    if _lark_ws_recv_method_log_installed:
        return
    from lark_oapi.ws.enum import FrameType
    from lark_oapi.ws.pb.pbbp2_pb2 import Frame as LarkWsPbFrame

    _orig = client_cls._handle_message

    async def _wrapped_handle_message(self: Any, msg: bytes) -> None:
        global _lark_ws_bootstrap_frames_left
        full = _lark_env_truthy("LARK_WS_LOG_FRAME_METHOD")
        want_log = full or (_lark_ws_bootstrap_frames_left > 0)
        if want_log and not full:
            _lark_ws_bootstrap_frames_left -= 1
        if want_log:
            try:
                pb = LarkWsPbFrame()
                pb.ParseFromString(msg)
                ft = FrameType(pb.method)
                logger.info(
                    "Lark WS recv frame.method=%s bytes=%s (DATA=push payload; CONTROL=heartbeat/config)",
                    getattr(ft, "name", str(ft)),
                    len(msg),
                )
            except Exception as ex:
                logger.warning("Lark WS recv frame parse failed: %s bytes=%s", ex, len(msg))
        return await _orig(self, msg)

    client_cls._handle_message = _wrapped_handle_message  # type: ignore[method-assign]
    _lark_ws_recv_method_log_installed = True


_lark_ws_card_action_patched = False


def _lark_ws_enable_card_actions(client_cls: Any) -> None:
    """Make card button clicks work over the long connection.

    The SDK's WS loop discards CARD frames (``elif message_type == MessageType.CARD: return`` in
    ws/client.py). We re-type the frame's ``type`` header card→event so the ORIGINAL handler takes
    the EVENT branch and calls ``_do_without_validation(payload)`` — which routes by the PAYLOAD's
    event_type (``card.action.trigger``) to the handler registered via
    ``register_p2_card_action_trigger`` and writes its returned toast/card back over the WS.
    Requires the Developer Console 「事件与回调 → 回调配置 → 使用长连接接收事件」 to be on so Feishu
    actually pushes these frames. Applied BEFORE the transport-log wrapper so that still logs 'card'.
    """
    global _lark_ws_card_action_patched
    if _lark_ws_card_action_patched or not _p0_card_buttons_enabled():
        return
    try:
        from lark_oapi.ws.const import HEADER_TYPE
    except Exception:
        logger.exception("card actions: HEADER_TYPE import failed — card buttons unavailable")
        return
    _orig = client_cls._handle_data_frame

    async def _card_aware_handle_data_frame(self: Any, frame: Any) -> None:
        try:
            for h in frame.headers:
                if getattr(h, "key", "") == HEADER_TYPE and getattr(h, "value", "") == "card":
                    h.value = "event"  # let the SDK's EVENT branch route it through the dispatcher
                    break
        except Exception:
            logger.exception("card actions: frame re-type failed")
        return await _orig(self, frame)

    client_cls._handle_data_frame = _card_aware_handle_data_frame  # type: ignore[method-assign]
    _lark_ws_card_action_patched = True
    logger.info("Lark WS card-action patch applied — card button clicks routed to the dispatcher "
                "(needs console 回调配置 → 使用长连接接收事件).")


def _lark_ws_install_transport_frame_log(client_cls: Any) -> None:
    """
    Log every DATA-frame ``header.type`` (e.g. ``event`` / ``card``). Must patch the **same** ``Client`` class
    later used by ``LarkWsClient(...)`` (import identity issues prevented logs on some deployments).
    """
    global _lark_ws_transport_log_installed, _lark_ws_saw_data_frame
    if _lark_ws_transport_log_installed:
        return
    if _cfg_str("LARK_WS_TRANSPORT_LOG", "1").strip().lower() in ("0", "false", "no", "off"):
        logger.info("Lark WS transport frame logging disabled (LARK_WS_TRANSPORT_LOG=0)")
        return

    from lark_oapi.ws.const import HEADER_TYPE
    from lark_oapi.ws import client as _lark_ws_client_mod

    _orig_hdf = client_cls._handle_data_frame

    async def _logged_handle_data_frame(self: Any, frame: Any) -> None:
        global _lark_ws_saw_data_frame
        try:
            hs = frame.headers
            t = _lark_ws_client_mod._get_by_key(hs, HEADER_TYPE)
            plen = len(frame.payload or b"")
            logger.info("Lark WS DATA frame header.type=%r payload_len=%s", t, plen)
            _lark_ws_saw_data_frame = True
        except Exception as ex:
            logger.warning("Lark WS DATA frame log failed: %s", ex)
        return await _orig_hdf(self, frame)

    client_cls._handle_data_frame = _logged_handle_data_frame  # type: ignore[method-assign]
    _lark_ws_transport_log_installed = True
    logger.info(
        "Lark WS transport frame log patch applied to %s._handle_data_frame",
        getattr(client_cls, "__name__", "Client"),
    )


def _lark_ws_start_no_data_watchdog() -> None:
    """If zero DATA frames in 120s, emit ERROR (console subscription / duplicate client)."""

    def _watch() -> None:
        time.sleep(120)
        if _lark_ws_saw_data_frame:
            return
        logger.error(
            "Lark WS: 启动 120 秒内未收到任何 DATA 帧 — 飞书未往本连接推事件。请逐项核对："
            "① 开发者后台「事件与回调」→ 订阅方式必须是「使用长连接接收事件」且保存成功（保存时本服务须已连接）；"
            "② 勿同时选「将回调发送至开发者服务器」；③ 已订阅「消息与群组」→「接收消息」；"
            "④ 机器人已在目标群且具备 @ 机器人相关权限；⑤ 同 APP_ID 仅一条 WS（关其它环境/旧进程）；"
            "⑥ 可设 LARK_WS_SDK_DEBUG=1 看 Lark SDK 原始日志；⑦ 默认会打前若干帧 frame.method：若始终无 DATA、仅有 CONTROL，"
            "说明链路通但飞书未往本连接推事件（订阅/权限/多实例）。⑧ 长连接模式下 IM 事件不会走 HTTP POST /webhook/event。"
        )

    threading.Thread(target=_watch, name="lark-ws-watchdog", daemon=True).start()


def _lark_ws_domain_try_order() -> List[str]:
    """Prefer ``LARK_HOST``, then try the other public Open Platform host (fixes 1000040351)."""
    seen: set = set()
    out: List[str] = []
    raw = (LARK_HOST or "").strip().rstrip("/")
    for d in (raw, "https://open.feishu.cn", "https://open.larksuite.com"):
        if not d or d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out


def start_lark_ws_client_blocking() -> None:
    """
    Official long-connection mode (no public Request URL, no HTTP challenge).
    Blocks until disconnect (or fatal error). Requires ``APP_ID`` + ``APP_SECRET``.
    """
    if not APP_ID or not APP_SECRET:
        raise RuntimeError("APP_ID and APP_SECRET are required for Lark WebSocket client")

    from lark_oapi import EventDispatcherHandler
    from lark_oapi.core.enum import LogLevel
    from lark_oapi.ws.client import Client as LarkWsClient

    global _lark_ws_saw_data_frame
    _lark_ws_saw_data_frame = False
    _n_boot = _lark_ws_reset_bootstrap_frame_budget()
    _lark_ws_enable_card_actions(LarkWsClient)  # before the log wrapper so it still logs 'card'
    _lark_ws_install_transport_frame_log(LarkWsClient)
    _lark_ws_install_recv_frame_method_log(LarkWsClient)
    if _n_boot:
        logger.info(
            "Lark WS bootstrap: will log first %s inbound protobuf frames at INFO "
            "(CONTROL vs DATA). Long-connection IM events do **not** produce HTTP POST /webhook/event.",
            _n_boot,
        )
    logger.info(
        "Reminder: with LARK_EVENT_MODE=ws, Feishu delivers IM events only on the WebSocket — "
        "expect journal lines like 'Lark WS recv frame.method=DATA' / 'Lark WS DATA frame', not POST /webhook/event."
    )
    if _cfg_str("LARK_WS_TRANSPORT_LOG", "1").strip().lower() not in ("0", "false", "no", "off"):
        _lark_ws_start_no_data_watchdog()

    # 飞书「使用长连接接收事件」文档：builder 前两参须为 **空字符串**（勿传 HTTP 回调的 Encrypt/Verification）。
    ws_use_http_keys = _lark_env_truthy("LARK_WS_USE_HTTP_KEYS")
    enc = (LARK_ENCRYPT_KEY or "") if ws_use_http_keys else ""
    ver = (VERIFICATION_TOKEN or "") if ws_use_http_keys else ""
    if ws_use_http_keys:
        logger.warning(
            "LARK_WS_USE_HTTP_KEYS=1 — passing encrypt/verification into WS handler (non-standard; "
            "prefer empty per Feishu long-connection doc)."
        )
    else:
        logger.info(
            "Lark WS EventDispatcherHandler.builder('', '') — HTTP 的 VERIFICATION_TOKEN/LARK_ENCRYPT_KEY 不用于长连接"
        )
    bld = EventDispatcherHandler.builder(enc, ver).register_p2_im_message_receive_v1(
        _on_ws_p2_im_message_receive_v1
    )
    if _lark_env_truthy("LARK_WS_REGISTER_IM_MESSAGE_V2"):
        bld = bld.register_p2_customized_event(
            "im.message.receive_v2", _on_ws_im_message_p2_customized
        )
        logger.info("LARK_WS_REGISTER_IM_MESSAGE_V2=1 — also handling im.message.receive_v2")
    else:
        logger.info(
            "LARK_WS_REGISTER_IM_MESSAGE_V2=0 — not subscribing to im.message.receive_v2 (avoids duplicate v1+v2)."
        )
    for raw_t in _cfg_str("LARK_WS_EXTRA_IM_TYPES", "").replace(";", ",").split(","):
        t = raw_t.strip()
        if not t:
            continue
        logger.info("Lark WS also registering custom event_type=%r (LARK_WS_EXTRA_IM_TYPES)", t)
        bld = bld.register_p2_customized_event(t, _on_ws_im_message_p2_customized)
    # Silence lark-oapi's "processor not found" ERROR for events we receive but don't act on.
    # The bot's own ACK/DONE reactions echo back as reaction events; task/* arrive from other
    # apps in the tenant. Extend via LARK_WS_IGNORE_EVENTS (comma/;-separated) as needed.
    _ignore_default = ("im.message.reaction.created_v1,im.message.reaction.deleted_v1,"
                       "im.message.recalled_v1,task.task.update_tenant_v1,"
                       "vc.meeting.recording_ended_v1,message")
    for _ignore_t in _cfg_str("LARK_WS_IGNORE_EVENTS", _ignore_default).replace(";", ",").split(","):
        _ignore_t = _ignore_t.strip()
        if _ignore_t:
            bld = bld.register_p2_customized_event(_ignore_t, _p0_ws_ignore_event)
    if _p0_om_enabled():
        for _et, _h in (
            ("vc.meeting.meeting_started_v1", _p0_om_on_started),
            ("vc.meeting.all_meeting_started_v1", _p0_om_on_started),  # alias if that variant is subscribed
            ("vc.meeting.join_meeting_v1", _p0_om_on_join),
            ("vc.meeting.leave_meeting_v1", _p0_om_on_leave),
            ("vc.meeting.meeting_ended_v1", _p0_om_on_ended),
            ("vc.meeting.all_meeting_ended_v1", _p0_om_on_ended),
            ("vc.meeting.recording_ready_v1", _p0_om_on_recording_ready),
            ("vc.meeting.recording_started_v1", _p0_ws_ignore_event),  # subscribed but not acted on
        ):
            bld = bld.register_p2_customized_event(_et, _h)
        logger.info(
            "p0 openmeeting enabled — subscribing VC meeting events (started/join/leave/ended/recording_ready); "
            "host=%s announce_chat=%s (needs scopes vc:reserve + vc:meeting:readonly + vc:record:readonly)",
            (_p0_om_host_open_id()[:12] or "?"),
            bool(_p0_om_announce_chat_default()),
        )
    if _p0_card_buttons_enabled():
        try:
            bld = bld.register_p2_card_action_trigger(_p0_card_action_handler)
            logger.info("p0 card buttons enabled — registered card.action.trigger handler "
                        "(needs console 回调配置 → 使用长连接接收事件).")
        except Exception:
            logger.exception("register_p2_card_action_trigger failed — card buttons unavailable")
    handler = bld.build()
    pmap = getattr(handler, "_processorMap", None) or {}
    logger.info("Lark WS p2 processors registered: %s", sorted(pmap.keys()))
    _lark_ws_patch_dispatcher_inbound_log(handler)

    level_name = _cfg_str("LARK_WS_LOG_LEVEL", "INFO").strip().upper()
    log_level = getattr(LogLevel, level_name, LogLevel.INFO)
    if _lark_env_truthy("LARK_WS_SDK_DEBUG"):
        log_level = LogLevel.DEBUG
        logger.info("LARK_WS_SDK_DEBUG=1 — Lark SDK internal logs at DEBUG")

    logger.warning(
        "长连接为集群投递：同 APP 若有多条 WS 或其它实例，仅随机一台会收到消息；请只保留一个 monitoring 进程。"
    )
    logger.warning(
        "若发消息后始终没有「Lark WS DATA frame」或「Lark WS inbound」日志：请到飞书开发者后台确认 "
        "「事件与回调」订阅方式为「使用长连接接收事件」并已保存成功（保存时本进程须在线）；"
        "且已订阅「接收消息」并具备群 @ 等权限；勿与「将回调发送至开发者服务器」混用。"
        " 调试可加 LARK_WS_LOG_FRAME_METHOD=1 看每条下行帧是 CONTROL 还是 DATA。"
    )

    last_domain_err: Optional[BaseException] = None
    global _lark_open_api_domain_override, _lark_oapi_client
    for domain in _lark_ws_domain_try_order():
        dnorm = domain.rstrip("/")
        with _lark_oapi_client_lock:
            _lark_oapi_client = None
        _lark_open_api_domain_override = dnorm
        cli = LarkWsClient(
            str(APP_ID).strip(),
            str(APP_SECRET).strip(),
            log_level=log_level,
            event_handler=handler,
            domain=dnorm,
            auto_reconnect=True,
        )
        _v2 = (
            " + im.message.receive_v2"
            if _lark_env_truthy("LARK_WS_REGISTER_IM_MESSAGE_V2")
            else ""
        )
        logger.info(
            "Lark WebSocket client starting (domain=%s); WS IM: p2 im.message.receive_v1%s",
            dnorm,
            _v2,
        )
        try:
            cli.start()
        except Exception as e:
            err = str(e)
            if "1000040351" in err or "incorrect domain" in err.lower():
                last_domain_err = e
                logger.warning(
                    "Lark WebSocket domain rejected on %r (%s) — trying alternate open-platform host if any.",
                    domain,
                    err,
                )
                continue
            raise

    if last_domain_err is not None:
        logger.error(
            "Lark WebSocket: every candidate domain failed with incorrect-domain (1000040351). "
            "Set LARK_HOST explicitly to the host shown in your Feishu/Lark developer console. Last: %s",
            last_domain_err,
        )
        raise last_domain_err


@app.route("/oauth/callback", methods=["GET"])
def _p0_oauth_callback():
    """Optional: auto-exchange the OAuth code if the redirect is reachable (ENABLE_HTTP=1 + public).

    Not required — the admin can instead copy code=… from the address bar and send /vccode <code>.
    """
    code = request.args.get("code", "")
    if not code:
        err = request.args.get("error", "")
        return (
            f"No authorization code (error={err or 'none'}). "
            "If you see code=… in the address bar, send it to the bot: /vccode <code>",
            400,
        )
    # CSRF: only complete an auto-exchange for a state this bot issued (single-use).
    if not _p0_vc_state_check(request.args.get("state", "")):
        return (
            "Invalid or expired state. Start over with /vcauth in Lark, "
            "or copy code=… from the address bar and send /vccode <code>.",
            400,
        )
    ok, msg = _p0_vc_oauth_exchange(code)
    if ok:
        return "Authorized. You can close this tab and use /meeting in Lark.", 200
    return f"Authorization failed: {msg}. You can also try /vccode <code> in chat.", 400


@app.route("/health", methods=["GET"])
def health():
    mode = _cfg_str("LARK_EVENT_MODE", "http").strip().lower() or "http"
    last_im = float(_lark_ws_last_im_monotonic or 0.0)
    ws_im_age: Optional[float] = None
    if last_im > 0:
        ws_im_age = round(time.monotonic() - last_im, 1)
    return jsonify(
        {
            "ok": True,
            "pid": os.getpid(),
            "listen_port": _cfg_listen_port(),
            "lark_event_mode": mode,
            "enable_http": _cfg_str("ENABLE_HTTP", "1"),
            "http_ignore_im_when_ws": _cfg_str("LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS", "0"),
            "app_id_prefix": ((APP_ID or "").strip()[:16] or None),
            "bot_open_id_known": bool((_lark_effective_bot_open_id() or "").strip()),
            "ws_saw_data_frame": bool(_lark_ws_saw_data_frame),
            "ws_last_im_age_sec": ws_im_age,
            "im_ingress_hint": (
                "POST /webhook/event must receive im.message when LARK_EVENT_MODE=http, "
                "or ws must log ws im.message.receive_v1 when LARK_EVENT_MODE=ws."
            ),
        }
    )


@app.route("/webhook/event", methods=["GET", "POST", "OPTIONS", "HEAD"], strict_slashes=False)
def webhook_event():
    # Chatbox: OPTIONS must not 405 — some clients preflight the callback URL.
    if request.method == "OPTIONS":
        return "", 204
    if request.method == "HEAD":
        return "", 200

    if request.method == "GET":
        # No secrets — use to confirm env + URL reachability from browser/curl.
        _listen_port = _cfg_listen_port()
        app_id = (APP_ID or "").strip()
        lark_sdk_version: Optional[str] = None
        try:
            from lark_oapi.core.const import VERSION as _lark_oapi_pkg_version  # type: ignore

            lark_sdk_version = str(_lark_oapi_pkg_version)
        except ImportError:
            lark_sdk_version = None
        return jsonify(
            {
                "ok": True,
                "hint": "Feishu must POST JSON to this path for events (HTTP mode only).",
                "lark_event_mode_tip": (
                    "默认 ``python main.py`` + ``LARK_EVENT_MODE=ws`` 使用官方 WebSocket 长连接，无需配置 Request URL。"
                    "若仍用 HTTP 回调，请设 LARK_EVENT_MODE=http；并核对下方 url_protocol_tip。"
                ),
                "url_protocol_tip": (
                    "Lark 请求 URL 校验走 POST。若控制台填了 https:// 而本服务只监听 http://（无 TLS），"
                    f"客户端会一直握手直到约 3s 超时 — 请改为 http://IP:{_listen_port}/webhook/event，或在前面加 Nginx/证书。"
                ),
                "lark_host": LARK_HOST,
                "lark_oapi_installed": lark_sdk_version is not None,
                "lark_oapi_version": lark_sdk_version,
                "app_id_prefix": (app_id[:12] + "…") if len(app_id) > 12 else app_id,
                "verification_token_configured": bool(VERIFICATION_TOKEN),
                "app_secret_configured": bool(APP_SECRET),
                "encrypt_key_configured": bool(LARK_ENCRYPT_KEY),
                "grafana_user_configured": bool(GRAFANA_USER),
                "feishu_url_verify_local_test_cn": (
                    "勿只 POST {\"challenge\":\"...\"}：不会被识别为 URL 校验，会走事件 token 校验 → 403 Invalid token（属正常）。"
                    "正确测本机延迟请用 legacy 体：{\"type\":\"url_verification\",\"token\":\"与 _CFG 中 VERIFICATION_TOKEN 一致\",\"challenge\":\"ping\"}，"
                    "应返回 HTTP 200 且 JSON 内含 challenge。"
                ),
                "feishu_url_verify_local_test_en": (
                    "Posting only {\"challenge\":\"...\"} is NOT a Feishu url_verification payload — it falls through to "
                    "event token verification → 403 is correct. For a local latency test use "
                    "{\"type\":\"url_verification\",\"token\":\"YOUR_VERIFICATION_TOKEN\",\"challenge\":\"ping\"} "
                    "(expect 200 and echoed challenge)."
                ),
                "feishu_timeout_local_200_cn": (
                    "若本机 curl 很快 200，但飞书控制台仍报约 3s 超时：多半是「飞书机房到你公网 IP」链路问题，而非 Python 处理慢。"
                    "请①用境外/另一台云的 curl 测公网 URL；②控制台 URL 必须与应用一致（http/https、端口）；③安全组放行源站入站；"
                    "④查看 journalctl 是否在点击校验时出现 webhook/event POST elapsed_ms=…（若无日志=请求未到进程）。"
                ),
                "feishu_timeout_local_200_en": (
                    "If local curl returns 200 quickly but the Lark console still shows ~3s timeout, the delay is usually "
                    "network/TLS/firewall path from Lark servers to your public URL — not Flask handler time. "
                    "curl the public URL from an external VPS; fix http vs https; open security groups; check logs for "
                    "webhook/event POST elapsed_ms when you click verify (no log means the request never reached the app)."
                ),
                "checklist_cn": [
                    "推荐：开发者后台「事件与回调」→ 使用长连接接收事件，运行 ``python main.py``（LARK_EVENT_MODE=ws，默认），无需公网 URL。",
                    "若用 HTTP：Request URL 须指向本服务 POST /webhook/event（公网可达），并设 LARK_EVENT_MODE=http。",
                    "订阅「消息与群组」→「接收消息 v1/v2」；群权限优先「仅 @ 本机器人」：im:message.group_at_msg:readonly（控制台名可能写为 group_at_msg）；"
                    "勿再开「读取群全部消息」类敏感 scope，否则飞书会把未 @ 你的消息也 POST 到本服务（只能靠代码二次过滤）。",
                    "VERIFICATION_TOKEN 与后台「Verification Token」一致（无多余空格）。",
                    "国内飞书应用将 LARK_HOST 设为 https://open.feishu.cn；国际用 https://open.larksuite.com。",
                    "机器人需能力「机器人」+ 权限「以应用身份发消息」等，且机器人在目标群内。",
                    "发 /monitoring 后看日志：handling im.message / monitoring background job / monitoring reply sent (background)。",
                    "飞书约 3s 超时：请用 python main.py 启动；webhook 先 200，Grafana 在后台线程执行。",
                    "发消息依赖 lark-oapi：pip install -U lark-oapi；GET 本 URL 可查看 lark_oapi_version。",
                    "lark_oapi_installed=false 只影响发消息，不影响「请求 URL 校验」；校验失败多半是 VERIFICATION_TOKEN 与后台不一致。",
                    "若用 systemd：可在 unit 里 Environment=VERIFICATION_TOKEN=… / Environment=PORT=5088（grafanagamebot 默认 5088；与同机 grafanaplatformbot 5002、Chatbox 5000 区分），或 EnvironmentFile=-/path/to/.env；修改后 daemon-reload && restart。",
                    f"若飞书提示 3s 超时：云厂商安全组/防火墙须放行公网入站 TCP {_listen_port}；本机 curl -m 5 -X POST http://IP:{_listen_port}/webhook/event -H Content-Type:application/json -d '{{...}}' 测连通。",
                    "仍超时：核对控制台 URL 与监听一致（http/https）；排查时设 LARK_WEBHOOK_WSGI_LOG=1 再看 journal。",
                    "curl 勿只发 {\"challenge\":\"ping\"}→403 正常；应用 {\"type\":\"url_verification\",\"token\":\"…\",\"challenge\":\"ping\"} 测 POST 延迟。",
                    "本机 200 仍超时：外网 curl POST url_verification；设 LARK_WEBHOOK_TIMING_LOG=1 看 elapsed_ms；或改用 ws 模式。",
                    "URL 校验文档常见「约 1s」总预算（含 RTT）：默认关闭 webhook 热路径 INFO 日志；排查时再设 LARK_WEBHOOK_WSGI_LOG=1 / LARK_WEBHOOK_TIMING_LOG=1。",
                    "HTTP 校验仍失败可改 LARK_EVENT_MODE=ws 用长连接，免 Request URL。",
                ],
            }
        )

    raw_in = _lark_safe_parse_json_body(request)
    if raw_in is None:
        snip = ""
        try:
            raw_b = request.get_data(cache=False, as_text=True)
            if raw_b:
                snip = raw_b[:300].replace("\n", " ")
        except Exception:
            pass
        logger.warning(
            "webhook POST body not JSON remote=%s ct=%r snip=%r",
            request.remote_addr,
            (request.headers.get("Content-Type") or ""),
            snip,
        )
        return jsonify({"error": "invalid json"}), 400

    if isinstance(raw_in, dict):
        fast_resp = _fast_plaintext_url_verification_response(raw_in)
        if fast_resp is not None:
            return fast_resp

    if request.method == "POST":
        logger.debug(
            "webhook POST remote=%s len=%s ct=%r",
            request.remote_addr,
            request.content_length,
            (request.headers.get("Content-Type") or "")[:120],
        )

    data = _feishu_maybe_decrypt_webhook_payload(raw_in)

    if isinstance(raw_in, dict) and raw_in.get("encrypt") is not None and data is raw_in:
        logger.error(
            "Webhook still encrypted — set LARK_ENCRYPT_KEY + pycryptodome, or disable 加密 (Chatbox logs this as 403)."
        )
        return jsonify({"error": "Invalid token"}), 403

    if not isinstance(data, dict):
        return jsonify({"error": "invalid payload"}), 400

    data = _lark_coerce_event_dict(data)
    uv_early = _lark_webhook_url_verification_response_or_none(data)
    if uv_early is not None:
        return uv_early

    data = _lark_normalize_webhook(data)
    data = _lark_coerce_event_dict(data)
    uv_after_norm = _lark_webhook_url_verification_response_or_none(data)
    if uv_after_norm is not None:
        return uv_after_norm

    if not _lark_verify_event_token(data):
        logger.warning(
            "webhook token mismatch: expected VERIFICATION_TOKEN, got %r schema=%r schema_v2=%s",
            _lark_extract_verification_token(data),
            data.get("schema"),
            _lark_is_schema_v2(data),
        )
        return jsonify({"error": "Invalid token"}), 403

    et = _lark_header_event_type(data)
    et_l = (et or "").lower()
    logger.info(
        "webhook POST event_type=%r schema=%r remote=%s",
        et or None,
        data.get("schema") if isinstance(data, dict) else None,
        request.remote_addr,
    )
    # Card interactions also require a fast 200; business logic should update the card asynchronously via Open API.
    if et_l.startswith("card.action"):
        try:
            extra = _lark_dispatch_card_action(data)
            if isinstance(extra, dict) and extra:
                return _lark_min_json_response(extra)
        except Exception:
            logger.exception("card.action handler failed")
        return _lark_feishu_webhook_ack_immediate()

    if _lark_ack_only_event_type(et):
        return _lark_feishu_webhook_ack_immediate()

    if et in ("im.message.receive_v1", "im.message.receive_v2"):
        if _lark_skip_http_im_message_when_ws_mode():
            logger.info(
                "webhook: skip %s (HTTP IM ignored while LARK_EVENT_MODE=ws; set LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS=0 to allow).",
                et,
            )
            return _lark_feishu_webhook_ack_immediate()
        return _handle_im_message_receive(data)

    logger.debug(
        "event ignored: event_type=%r keys=%s (subscribe 消息与群组 → 接收消息 v2.0)",
        et,
        list(data.keys())[:20],
    )
    return _lark_feishu_webhook_ack_immediate()


@app.route("/grafana/ping", methods=["GET"])
def grafana_ping():
    """Optional: verify .env login and that the dashboard URL is reachable."""
    try:
        r = fetch_grafana_dashboard()
        return jsonify(
            {
                "status_code": r.status_code,
                "final_url": r.url,
                "bytes": len(r.content),
            }
        )
    except Exception as e:
        logger.exception("grafana ping failed")
        return jsonify({"error": str(e)}), 500


@app.route("/metrics/request-total-1m", methods=["GET"])
def metrics_request_total_1m():
    """Primary Grafana panel series (default LiveSlots Online Number, 1-minute step). Poll from cron or Lark."""
    try:
        if not MONITORING_HTTP_PRIMARY_ENABLE:
            return jsonify(
                {
                    "error": "MONITORING_HTTP_PRIMARY_ENABLE=0 — primary panel (LiveSlots) disabled on this bot"
                }
            ), 404
        data = fetch_request_total_1m_series()
        data["httpAnalysis"] = _http_analysis_for_payload(data)
        return jsonify(data)
    except Exception as e:
        logger.exception("request-total-1m failed")
        return jsonify({"error": str(e)}), 500


def run_monitoring_bot() -> None:
    """
    Process entrypoint: HTTP-only, WebSocket-only, or WS + HTTP sidecar (see module docstring).
    Uses :data:`app`, :data:`logger`, :func:`start_lark_ws_client_blocking` from this module.
    """
    logger.info(
        "monitoring bot pid=%s — duplicate replies: check two processes (same APP_ID) or IM dedupe logs.",
        os.getpid(),
    )
    logger.info(
        "DEPLOY git-restart: enable=%s trigger=%r allowed_user=%s service=%s",
        DEPLOY_ENABLE,
        (DEPLOY_TRIGGER or "/deploy"),
        ((DEPLOY_ALLOWED_USER_OPEN_ID[:20] + "…") if len(DEPLOY_ALLOWED_USER_OPEN_ID or "") > 20 else (DEPLOY_ALLOWED_USER_OPEN_ID or "(none)")),
        DEPLOY_SYSTEMD_SERVICE,
    )
    _start_grafana_playwright_keeper_if_enabled()
    _start_monitoring_watchdog_if_enabled()
    _p0_start_doc_preload_if_enabled()
    if _lark_env_truthy("MONITORING_WATCH_ENABLE") and not (MONITORING_ALERT_CHAT_ID or "").strip():
        logger.error(
            "MONITORING_WATCH_ENABLE=1 but MONITORING_ALERT_CHAT_ID is empty — "
            "watchdog will never post auto-alerts to any Lark group; set the target group chat_id "
            "(oc_… from im.message logs or Feishu open API)."
        )
    port = _cfg_listen_port()
    if MONITORING_AT_MENTION_ENABLE or MONITORING_TRIGGER_REQUIRES_AT_BOT:
        _oid = _lark_effective_bot_open_id()
        if MONITORING_AT_MENTION_ENABLE and not _oid:
            logger.warning(
                "MONITORING_AT_MENTION_ENABLE is on but bot open_id unknown — set LARK_BOT_OPEN_ID or ensure bot/v3/info works."
            )
        if MONITORING_TRIGGER_REQUIRES_AT_BOT and not _oid:
            if MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER:
                logger.info(
                    "MONITORING_TRIGGER_REQUIRES_AT_BOT: bot open_id unresolved — /mo may still run when HTTP text "
                    "contains Feishu @_user_N (MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER=1); set LARK_BOT_OPEN_ID for strict mention matching."
                )
            else:
                logger.warning(
                    "MONITORING_TRIGGER_REQUIRES_AT_BOT is on but bot open_id unknown — @ /mo will not match; set LARK_BOT_OPEN_ID, "
                    "fix APP_ID/APP_SECRET for bot/v3/info, enable MONITORING_MO_ALLOW_FEISHU_AT_PLACEHOLDER for HTTP, or set MONITORING_TRIGGER_REQUIRES_AT_BOT=0."
                )
    if int(GRAFANA_QUERY_LOOKBACK_SECONDS) != 900:
        logger.warning(
            "GRAFANA_QUERY_LOOKBACK_SECONDS=%s (default 900 = 15m) — /monitoring Prometheus window differs from default 15 minutes",
            GRAFANA_QUERY_LOOKBACK_SECONDS,
        )
    raw_mode = _cfg_str("LARK_EVENT_MODE", "http").strip().lower()
    mode = raw_mode if raw_mode else "http"
    if not MONITORING_LIVESLOT_BET_ENABLE:
        logger.info(
            "Liveslot 下注Bet/min monitoring disabled (MONITORING_LIVESLOT_BET_ENABLE=0) — no fetch, no alerts"
        )
    if not MONITORING_LIVESLOT_SPIN_COUNT_ENABLE:
        logger.info(
            "Liveslots-Spin-Bet spin_count monitoring disabled "
            "(MONITORING_LIVESLOT_SPIN_COUNT_ENABLE=0) — no fetch, no alerts"
        )
    elif MONITORING_LIVESLOT_SPIN_COUNT_ENABLE:
        logger.info(
            "Liveslots-Spin-Bet spin_count zero alert: value=0 for >%ss triggers alert "
            "(MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS)",
            MONITORING_LIVESLOT_SPIN_COUNT_ZERO_ALERT_SECONDS,
        )
    logger.info(
        "IM ingress config: LARK_EVENT_MODE=%s ENABLE_HTTP=%s "
        "LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS=%s listen=0.0.0.0:%s APP_ID=%s…",
        mode,
        _cfg_str("ENABLE_HTTP", "1"),
        _cfg_str("LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS", "0"),
        port,
        ((APP_ID or "").strip()[:16] or "?"),
    )
    if mode == "ws":
        logger.warning(
            "LARK_EVENT_MODE=ws — Feishu IM normally does **not** POST to /webhook/event; "
            "you must see 'ws im.message.receive_v1' when users send /mo. "
            "If you use Request URL in the developer console, set LARK_EVENT_MODE=http in systemd Environment=."
        )

    def run_http() -> None:
        stack = _cfg_str("HTTP_SERVER", "flask").strip().lower()
        use_waitress = stack in ("waitress", "wsgi")
        if not use_waitress:
            logger.info(
                "HTTP (Flask threaded=True, Chatbox/main.py style) on 0.0.0.0:%s — "
                "/health /grafana/ping /webhook/event (set HTTP_SERVER=waitress for Waitress)",
                port,
            )
            app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False, debug=False)
            return
        try:
            from waitress import serve

            try:
                threads = _cfg_int("WAITRESS_THREADS", 24)
            except ValueError:
                threads = 24
            threads = max(4, min(threads, 128))
            logger.info(
                "HTTP (Waitress) on 0.0.0.0:%s threads=%s — /health /grafana/ping /webhook/event "
                "(raise WAITRESS_THREADS if webhooks queue behind slow requests)",
                port,
                threads,
            )
            serve(app, host="0.0.0.0", port=port, threads=threads, channel_timeout=120)
        except ImportError:
            logger.warning("waitress not installed — pip install waitress; falling back to Flask threaded server")
            app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False, debug=False)

    if mode == "http":
        logger.info(
            "LARK_EVENT_MODE=http — **WebSocket disabled**; Feishu IM/events only via POST /webhook/event. "
            "Use Request URL mode in the developer console (not long-connection)."
        )
        hint = _cfg_str("LARK_WEBHOOK_PUBLIC_URL", "").strip()
        if hint:
            logger.info("Feishu developer console → 事件与回调 → Request URL (示例配置): %s", hint)
            if hint.lower().startswith("https://"):
                logger.error(
                    "LARK_WEBHOOK_PUBLIC_URL / 控制台若使用 https:// 而本进程仅 plain HTTP，飞书会 TLS 握手失败或卡住≈3s。"
                    "请改为 http://…:%s/webhook/event，或在前面加 Nginx/证书终止 TLS。",
                    port,
                )
            if hint.rstrip("/").endswith("/webhook/event/"):
                logger.warning(
                    "Request URL 尽量不要带末尾 /；已启用 strict_slashes=False，仍建议与控制台完全一致。"
                )
        else:
            logger.info(
                "Set LARK_WEBHOOK_PUBLIC_URL in _CFG to log your Feishu Request URL hint "
                "(e.g. http://YOUR_IP:%s/webhook/event).",
                port,
            )
        logger.warning(
            "飞书 HTTP「请求网址校验」文档常写 **约 1 秒内** 返回 challenge（含网络往返）；推送事件常见 **约 3 秒**。"
            "webhook 热路径默认 **不写** WSGI/耗时 INFO，避免 journald 延迟；若仍超时，先试 ``LARK_EVENT_MODE=ws`` 长连接免 URL 校验，"
            "或在前面加 Nginx+HTTPS；排查时再设 LARK_WEBHOOK_WSGI_LOG=1。"
        )
        run_http()
        return

    if mode != "ws":
        raise SystemExit(f"Unknown LARK_EVENT_MODE={mode!r} (use ``http`` for webhook-only, or ``ws`` for long connection)")

    http_on = _cfg_str("ENABLE_HTTP", "1").strip().lower() in ("1", "true", "yes", "on")
    http_thread: Optional[threading.Thread] = None
    if http_on:
        http_thread = threading.Thread(target=run_http, name="http-sidecar", daemon=False)
        http_thread.start()
        time.sleep(0.2)
    else:
        logger.info("ENABLE_HTTP=0 — only Lark WebSocket client (no HTTP listener)")

    try:
        start_lark_ws_client_blocking()
    except Exception:
        logger.exception(
            "Lark WebSocket client failed to start or exited (check APP_ID/APP_SECRET/LARK_HOST, "
            "egress firewall, and Feishu app long-connection mode)."
        )
        if http_on and http_thread is not None:
            logger.warning(
                "Continuing with HTTP sidecar only — use POST /webhook/event for events, "
                "or set LARK_EVENT_MODE=http after fixing credentials."
            )
            http_thread.join()
            return
        raise SystemExit(1)


if __name__ == "__main__":
    run_monitoring_bot()