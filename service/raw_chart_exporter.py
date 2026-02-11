from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from service.chat_client import ChatCallResult


def _sanitize_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    out = "".join("_" if c in bad else c for c in (name or "").strip())
    return out or "user"


def _coerce_number(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _extract_json_object_string(s: str) -> str | None:
    """
    Sometimes `content` is almost JSON but with leading text.
    We try to cut from first '{' to last '}'.
    """
    if not s:
        return None
    a = s.find("{")
    b = s.rfind("}")
    if a == -1 or b == -1 or b <= a:
        return None
    return s[a : b + 1]


def _parse_dashboard_content_to_payload(content: str) -> dict[str, Any] | None:
    """
    Expected format (from api SSE last chunk):
      content = '{"conv_uid": "...", "template_name": "...", "charts": [...]}'
    """
    if not content:
        return None
    txt = content.strip()
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    sub = _extract_json_object_string(txt)
    if not sub:
        return None
    try:
        obj2 = json.loads(sub)
        return obj2 if isinstance(obj2, dict) else None
    except Exception:
        return None


def extract_dashboard_payload_from_result(res: ChatCallResult) -> dict[str, Any] | None:
    """
    Returns the dashboard payload dict containing `charts` if available.
    Priority:
    - assistant_content (already derived from last SSE chunk)
    - response_json -> choices[0].message.content
    """
    payload = _parse_dashboard_content_to_payload(res.assistant_content or "")
    if payload and isinstance(payload.get("charts"), list):
        return payload

    rj = res.response_json or {}
    try:
        content = rj["choices"][0]["message"]["content"]
        payload2 = _parse_dashboard_content_to_payload(content if isinstance(content, str) else "")
        if payload2 and isinstance(payload2.get("charts"), list):
            return payload2
    except Exception:
        pass
    return None


def _escape_html(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ---------------------------------------------------------------------------
# Supported chart_type values (first match wins).
# IndicatorValue is intentionally excluded – skip entirely.
# ---------------------------------------------------------------------------
_SUPPORTED_CHART_TYPES = {"BarChart", "LineChart", "PieChart", "Table"}


def _pick_chart_type(chart_type_raw: str) -> str | None:
    """
    Given a comma-separated chart_type string like
      "BarChart, LineChart, IndicatorValue"
    return the FIRST token that we support, or None.
    """
    for t in chart_type_raw.split(","):
        t = t.strip()
        if t in _SUPPORTED_CHART_TYPES:
            return t
    return None


@dataclass(frozen=True)
class RawChartExportResult:
    html_path: Path
    chart_count: int


def _render_charts_to_html(
    payload: dict[str, Any],
    out_path: Path,
    page_title: str,
) -> RawChartExportResult | None:
    """Shared renderer: build pyecharts Page from a dashboard payload and save to *out_path*."""
    # Lazy import so non-dashboard flows don't require pyecharts at runtime.
    from pyecharts import options as opts
    from pyecharts.charts import Bar, Line, Page, Pie
    from pyecharts.components import Table

    charts = payload.get("charts")
    if not isinstance(charts, list) or not charts:
        return None

    page = Page(page_title=page_title, layout=Page.SimplePageLayout)
    added = 0

    # Collect description HTML blocks to inject after render.
    desc_blocks: list[str] = []

    for idx, ch in enumerate(charts, start=1):
        if not isinstance(ch, dict):
            continue

        chart_name = str(ch.get("chart_name") or f"Chart {idx}")
        chart_desc = str(ch.get("chart_desc") or "")
        chart_sql = str(ch.get("chart_sql") or "")
        chart_type_raw = str(ch.get("chart_type") or "")

        chosen = _pick_chart_type(chart_type_raw)
        if chosen is None:
            continue

        values = ch.get("values")
        if not isinstance(values, list) or not values:
            continue

        # Normalize values: group by `type`, x-axis by `name`
        xs: list[str] = []
        series_map: dict[str, dict[str, float]] = {}
        for it in values:
            if not isinstance(it, dict):
                continue
            x = str(it.get("name") or "").strip()
            sname = str(it.get("type") or "value").strip() or "value"
            y = _coerce_number(it.get("value"))
            if not x or y is None:
                continue
            if x not in xs:
                xs.append(x)
            series_map.setdefault(sname, {})[x] = y

        if not xs or not series_map:
            continue

        # Build description block
        desc_html_parts = [
            f'<div class="chart-info" style="max-width:900px;margin:30px auto 6px;'
            f'font-family:sans-serif;">',
            f'<h3 style="margin:0 0 6px;color:#333;">{_escape_html(chart_name)}</h3>',
        ]
        if chart_desc:
            desc_html_parts.append(
                f'<p style="margin:0 0 6px;color:#555;line-height:1.6;'
                f'white-space:pre-wrap;word-break:break-word;">'
                f'{_escape_html(chart_desc)}</p>'
            )
        if chart_sql:
            desc_html_parts.append(
                f'<details style="margin:0 0 8px;"><summary style="cursor:pointer;'
                f'color:#1a73e8;font-size:13px;">SQL</summary>'
                f'<pre style="background:#f5f5f5;padding:8px;border-radius:4px;'
                f'font-size:12px;overflow-x:auto;white-space:pre-wrap;">'
                f'{_escape_html(chart_sql)}</pre></details>'
            )
        desc_html_parts.append('</div>')
        desc_blocks.append("\n".join(desc_html_parts))

        # Table
        if chosen == "Table":
            col_keys = list(series_map.keys())
            headers = ["name", *col_keys]
            rows: list[list[Any]] = []
            for x in xs:
                row: list[Any] = [x]
                for ck in col_keys:
                    v = series_map.get(ck, {}).get(x)
                    row.append("" if v is None else v)
                rows.append(row)

            tbl = Table()
            tbl.add(headers, rows)
            tbl.set_global_opts(
                title_opts=opts.ComponentTitleOpts(title="", subtitle="")
            )
            page.add(tbl)
            added += 1
            continue

        def _title_opts() -> opts.TitleOpts:
            return opts.TitleOpts(title="", subtitle="")

        if chosen == "PieChart":
            s0 = next(iter(series_map.keys()))
            data_pair = [(x, series_map[s0].get(x, 0.0)) for x in xs]
            pie = (
                Pie()
                .add(
                    series_name=s0,
                    data_pair=data_pair,
                    radius=["30%", "70%"],
                )
                .set_global_opts(
                    title_opts=_title_opts(),
                    legend_opts=opts.LegendOpts(
                        orient="vertical", pos_left="2%", pos_top="10%"
                    ),
                )
                .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
            )
            page.add(pie)
            added += 1
            continue

        if chosen in ("LineChart", "BarChart"):
            if chosen == "LineChart":
                c = Line().add_xaxis(xs)
            else:
                c = Bar().add_xaxis(xs)

            for sname, xy in series_map.items():
                ys = [xy.get(x) for x in xs]
                if chosen == "LineChart":
                    c.add_yaxis(
                        series_name=sname,
                        y_axis=ys,
                        is_smooth=True,
                        label_opts=opts.LabelOpts(is_show=False),
                    )
                else:
                    c.add_yaxis(
                        series_name=sname,
                        y_axis=ys,
                        label_opts=opts.LabelOpts(is_show=False),
                    )

            c.set_global_opts(
                title_opts=_title_opts(),
                tooltip_opts=opts.TooltipOpts(trigger="axis"),
                xaxis_opts=opts.AxisOpts(
                    type_="category",
                    axislabel_opts=opts.LabelOpts(rotate=30),
                ),
            )
            page.add(c)
            added += 1
            continue

    if added <= 0:
        return None

    # Render to file
    out_path.parent.mkdir(parents=True, exist_ok=True)
    page.render(str(out_path))

    # Post-process: inject description text blocks before each chart
    if desc_blocks:
        import re

        html = out_path.read_text(encoding="utf-8")
        chart_div_pattern = re.compile(
            r'(?=(<div\s+id="[a-f0-9]+"[\s>]))'
        )

        all_positions: list[int] = []
        for m in chart_div_pattern.finditer(html):
            all_positions.append(m.start())

        if len(all_positions) == len(desc_blocks):
            for pos, desc in reversed(list(zip(all_positions, desc_blocks))):
                html = html[:pos] + desc + "\n" + html[pos:]
            out_path.write_text(html, encoding="utf-8")

    return RawChartExportResult(html_path=out_path, chart_count=added)


def render_single_dashboard_html(
    res: ChatCallResult,
    prompt_id: str,
    export_dir: Path,
    user_name: str,
) -> RawChartExportResult | None:
    """Render a single dashboard response to its own HTML file.

    File is saved as ``export_dir / {prompt_id}.html``.
    Returns None when the response has no usable chart payload.
    """
    if not res or not res.ok:
        return None

    payload = extract_dashboard_payload_from_result(res)
    if not payload:
        return None

    safe_name = _sanitize_filename(prompt_id)
    out_path = export_dir / f"{safe_name}.html"

    return _render_charts_to_html(
        payload=payload,
        out_path=out_path,
        page_title=f"Dashboard - {user_name} - {prompt_id}",
    )


def export_raw_chart_html_for_dashboard(
    responses_in_prompt_order: list[ChatCallResult],
    export_dir: Path,
    user_name: str,
) -> RawChartExportResult | None:
    """
    Legacy: takes the *last usable* response and renders all its charts
    into a single raw_chart.html file.
    """
    last_payload: dict[str, Any] | None = None
    for res in reversed(responses_in_prompt_order):
        if not res or not res.ok:
            continue
        p = extract_dashboard_payload_from_result(res)
        if p:
            last_payload = p
            break

    if not last_payload:
        return None

    export_dir.mkdir(parents=True, exist_ok=True)
    out_path = export_dir / "raw_chart.html"

    return _render_charts_to_html(
        payload=last_payload,
        out_path=out_path,
        page_title=f"Dashboard - {user_name}",
    )
