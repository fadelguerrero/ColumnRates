from __future__ import annotations

import csv
import io
import json
import math
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

from flask import Flask, jsonify, make_response, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
HTML_FILE = BASE_DIR / "column_rate_frontend.html"
DATA_FILE = BASE_DIR / "column_reo_rate_data.json"
APP_VERSION = "v2.2"

app = Flask(__name__)

BAR_DATA: dict[str, dict[str, float | None]] = {
    "N10": {"d": 10, "mass": 10 * 10 / 162, "lap40": None, "lap50": None, "lap65": None, "hook": 120},
    "N12": {"d": 12, "mass": 12 * 12 / 162, "lap40": 450, "lap50": 450, "lap65": 350, "hook": 130},
    "N16": {"d": 16, "mass": 16 * 16 / 162, "lap40": 650, "lap50": 600, "lap65": 500, "hook": 150},
    "N20": {"d": 20, "mass": 20 * 20 / 162, "lap40": 850, "lap50": 800, "lap65": 650, "hook": 180},
    "N24": {"d": 24, "mass": 24 * 24 / 162, "lap40": 1100, "lap50": 950, "lap65": 850, "hook": 220},
    "N28": {"d": 28, "mass": 28 * 28 / 162, "lap40": 1350, "lap50": 1200, "lap65": 1050, "hook": 280},
    "N32": {"d": 32, "mass": 32 * 32 / 162, "lap40": 1600, "lap50": 1450, "lap65": 1250, "hook": 320},
    "N36": {"d": 36, "mass": 36 * 36 / 162, "lap40": 1900, "lap50": 1700, "lap65": 1500, "hook": 355},
    "N40": {"d": 40, "mass": 40 * 40 / 162, "lap40": 2150, "lap50": 1950, "lap65": 1750, "hook": 395},
}
VERT_BAR_KEYS = [key for key in BAR_DATA if key != "N10"]
LIG_BAR_KEYS = list(BAR_DATA.keys())

EXCEL_TEMPLATE_COLUMNS = [
    "id", "designName", "level", "type", "fc", "storyHeight", "cover", "sectionNote",
    "rectB", "rectD", "barsNx", "barsNy", "circDia", "circBarCount", "vertBar", "useBundle", "bundleCount", "ligBar", "ligSpacing"
]
SCHEDULE_EXPORT_COLUMNS = [
    "designName", "level", "shape", "sectionText", "sectionNote", "fc", "storyHeight", "cover", "vertBar", "ligBar",
    "ligSpacing", "bundleCount", "bundleLabel", "arrangementText", "positionCount", "longCount", "totalVertAreaMm2", "steelRatioPct",
    "lapMm", "hookMm", "ligSets", "longMassKg", "ligMassKg", "totalSteelKg", "concVolM3", "rateKgM3"
]

XML_NS = {
    "ss": "urn:schemas-microsoft-com:office:spreadsheet",
}


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    text = str(value).replace(",", "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_int(value: Any) -> int:
    return int(round(parse_number(value)))


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"true", "yes", "y", "1", "on"}


def parse_bar_key(value: Any, fallback: str) -> str:
    clean = str(value or "").strip().upper()
    return clean if clean in BAR_DATA else fallback


def strength_bucket(fc: float) -> int | None:
    if fc >= 65:
        return 65
    if fc >= 50:
        return 50
    if fc >= 40:
        return 40
    return None


def lap_length(bar_key: str, fc: float) -> float | None:
    bucket = strength_bucket(fc)
    if bucket == 40:
        return BAR_DATA[bar_key]["lap40"]
    if bucket == 50:
        return BAR_DATA[bar_key]["lap50"]
    if bucket == 65:
        return BAR_DATA[bar_key]["lap65"]
    return None


def lig_set_count(height_mm: float, spacing_mm: float) -> int:
    return max(2, math.ceil(height_mm / spacing_mm) + 1)


def bar_area_mm2(bar_key: str) -> float:
    d = float(BAR_DATA[bar_key]["d"])
    return math.pi * d * d / 4


def make_bundle_label(bundle_count: int) -> str:
    return f"bundled x{bundle_count}" if bundle_count > 1 else "single"


def default_form_data() -> dict[str, Any]:
    return {
        "type": "rect",
        "designName": "",
        "level": "",
        "fc": 50,
        "storyHeight": 3300,
        "cover": 40,
        "sectionNote": "",
        "rectB": 600,
        "rectD": 600,
        "barsNx": 4,
        "barsNy": 4,
        "circDia": 600,
        "circBarCount": 12,
        "vertBar": "N20",
        "useBundle": False,
        "bundleCount": 1,
        "ligBar": "N12",
        "ligSpacing": 200,
    }


def normalize_form_data(raw: dict[str, Any] | None) -> dict[str, Any]:
    raw = raw or {}
    data = deepcopy(default_form_data())
    data.update({k: v for k, v in raw.items() if k in data or k == "id"})
    use_bundle = parse_bool(raw.get("useBundle", data["useBundle"])) or parse_int(raw.get("bundleCount", 1)) > 1
    bundle_count = 2 if use_bundle else 1
    data.update(
        {
            "type": "circ" if str(raw.get("type", data["type"])) .strip().lower() in {"circ", "circular", "circle"} else "rect",
            "designName": str(raw.get("designName", data["designName"]) or "").strip(),
            "level": str(raw.get("level", data["level"]) or "").strip(),
            "fc": parse_number(raw.get("fc", data["fc"])),
            "storyHeight": parse_number(raw.get("storyHeight", data["storyHeight"])),
            "cover": parse_number(raw.get("cover", data["cover"])),
            "sectionNote": str(raw.get("sectionNote", data["sectionNote"]) or "").strip(),
            "rectB": parse_number(raw.get("rectB", data["rectB"])),
            "rectD": parse_number(raw.get("rectD", data["rectD"])),
            "barsNx": parse_int(raw.get("barsNx", data["barsNx"])),
            "barsNy": parse_int(raw.get("barsNy", data["barsNy"])),
            "circDia": parse_number(raw.get("circDia", data["circDia"])),
            "circBarCount": parse_int(raw.get("circBarCount", data["circBarCount"])),
            "vertBar": parse_bar_key(raw.get("vertBar", data["vertBar"]), "N20"),
            "useBundle": use_bundle,
            "bundleCount": bundle_count,
            "ligBar": parse_bar_key(raw.get("ligBar", data["ligBar"]), "N12"),
            "ligSpacing": parse_number(raw.get("ligSpacing", data["ligSpacing"])),
        }
    )
    if data["type"] == "rect":
        data["circDia"] = parse_number(raw.get("circDia", 0))
        data["circBarCount"] = parse_int(raw.get("circBarCount", 0))
    else:
        data["rectB"] = parse_number(raw.get("rectB", 0))
        data["rectD"] = parse_number(raw.get("rectD", 0))
        data["barsNx"] = parse_int(raw.get("barsNx", 0))
        data["barsNy"] = parse_int(raw.get("barsNy", 0))
    return data


def validate(data: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    if not data["designName"]:
        errs.append("Design/Mark is required.")
    if not data["level"]:
        errs.append("Level is required.")
    if data["fc"] < 40:
        errs.append("Concrete strength below 40 MPa is outside the uploaded lap table.")
    if data["storyHeight"] <= 0:
        errs.append("Storey height must be greater than zero.")
    if data["cover"] < 0:
        errs.append("Concrete cover cannot be negative.")
    if data["ligSpacing"] <= 0:
        errs.append("Lig spacing must be greater than zero.")
    if data["vertBar"] not in BAR_DATA or lap_length(data["vertBar"], data["fc"]) is None:
        errs.append("Vertical bar size is invalid for the uploaded lap table.")
    if data["ligBar"] not in BAR_DATA:
        errs.append("Lig bar size is invalid.")
    if data["bundleCount"] not in (1, 2):
        errs.append("Bundle option can only be single bars or bundled x2.")

    if data["type"] == "rect":
        if data["rectB"] <= 0 or data["rectD"] <= 0:
            errs.append("Rectangular section dimensions must be greater than zero.")
        if data["barsNx"] < 2 or data["barsNy"] < 2:
            errs.append("Rectangular arrangement requires bars along width and depth to be at least 2.")
        if 2 * data["cover"] >= data["rectB"] or 2 * data["cover"] >= data["rectD"]:
            errs.append("Concrete cover is too large for the rectangular section.")
    else:
        if data["circDia"] <= 0:
            errs.append("Circular diameter must be greater than zero.")
        if data["circBarCount"] < 3:
            errs.append("Circular section requires at least 3 bar positions.")
        if 2 * data["cover"] >= data["circDia"]:
            errs.append("Concrete cover is too large for the circular section.")
    return errs


def calc_rect(data: dict[str, Any]) -> dict[str, Any]:
    vert = BAR_DATA[data["vertBar"]]
    lig = BAR_DATA[data["ligBar"]]
    lap = float(lap_length(data["vertBar"], data["fc"]))
    hook = float(lig["hook"])
    position_count = 2 * (data["barsNx"] + data["barsNy"]) - 4
    long_count = position_count * data["bundleCount"]
    long_bar_len_mm = data["storyHeight"] + lap
    long_mass_kg = long_count * (long_bar_len_mm / 1000) * float(vert["mass"])

    clear_b = data["rectB"] - 2 * data["cover"]
    clear_d = data["rectD"] - 2 * data["cover"]
    outer_lig_len_mm = 2 * (clear_b + clear_d) + 2 * hook
    internal_depth_span_count = max(data["barsNx"] - 2, 0)
    internal_width_span_count = max(data["barsNy"] - 2, 0)
    internal_depth_lig_len_mm = clear_d + 2 * hook
    internal_width_lig_len_mm = clear_b + 2 * hook
    lig_sets = lig_set_count(data["storyHeight"], data["ligSpacing"])
    total_lig_len_mm_per_set = (
        outer_lig_len_mm
        + internal_depth_span_count * internal_depth_lig_len_mm
        + internal_width_span_count * internal_width_lig_len_mm
    )
    lig_mass_kg = lig_sets * (total_lig_len_mm_per_set / 1000) * float(lig["mass"])

    vert_bar_area_mm2 = bar_area_mm2(data["vertBar"])
    lig_bar_area_mm2 = bar_area_mm2(data["ligBar"])
    total_vert_area_mm2 = long_count * vert_bar_area_mm2
    gross_area_mm2 = data["rectB"] * data["rectD"]
    steel_ratio_pct = 100 * total_vert_area_mm2 / gross_area_mm2
    conc_vol_m3 = (gross_area_mm2 * data["storyHeight"]) / 1e9
    total_steel_kg = long_mass_kg + lig_mass_kg
    rate_kg_m3 = total_steel_kg / conc_vol_m3

    return {
        "shape": "Rectangular",
        "sectionText": f"{int(round(data['rectB'])):,} × {int(round(data['rectD'])):,}",
        "areaMm2": gross_area_mm2,
        "clearB": clear_b,
        "clearD": clear_d,
        "positionCount": position_count,
        "longCount": long_count,
        "longBarLenMm": long_bar_len_mm,
        "lapMm": lap,
        "hookMm": hook,
        "ligSets": lig_sets,
        "outerLigLenMm": outer_lig_len_mm,
        "internalDepthSpanCount": internal_depth_span_count,
        "internalWidthSpanCount": internal_width_span_count,
        "internalDepthLigLenMm": internal_depth_lig_len_mm,
        "internalWidthLigLenMm": internal_width_lig_len_mm,
        "totalLigLenMmPerSet": total_lig_len_mm_per_set,
        "vertBarAreaMm2": vert_bar_area_mm2,
        "ligBarAreaMm2": lig_bar_area_mm2,
        "totalVertAreaMm2": total_vert_area_mm2,
        "steelRatioPct": steel_ratio_pct,
        "longMassKg": long_mass_kg,
        "ligMassKg": lig_mass_kg,
        "totalSteelKg": total_steel_kg,
        "concVolM3": conc_vol_m3,
        "rateKgM3": rate_kg_m3,
        "arrangementText": f"{data['barsNx']} × {data['barsNy']}",
        "bundleLabel": make_bundle_label(data["bundleCount"]),
        "vertDesc": (
            f"{long_count}-{data['vertBar']} ({data['barsNx']}×{data['barsNy']} positions, bundled x{data['bundleCount']})"
            if data["bundleCount"] > 1
            else f"{long_count}-{data['vertBar']} ({data['barsNx']}×{data['barsNy']})"
        ),
        "ligDesc": f"{data['ligBar']} @ {int(round(data['ligSpacing'])):,}",
    }


def calc_circ(data: dict[str, Any]) -> dict[str, Any]:
    vert = BAR_DATA[data["vertBar"]]
    lig = BAR_DATA[data["ligBar"]]
    lap = float(lap_length(data["vertBar"], data["fc"]))
    hook = float(lig["hook"])
    position_count = data["circBarCount"]
    long_count = position_count * data["bundleCount"]
    long_bar_len_mm = data["storyHeight"] + lap
    long_mass_kg = long_count * (long_bar_len_mm / 1000) * float(vert["mass"])

    clear_dia = data["circDia"] - 2 * data["cover"]
    hoop_len_mm = math.pi * clear_dia + 2 * hook
    lig_sets = lig_set_count(data["storyHeight"], data["ligSpacing"])
    lig_mass_kg = lig_sets * (hoop_len_mm / 1000) * float(lig["mass"])

    gross_area_mm2 = math.pi * data["circDia"] * data["circDia"] / 4
    vert_bar_area_mm2 = bar_area_mm2(data["vertBar"])
    lig_bar_area_mm2 = bar_area_mm2(data["ligBar"])
    total_vert_area_mm2 = long_count * vert_bar_area_mm2
    steel_ratio_pct = 100 * total_vert_area_mm2 / gross_area_mm2
    conc_vol_m3 = (gross_area_mm2 * data["storyHeight"]) / 1e9
    total_steel_kg = long_mass_kg + lig_mass_kg
    rate_kg_m3 = total_steel_kg / conc_vol_m3

    return {
        "shape": "Circular",
        "sectionText": f"Ø{int(round(data['circDia'])):,}",
        "areaMm2": gross_area_mm2,
        "clearDia": clear_dia,
        "positionCount": position_count,
        "longCount": long_count,
        "longBarLenMm": long_bar_len_mm,
        "lapMm": lap,
        "hookMm": hook,
        "ligSets": lig_sets,
        "hoopLenMm": hoop_len_mm,
        "vertBarAreaMm2": vert_bar_area_mm2,
        "ligBarAreaMm2": lig_bar_area_mm2,
        "totalVertAreaMm2": total_vert_area_mm2,
        "steelRatioPct": steel_ratio_pct,
        "longMassKg": long_mass_kg,
        "ligMassKg": lig_mass_kg,
        "totalSteelKg": total_steel_kg,
        "concVolM3": conc_vol_m3,
        "rateKgM3": rate_kg_m3,
        "arrangementText": f"{data['circBarCount']} positions",
        "bundleLabel": make_bundle_label(data["bundleCount"]),
        "vertDesc": (
            f"{long_count}-{data['vertBar']} ({data['circBarCount']} positions, bundled x{data['bundleCount']})"
            if data["bundleCount"] > 1
            else f"{long_count}-{data['vertBar']}"
        ),
        "ligDesc": f"{data['ligBar']} hoop @ {int(round(data['ligSpacing'])):,}",
    }


def calculate_form(raw: dict[str, Any] | None) -> tuple[dict[str, Any] | None, list[str]]:
    data = normalize_form_data(raw)
    errors = validate(data)
    if errors:
        return None, errors
    result = calc_rect(data) if data["type"] == "rect" else calc_circ(data)
    return {**data, **result}, []


def read_saved_items() -> list[dict[str, Any]]:
    if not DATA_FILE.exists():
        return []
    try:
        data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return []
        items: list[dict[str, Any]] = []
        changed = False
        for item in data:
            if not isinstance(item, dict):
                continue
            recalculated = recalculate_saved_entry(item)
            if recalculated != item:
                changed = True
            items.append(recalculated)
        if changed:
            write_saved_items(items)
        return items
    except Exception:
        return []


def write_saved_items(items: list[dict[str, Any]]) -> None:
    DATA_FILE.write_text(json.dumps(items, indent=2), encoding="utf-8")


def recalculate_saved_entry(item: dict[str, Any]) -> dict[str, Any]:
    form = saved_entry_to_form_data(item)
    result, errors = calculate_form(form)
    if errors or result is None:
        return item
    updated = {**item, **result}
    if "id" not in updated or not str(updated["id"]).strip():
        updated["id"] = str(uuid.uuid4())
    if "savedAt" not in updated or not str(updated["savedAt"]).strip():
        updated["savedAt"] = iso_now()
    return updated


def saved_entry_to_form_data(item: dict[str, Any]) -> dict[str, Any]:
    return normalize_form_data(item)


def save_entry(raw: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    result, errors = calculate_form(raw)
    if errors or result is None:
        return None, errors

    entry_id = str(raw.get("id") or "").strip() or str(uuid.uuid4())
    saved_at = str(raw.get("savedAt") or "").strip() or iso_now()
    entry = {"id": entry_id, "savedAt": saved_at, **result}

    items = read_saved_items()
    for idx, item in enumerate(items):
        if item.get("id") == entry_id:
            items[idx] = entry
            write_saved_items(items)
            return entry, []
    items.append(entry)
    write_saved_items(items)
    return entry, []


def delete_entry(entry_id: str) -> bool:
    items = read_saved_items()
    filtered = [item for item in items if item.get("id") != entry_id]
    if len(filtered) == len(items):
        return False
    write_saved_items(filtered)
    return True


def reorder_entry(entry_id: str, direction: int) -> bool:
    items = read_saved_items()
    idx = next((i for i, item in enumerate(items) if item.get("id") == entry_id), None)
    if idx is None:
        return False
    new_idx = idx + direction
    if new_idx < 0 or new_idx >= len(items):
        return False
    item = items.pop(idx)
    items.insert(new_idx, item)
    write_saved_items(items)
    return True


def fmt_trim(value: float, dp: int = 2) -> str:
    text = f"{value:.{dp}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def build_csv(items: list[dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Design", "Level", "Shape", "Section", "Section Note", "Concrete Strength MPa", "Storey Height mm",
        "Cover mm", "Vertical Bar", "Bundled", "Bundle Count", "Lig Bar", "Lig Spacing mm", "Arrangement", "Bar Positions",
        "Longitudinal Count", "Longitudinal Steel Area As mm2", "Longitudinal Steel Ratio pct", "Lap mm", "Hook mm", "Lig Sets",
        "Longitudinal Steel kg", "Lig Steel kg", "Total Steel kg", "Concrete Volume m3", "Rate kg/m3"
    ])
    for x in items:
        writer.writerow([
            x.get("designName", ""), x.get("level", ""), x.get("shape", ""), x.get("sectionText", ""), x.get("sectionNote", ""),
            x.get("fc", ""), x.get("storyHeight", ""), x.get("cover", ""), x.get("vertBar", ""),
            "Yes" if x.get("bundleCount", 1) > 1 else "No", x.get("bundleCount", 1), x.get("ligBar", ""), x.get("ligSpacing", ""),
            x.get("arrangementText", ""), x.get("positionCount", ""), x.get("longCount", ""),
            fmt_trim(float(x.get("totalVertAreaMm2", 0)), 0), fmt_trim(float(x.get("steelRatioPct", 0)), 2), x.get("lapMm", ""),
            x.get("hookMm", ""), x.get("ligSets", ""), fmt_trim(float(x.get("longMassKg", 0)), 2), fmt_trim(float(x.get("ligMassKg", 0)), 2),
            fmt_trim(float(x.get("totalSteelKg", 0)), 2), fmt_trim(float(x.get("concVolM3", 0)), 3), fmt_trim(float(x.get("rateKgM3", 0)), 2)
        ])
    return output.getvalue()


def xml_escape(value: Any) -> str:
    return (
        str(value if value is not None else "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def spreadsheet_cell(value: Any, cell_type: str = "String", style_id: str = "") -> str:
    style_attr = f' ss:StyleID="{style_id}"' if style_id else ""
    return f'<Cell{style_attr}><Data ss:Type="{cell_type}">{xml_escape(value)}</Data></Cell>'


def build_excel_xml(items: list[dict[str, Any]]) -> str:
    batch_rows = items if items else [{
        "id": "", "designName": "", "level": "", "type": "rect", "fc": 50, "storyHeight": 3300, "cover": 40,
        "sectionNote": "", "rectB": 600, "rectD": 600, "barsNx": 4, "barsNy": 4, "circDia": "", "circBarCount": "",
        "vertBar": "N20", "useBundle": False, "bundleCount": 2, "ligBar": "N12", "ligSpacing": 200,
    }]

    batch_xml_rows: list[str] = []
    batch_xml_rows.append(f"<Row>{''.join(spreadsheet_cell(h, 'String', 'Header') for h in EXCEL_TEMPLATE_COLUMNS)}</Row>")
    for item in batch_rows:
        row_cells = []
        for col in EXCEL_TEMPLATE_COLUMNS:
            raw = item.get(col, "")
            cell_type = "Number" if isinstance(raw, (int, float)) and raw != "" else "String"
            row_cells.append(spreadsheet_cell(raw, cell_type))
        batch_xml_rows.append(f"<Row>{''.join(row_cells)}</Row>")

    schedule_xml_rows: list[str] = []
    schedule_xml_rows.append(f"<Row>{''.join(spreadsheet_cell(h, 'String', 'Header') for h in SCHEDULE_EXPORT_COLUMNS)}</Row>")
    for item in items:
        row_cells = []
        for col in SCHEDULE_EXPORT_COLUMNS:
            raw = item.get(col, "")
            cell_type = "Number" if isinstance(raw, (int, float)) and raw != "" else "String"
            row_cells.append(spreadsheet_cell(raw, cell_type))
        schedule_xml_rows.append(f"<Row>{''.join(row_cells)}</Row>")
    if not items:
        schedule_xml_rows.append(f"<Row>{''.join(spreadsheet_cell('', 'String') for _ in SCHEDULE_EXPORT_COLUMNS)}</Row>")

    notes_rows = [
        ["This workbook is intended for batch input and re-import into the tool."],
        ["Edit only the BatchInput sheet."],
        ["type values: rect or circ."],
        ["For rectangular rows fill rectB, rectD, barsNx and barsNy."],
        ["For circular rows fill circDia and circBarCount."],
        ["useBundle values: TRUE/FALSE, YES/NO, 1/0, or blank."],
        ["bundleCount is fixed at 2 when useBundle is true. If useBundle is false, bundleCount is ignored."],
        ["Vertical bar values must match exactly: N12, N16, N20, N24, N28, N32, N36, N40. Lig values may also use N10."],
        ["Import merges by id when an id exists, otherwise it appends as a new saved design."],
    ]
    notes_xml_rows = ''.join(f"<Row>{''.join(spreadsheet_cell(v, 'String') for v in row)}</Row>" for row in notes_rows)

    return f'''<?xml version="1.0"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:x="urn:schemas-microsoft-com:office:excel"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:html="http://www.w3.org/TR/REC-html40">
 <Styles>
  <Style ss:ID="Header">
   <Font ss:Bold="1" />
   <Interior ss:Color="#DCEEFF" ss:Pattern="Solid" />
  </Style>
 </Styles>
 <Worksheet ss:Name="BatchInput">
  <Table>
   {''.join(batch_xml_rows)}
  </Table>
 </Worksheet>
 <Worksheet ss:Name="SavedSchedule">
  <Table>
   {''.join(schedule_xml_rows)}
  </Table>
 </Worksheet>
 <Worksheet ss:Name="ReadMe">
  <Table>
   {notes_xml_rows}
  </Table>
 </Worksheet>
</Workbook>'''


def get_local_name(tag: str) -> str:
    return tag.rsplit('}', 1)[-1] if '}' in tag else tag


def parse_spreadsheet_xml(text: str) -> list[list[str]]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise ValueError("The selected file is not a valid Excel XML workbook.") from exc

    worksheets = [n for n in root.iter() if get_local_name(n.tag) == "Worksheet"]
    if not worksheets:
        raise ValueError("No worksheets were found in the workbook.")

    target = None
    for ws in worksheets:
        name = ws.attrib.get(f"{{{XML_NS['ss']}}}Name") or ws.attrib.get("ss:Name") or ws.attrib.get("Name") or ""
        if name == "BatchInput":
            target = ws
            break
    if target is None:
        target = worksheets[0]

    table = next((n for n in target if get_local_name(n.tag) == "Table"), None)
    if table is None:
        raise ValueError("The workbook does not contain a readable table.")

    parsed_rows: list[list[str]] = []
    for row in [n for n in table if get_local_name(n.tag) == "Row"]:
        values: list[str] = []
        cursor = 1
        cells = [n for n in row if get_local_name(n.tag) == "Cell"]
        for cell in cells:
            idx_attr = cell.attrib.get(f"{{{XML_NS['ss']}}}Index") or cell.attrib.get("ss:Index") or cell.attrib.get("Index")
            target_index = int(idx_attr) if idx_attr else cursor
            while len(values) < target_index - 1:
                values.append("")
            data_node = next((n for n in cell if get_local_name(n.tag) == "Data"), None)
            values.append(data_node.text if data_node is not None and data_node.text is not None else "")
            cursor = target_index + 1
        parsed_rows.append(values)
    return parsed_rows


def merge_imported_entries(imported: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing = read_saved_items()
    by_id = {str(item.get("id")): item for item in existing}
    for item in imported:
        by_id[str(item["id"])] = item
    merged_ids = list(by_id.keys())
    order = [str(item.get("id")) for item in existing if str(item.get("id")) in by_id]
    for item in imported:
        if str(item["id"]) not in order:
            order.append(str(item["id"]))
    merged = [by_id[item_id] for item_id in order if item_id in by_id]
    write_saved_items(merged)
    return merged


def build_imported_form_data(row_obj: dict[str, Any]) -> dict[str, Any]:
    return normalize_form_data(row_obj)


@app.get("/")
def index():
    return send_from_directory(BASE_DIR, HTML_FILE.name)


@app.get("/api/config")
def api_config():
    return jsonify({
        "appVersion": APP_VERSION,
        "barData": BAR_DATA,
        "vertBarKeys": VERT_BAR_KEYS,
        "ligBarKeys": LIG_BAR_KEYS,
        "defaults": default_form_data(),
    })


@app.post("/api/calculate")
def api_calculate():
    payload = request.get_json(silent=True) or {}
    result, errors = calculate_form(payload)
    if errors or result is None:
        return jsonify({"ok": False, "errors": errors}), 400
    return jsonify({"ok": True, "result": result})


@app.get("/api/designs")
def api_get_designs():
    return jsonify({"ok": True, "items": read_saved_items()})


@app.post("/api/designs")
def api_save_design():
    payload = request.get_json(silent=True) or {}
    entry, errors = save_entry(payload)
    if errors or entry is None:
        return jsonify({"ok": False, "errors": errors}), 400
    return jsonify({"ok": True, "item": entry, "items": read_saved_items()})


@app.delete("/api/designs")
def api_clear_designs():
    write_saved_items([])
    return jsonify({"ok": True, "items": []})


@app.delete("/api/designs/<entry_id>")
def api_delete_design(entry_id: str):
    if not delete_entry(entry_id):
        return jsonify({"ok": False, "errors": ["Saved design not found."]}), 404
    return jsonify({"ok": True, "items": read_saved_items()})


@app.post("/api/designs/reorder")
def api_reorder_design():
    payload = request.get_json(silent=True) or {}
    entry_id = str(payload.get("id") or "").strip()
    direction = parse_int(payload.get("direction", 0))
    if direction not in (-1, 1):
        return jsonify({"ok": False, "errors": ["Direction must be -1 or 1."]}), 400
    if not reorder_entry(entry_id, direction):
        return jsonify({"ok": False, "errors": ["Unable to reorder the saved design."]}), 400
    return jsonify({"ok": True, "items": read_saved_items()})


@app.get("/api/export/json")
def api_export_json():
    items = read_saved_items()
    response = make_response(json.dumps(items, indent=2))
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response.headers["Content-Disposition"] = "attachment; filename=column_reinforcement_schedule.json"
    return response


@app.get("/api/export/csv")
def api_export_csv():
    csv_text = build_csv(read_saved_items())
    response = make_response(csv_text)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = "attachment; filename=column_reinforcement_schedule.csv"
    return response


@app.get("/api/export/excel")
def api_export_excel():
    xml_text = build_excel_xml(read_saved_items())
    response = make_response(xml_text)
    response.headers["Content-Type"] = "application/vnd.ms-excel"
    response.headers["Content-Disposition"] = "attachment; filename=column_reinforcement_schedule.xls"
    return response


@app.post("/api/import/json")
def api_import_json():
    uploaded = request.files.get("file")
    if uploaded is None or not uploaded.filename:
        return jsonify({"ok": False, "errors": ["No JSON file was provided."]}), 400
    try:
        raw = json.loads(uploaded.read().decode("utf-8"))
    except Exception:
        return jsonify({"ok": False, "errors": ["The selected file is not a valid JSON file."]}), 400

    rows = raw if isinstance(raw, list) else raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(rows, list) or not rows:
        return jsonify({"ok": False, "errors": ["The JSON file does not contain any saved designs."]}), 400

    imported: list[dict[str, Any]] = []
    errors: list[str] = []
    for i, row_obj in enumerate(rows, start=1):
        if not isinstance(row_obj, dict):
            errors.append(f"Item {i}: Invalid JSON object.")
            continue
        form_data = build_imported_form_data(row_obj)
        calc, calc_errors = calculate_form(form_data)
        if calc_errors or calc is None:
            errors.append(f"Item {i}: {' '.join(calc_errors)}")
            continue
        imported.append({
            **row_obj,
            "id": str(row_obj.get("id") or "").strip() or str(uuid.uuid4()),
            "savedAt": str(row_obj.get("savedAt") or "").strip() or iso_now(),
            **calc,
        })

    if not imported and errors:
        return jsonify({"ok": False, "errors": errors[:5]}), 400

    merged = merge_imported_entries(imported)
    return jsonify({
        "ok": True,
        "items": merged,
        "importedCount": len(imported),
        "skippedCount": len(errors),
        "errors": errors[:5],
    })


@app.post("/api/import/excel")
def api_import_excel():
    uploaded = request.files.get("file")
    if uploaded is None or not uploaded.filename:
        return jsonify({"ok": False, "errors": ["No Excel file was provided."]}), 400
    try:
        rows = parse_spreadsheet_xml(uploaded.read().decode("utf-8"))
    except Exception as exc:
        return jsonify({"ok": False, "errors": [str(exc)]}), 400

    if not rows:
        return jsonify({"ok": False, "errors": ["The workbook does not contain any rows."]}), 400

    headers = [str(h or "").strip() for h in rows[0]]
    if "designName" not in headers or "type" not in headers:
        return jsonify({"ok": False, "errors": ["The workbook does not match the exported BatchInput template."]}), 400

    imported: list[dict[str, Any]] = []
    errors: list[str] = []
    for row_index, row in enumerate(rows[1:], start=2):
        row_obj = {header: (row[idx] if idx < len(row) else "") for idx, header in enumerate(headers)}
        if all(str(row_obj.get(header, "")).strip() == "" for header in headers):
            continue
        form_data = build_imported_form_data(row_obj)
        calc, calc_errors = calculate_form(form_data)
        if calc_errors or calc is None:
            errors.append(f"Row {row_index}: {' '.join(calc_errors)}")
            continue
        imported.append({
            "id": str(row_obj.get("id") or "").strip() or str(uuid.uuid4()),
            "savedAt": iso_now(),
            **calc,
        })

    if not imported and errors:
        return jsonify({"ok": False, "errors": errors[:5]}), 400

    merged = merge_imported_entries(imported)
    return jsonify({
        "ok": True,
        "items": merged,
        "importedCount": len(imported),
        "skippedCount": len(errors),
        "errors": errors[:5],
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
