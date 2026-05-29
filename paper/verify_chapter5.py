#!/usr/bin/env python3
"""Verify Chapter 5 numerical claims against benchmark CSV data."""

import csv
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parent.parent
CSV_PATH = REPO / "benches" / "bench_results_workstation.csv"
TEX_PATH = REPO / "paper" / "chapters" / "chapter5.tex"
PAYLOAD_MIB = 2.64


# --- Data loading ---

def load_csv(path: Path) -> dict:
    """Load CSV into nested dict: data[scenario][backend] = row dict."""
    data = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            scenario = row["Scenario"]
            backend = row["Backend"]
            data.setdefault(scenario, {})[backend] = {
                "p50": float(row["p50_median"]),
                "p99": float(row["p99_median"]),
                "tput": float(row["MB/s_median"]),
            }
    return data


def det(row: dict) -> float:
    return row["p99"] / row["p50"]


# --- Number parsing ---

def parse_latex_number(s: str) -> float:
    """Parse LaTeX-formatted number: '22{,}278' -> 22278, '0.19' -> 0.19."""
    s = s.replace("{,}", "").replace(",", "")
    return float(s)


def decimal_places(s: str) -> int:
    """Count decimal places in a number string."""
    s = s.replace("{,}", "").replace(",", "")
    if "." in s:
        return len(s.split(".")[1])
    return 0


# --- Table verification ---

TABLE_DEFS = {
    "tab:latency_11": {
        "scenario": "Latency (1:1)",
        "backends": [
            "RsIpc_copy", "PosixIpc", "MpPipe", "MpShm",
            "RsIpc_zerocopy", "ZeroMQ", "MpQueue",
        ],
        "columns": ["p50", "p99", "tput", "det"],
    },
    "tab:scalability_110": {
        "scenario": "Scalability (1:10)",
        "backends": [
            "RsIpc_zerocopy", "RsIpc_copy", "MpPipe", "PosixIpc",
            "MpShm", "MpQueue", "ZeroMQ",
        ],
        "columns": ["p50", "p99", "tput", "det"],
    },
    "tab:contention_101": {
        "scenario": "Contention (10:1)",
        "backends": [
            "MpPipe", "RsIpc_zerocopy", "MpShm", "RsIpc_copy",
            "MpQueue", "PosixIpc", "ZeroMQ",
        ],
        "columns": ["p50", "p99", "tput", "det"],
    },
    "tab:size_scaling": {
        "scenarios": ["Size 1MB", "Size 2MB", "Size 4MB", "Size 8MB", "Size 16MB", "Size 32MB"],
        "backends": [
            "RsIpc_zerocopy", "RsIpc_copy", "MpQueue", "MpPipe",
            "MpShm", "ZeroMQ", "PosixIpc",
        ],
        "columns": ["tput"],
    },
}


def extract_table(tex: str, label: str) -> list[str]:
    """Extract data rows from a LaTeX table by label."""
    label_pos = tex.find(r"\label{" + label + "}")
    if label_pos == -1:
        return []
    begin = tex.rfind(r"\begin{table}", 0, label_pos)
    end = tex.find(r"\end{table}", label_pos)
    if begin == -1 or end == -1:
        return []
    table_text = tex[begin:end]
    tabular = re.search(r"\\begin\{tabular\}.*?\\end\{tabular\}", table_text, re.DOTALL)
    if not tabular:
        return []
    lines = tabular.group(0).split("\n")
    data_rows = []
    hline_count = 0
    for line in lines:
        line = line.strip()
        if "\\hline" in line or "\\bottomrule" in line or "\\midrule" in line:
            hline_count += 1
            if hline_count >= 3:
                break
            continue
        if hline_count >= 2 and "&" in line and "\\textbf" not in line:
            data_rows.append(line)
    return data_rows


def verify_standard_table(tex: str, label: str, table_def: dict, data: dict, verbose: bool) -> tuple[int, int, list[str]]:
    """Verify a standard table (latency/scalability/contention)."""
    rows = extract_table(tex, label)
    scenario = table_def["scenario"]
    backends = table_def["backends"]
    passed = 0
    total = 0
    errors = []

    for i, row_text in enumerate(rows):
        if i >= len(backends):
            break
        backend = backends[i]
        csv_row = data.get(scenario, {}).get(backend)
        if not csv_row:
            errors.append(f"  Missing CSV data: {scenario}/{backend}")
            continue

        cells = [c.strip().rstrip("\\").strip() for c in row_text.split("&")]
        if len(cells) < 5:
            errors.append(f"  Row parse error: {row_text[:60]}")
            continue

        value_cells = cells[1:]
        expected_vals = [csv_row["p50"], csv_row["p99"], csv_row["tput"], det(csv_row)]

        for j, (cell_str, expected) in enumerate(zip(value_cells, expected_vals)):
            total += 1
            cell_str = cell_str.strip()
            dp = decimal_places(cell_str)
            found = parse_latex_number(cell_str)
            exp_rounded = round(expected, dp) if dp > 0 else round(expected)
            if abs(found - exp_rounded) < 0.5 * 10**(-dp):
                passed += 1
            else:
                col_name = table_def["columns"][j]
                errors.append(f"  {backend} {col_name}: found {found}, expected {exp_rounded} (raw {expected:.4f})")

    return passed, total, errors


def verify_size_table(tex: str, label: str, table_def: dict, data: dict, verbose: bool) -> tuple[int, int, list[str]]:
    """Verify the size scaling table (throughput integers only)."""
    rows = extract_table(tex, label)
    scenarios = table_def["scenarios"]
    backends = table_def["backends"]
    passed = 0
    total = 0
    errors = []

    for i, row_text in enumerate(rows):
        if i >= len(scenarios):
            break
        scenario = scenarios[i]
        cells = [c.strip().rstrip("\\").strip() for c in row_text.split("&")]
        if len(cells) < 8:
            errors.append(f"  Row parse error for {scenario}: {row_text[:60]}")
            continue

        value_cells = cells[1:]
        for j, (cell_str, backend) in enumerate(zip(value_cells, backends)):
            total += 1
            csv_row = data.get(scenario, {}).get(backend)
            if not csv_row:
                errors.append(f"  Missing: {scenario}/{backend}")
                continue
            found = parse_latex_number(cell_str)
            expected = round(csv_row["tput"])
            if abs(found - expected) <= 1:
                passed += 1
            else:
                errors.append(f"  {scenario} {backend}: found {found}, expected {expected} (raw {csv_row['tput']:.1f})")

    return passed, total, errors


# --- Prose verification ---

@dataclass
class Check:
    desc: str
    expected: float
    anchor: str
    precision: int
    check_type: str = "exact"  # "exact", "upper_bound", "lower_bound"


def build_prose_checks(data: dict) -> list[Check]:
    """Build all prose verification checks from CSV data."""
    d = data
    zc_11 = d["Latency (1:1)"]["RsIpc_zerocopy"]
    cp_11 = d["Latency (1:1)"]["RsIpc_copy"]
    mq_11 = d["Latency (1:1)"]["MpQueue"]
    mp_11 = d["Latency (1:1)"]["MpPipe"]
    shm_11 = d["Latency (1:1)"]["MpShm"]
    zmq_11 = d["Latency (1:1)"]["ZeroMQ"]
    pos_11 = d["Latency (1:1)"]["PosixIpc"]

    zc_110 = d["Scalability (1:10)"]["RsIpc_zerocopy"]
    cp_110 = d["Scalability (1:10)"]["RsIpc_copy"]
    mq_110 = d["Scalability (1:10)"]["MpQueue"]
    mp_110 = d["Scalability (1:10)"]["MpPipe"]
    shm_110 = d["Scalability (1:10)"]["MpShm"]
    zmq_110 = d["Scalability (1:10)"]["ZeroMQ"]
    pos_110 = d["Scalability (1:10)"]["PosixIpc"]

    zc_101 = d["Contention (10:1)"]["RsIpc_zerocopy"]
    cp_101 = d["Contention (10:1)"]["RsIpc_copy"]
    mq_101 = d["Contention (10:1)"]["MpQueue"]
    mp_101 = d["Contention (10:1)"]["MpPipe"]
    shm_101 = d["Contention (10:1)"]["MpShm"]
    zmq_101 = d["Contention (10:1)"]["ZeroMQ"]
    pos_101 = d["Contention (10:1)"]["PosixIpc"]

    zc_1 = d["Size 1MB"]["RsIpc_zerocopy"]
    zc_2 = d["Size 2MB"]["RsIpc_zerocopy"]
    zc_4 = d["Size 4MB"]["RsIpc_zerocopy"]
    zc_8 = d["Size 8MB"]["RsIpc_zerocopy"]
    zc_16 = d["Size 16MB"]["RsIpc_zerocopy"]
    zc_32 = d["Size 32MB"]["RsIpc_zerocopy"]
    cp_4 = d["Size 4MB"]["RsIpc_copy"]
    pos_32 = d["Size 32MB"]["PosixIpc"]
    shm_32 = d["Size 32MB"]["MpShm"]
    mq_32 = d["Size 32MB"]["MpQueue"]
    mp_32 = d["Size 32MB"]["MpPipe"]
    zmq_sizes = [d[f"Size {s}MB"]["ZeroMQ"] for s in [1, 2, 4, 8, 16, 32]]

    # next best at 1MB (excluding zerocopy)
    others_1mb = [d["Size 1MB"][b]["tput"] for b in
                  ["RsIpc_copy", "MpQueue", "MpPipe", "MpShm", "ZeroMQ", "PosixIpc"]]
    next_best_1mb = max(others_1mb)

    # PosixIpc det across scenarios
    pos_dets = [det(pos_11), det(pos_110), det(pos_101)]

    checks = [
        # --- 1:1 Analysis (line ~143) ---
        Check("1:1: MpQueue/copy ratio",
              mq_11["p50"] / cp_11["p50"],
              r"(\d+\.\d)\$\\times\$ lower than \\texttt\{multiprocessing\.Queue\}",
              1),
        Check("1:1: MpPipe/copy ratio",
              mp_11["p50"] / cp_11["p50"],
              r"(\d+\.\d)\$\\times\$ lower than MpPipe",
              1),
        Check("1:1: Copy throughput",
              cp_11["tput"],
              r"throughput \((\d+\{,\}\d+)~MiB/s\) exceeds the naive",
              0),
        Check("1:1: Copy pipelining factor",
              cp_11["tput"] / (PAYLOAD_MIB / (cp_11["p50"] / 1000)),
              r"by a factor of (\d+\.\d)\$\\times\$, reflecting",
              1),
        Check("1:1: ZeroMQ throughput",
              zmq_11["tput"],
              r"throughput of (\d\{,\}\d+)~MiB/s \(a",
              0),
        Check("1:1: ZeroMQ pipelining factor",
              zmq_11["tput"] / (PAYLOAD_MIB / (zmq_11["p50"] / 1000)),
              r"a (\d+\.\d)\$\\times\$ pipelining factor\)",
              1),

        # --- 1:10 Analysis (line ~178) ---
        Check("1:10: zc scalability ratio",
              zc_110["p50"] / zc_11["p50"],
              r"scalability ratio of (\d+\.\d)\$\\times\$ relative",
              1),
        Check("1:10: PosixIpc degradation",
              pos_110["p50"] / pos_11["p50"],
              r"degrades (\d+)\$\\times\$ from its 1:1 latency",
              0),

        # --- Scalability caption (line 188) ---
        Check("Caption: zc scalability ratio",
              zc_110["p50"] / zc_11["p50"],
              r"degrades only (\d+\.\d)\$\\times\$ from its 1:1 baseline",
              1),
        Check("Caption: PosixIpc degradation",
              pos_110["p50"] / pos_11["p50"],
              r"PosixIpc degrades (\d+\.\d)\$\\times\$, demonstrating",
              1),

        # --- Determinism (line ~256-265) ---
        Check("Determinism: max in 1:1/1:10",
              max(det(zmq_110), det(pos_110), det(mq_110), det(shm_110),
                  det(mp_110), det(zc_110), det(cp_110),
                  det(zmq_11), det(pos_11), det(mq_11), det(shm_11),
                  det(mp_11), det(zc_11), det(cp_11)),
              r"below (\d+\.\d)\$\\times\$, with RsIpc",
              1, "upper_bound"),
        Check("Determinism: zc range low (1:1)",
              det(zc_11),
              r"achieving (\d+\.\d\d)--\d+\.\d+\$\\times\$",
              2),
        Check("Determinism: zc range high (1:10)",
              det(zc_110),
              r"achieving \d+\.\d\d--(\d+\.\d\d)\$\\times\$",
              2),
        Check("Determinism: PosixIpc 10:1",
              det(pos_101),
              r"PosixIpc at (\d+\.\d+)\$\\times\$, ZeroMQ",
              2),
        Check("Determinism: ZeroMQ 10:1",
              det(zmq_101),
              r"ZeroMQ at (\d+\.\d+)\$\\times\$, RsIpc \(copy\)",
              2),
        Check("Determinism: RsIpc copy 10:1",
              det(cp_101),
              r"RsIpc \(copy\) at (\d+\.\d+)\$\\times\$, and MpQueue",
              2),
        Check("Determinism: MpQueue 10:1",
              det(mq_101),
              r"MpQueue at (\d+\.\d+)\$\\times\$\.",
              2),
        Check("Determinism: MpShm 10:1",
              det(shm_101),
              r"MpShm at (\d+\.\d+)\$\\times\$, caused",
              2),
        Check("Determinism: zc catastrophic",
              det(zc_101),
              r"RsIpc \(zerocopy\) at (\d+\.\d)\$\\times\$ and MpPipe",
              1),
        Check("Determinism: MpPipe catastrophic",
              det(mp_101),
              r"MpPipe at (\d+\.\d)\$\\times\$, caused by",
              1),
        Check("Real-time example: zc p50",
              zc_101["p50"],
              r"p50 = (\d+\.\d+)~ms and p99 = \d+~ms \(ratio",
              2),
        Check("Real-time example: zc p99",
              zc_101["p99"],
              r"p99 = (\d+)~ms \(ratio \d+\$\\times\$\) because",
              0),
        Check("Real-time example: ratio",
              det(zc_101),
              r"\(ratio (\d+)\$\\times\$\) because",
              0),

        # --- Throughput (line ~279-283) ---
        Check("Throughput 1:1: copy",
              cp_11["tput"],
              r"achieves ([\d,]+)~MiB/s sustained throughput",
              0),
        Check("Throughput 1:1: PosixIpc",
              pos_11["tput"],
              r"PosixIpc follows at ([\d,]+)~MiB/s, confirming",
              0),
        Check("Throughput 1:10: zc",
              zc_110["tput"],
              r"highest throughput \(([\d,]+)~MiB/s\): \d+\.\d",
              0),
        Check("Throughput 1:10: zc/ZeroMQ ratio",
              zc_110["tput"] / zmq_110["tput"],
              r"MiB/s\): (\d+\.\d)\$\\times\$ higher than ZeroMQ \(\d+",
              1),
        Check("Throughput 1:10: ZeroMQ",
              zmq_110["tput"],
              r"higher than ZeroMQ \((\d+)~MiB/s\) and",
              0),
        Check("Throughput 1:10: zc/MpShm ratio",
              zc_110["tput"] / shm_110["tput"],
              r"(\d+\.\d)\$\\times\$ higher than MpShm \(\d+",
              1),
        Check("Throughput 1:10: MpShm",
              shm_110["tput"],
              r"higher than MpShm \((\d+)~MiB/s\)\. Multiple",
              0),
        Check("Throughput 10:1: copy",
              cp_101["tput"],
              r"highest aggregate throughput \(([\d,]+)~MiB/s\) because",
              0),
        Check("Throughput 10:1: PosixIpc",
              pos_101["tput"],
              r"PosixIpc follows at ([\d,]+)~MiB/s, benefiting",
              0),

        # --- Size Scaling prose (line ~318-326) ---
        Check("Size: small zc low (1MB)",
              zc_1["tput"],
              r"achieves ([\d,]+)--[\d,]+~MiB/s, far exceeding",
              0),
        Check("Size: small zc high (2MB)",
              zc_2["tput"],
              r"achieves [\d,]+--([\d,]+)~MiB/s, far exceeding",
              0),
        Check("Size: peak throughput (8MB)",
              zc_8["tput"],
              r"peak throughput of ([\d,]+)~MiB/s at 8",
              0),
        Check("Size 32MB: zc",
              zc_32["tput"],
              r"RsIpc \(zerocopy\) at ([\d,]+)~MiB/s, PosixIpc at",
              0),
        Check("Size 32MB: PosixIpc",
              pos_32["tput"],
              r"PosixIpc at ([\d,]+)~MiB/s, and MpShm at",
              0),
        Check("Size 32MB: MpShm",
              shm_32["tput"],
              r"and MpShm at ([\d,]+)~MiB/s, all bounded",
              0),
        Check("Size 32MB: MpQueue",
              mq_32["tput"],
              r"\(MpQueue ([\d,]+)~MiB/s, MpPipe",
              0),
        Check("Size 32MB: MpPipe",
              mp_32["tput"],
              r"MpPipe ([\d,]+)~MiB/s\) due to",
              0),
        Check("Size: ZeroMQ 1MB",
              zmq_sizes[0]["tput"],
              r"\((\d,\d+) \$\\rightarrow\$",
              0),
        Check("Size: ZeroMQ 2MB",
              zmq_sizes[1]["tput"],
              r"\$\\rightarrow\$ (\d,\d+) \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$",
              0),
        Check("Size: ZeroMQ 4MB",
              zmq_sizes[2]["tput"],
              r"\$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ (\d,\d+) \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$",
              0),
        Check("Size: ZeroMQ 8MB",
              zmq_sizes[3]["tput"],
              r"\$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ (\d,\d+) \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$",
              0),
        Check("Size: ZeroMQ 16MB",
              zmq_sizes[4]["tput"],
              r"\$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ \d,\d+ \$\\rightarrow\$ (\d,\d+) \$\\rightarrow\$",
              0),
        Check("Size: ZeroMQ 32MB",
              zmq_sizes[5]["tput"],
              r"\$\\rightarrow\$ (\d,\d+)~MiB/s\)",
              0),

        # --- Copy vs Zerocopy (line ~334-349) ---
        Check("CopyZc: copy 10:1 p99",
              cp_101["p99"],
              r"bounded tail latency \((\d+\.\d+)~ms p99\)",
              2),
        Check("CopyZc: copy det bound",
              det(cp_101),
              r"remains below (\d+\.\d)\$\\times\$ regardless",
              1, "upper_bound"),
        Check("CopyZc: zc 4MB throughput",
              zc_4["tput"],
              r"achieves ([\d,]+)~MiB/s versus [\d,]+~MiB/s for copy mode",
              0),
        Check("CopyZc: copy 4MB throughput",
              cp_4["tput"],
              r"versus ([\d,]+)~MiB/s for copy mode",
              0),
        Check("CopyZc: zc/copy throughput ratio",
              zc_110["tput"] / cp_110["tput"],
              r"achieves (\d+\.\d+)\$\\times\$ higher throughput",
              2),
        Check("CopyZc: zc 1:10 tput (comparison)",
              zc_110["tput"],
              r"higher throughput.*\(([\d,]+) vs",
              0),
        Check("CopyZc: copy 1:10 tput (comparison)",
              cp_110["tput"],
              r"vs\.\\ ([\d,]+)~MiB/s\)",
              0),
        Check("CopyZc: latency ratio",
              cp_110["p50"] / zc_110["p50"],
              r"(\d+\.\d+)\$\\times\$ lower latency",
              2),
        Check("CopyZc: zc 1:10 latency",
              zc_110["p50"],
              r"lower latency \((\d+\.\d+) vs",
              2),
        Check("CopyZc: copy 1:10 latency",
              cp_110["p50"],
              r"lower latency.*vs\.\\ (\d+\.\d+)~ms\)",
              2),

        # --- Discussion: Where rs_ipc Excels (line ~450-456) ---
        Check("Discussion: zc/ZeroMQ ratio",
              zc_110["tput"] / zmq_110["tput"],
              r"achieves (\d+\.\d)\$\\times\$ higher throughput than ZeroMQ",
              1),
        Check("Discussion: zc/MpShm ratio",
              zc_110["tput"] / shm_110["tput"],
              r"(\d+\.\d)\$\\times\$ higher than MpShm",
              1),
        Check("Discussion: zc/PosixIpc ratio",
              zc_110["tput"] / pos_110["tput"],
              r"(\d+\.\d)\$\\times\$ higher than PosixIpc",
              1),
        Check("Discussion: zc scalability",
              zc_110["p50"] / zc_11["p50"],
              r"is (\d+\.\d)\$\\times\$ for RsIpc \(zerocopy\) compared",
              1),
        Check("Discussion: PosixIpc scalability",
              pos_110["p50"] / pos_11["p50"],
              r"compared to (\d+\.\d)\$\\times\$ for PosixIpc",
              1),
        Check("Discussion: ZeroMQ scalability",
              zmq_110["p50"] / zmq_11["p50"],
              r"(\d+\.\d)\$\\times\$ for ZeroMQ",
              1),
        Check("Discussion: copy det bound",
              det(cp_101),
              r"below (\d+\.\d)\$\\times\$ even under 10:1",
              1, "upper_bound"),
        Check("Discussion: copy 10:1 p99",
              cp_101["p99"],
              r"\(p99 = (\d+\.\d+)~ms\).*zerocopy mode achieves lower",
              2),
        Check("Discussion: small size advantage",
              zc_1["tput"] / next_best_1mb,
              r"(\d+\.\d)\$\\times\$ advantage over the next",
              1),

        # --- Critical Weakness (line ~460-466) ---
        Check("Weakness: zc det",
              det(zc_101),
              r"determinism ratio of (\d+\.\d)\$\\times\$.*\(p99",
              1),
        Check("Weakness: zc p99",
              zc_101["p99"],
              r"ratio of \d+\.\d.*\(p99 = (\d+\.\d+)~ms\) in the 10:1",
              2),
        Check("Weakness: copy p99",
              cp_101["p99"],
              r"p99 of (\d+\.\d+)~ms---\d+\$\\times\$ better",
              2),
        Check("Weakness: copy/zc p99 ratio",
              zc_101["p99"] / cp_101["p99"],
              r"ms---(\d+)\$\\times\$ better---because",
              0),
        Check("Weakness: PosixIpc det",
              det(pos_101),
              r"determinism \((\d+\.\d+)\$\\times\$, p99",
              2),
        Check("Weakness: PosixIpc p99",
              pos_101["p99"],
              r"p99 = (\d+\.\d+)~ms\) despite",
              2),

        # --- Positioning (line ~475-479) ---
        Check("Positioning: ZeroMQ 1:10 slowness",
              zmq_110["p50"] / zc_110["p50"],
              r"it is (\d+\.\d)\$\\times\$ \(1:10\)",
              1),
        Check("Positioning: ZeroMQ 1:1 slowness",
              zmq_11["p50"] / cp_11["p50"],
              r"to (\d+\.\d)\$\\times\$ \(1:1\)",
              1),
        Check("Positioning: PosixIpc contention det",
              det(pos_101),
              r"fairness \((\d+\.\d+)\$\\times\$ determinism\)",
              2),
        Check("Positioning: PosixIpc 1:10 latency",
              pos_110["p50"],
              r"1:10 latency \((\d+\.\d+)~ms\) is",
              2),
        Check("Positioning: PosixIpc/zc ratio",
              pos_110["p50"] / zc_110["p50"],
              r"is (\d+\.\d)\$\\times\$ worse",
              1),

        # --- Design Guidelines (line ~588-597) ---
        Check("Guidelines: Queue/copy ratio",
              mq_11["p50"] / cp_11["p50"],
              r"It achieves (\d+\.\d)\$\\times\$ lower latency than \\texttt\{multiprocessing\.Queue\}",
              1),
        Check("Guidelines: Pipe/copy ratio",
              mp_11["p50"] / cp_11["p50"],
              r"(\d+\.\d)\$\\times\$ lower than MpPipe, with minimal",
              1),
        Check("Guidelines: zc throughput",
              zc_110["tput"],
              r"highest throughput \(([\d,]+)~MiB/s\) with strong",
              0),
        Check("Guidelines: zc det",
              det(zc_110),
              r"predictability \((\d+\.\d+)\$\\times\$ determinism\)",
              2),
        Check("Guidelines: zc det failure",
              det(zc_101),
              r"due to the (\d+\.\d)\$\\times\$ determinism ratio",
              1),
        Check("Guidelines: raw byte low",
              min(zc_1["tput"], zc_2["tput"], zc_4["tput"], zc_8["tput"]),
              r"achieves ([\d,]+)--[\d,]+~MiB/s for messages",
              -3),
        Check("Guidelines: raw byte high",
              max(zc_1["tput"], zc_2["tput"], zc_4["tput"], zc_8["tput"]),
              r"achieves [\d,]+--([\d,]+)~MiB/s for messages",
              -3),
        Check("Guidelines: PosixIpc det low",
              min(pos_dets),
              r"ratios \((\d+\.\d+)--\d+\.\d+\$\\times\$\) across",
              2),
        Check("Guidelines: PosixIpc det high",
              max(pos_dets),
              r"ratios \(\d+\.\d+--(\d+\.\d+)\$\\times\$\) across",
              2),

        # --- Overhead Decomposition (line ~418) ---
        Check("Decomposition: total latency",
              zc_11["p50"] * 1000,
              r"median latency of (\d+)~\\textmu",
              -1),

        # --- Figure Captions ---
        Check("Caption: determinism PosixIpc",
              det(pos_101),
              r"PosixIpc maintains (\d+\.\d+)\$\\times\$ while",
              2),
        Check("Caption: determinism zc",
              det(zc_101),
              r"degrades to (\d+\.\d)\$\\times\$",
              1),
        Check("Caption: CDF tail extent",
              max(zc_101["p99"], mp_101["p99"]),
              r"tail latency exceeding (\d+)~ms",
              0, "lower_bound"),
        Check("Caption: scalability zc",
              zc_110["p50"] / zc_11["p50"],
              r"degrades only (\d+\.\d)\$\\times\$ from",
              1),
        Check("Caption: scalability PosixIpc",
              pos_110["p50"] / pos_11["p50"],
              r"PosixIpc degrades (\d+\.\d)\$\\times\$",
              1),
    ]

    return checks


def find_line_number(tex: str, match_start: int) -> int:
    """Get 1-based line number for a character offset."""
    return tex[:match_start].count("\n") + 1


def verify_prose(tex: str, checks: list[Check], verbose: bool) -> tuple[int, int, list[str]]:
    """Run all prose checks against the LaTeX text."""
    passed = 0
    total = 0
    errors = []

    for check in checks:
        total += 1
        m = re.search(check.anchor, tex)
        if not m:
            errors.append(f"  ✗ {check.desc}: anchor not found — {check.anchor[:60]}")
            continue
        if len(re.findall(check.anchor, tex)) > 1 and verbose:
            line = find_line_number(tex, m.start())
            errors.append(f"  ? {check.desc}: multiple matches (using first at line {line})")

        raw_found = m.group(1)
        found = parse_latex_number(raw_found)
        line = find_line_number(tex, m.start())

        if check.precision < 0:
            expected_rounded = round(check.expected, check.precision)
        elif check.precision == 0:
            expected_rounded = round(check.expected)
        else:
            expected_rounded = round(check.expected, check.precision)

        ok = False
        if check.check_type == "exact":
            if check.precision < 0:
                ok = abs(found - expected_rounded) < 0.5 * 10 ** (-check.precision)
            elif check.precision == 0:
                ok = abs(found - expected_rounded) <= 0.5
            else:
                ok = abs(found - expected_rounded) < 0.5 * 10 ** (-check.precision)
        elif check.check_type == "upper_bound":
            ok = check.expected < found
        elif check.check_type == "lower_bound":
            ok = check.expected > found

        if ok:
            passed += 1
            if verbose:
                print(f"  ✓ {check.desc} = {found} (expected {expected_rounded}, line {line})")
        else:
            if check.check_type == "exact":
                errors.append(
                    f"  ✗ {check.desc}: found {found}, expected {expected_rounded} "
                    f"(raw {check.expected:.4f}, line {line})"
                )
            elif check.check_type == "upper_bound":
                errors.append(
                    f"  ✗ {check.desc}: actual {expected_rounded} NOT < bound {found} "
                    f"(raw {check.expected:.4f}, line {line})"
                )
            elif check.check_type == "lower_bound":
                errors.append(
                    f"  ✗ {check.desc}: actual {expected_rounded} NOT > threshold {found} "
                    f"(raw {check.expected:.4f}, line {line})"
                )

    return passed, total, errors


# --- Main ---

def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv

    print(f"Verifying {TEX_PATH.relative_to(REPO)} against {CSV_PATH.relative_to(REPO)}...\n")

    data = load_csv(CSV_PATH)
    tex = TEX_PATH.read_text()

    total_passed = 0
    total_checks = 0
    all_errors = []

    # Table verification
    print("Tables:")
    for label, table_def in TABLE_DEFS.items():
        if "scenario" in table_def:
            p, t, errs = verify_standard_table(tex, label, table_def, data, verbose)
        else:
            p, t, errs = verify_size_table(tex, label, table_def, data, verbose)
        total_passed += p
        total_checks += t
        status = "✓" if not errs else "✗"
        print(f"  {status} {label} — {p}/{t} values correct")
        if errs:
            all_errors.extend(errs)
            for e in errs:
                print(e)

    # Prose verification
    print(f"\nProse ({len(build_prose_checks(data))} claims):")
    checks = build_prose_checks(data)
    p, t, errs = verify_prose(tex, checks, verbose)
    total_passed += p
    total_checks += t
    if errs:
        all_errors.extend(errs)
        for e in errs:
            print(e)
    if not errs:
        print(f"  ✓ All {t} claims verified")

    # Summary
    failed = total_checks - total_passed
    print(f"\nResults: {total_passed}/{total_checks} passed", end="")
    if failed:
        print(f", {failed} FAILED")
    else:
        print()

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
