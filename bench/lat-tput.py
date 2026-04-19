#!/usr/bin/env python3

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def resolve_input_path(user_path: Optional[str]) -> Path:
    if user_path:
        return Path(user_path).expanduser().resolve()

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    candidates = [
        Path.cwd() / "result.txt",
        Path.cwd() / "build" / "app" / "result.txt",
        repo_root / "result.txt",
        repo_root / "build" / "app" / "result.txt",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


def parse_result_file(path: Path) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or stripped.startswith("-"):
                continue

            parts = stripped.split()
            if len(parts) != 6:
                continue

            try:
                row = {
                    "clientCount": float(parts[0]),
                    "latAvg": float(parts[1]),
                    "latP50": float(parts[2]),
                    "latP90": float(parts[3]),
                    "latP99": float(parts[4]),
                    "throughput": float(parts[5]),
                }
            except ValueError:
                continue

            rows.append(row)

    if not rows:
        raise ValueError(f"No benchmark rows found in {path}")

    rows.sort(key=lambda item: item["throughput"])
    return rows


def plot_latency_throughput(rows: List[Dict[str, float]], output_path: Path) -> None:
    throughput = [row["throughput"] for row in rows]

    series = [
        ("latAvg", "tab:blue"),
        ("latP50", "tab:green"),
        ("latP90", "tab:orange"),
        ("latP99", "tab:red"),
    ]

    plt.figure(figsize=(10, 6))
    for name, color in series:
        latency_values = [row[name] for row in rows]
        plt.plot(throughput, latency_values, marker="o", color=color, linewidth=1.8, label=name)
        plt.scatter(throughput, latency_values, color=color, s=22)

    plt.title("Latency-Throughput Curve")
    plt.xlabel("Throughput (ops/sec)")
    plt.ylabel("Latency (ms)")
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.legend()
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=220)


def main() -> None:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(
        description="Generate latency-throughput plot from result.txt"
    )
    parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Path to result.txt (default: auto-detect)",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        default=str(script_dir / "lat-tput.png"),
        help="Output image path (default: bench/lat-tput.png)",
    )
    args = parser.parse_args()

    input_path = resolve_input_path(args.input_path)
    if not input_path.exists():
        raise FileNotFoundError(
            f"Could not find result file. Tried: {input_path}. "
            "Pass --input /path/to/result.txt"
        )

    rows = parse_result_file(input_path)
    output_path = Path(args.output_path).expanduser().resolve()
    plot_latency_throughput(rows, output_path)

    print(f"Read {len(rows)} rows from {input_path}")
    print(f"Saved plot to {output_path}")


if __name__ == "__main__":
    main()
