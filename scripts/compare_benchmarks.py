import json
import sys


def format_rate(rate):
    if rate >= 1e9:
        return f"{rate / 1e9:.2f}B ops/s"
    elif rate >= 1e6:
        return f"{rate / 1e6:.2f}M ops/s"
    elif rate >= 1e3:
        return f"{rate / 1e3:.1f}k ops/s"
    else:
        return f"{rate:.0f} ops/s"


def main():
    if len(sys.argv) < 3:
        sys.exit(1)

    base_path = sys.argv[1]
    pr_path = sys.argv[2]
    base_name = sys.argv[3] if len(sys.argv) > 3 else "Base"
    pr_name = sys.argv[4] if len(sys.argv) > 4 else "PR"

    try:
        with open(base_path, "r", encoding="utf-8") as f:
            base_data = json.load(f)
    except Exception:
        base_data = {}

    try:
        with open(pr_path, "r", encoding="utf-8") as f:
            pr_data = json.load(f)
    except Exception:
        pr_data = {}

    base_map = {item["label"]: item.get("ops_per_sec", 0.0) for item in base_data.get("throughput", [])}
    pr_map = {item["label"]: item.get("ops_per_sec", 0.0) for item in pr_data.get("throughput", [])}

    all_labels = []
    for item in pr_data.get("throughput", []):
        all_labels.append(item["label"])
    for item in base_data.get("throughput", []):
        if item["label"] not in all_labels:
            all_labels.append(item["label"])

    lines = []
    lines.append("### ⚡ FyroDB Automated Benchmark Comparison")
    lines.append("")
    lines.append(f"Performance comparison of **`{pr_name}`** against base branch **`{base_name}`**:")
    lines.append("")
    lines.append(f"| Workload | Base (`{base_name}`) | PR (`{pr_name}`) | Delta | Status |")
    lines.append("| :--- | :--- | :--- | :--- | :---: |")

    gains = 0
    neutral = 0
    regressions = 0

    for label in all_labels:
        b_val = base_map.get(label)
        p_val = pr_map.get(label)
        if b_val is not None and p_val is not None:
            pct = ((p_val - b_val) / b_val) * 100.0 if b_val > 0 else 0.0
            if pct >= 3.0:
                badge = "🚀"
                delta_str = f"+{pct:.2f}%"
                gains += 1
            elif pct <= -3.0:
                badge = "⚠️"
                delta_str = f"{pct:.2f}%"
                regressions += 1
            else:
                badge = "➖"
                delta_str = f"{pct:+.2f}%"
                neutral += 1
            lines.append(f"| **{label}** | {format_rate(b_val)} | {format_rate(p_val)} | {delta_str} | {badge} |")
        elif p_val is not None:
            lines.append(f"| **{label}** | *N/A* | {format_rate(p_val)} | *new* | 🆕 |")
        else:
            lines.append(f"| **{label}** | {format_rate(b_val)} | *N/A* | *removed* | ❌ |")

    lines.append("")
    lines.append(f"> **Summary:** 🚀 **{gains}** improvements | ➖ **{neutral}** steady | ⚠️ **{regressions}** regressions")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
