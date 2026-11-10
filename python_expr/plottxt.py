# -*- coding: utf-8 -*-
import csv
from pathlib import Path


def load_rows(path: Path) -> set[tuple[str, ...]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        next(reader, None)
        return {tuple(row) for row in reader if row}


def print_metric(name: str, numerator: int, denominator: int) -> None:
    value = f"{numerator / denominator:.4f}" if denominator else "N/A"
    print(f"{name}: {value}")


def main() -> None:
    outputs = Path("outputs")
    user = load_rows(outputs / "original-target.csv")
    dme = load_rows(outputs / "dme-target.csv")
    consumer = load_rows(outputs / "consumer-target.csv")

    
    tp = len(user & consumer)
    fp = len(consumer - user)
    fn = len(user - consumer)

    print(f"TP: {tp:,} | FP: {fp:,} | FN: {fn:,}")
    print_metric("Recall", tp, tp + fn)
    print_metric("Precision", tp, tp + fp)
    print_metric("F1 score", 2 * tp, 2 * tp + fp + fn)
    print_metric("MRE", fp + fn, 2 * tp + fp + fn)

if __name__ == "__main__":
    main()
