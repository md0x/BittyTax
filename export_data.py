# export_events_with_balances.py
from decimal import Decimal
import csv
import sys
import argparse
import shutil
from typing import Dict, List, Optional

from bittytax.bittytax import _do_import, _do_tax
from bittytax.audit import AuditRecords, AuditLogEntry
from bittytax.bt_types import TaxRules, TrType
from bittytax.tax_event import TaxEventCapitalGains as TECG
from bittytax.tax import CalculateCapitalGains as CCG
from bittytax.t_record import TransactionRecord

INPUT = "pablo-data/all_records_250713.xlsx"  # change to your file
TAX_RULES = TaxRules.UK_INDIVIDUAL  # or a UK_COMPANY_* rule

# Monkey-patch to tag capital gains events with the originating transaction record and tid
_orig_init_cg = TECG.__init__


def _patched_init_cg(self, disposal_type, b, s, cost, fees):
    _orig_init_cg(self, disposal_type, b, s, cost, fees)
    self.t_record = getattr(s, "t_record", None)
    self.tid = getattr(s, "tid", None)


TECG.__init__ = _patched_init_cg  # type: ignore[assignment]

INCOME_TYPES = {
    TrType.MINING,
    TrType.STAKING,
    TrType.DIVIDEND,
    TrType.INTEREST,
    TrType.INCOME,
}


def fmt_qty(x: Optional[Decimal]) -> str:
    return "" if x is None else f"{x.normalize():f}"


def fmt_val(x: Optional[Decimal]) -> str:
    return "" if x is None else f"{x:.2f}"


def ts(tr: TransactionRecord) -> str:
    return tr._format_timestamp()


def _should_output_csv(force_csv: bool, force_table: bool) -> bool:
    # Default behavior: table when interactive TTY, CSV when redirected
    if force_csv:
        return True
    if force_table:
        return False
    return not sys.stdout.isatty()


def fmt_tx(tr: TransactionRecord) -> str:
    txr = getattr(getattr(tr, "t_row", None), "tx_raw", None)
    if not txr:
        return ""
    if txr.tx_hash:
        return txr.tx_hash
    parts = []
    if txr.tx_src:
        parts.append(txr.tx_src)
    if txr.tx_dest:
        parts.append(txr.tx_dest)
    if len(parts) == 2:
        return f"{parts[0]}->{parts[1]}"
    if parts:
        return parts[0]
    return ""


def _truncate_cell(text: str, max_width: int) -> str:
    if len(text) <= max_width:
        return text
    if max_width <= 1:
        return text[:max_width]
    return text[: max_width - 1] + "…"


def _print_ascii_table(headers: List[str], rows: List[List[str]]) -> None:
    # Determine terminal width to choose reasonable column caps
    term_cols = shutil.get_terminal_size((120, 24)).columns
    # Base max width per column; try to avoid lines longer than terminal width
    # Start with a soft cap and adjust down if needed
    soft_cap = 28

    # Indices that should be right-aligned (numeric-like)
    numeric_indices = {
        5,  # change_qty
        6,  # fee_qty
        7,  # balance_wallet_after
        8,  # total_balance_after
        9,  # sell_proceeds_value_ccy
        10,  # sell_cost_value_ccy
        11,  # sell_fees_value_ccy
        12,  # sell_gain_value_ccy
        13,  # income_amount_value_ccy
        14,  # income_fees_value_ccy
        16,  # counter_change_qty
        17,  # counter_balance_wallet_after
        18,  # counter_total_balance_after
    }

    # Truncate cells with a preliminary cap to bound computation
    prelim_rows: List[List[str]] = []
    for r in rows:
        prelim_rows.append([_truncate_cell(c, soft_cap) for c in r])

    # Compute widths
    widths: List[int] = []
    for col_idx, header in enumerate(headers):
        col_items = [row[col_idx] for row in prelim_rows] if prelim_rows else []
        max_len = max([len(header)] + [len(x) for x in col_items])
        widths.append(max_len)

    # If the table is too wide, reduce columns down to a minimum
    # Total = sum(widths) + 3 * (ncols - 1) (separators); use 1 space padding around |
    def total_width(ws: List[int]) -> int:
        return sum(ws) + 3 * (len(ws) - 1) + 2  # initial and trailing spaces

    if total_width(widths) > term_cols:
        # Compute how much we must shave off and distribute
        over_by = total_width(widths) - term_cols
        # Minimum width per column is header length (at least 4)
        min_widths = [max(min(12, len(h)), 4) for h in headers]
        # Iteratively reduce the widest columns until it fits or cannot reduce further
        while over_by > 0:
            widest_idx = max(range(len(widths)), key=lambda i: widths[i] - min_widths[i])
            if widths[widest_idx] <= min_widths[widest_idx]:
                break
            widths[widest_idx] -= 1
            over_by -= 1

        # Apply final truncation to cells per new widths
        prelim_rows = [
            [_truncate_cell(cell, widths[i]) for i, cell in enumerate(row)] for row in prelim_rows
        ]

    # Builders
    def fmt_cell(text: str, width: int, right_align: bool) -> str:
        if right_align:
            return text.rjust(width)
        return text.ljust(width)

    # Header
    header_cells = [fmt_cell(h, widths[i], False) for i, h in enumerate(headers)]
    header_line = " | ".join(header_cells)
    sep_line = "-+-".join(["-" * widths[i] for i in range(len(headers))])

    print(" " + header_line)
    print(" " + sep_line)

    # Rows
    for row in prelim_rows:
        cells = [fmt_cell(row[i], widths[i], i in numeric_indices) for i in range(len(headers))]
        print(" " + " | ".join(cells))


def main():
    parser = argparse.ArgumentParser(
        description="Export BittyTax audit events with optional table output"
    )
    parser.add_argument("--csv", action="store_true", help="Force CSV output to stdout")
    parser.add_argument("--table", action="store_true", help="Force ASCII table output to stdout")
    parser.add_argument("--input", default=INPUT, help="Path to BittyTax input workbook (.xlsx)")
    args = parser.parse_args()

    trs: List[TransactionRecord] = _do_import(args.input)

    # Build audit stream (per-event balances)
    audit = AuditRecords(trs)

    # Run tax to obtain capital gains events
    tax, _ = _do_tax(trs, TAX_RULES, skip_integrity_check=True)

    # Aggregate disposal P/L per TransactionRecord using the stable main TID (tid[0])
    tr_to_pl: Dict[int, Dict[str, Decimal]] = {}
    for year in sorted(tax.tax_events):
        if year not in CCG.CG_DATA_INDIVIDUAL:
            continue
        for te in tax.tax_events[year]:
            if getattr(te, "t_record", None) is None:
                continue
            tr = te.t_record
            if tr.tid:
                tr_key = tr.tid[0]
                agg = tr_to_pl.setdefault(
                    tr_key,
                    {
                        "proceeds": Decimal(0),
                        "cost": Decimal(0),
                        "fees": Decimal(0),
                        "gain": Decimal(0),
                    },
                )
                agg["proceeds"] += te.proceeds
                agg["cost"] += te.cost
                agg["fees"] += te.fees
                agg["gain"] += te.gain

    # Map each TransactionRecord (by tid[0]) to its own audit entries (BUY/SELL/FEE)
    tr_to_entries: Dict[int, Dict[str, AuditLogEntry]] = {}
    for asset, entries in audit.audit_log.items():
        for e in entries:
            tr = e.t_record
            if tr.tid:
                key = tr.tid[0]
                parts = tr_to_entries.setdefault(key, {})
                parts[e.tr_part.name] = e  # "BUY", "SELL", "FEE"

    headers = [
        "asset",
        "timestamp",
        "record_type",  # e.g., Trade, Spend, Income, Staking
        "part",  # BUY / SELL / FEE
        "wallet",
        "change_qty",  # asset units (+ for BUY, - for SELL)
        "fee_qty",  # asset units (fee paid in this asset)
        "balance_wallet_after",  # asset units
        "total_balance_after",  # asset units, across wallets
        "sell_proceeds_value_ccy",  # reporting currency
        "sell_cost_value_ccy",  # reporting currency
        "sell_fees_value_ccy",  # reporting currency
        "sell_gain_value_ccy",  # reporting currency
        "income_amount_value_ccy",  # reporting currency (for income-type BUY)
        "income_fees_value_ccy",  # reporting currency (for income-type BUY)
        "counter_asset",  # asset spent/received on the other side of the record
        "counter_change_qty",  # counter asset units (sign reflects its own event)
        "counter_balance_wallet_after",  # counter asset wallet balance after its own event
        "counter_total_balance_after",  # counter asset total balance after its own event
        "tx",
    ]

    data_rows: List[List[str]] = []
    for asset in sorted(audit.audit_log):
        for e in audit.audit_log[asset]:
            tr = e.t_record
            record_type = tr.t_type.value

            # Base event fields
            row = [
                asset,
                ts(tr),
                record_type,
                e.tr_part.value,
                e.wallet,
                fmt_qty(e.change),
                fmt_qty(e.fee),
                fmt_qty(e.balance),
                fmt_qty(e.total),
            ]

            # SELL P/L (if any) aggregated per transaction
            sell_cols = ["", "", "", ""]
            if e.tr_part.name == "SELL" and tr.tid:
                tr_key = tr.tid[0]
                pl = tr_to_pl.get(tr_key)
                if pl:
                    sell_cols = [
                        fmt_val(pl["proceeds"]),
                        fmt_val(pl["cost"]),
                        fmt_val(pl["fees"]),
                        fmt_val(pl["gain"]),
                    ]

            # INCOME amounts (for income-type BUY)
            income_cols = ["", ""]
            if e.tr_part.name == "BUY" and tr.t_type in INCOME_TYPES and tr.buy:
                income_cols = [
                    fmt_val(tr.buy.cost if tr.buy.cost is not None else None),
                    fmt_val(tr.buy.fee_value if tr.buy.fee_value is not None else None),
                ]

            # Counter asset info (currency used on the other side), amount and that side's balances
            counter_asset = ""
            counter_change_qty = ""
            counter_wallet_bal_after = ""
            counter_total_bal_after = ""

            if tr.tid:
                tr_key = tr.tid[0]
                parts = tr_to_entries.get(tr_key, {})
                if e.tr_part.name == "BUY":
                    # Other side is SELL (amount spent to acquire this BUY)
                    if tr.sell:
                        counter_asset = tr.sell.asset
                        counter_change_qty = fmt_qty(-tr.sell.quantity)
                    if "SELL" in parts:
                        counter_wallet_bal_after = fmt_qty(parts["SELL"].balance)
                        counter_total_bal_after = fmt_qty(parts["SELL"].total)
                elif e.tr_part.name == "SELL":
                    # Other side is BUY (amount received from this SELL)
                    if tr.buy:
                        counter_asset = tr.buy.asset
                        counter_change_qty = fmt_qty(tr.buy.quantity)
                    if "BUY" in parts:
                        counter_wallet_bal_after = fmt_qty(parts["BUY"].balance)
                        counter_total_bal_after = fmt_qty(parts["BUY"].total)
                # FEE has no counter

            data_rows.append(
                row
                + sell_cols
                + income_cols
                + [
                    counter_asset,
                    counter_change_qty,
                    counter_wallet_bal_after,
                    counter_total_bal_after,
                    fmt_tx(tr),
                ]
            )

    if _should_output_csv(force_csv=args.csv, force_table=args.table):
        writer = csv.writer(sys.stdout, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(data_rows)
    else:
        _print_ascii_table(headers, data_rows)


if __name__ == "__main__":
    main()
