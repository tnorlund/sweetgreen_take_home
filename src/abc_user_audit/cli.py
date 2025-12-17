import argparse
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
from dateutil.relativedelta import relativedelta
from email_validator import EmailNotValidError, validate_email
from phonenumbers import NumberParseException, is_valid_number, parse as parse_phone


ALLOWED_STATUSES = {"active", "cancelled"}


@dataclass
class Anomaly:
    row_id: str
    column: Optional[str]
    value: Optional[str]
    issue: str


def load_users(csv_path: str) -> pd.DataFrame:
    """Load the legacy users export and parse dates where possible."""
    df = pd.read_csv(csv_path, dtype=str)
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    df["birth_date_parsed"] = pd.to_datetime(df["birth_date"], errors="coerce")
    df["created_at_parsed"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df


def validate_row(row: pd.Series) -> List[Anomaly]:
    anomalies: List[Anomaly] = []
    row_id = row.get("id", "unknown")

    def add(column: Optional[str], issue: str, value: Optional[str] = None) -> None:
        anomalies.append(
            Anomaly(row_id=str(row_id), column=column, value=value, issue=issue)
        )

    # Required fields
    for col in ["first_name", "last_name", "email", "phone", "status", "birth_date", "created_at"]:
        if pd.isna(row.get(col)) or str(row.get(col)).strip() == "":
            add(col, "missing value")

    # Email validity
    email_value = row.get("email")
    if pd.notna(email_value):
        try:
            validate_email(str(email_value))
        except EmailNotValidError as exc:
            add("email", f"invalid email: {exc}", str(email_value))

    # Phone validity (10-digit US numbers)
    phone_value = row.get("phone")
    if pd.notna(phone_value):
        try:
            phone_obj = parse_phone(str(phone_value), region="US")
            if not is_valid_number(phone_obj):
                add("phone", "invalid phone number", str(phone_value))
        except NumberParseException as exc:
            add("phone", f"invalid phone number: {exc}", str(phone_value))

    # Status validity
    status_value = str(row.get("status", "")).lower()
    if status_value and status_value not in ALLOWED_STATUSES:
        add("status", "status not in allowed set", status_value)

    # Date parsing and age checks
    birth = row.get("birth_date_parsed")
    created = row.get("created_at_parsed")
    if pd.isna(birth):
        add("birth_date", "unparseable birth_date", str(row.get("birth_date")))
    if pd.isna(created):
        add("created_at", "unparseable created_at", str(row.get("created_at")))

    if pd.notna(birth) and pd.notna(created):
        if created < birth:
            add("created_at", "created_at precedes birth_date", str(row.get("created_at")))
        else:
            age_years = relativedelta(created, birth).years
            if age_years < 18:
                add("birth_date", "user is under 18 at account creation", str(row.get("birth_date")))

    return anomalies


def run_anomaly_checks(df: pd.DataFrame) -> pd.DataFrame:
    all_anomalies: List[Anomaly] = []
    for _, row in df.iterrows():
        all_anomalies.extend(validate_row(row))
    return pd.DataFrame([a.__dict__ for a in all_anomalies])

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run data quality checks against the legacy users CSV."
    )
    parser.add_argument(
        "--csv",
        default="challenge_dataset (1).csv",
        help="Path to the legacy users CSV export.",
    )
    parser.add_argument(
        "--anomalies-out",
        default=None,
        help="Optional path to write anomalies as CSV.",
    )
    args = parser.parse_args()

    df = load_users(args.csv)
    anomalies = run_anomaly_checks(df)

    print(f"\nAnomalies found: {len(anomalies)}")
    if anomalies.empty:
        print("No anomalies detected.")
    else:
        # Summary of counts by issue type
        counts = (
            anomalies.groupby("issue")
            .size()
            .sort_values(ascending=False)
            .reset_index(name="count")
        )
        print("\nCounts by issue:")
        print(counts.to_string(index=False))

        print("\nSample anomalies (first 20):")
        print(anomalies.head(20).to_string(index=False))

        if args.anomalies_out:
            anomalies.to_csv(args.anomalies_out, index=False)
            print(f"\nFull anomalies written to {args.anomalies_out}")


if __name__ == "__main__":
    main()
