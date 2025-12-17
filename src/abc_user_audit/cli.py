import argparse
from dataclasses import dataclass
from typing import List, Optional

import great_expectations as ge
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
    df = pd.read_csv(csv_path, dtype=str).applymap(
        lambda x: x.strip() if isinstance(x, str) else x
    )
    df["birth_date_parsed"] = pd.to_datetime(
        df["birth_date"], errors="coerce", infer_datetime_format=True
    )
    df["created_at_parsed"] = pd.to_datetime(
        df["created_at"], errors="coerce", infer_datetime_format=True
    )
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


def run_expectations(df: pd.DataFrame) -> pd.DataFrame:
    """Run a handful of Great Expectations checks and return a compact summary."""
    gx_df = ge.from_pandas(df)
    results = [
        gx_df.expect_column_values_to_not_be_null("id"),
        gx_df.expect_column_values_to_not_be_null("first_name"),
        gx_df.expect_column_values_to_not_be_null("last_name"),
        gx_df.expect_column_values_to_not_be_null("email"),
        gx_df.expect_column_values_to_not_be_null("phone"),
        gx_df.expect_column_values_to_not_be_null("status"),
        gx_df.expect_column_values_to_be_in_set("status", list(ALLOWED_STATUSES)),
        gx_df.expect_column_values_to_match_regex("phone", r"^\\d{10}$"),
        gx_df.expect_column_values_to_match_regex("birth_date", r"^\\d{1,2}/\\d{1,2}/\\d{4}$"),
        gx_df.expect_column_values_to_match_regex("created_at", r"^\\d{1,2}/\\d{1,2}/\\d{4}( .*)?$"),
    ]
    return pd.DataFrame(
        {
            "expectation": [
                res["expectation_config"]["expectation_type"] for res in results
            ],
            "success": [res["success"] for res in results],
            "unexpected_percent": [
                res["result"].get("unexpected_percent", 0) for res in results
            ],
        }
    )


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
    expectations = run_expectations(df)

    print("\nGreat Expectations summary:")
    print(expectations.to_string(index=False))

    print(f"\nAnomalies found: {len(anomalies)}")
    if not anomalies.empty:
        print(anomalies.head(20).to_string(index=False))
        if args.anomalies_out:
            anomalies.to_csv(args.anomalies_out, index=False)
            print(f"\nFull anomalies written to {args.anomalies_out}")


if __name__ == "__main__":
    main()
