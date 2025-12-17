# sweetgreen_take_home

Python package to analyze the legacy `users_old` export (`challenge_dataset (1).csv`) against business rules and the stricter `users_new` schema. The package will surface row- and field-level anomalies to support the data migration.

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# Run checks
abc-audit --csv "challenge_dataset (1).csv" --anomalies-out anomalies.csv
```

## Development

- Run tests: `pytest`
- Format: `black .` and `isort .`
- Type check: `mypy src`

## Notes

- Dependencies are managed via `pyproject.toml` (build backend: hatchling).
- The source package lives under `src/abc_user_audit`. Add modules there for validators, anomaly reporting, and CLI entrypoints.
- CSV/inputs for the exercise are in the repository root.
