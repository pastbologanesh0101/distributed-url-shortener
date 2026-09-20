# Contributing

## Running the tests

```bash
python -m unittest discover -s tests -v
```

All 20+ tests should pass before you open a PR. If you add or change
behavior in `dus/`, add or update a test in the matching `tests/test_*.py`
file rather than relying on the demo script alone.

You can also run the end-to-end smoke test:

```bash
python demo.py
```

## Code style

This project is pure standard-library Python with no external
dependencies (no linter/formatter config is checked in, so please match
the existing style by eye):

- `from __future__ import annotations` at the top of modules in `dus/`,
  with `typing` (`Dict`, `List`, `Optional`, ...) used for signatures.
- Every module and public class/method has a short docstring explaining
  *why*, not just *what* — see `dus/ring.py` and `dus/router.py` for the
  expected level of detail (the docstrings are part of the project's
  documentation, not boilerplate).
- Section header comments (`# ---- section name ----`) group related
  methods within a class, e.g. "health", "cluster membership", "key
  routing" in `dus/node.py` / `dus/router.py`.
- Raise a specific, named exception (`NodeDownError`,
  `NoAvailableReplicaError`) rather than a bare `Exception`, and validate
  constructor/parameter invariants eagerly (e.g. `if replicas < 1: raise
  ValueError(...)`).
- Tests are plain `unittest.TestCase` methods with descriptive
  `test_*` names that read like a sentence describing the guarantee
  being checked, and a docstring when the "why" isn't obvious from the
  name alone.

## Submitting changes

1. Fork/branch, make a focused change (one concern per commit/PR).
2. Add or update tests for any behavior change.
3. Run `python -m unittest discover -s tests -v` and `python demo.py`
   locally and confirm both succeed.
4. Open a PR describing *why* the change is needed, not just what
   changed — this project's docstrings and README follow that convention
   throughout, and PRs should too.

CI (`.github/workflows/tests.yml`) runs the same test suite and demo
script automatically on every push and pull request.
