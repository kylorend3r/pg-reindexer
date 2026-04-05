# Contributing to pg-reindexer

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (stable, edition 2021)
- PostgreSQL instance for integration testing (optional but recommended)

## Development Setup

```bash
git clone https://github.com/kylorend3r/pg-reindexer.git
cd pg-reindexer
cargo build
```

## Building and Testing

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Run with output
cargo test -- --nocapture
```

## Submitting Changes

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, ensuring all tests pass.
3. Remove any `.log` files from the project root before committing.
4. Open a pull request against `main` with a clear description of the change.

### Commit Message Format

Use [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | When to use |
|--------|-------------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `refactor:` | Code restructuring without behavior change |
| `perf:` | Performance improvement |
| `test:` | Adding or updating tests |
| `docs:` | Documentation only |
| `chore:` | Tooling, CI, or dependency updates |

### PR Checklist

- [ ] `cargo test` passes
- [ ] No `.log` files committed
- [ ] Commit messages follow the conventional format above
- [ ] New CLI flags are documented in `README.md`

## Reporting Bugs

Open an issue on [GitHub Issues](https://github.com/kylorend3r/pg-reindexer/issues). Include:

- pg-reindexer version (`--version`)
- PostgreSQL version
- Command used (redact credentials)
- Observed vs. expected behavior
