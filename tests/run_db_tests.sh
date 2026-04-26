#!/bin/bash
# Helper script to run pg-reindexer test suites
# 
# Usage:
#   ./tests/run_db_tests.sh                    # Run all tests
#   ./tests/run_db_tests.sh --cli              # Run only CLI tests
#   ./tests/run_db_tests.sh --db               # Run only DB tests
#   ./tests/run_db_tests.sh --logging          # Run only logging/dry-run tests
#   ./tests/run_db_tests.sh --signal           # Run only signal handling tests
#   ./tests/run_db_tests.sh --all              # Run all tests (default)
#   PG_PASSWORD=mypass ./tests/run_db_tests.sh --db

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default test configuration
export PG_HOST=${PG_HOST:-localhost}
export PG_PORT=${PG_PORT:-5432}
export PG_DATABASE=${PG_DATABASE:-benchmark}
export PG_USER=${PG_USER:-superuser}
export PG_PASSWORD=${PG_PASSWORD:-test123}

# SSL configuration (optional — only required for --ssl-db)
# PG_SSL_CA_CERT     path to server CA cert (.pem) for verify-ca / verify-full
# PG_SSL_CLIENT_CERT path to client cert (.pem) for mutual TLS tests
# PG_SSL_CLIENT_KEY  path to client private key (.pem) for mutual TLS tests

# Parse command line arguments
RUN_CLI=false
RUN_DB=false
RUN_LOGGING=false
RUN_CHAOS=false
RUN_SIGNAL=false
RUN_PLAN=false
RUN_SSL=false
RUN_SSL_DB=false
RUN_ALL=true

if [ $# -gt 0 ]; then
    RUN_ALL=false
    for arg in "$@"; do
        case $arg in
            --cli|--cli-validation)
                RUN_CLI=true
                ;;
            --db|--db-validation|--database)
                RUN_DB=true
                ;;
            --logging|--dry-run)
                RUN_LOGGING=true
                ;;
            --chaos|--chaos-tests)
                RUN_CHAOS=true
                ;;
            --signal|--signal-handling)
                RUN_SIGNAL=true
                ;;
            --plan|--plan-subcommand)
                RUN_PLAN=true
                ;;
            --ssl|--ssl-cli)
                RUN_SSL=true
                ;;
            --ssl-db|--ssl-database)
                RUN_SSL_DB=true
                ;;
            --all)
                RUN_ALL=true
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --cli, --cli-validation    Run CLI validation tests only"
                echo "  --db, --db-validation      Run database integration tests only"
                echo "  --logging, --dry-run       Run logging and dry-run tests only"
                echo "  --chaos, --chaos-tests     Run chaos tests only (requires database)"
                echo "  --signal, --signal-handling Run signal handling tests only"
                echo "  --plan, --plan-subcommand  Run plan subcommand tests only"
                echo "  --ssl                      Run SSL CLI tests (no DB needed)"
                echo "  --ssl-db                   Run SSL DB integration tests (requires SSL-enabled PostgreSQL)"
                echo "  --all                      Run all tests except --ssl-db (default)"
                echo "  --help, -h                 Show this help message"
                echo ""
                echo "Environment variables:"
                echo "  PG_HOST            PostgreSQL host (default: localhost)"
                echo "  PG_PORT            PostgreSQL port (default: 5432)"
                echo "  PG_DATABASE        Database name (default: benchmark)"
                echo "  PG_USER            Database user (default: superuser)"
                echo "  PG_PASSWORD        Database password (default: test123)"
                echo "  PG_SSL_CA_CERT     Path to server CA cert (.pem) — optional, for verify-ca/verify-full"
                echo "  PG_SSL_CLIENT_CERT Path to client cert (.pem)    — optional, for mutual TLS tests"
                echo "  PG_SSL_CLIENT_KEY  Path to client key (.pem)     — optional, for mutual TLS tests"
                echo ""
                echo "Examples:"
                echo "  $0 --cli                         # Run CLI tests only"
                echo "  $0 --db                          # Run DB tests only"
                echo "  $0 --ssl                         # Run SSL CLI tests"
                echo "  $0 --ssl-db                      # Run SSL DB tests (sslmode=require, no certs needed)"
                echo "  PG_PASSWORD=mypass $0 --ssl-db   # SSL DB tests with custom password"
                echo "  $0 --all                         # Run all tests"
                exit 0
                ;;
            *)
                echo -e "${RED}Unknown option: $arg${NC}"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
fi

# If specific tests selected, don't run all
if [ "$RUN_CLI" = true ] || [ "$RUN_DB" = true ] || [ "$RUN_LOGGING" = true ] || [ "$RUN_CHAOS" = true ] || [ "$RUN_SIGNAL" = true ] || [ "$RUN_PLAN" = true ] || [ "$RUN_SSL" = true ] || [ "$RUN_SSL_DB" = true ]; then
    RUN_ALL=false
fi

echo -e "${GREEN}PostgreSQL Reindexer - Test Suite Runner${NC}"
echo ""

# Show which tests will run
echo -e "${BLUE}Test Suites to Run:${NC}"
if [ "$RUN_ALL" = true ]; then
    echo "  ✓ CLI Validation Tests"
    echo "  ✓ Database Integration Tests"
    echo "  ✓ Logging and Dry-Run Tests"
    echo "  ✓ Signal Handling Tests"
    echo "  ✓ Chaos Tests"
    echo "  ✓ Plan Subcommand Tests"
    echo "  ✓ SSL CLI Tests"
    echo "  (SSL DB Tests skipped — use --ssl-db to run them)"
else
    [ "$RUN_CLI" = true ] && echo "  ✓ CLI Validation Tests"
    [ "$RUN_DB" = true ] && echo "  ✓ Database Integration Tests"
    [ "$RUN_LOGGING" = true ] && echo "  ✓ Logging and Dry-Run Tests"
    [ "$RUN_SIGNAL" = true ] && echo "  ✓ Signal Handling Tests"
    [ "$RUN_CHAOS" = true ] && echo "  ✓ Chaos Tests"
    [ "$RUN_PLAN" = true ] && echo "  ✓ Plan Subcommand Tests"
    [ "$RUN_SSL" = true ] && echo "  ✓ SSL CLI Tests"
    [ "$RUN_SSL_DB" = true ] && echo "  ✓ SSL DB Integration Tests"
fi
echo ""

# Check if DB tests are needed
NEED_DB=false
if [ "$RUN_ALL" = true ] || [ "$RUN_DB" = true ] || [ "$RUN_CHAOS" = true ] || [ "$RUN_SSL_DB" = true ]; then
    NEED_DB=true
fi

if [ "$NEED_DB" = true ]; then
    echo "Database Configuration:"
    echo "  Host:     $PG_HOST"
    echo "  Port:     $PG_PORT"
    echo "  Database: $PG_DATABASE"
    echo "  User:     $PG_USER"
    echo ""

    # Check if PostgreSQL is accessible
    echo -e "${YELLOW}Checking PostgreSQL connection...${NC}"
    if command -v pg_isready &> /dev/null; then
        if pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ PostgreSQL is ready${NC}"
        else
            echo -e "${RED}✗ PostgreSQL is not accessible at $PG_HOST:$PG_PORT${NC}"
            echo "  Please ensure PostgreSQL is running and accessible"
            exit 1
        fi
    else
        echo -e "${YELLOW}⚠ pg_isready not found, skipping connection check${NC}"
    fi
    echo ""
fi

# Track overall success
OVERALL_SUCCESS=true
FAILED_SUITES=()

# Run CLI Validation Tests
if [ "$RUN_ALL" = true ] || [ "$RUN_CLI" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running CLI Validation Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if cargo test --test cli_validation -- --nocapture; then
        echo ""
        echo -e "${GREEN}✓ CLI Validation Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ CLI Validation Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("CLI Validation")
    fi
    echo ""
fi

# Run Database Integration Tests
if [ "$RUN_ALL" = true ] || [ "$RUN_DB" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running Database Integration Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if cargo test --test db_validation -- --ignored --nocapture; then
        echo ""
        echo -e "${GREEN}✓ Database Integration Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ Database Integration Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("Database Integration")
    fi
    echo ""
fi

# Run Logging and Dry-Run Tests
if [ "$RUN_ALL" = true ] || [ "$RUN_LOGGING" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running Logging and Dry-Run Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if cargo test --test logging_and_dry_run -- --nocapture; then
        echo ""
        echo -e "${GREEN}✓ Logging and Dry-Run Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ Logging and Dry-Run Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("Logging and Dry-Run")
    fi
    echo ""
fi

# Run Signal Handling Tests
if [ "$RUN_ALL" = true ] || [ "$RUN_SIGNAL" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running Signal Handling Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if cargo test --test signal_handling -- --nocapture; then
        echo ""
        echo -e "${GREEN}✓ Signal Handling Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ Signal Handling Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("Signal Handling")
    fi
    echo ""
fi

# Run Chaos Tests
if [ "$RUN_ALL" = true ] || [ "$RUN_CHAOS" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running Chaos Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if cargo test --test chaos_tests -- --ignored --nocapture; then
        echo ""
        echo -e "${GREEN}✓ Chaos Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ Chaos Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("Chaos Tests")
    fi
    echo ""
fi

# Run Plan Subcommand Tests
if [ "$RUN_ALL" = true ] || [ "$RUN_PLAN" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running Plan Subcommand Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    if cargo test --test plan_subcommand -- --nocapture; then
        echo ""
        echo -e "${GREEN}✓ Plan Subcommand Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ Plan Subcommand Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("Plan Subcommand")
    fi
    echo ""
fi

# Run SSL CLI Tests (no DB required)
if [ "$RUN_ALL" = true ] || [ "$RUN_SSL" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running SSL CLI Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    if cargo test --test ssl_tests -- --nocapture; then
        echo ""
        echo -e "${GREEN}✓ SSL CLI Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ SSL CLI Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("SSL CLI")
    fi
    echo ""
fi

# Run SSL DB Integration Tests (requires SSL-enabled PostgreSQL)
if [ "$RUN_SSL_DB" = true ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Running SSL DB Integration Tests...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "SSL Configuration:"
    echo "  sslmode:         require (self-signed) / verify-ca / verify-full"
    [ -n "$PG_SSL_CA_CERT" ]     && echo "  PG_SSL_CA_CERT:      $PG_SSL_CA_CERT"
    [ -n "$PG_SSL_CLIENT_CERT" ] && echo "  PG_SSL_CLIENT_CERT:  $PG_SSL_CLIENT_CERT"
    [ -n "$PG_SSL_CLIENT_KEY" ]  && echo "  PG_SSL_CLIENT_KEY:   $PG_SSL_CLIENT_KEY"
    echo ""

    # Run tests that always work (sslmode=require, no cert files)
    SSL_TESTS="test_sslmode_require_connects test_sslmode_require_pg_stat_ssl_is_true test_sslmode_disable_is_rejected_when_server_requires_ssl"

    # Add verify-ca/verify-full tests only when a CA cert is provided
    if [ -n "$PG_SSL_CA_CERT" ]; then
        SSL_TESTS="$SSL_TESTS test_sslmode_verify_ca_connects test_sslmode_verify_full_connects"
    else
        echo -e "${YELLOW}  ⚠ PG_SSL_CA_CERT not set — skipping verify-ca and verify-full tests${NC}"
    fi
    echo ""

    if cargo test --test ssl_tests $SSL_TESTS -- --include-ignored --nocapture; then
        echo ""
        echo -e "${GREEN}✓ SSL DB Integration Tests passed!${NC}"
    else
        echo ""
        echo -e "${RED}✗ SSL DB Integration Tests failed${NC}"
        OVERALL_SUCCESS=false
        FAILED_SUITES+=("SSL DB Integration")
    fi
    echo ""
fi

# Final summary
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
if [ "$OVERALL_SUCCESS" = true ]; then
    echo -e "${GREEN}✓ All test suites passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some test suites failed:${NC}"
    for suite in "${FAILED_SUITES[@]}"; do
        echo -e "${RED}  - $suite${NC}"
    done
    exit 1
fi

