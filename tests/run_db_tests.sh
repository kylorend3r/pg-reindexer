#!/bin/bash
# Helper script to run pg-reindexer test suites
# 
# Usage:
#   ./tests/run_db_tests.sh                    # Run all tests
#   ./tests/run_db_tests.sh --cli              # Run only CLI tests
#   ./tests/run_db_tests.sh --db               # Run only DB tests
#   ./tests/run_db_tests.sh --logging          # Run only logging/dry-run tests
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

# Parse command line arguments
RUN_CLI=false
RUN_DB=false
RUN_LOGGING=false
RUN_CHAOS=false
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
            --all)
                RUN_ALL=true
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --cli, --cli-validation    Run CLI validation tests only"
                echo "  --db, --db-validation      Run database integration tests only"
                echo "  --logging, --dry-run      Run logging and dry-run tests only"
                echo "  --chaos, --chaos-tests    Run chaos tests only (requires database)"
                echo "  --all                     Run all tests (default)"
                echo "  --help, -h                Show this help message"
                echo ""
                echo "Environment variables:"
                echo "  PG_HOST       PostgreSQL host (default: localhost)"
                echo "  PG_PORT       PostgreSQL port (default: 5432)"
                echo "  PG_DATABASE   Database name (default: benchmark)"
                echo "  PG_USER       Database user (default: superuser)"
                echo "  PG_PASSWORD   Database password (default: test123)"
                echo ""
                echo "Examples:"
                echo "  $0 --cli                    # Run CLI tests only"
                echo "  $0 --db                    # Run DB tests only"
                echo "  $0 --chaos                 # Run chaos tests only"
                echo "  $0 --all                   # Run all tests"
                echo "  PG_PASSWORD=mypass $0 --db # Run DB tests with custom password"
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
if [ "$RUN_CLI" = true ] || [ "$RUN_DB" = true ] || [ "$RUN_LOGGING" = true ] || [ "$RUN_CHAOS" = true ]; then
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
    echo "  ✓ Chaos Tests"
else
    [ "$RUN_CLI" = true ] && echo "  ✓ CLI Validation Tests"
    [ "$RUN_DB" = true ] && echo "  ✓ Database Integration Tests"
    [ "$RUN_LOGGING" = true ] && echo "  ✓ Logging and Dry-Run Tests"
    [ "$RUN_CHAOS" = true ] && echo "  ✓ Chaos Tests"
fi
echo ""

# Check if DB tests are needed
NEED_DB=false
if [ "$RUN_ALL" = true ] || [ "$RUN_DB" = true ] || [ "$RUN_CHAOS" = true ]; then
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

