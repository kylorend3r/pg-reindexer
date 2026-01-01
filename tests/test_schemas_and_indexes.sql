-- Test SQL script for PostgreSQL Reindexer
-- This script creates multiple schemas with tables containing various types of indexes
-- Run this script to set up test data for the reindexer tool

-- ============================================================================
-- Schema 1: ecommerce
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Table: users (with primary key and unique constraints)
CREATE TABLE IF NOT EXISTS ecommerce.users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(100) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active',
    phone_number VARCHAR(20)
);

-- Unique constraint index (constraint type)
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique ON ecommerce.users(email);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_unique ON ecommerce.users(username);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_users_created_at ON ecommerce.users(created_at);
CREATE INDEX IF NOT EXISTS idx_users_status ON ecommerce.users(status);
CREATE INDEX IF NOT EXISTS idx_users_last_name ON ecommerce.users(last_name);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_users_name_composite ON ecommerce.users(first_name, last_name);

-- Partial index (only for active users)
CREATE INDEX IF NOT EXISTS idx_users_active_created ON ecommerce.users(created_at) WHERE status = 'active';

-- Expression index
CREATE INDEX IF NOT EXISTS idx_users_email_lower ON ecommerce.users(LOWER(email));

-- Table: products (with various indexes)
CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    category_id INTEGER,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

-- Unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS idx_products_sku_unique ON ecommerce.products(sku);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_products_category ON ecommerce.products(category_id);
CREATE INDEX IF NOT EXISTS idx_products_price ON ecommerce.products(price);
CREATE INDEX IF NOT EXISTS idx_products_created_at ON ecommerce.products(created_at);
CREATE INDEX IF NOT EXISTS idx_products_is_active ON ecommerce.products(is_active);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_products_category_active ON ecommerce.products(category_id, is_active);

-- Partial index (only active products)
CREATE INDEX IF NOT EXISTS idx_products_active_price ON ecommerce.products(price) WHERE is_active = true;

-- Table: orders (with foreign keys and indexes)
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES ecommerce.users(user_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    shipping_address TEXT,
    payment_method VARCHAR(50)
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON ecommerce.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON ecommerce.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON ecommerce.orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_total_amount ON ecommerce.orders(total_amount);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_orders_user_status ON ecommerce.orders(user_id, status);

-- Partial index (only completed orders)
CREATE INDEX IF NOT EXISTS idx_orders_completed_date ON ecommerce.orders(order_date) WHERE status = 'completed';

-- ============================================================================
-- Schema 2: analytics
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS analytics;

-- Table: page_views (with timestamp indexes)
CREATE TABLE IF NOT EXISTS analytics.page_views (
    view_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER,
    page_url VARCHAR(500) NOT NULL,
    view_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id VARCHAR(100),
    referrer VARCHAR(500),
    device_type VARCHAR(50),
    user_agent TEXT
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_page_views_user_id ON analytics.page_views(user_id);
CREATE INDEX IF NOT EXISTS idx_page_views_timestamp ON analytics.page_views(view_timestamp);
CREATE INDEX IF NOT EXISTS idx_page_views_session_id ON analytics.page_views(session_id);
CREATE INDEX IF NOT EXISTS idx_page_views_device_type ON analytics.page_views(device_type);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_page_views_user_timestamp ON analytics.page_views(user_id, view_timestamp);

-- Partial index (recent views only)
CREATE INDEX IF NOT EXISTS idx_page_views_recent ON analytics.page_views(view_timestamp) 
    WHERE view_timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days';

-- Expression index (extract date from timestamp)
CREATE INDEX IF NOT EXISTS idx_page_views_date ON analytics.page_views(DATE(view_timestamp));

-- Table: events (with various event types)
CREATE TABLE IF NOT EXISTS analytics.events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id INTEGER,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    properties JSONB,
    session_id VARCHAR(100)
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_events_type ON analytics.events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON analytics.events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON analytics.events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON analytics.events(session_id);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_events_type_timestamp ON analytics.events(event_type, event_timestamp);

-- GIN index for JSONB (for JSON queries)
CREATE INDEX IF NOT EXISTS idx_events_properties_gin ON analytics.events USING GIN(properties);

-- Partial index (specific event types)
CREATE INDEX IF NOT EXISTS idx_events_purchase_timestamp ON analytics.events(event_timestamp) 
    WHERE event_type = 'purchase';

-- Table: user_sessions
CREATE TABLE IF NOT EXISTS analytics.user_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id INTEGER,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds INTEGER,
    page_count INTEGER DEFAULT 0
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON analytics.user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_started_at ON analytics.user_sessions(started_at);
CREATE INDEX IF NOT EXISTS idx_sessions_duration ON analytics.user_sessions(duration_seconds);

-- Partial index (active sessions)
CREATE INDEX IF NOT EXISTS idx_sessions_active ON analytics.user_sessions(started_at) 
    WHERE ended_at IS NULL;

-- ============================================================================
-- Schema 3: inventory
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS inventory;

-- Table: warehouses
CREATE TABLE IF NOT EXISTS inventory.warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    code VARCHAR(20) NOT NULL,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    capacity INTEGER,
    is_active BOOLEAN DEFAULT true
);

-- Unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS idx_warehouses_code_unique ON inventory.warehouses(code);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_warehouses_location ON inventory.warehouses(location);
CREATE INDEX IF NOT EXISTS idx_warehouses_is_active ON inventory.warehouses(is_active);

-- Table: stock_items
CREATE TABLE IF NOT EXISTS inventory.stock_items (
    stock_id SERIAL PRIMARY KEY,
    warehouse_id INTEGER NOT NULL REFERENCES inventory.warehouses(warehouse_id),
    product_id INTEGER,
    quantity INTEGER NOT NULL DEFAULT 0,
    reserved_quantity INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    location_code VARCHAR(50)
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_stock_warehouse_id ON inventory.stock_items(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_stock_product_id ON inventory.stock_items(product_id);
CREATE INDEX IF NOT EXISTS idx_stock_quantity ON inventory.stock_items(quantity);
CREATE INDEX IF NOT EXISTS idx_stock_last_updated ON inventory.stock_items(last_updated);
CREATE INDEX IF NOT EXISTS idx_stock_location_code ON inventory.stock_items(location_code);

-- Composite index
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_warehouse_product_unique ON inventory.stock_items(warehouse_id, product_id);

-- Partial index (low stock items)
CREATE INDEX IF NOT EXISTS idx_stock_low_quantity ON inventory.stock_items(warehouse_id, product_id) 
    WHERE quantity < 10;

-- Table: movements
CREATE TABLE IF NOT EXISTS inventory.movements (
    movement_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES inventory.stock_items(stock_id),
    movement_type VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL,
    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_number VARCHAR(100),
    notes TEXT
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_movements_stock_id ON inventory.movements(stock_id);
CREATE INDEX IF NOT EXISTS idx_movements_type ON inventory.movements(movement_type);
CREATE INDEX IF NOT EXISTS idx_movements_date ON inventory.movements(movement_date);
CREATE INDEX IF NOT EXISTS idx_movements_reference ON inventory.movements(reference_number);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_movements_stock_date ON inventory.movements(stock_id, movement_date);

-- Partial index (recent movements)
CREATE INDEX IF NOT EXISTS idx_movements_recent ON inventory.movements(movement_date) 
    WHERE movement_date > CURRENT_TIMESTAMP - INTERVAL '90 days';

-- ============================================================================
-- Schema 4: reporting
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS reporting;

-- Table: daily_sales
CREATE TABLE IF NOT EXISTS reporting.daily_sales (
    report_id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    product_id INTEGER,
    total_sales DECIMAL(12, 2) NOT NULL,
    quantity_sold INTEGER NOT NULL,
    average_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_daily_sales_date ON reporting.daily_sales(report_date);
CREATE INDEX IF NOT EXISTS idx_daily_sales_product_id ON reporting.daily_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_daily_sales_total ON reporting.daily_sales(total_sales);

-- Composite unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_sales_date_product_unique ON reporting.daily_sales(report_date, product_id);

-- Partial index (recent reports)
CREATE INDEX IF NOT EXISTS idx_daily_sales_recent ON reporting.daily_sales(report_date) 
    WHERE report_date > CURRENT_DATE - INTERVAL '365 days';

-- Table: monthly_summaries
CREATE TABLE IF NOT EXISTS reporting.monthly_summaries (
    summary_id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    total_orders INTEGER NOT NULL,
    unique_customers INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_monthly_year ON reporting.monthly_summaries(year);
CREATE INDEX IF NOT EXISTS idx_monthly_month ON reporting.monthly_summaries(month);
CREATE INDEX IF NOT EXISTS idx_monthly_revenue ON reporting.monthly_summaries(total_revenue);

-- Composite unique index
CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_year_month_unique ON reporting.monthly_summaries(year, month);

-- Composite index for sorting
CREATE INDEX IF NOT EXISTS idx_monthly_year_month ON reporting.monthly_summaries(year DESC, month DESC);

-- ============================================================================
-- Schema 5: audit
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS audit;

-- Table: audit_log
CREATE TABLE IF NOT EXISTS audit.audit_log (
    log_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_id INTEGER NOT NULL,
    action VARCHAR(20) NOT NULL,
    changed_by INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_values JSONB,
    new_values JSONB
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_audit_table_name ON audit.audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_record_id ON audit.audit_log(record_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit.audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_changed_by ON audit.audit_log(changed_by);
CREATE INDEX IF NOT EXISTS idx_audit_changed_at ON audit.audit_log(changed_at);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_audit_table_record ON audit.audit_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_audit_table_action ON audit.audit_log(table_name, action);

-- GIN indexes for JSONB
CREATE INDEX IF NOT EXISTS idx_audit_old_values_gin ON audit.audit_log USING GIN(old_values);
CREATE INDEX IF NOT EXISTS idx_audit_new_values_gin ON audit.audit_log USING GIN(new_values);

-- Partial index (recent audit logs)
CREATE INDEX IF NOT EXISTS idx_audit_recent ON audit.audit_log(changed_at) 
    WHERE changed_at > CURRENT_TIMESTAMP - INTERVAL '180 days';

-- Table: user_activity
CREATE TABLE IF NOT EXISTS audit.user_activity (
    activity_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    activity_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip_address INET,
    user_agent TEXT,
    details JSONB
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_activity_user_id ON audit.user_activity(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_type ON audit.user_activity(activity_type);
CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON audit.user_activity(activity_timestamp);
CREATE INDEX IF NOT EXISTS idx_activity_ip ON audit.user_activity(ip_address);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_activity_user_timestamp ON audit.user_activity(user_id, activity_timestamp);

-- GIN index for JSONB
CREATE INDEX IF NOT EXISTS idx_activity_details_gin ON audit.user_activity USING GIN(details);

-- Partial index (recent activity)
CREATE INDEX IF NOT EXISTS idx_activity_recent ON audit.user_activity(activity_timestamp) 
    WHERE activity_timestamp > CURRENT_TIMESTAMP - INTERVAL '90 days';

-- ============================================================================
-- Public Schema: Core application tables
-- ============================================================================

-- Table: categories (with hierarchical structure)
CREATE TABLE IF NOT EXISTS public.categories (
    category_id SERIAL PRIMARY KEY,
    parent_category_id INTEGER REFERENCES public.categories(category_id),
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    description TEXT,
    display_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_slug_unique ON public.categories(slug);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_categories_parent_id ON public.categories(parent_category_id);
CREATE INDEX IF NOT EXISTS idx_categories_name ON public.categories(name);
CREATE INDEX IF NOT EXISTS idx_categories_is_active ON public.categories(is_active);
CREATE INDEX IF NOT EXISTS idx_categories_display_order ON public.categories(display_order);
CREATE INDEX IF NOT EXISTS idx_categories_created_at ON public.categories(created_at);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_categories_parent_active ON public.categories(parent_category_id, is_active);

-- Partial index (active categories only)
CREATE INDEX IF NOT EXISTS idx_categories_active_display ON public.categories(display_order) WHERE is_active = true;

-- Table: product_reviews (with ratings and moderation)
CREATE TABLE IF NOT EXISTS public.product_reviews (
    review_id BIGSERIAL PRIMARY KEY,
    product_id INTEGER,
    user_id INTEGER,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(255),
    review_text TEXT,
    is_verified_purchase BOOLEAN DEFAULT false,
    is_moderated BOOLEAN DEFAULT false,
    helpful_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON public.product_reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON public.product_reviews(user_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON public.product_reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_is_moderated ON public.product_reviews(is_moderated);
CREATE INDEX IF NOT EXISTS idx_reviews_created_at ON public.product_reviews(created_at);
CREATE INDEX IF NOT EXISTS idx_reviews_helpful_count ON public.product_reviews(helpful_count);

-- Composite index
CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_product_user_unique ON public.product_reviews(product_id, user_id);
CREATE INDEX IF NOT EXISTS idx_reviews_product_rating ON public.product_reviews(product_id, rating);
CREATE INDEX IF NOT EXISTS idx_reviews_product_moderated ON public.product_reviews(product_id, is_moderated);

-- Partial index (moderated reviews only)
CREATE INDEX IF NOT EXISTS idx_reviews_moderated_rating ON public.product_reviews(rating, helpful_count) 
    WHERE is_moderated = true;

-- Expression index (average rating calculation helper)
CREATE INDEX IF NOT EXISTS idx_reviews_product_rating_avg ON public.product_reviews(product_id, rating);

-- Table: payment_transactions (with complex status tracking)
CREATE TABLE IF NOT EXISTS public.payment_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    order_id INTEGER,
    user_id INTEGER,
    payment_method VARCHAR(50) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) NOT NULL,
    processor_response_code VARCHAR(20),
    processor_response_text TEXT,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    refunded_amount DECIMAL(12, 2) DEFAULT 0,
    metadata JSONB
);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_transactions_order_id ON public.payment_transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON public.payment_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON public.payment_transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_payment_method ON public.payment_transactions(payment_method);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON public.payment_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_amount ON public.payment_transactions(amount);
CREATE INDEX IF NOT EXISTS idx_transactions_currency ON public.payment_transactions(currency);
CREATE INDEX IF NOT EXISTS idx_transactions_processor_code ON public.payment_transactions(processor_response_code);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_transactions_user_status ON public.payment_transactions(user_id, status);
CREATE INDEX IF NOT EXISTS idx_transactions_date_status ON public.payment_transactions(transaction_date, status);
CREATE INDEX IF NOT EXISTS idx_transactions_method_status ON public.payment_transactions(payment_method, status);

-- GIN index for JSONB
CREATE INDEX IF NOT EXISTS idx_transactions_metadata_gin ON public.payment_transactions USING GIN(metadata);

-- Partial indexes
CREATE INDEX IF NOT EXISTS idx_transactions_failed ON public.payment_transactions(transaction_date, processor_response_code) 
    WHERE status = 'failed';
CREATE INDEX IF NOT EXISTS idx_transactions_refunded ON public.payment_transactions(transaction_date, refunded_amount) 
    WHERE refunded_amount > 0;
CREATE INDEX IF NOT EXISTS idx_transactions_pending ON public.payment_transactions(transaction_date) 
    WHERE status = 'pending';

-- Table: system_configurations (key-value store with versioning)
CREATE TABLE IF NOT EXISTS public.system_configurations (
    config_id SERIAL PRIMARY KEY,
    config_key VARCHAR(255) NOT NULL,
    config_value TEXT,
    config_type VARCHAR(50) DEFAULT 'string',
    is_encrypted BOOLEAN DEFAULT false,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by INTEGER
);

-- Unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS idx_config_key_unique ON public.system_configurations(config_key);

-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_config_type ON public.system_configurations(config_type);
CREATE INDEX IF NOT EXISTS idx_config_is_encrypted ON public.system_configurations(is_encrypted);
CREATE INDEX IF NOT EXISTS idx_config_version ON public.system_configurations(version);
CREATE INDEX IF NOT EXISTS idx_config_updated_at ON public.system_configurations(updated_at);
CREATE INDEX IF NOT EXISTS idx_config_updated_by ON public.system_configurations(updated_by);

-- Composite index
CREATE INDEX IF NOT EXISTS idx_config_type_encrypted ON public.system_configurations(config_type, is_encrypted);

-- Expression index (for case-insensitive key lookups)
CREATE INDEX IF NOT EXISTS idx_config_key_lower ON public.system_configurations(LOWER(config_key));

-- ============================================================================
-- Public Schema: Partitioned Table - Transaction Log (Complex Partitioning)
-- ============================================================================

-- Create a range-partitioned table for transaction logs
-- This will be partitioned by month for the last 2 years
CREATE TABLE IF NOT EXISTS public.transaction_log (
    log_id BIGSERIAL,
    transaction_id BIGINT NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    entity_type VARCHAR(100) NOT NULL,
    entity_id BIGINT NOT NULL,
    action VARCHAR(50) NOT NULL,
    old_state JSONB,
    new_state JSONB,
    changed_by INTEGER,
    change_reason TEXT,
    ip_address INET,
    user_agent TEXT,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processing_time_ms INTEGER,
    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT,
    metadata JSONB,
    PRIMARY KEY (log_id, log_timestamp)
) PARTITION BY RANGE (log_timestamp);

-- Create partitions for the last 24 months (2 years)
-- We'll create partitions dynamically, but here's a template for current and future months
DO $$
DECLARE
    partition_date TIMESTAMP;
    partition_name TEXT;
    start_date TIMESTAMP;
    end_date TIMESTAMP;
    i INTEGER;
BEGIN
    -- Create partitions for the last 24 months
    FOR i IN 0..23 LOOP
        partition_date := DATE_TRUNC('month', CURRENT_TIMESTAMP) - (i::TEXT || ' months')::INTERVAL;
        start_date := partition_date;
        end_date := partition_date + INTERVAL '1 month';
        partition_name := 'transaction_log_' || TO_CHAR(partition_date, 'YYYY_MM');
        
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.transaction_log
            FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );
    END LOOP;
    
    -- Create a default partition for future dates
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS public.transaction_log_default PARTITION OF public.transaction_log
        DEFAULT'
    );
END $$;

-- Global indexes on the partitioned table (these will be created on all partitions)
-- Note: In PostgreSQL, indexes on partitioned tables are automatically created on all partitions

-- Primary key index (already created via PRIMARY KEY constraint)
-- Regular B-tree indexes
CREATE INDEX IF NOT EXISTS idx_transaction_log_transaction_id ON public.transaction_log(transaction_id);
CREATE INDEX IF NOT EXISTS idx_transaction_log_type ON public.transaction_log(transaction_type);
CREATE INDEX IF NOT EXISTS idx_transaction_log_entity_type ON public.transaction_log(entity_type);
CREATE INDEX IF NOT EXISTS idx_transaction_log_entity_id ON public.transaction_log(entity_id);
CREATE INDEX IF NOT EXISTS idx_transaction_log_action ON public.transaction_log(action);
CREATE INDEX IF NOT EXISTS idx_transaction_log_changed_by ON public.transaction_log(changed_by);
CREATE INDEX IF NOT EXISTS idx_transaction_log_status ON public.transaction_log(status);
CREATE INDEX IF NOT EXISTS idx_transaction_log_timestamp ON public.transaction_log(log_timestamp);
CREATE INDEX IF NOT EXISTS idx_transaction_log_processing_time ON public.transaction_log(processing_time_ms);
CREATE INDEX IF NOT EXISTS idx_transaction_log_ip_address ON public.transaction_log(ip_address);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_transaction_log_entity_composite ON public.transaction_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_transaction_log_type_action ON public.transaction_log(transaction_type, action);
CREATE INDEX IF NOT EXISTS idx_transaction_log_timestamp_status ON public.transaction_log(log_timestamp, status);
CREATE INDEX IF NOT EXISTS idx_transaction_log_entity_timestamp ON public.transaction_log(entity_type, entity_id, log_timestamp);
CREATE INDEX IF NOT EXISTS idx_transaction_log_changed_by_timestamp ON public.transaction_log(changed_by, log_timestamp);

-- GIN indexes for JSONB columns
CREATE INDEX IF NOT EXISTS idx_transaction_log_old_state_gin ON public.transaction_log USING GIN(old_state);
CREATE INDEX IF NOT EXISTS idx_transaction_log_new_state_gin ON public.transaction_log USING GIN(new_state);
CREATE INDEX IF NOT EXISTS idx_transaction_log_metadata_gin ON public.transaction_log USING GIN(metadata);

-- Partial indexes on partitioned table
CREATE INDEX IF NOT EXISTS idx_transaction_log_failed ON public.transaction_log(log_timestamp, error_message) 
    WHERE status = 'failed';
CREATE INDEX IF NOT EXISTS idx_transaction_log_slow_queries ON public.transaction_log(log_timestamp, processing_time_ms) 
    WHERE processing_time_ms > 1000;
CREATE INDEX IF NOT EXISTS idx_transaction_log_recent ON public.transaction_log(log_timestamp, transaction_type) 
    WHERE log_timestamp > CURRENT_TIMESTAMP - INTERVAL '7 days';
CREATE INDEX IF NOT EXISTS idx_transaction_log_specific_actions ON public.transaction_log(log_timestamp, entity_type) 
    WHERE action IN ('create', 'update', 'delete');

-- Expression indexes
CREATE INDEX IF NOT EXISTS idx_transaction_log_date ON public.transaction_log(DATE(log_timestamp));
CREATE INDEX IF NOT EXISTS idx_transaction_log_hour ON public.transaction_log(EXTRACT(HOUR FROM log_timestamp));
CREATE INDEX IF NOT EXISTS idx_transaction_log_entity_type_lower ON public.transaction_log(LOWER(entity_type));

-- ============================================================================
-- Public Schema: Another Partitioned Table - Event Stream (List Partitioning)
-- ============================================================================

-- Create a list-partitioned table for event streams by event category
CREATE TABLE IF NOT EXISTS public.event_stream (
    event_id BIGSERIAL,
    event_category VARCHAR(50) NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    event_source VARCHAR(100) NOT NULL,
    user_id INTEGER,
    session_id VARCHAR(100),
    event_data JSONB NOT NULL,
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT false,
    processing_attempts INTEGER DEFAULT 0,
    error_details TEXT,
    PRIMARY KEY (event_id, event_category)
) PARTITION BY LIST (event_category);

-- Create partitions for different event categories
CREATE TABLE IF NOT EXISTS public.event_stream_user_events PARTITION OF public.event_stream
    FOR VALUES IN ('user_login', 'user_logout', 'user_registration', 'user_profile_update');

CREATE TABLE IF NOT EXISTS public.event_stream_order_events PARTITION OF public.event_stream
    FOR VALUES IN ('order_created', 'order_updated', 'order_cancelled', 'order_completed', 'order_shipped');

CREATE TABLE IF NOT EXISTS public.event_stream_payment_events PARTITION OF public.event_stream
    FOR VALUES IN ('payment_initiated', 'payment_success', 'payment_failed', 'payment_refunded');

CREATE TABLE IF NOT EXISTS public.event_stream_product_events PARTITION OF public.event_stream
    FOR VALUES IN ('product_viewed', 'product_added_to_cart', 'product_removed_from_cart', 'product_purchased');

CREATE TABLE IF NOT EXISTS public.event_stream_system_events PARTITION OF public.event_stream
    FOR VALUES IN ('system_error', 'system_warning', 'system_info', 'system_maintenance');

CREATE TABLE IF NOT EXISTS public.event_stream_default PARTITION OF public.event_stream
    DEFAULT;

-- Indexes on the list-partitioned table
CREATE INDEX IF NOT EXISTS idx_event_stream_name ON public.event_stream(event_name);
CREATE INDEX IF NOT EXISTS idx_event_stream_source ON public.event_stream(event_source);
CREATE INDEX IF NOT EXISTS idx_event_stream_user_id ON public.event_stream(user_id);
CREATE INDEX IF NOT EXISTS idx_event_stream_session_id ON public.event_stream(session_id);
CREATE INDEX IF NOT EXISTS idx_event_stream_timestamp ON public.event_stream(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_event_stream_processed ON public.event_stream(processed);
CREATE INDEX IF NOT EXISTS idx_event_stream_processing_attempts ON public.event_stream(processing_attempts);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_event_stream_category_name ON public.event_stream(event_category, event_name);
CREATE INDEX IF NOT EXISTS idx_event_stream_user_timestamp ON public.event_stream(user_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_event_stream_source_timestamp ON public.event_stream(event_source, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_event_stream_category_timestamp ON public.event_stream(event_category, event_timestamp);

-- GIN index for JSONB
CREATE INDEX IF NOT EXISTS idx_event_stream_data_gin ON public.event_stream USING GIN(event_data);

-- Partial indexes
CREATE INDEX IF NOT EXISTS idx_event_stream_unprocessed ON public.event_stream(event_timestamp, event_category) 
    WHERE processed = false;
CREATE INDEX IF NOT EXISTS idx_event_stream_failed ON public.event_stream(event_timestamp, error_details) 
    WHERE processing_attempts > 3;
CREATE INDEX IF NOT EXISTS idx_event_stream_recent ON public.event_stream(event_timestamp, event_name) 
    WHERE event_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- Expression indexes
CREATE INDEX IF NOT EXISTS idx_event_stream_date ON public.event_stream(DATE(event_timestamp));
CREATE INDEX IF NOT EXISTS idx_event_stream_category_lower ON public.event_stream(LOWER(event_category));

-- ============================================================================
-- Cross-Schema Relationships (Making it more complicated)
-- ============================================================================

-- Add foreign key from public.product_reviews to ecommerce.products (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ecommerce' AND table_name = 'products') THEN
        -- Add foreign key constraint if the table exists
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE constraint_name = 'fk_reviews_product_id' 
            AND table_schema = 'public'
        ) THEN
            ALTER TABLE public.product_reviews 
            ADD CONSTRAINT fk_reviews_product_id 
            FOREIGN KEY (product_id) REFERENCES ecommerce.products(product_id);
        END IF;
    END IF;
END $$;

-- Add foreign key from public.payment_transactions to ecommerce.orders (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ecommerce' AND table_name = 'orders') THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE constraint_name = 'fk_transactions_order_id' 
            AND table_schema = 'public'
        ) THEN
            ALTER TABLE public.payment_transactions 
            ADD CONSTRAINT fk_transactions_order_id 
            FOREIGN KEY (order_id) REFERENCES ecommerce.orders(order_id);
        END IF;
    END IF;
END $$;

-- ============================================================================
-- Data Population: Random Test Data
-- ============================================================================

-- Generate random data for all tables
-- This will populate tables with realistic test data for reindexing operations

-- ============================================================================
-- Ecommerce Schema: Users, Products, Orders
-- ============================================================================

-- Populate users table (100 users)
INSERT INTO ecommerce.users (email, username, first_name, last_name, created_at, status, phone_number)
SELECT 
    'user' || generate_series || '@example.com',
    'user' || generate_series,
    'FirstName' || generate_series,
    'LastName' || generate_series,
    CURRENT_TIMESTAMP - (random() * INTERVAL '365 days'),
    CASE WHEN random() < 0.8 THEN 'active' ELSE 'inactive' END,
    '+1-555-' || LPAD(floor(random() * 10000)::text, 4, '0')
FROM generate_series(1, 100)
ON CONFLICT DO NOTHING;

-- Populate products table (200 products)
INSERT INTO ecommerce.products (sku, name, description, price, category_id, stock_quantity, created_at, is_active)
SELECT 
    'SKU-' || LPAD(generate_series::text, 6, '0'),
    'Product ' || generate_series,
    'Description for product ' || generate_series || ' with various features and specifications.',
    round((random() * 1000 + 10)::numeric, 2),
    floor(random() * 20 + 1),
    floor(random() * 1000),
    CURRENT_TIMESTAMP - (random() * INTERVAL '180 days'),
    random() < 0.9
FROM generate_series(1, 200)
ON CONFLICT DO NOTHING;

-- Populate orders table (500 orders)
INSERT INTO ecommerce.orders (user_id, order_date, total_amount, status, shipping_address, payment_method)
SELECT 
    floor(random() * 100 + 1),
    CURRENT_TIMESTAMP - (random() * INTERVAL '90 days'),
    round((random() * 500 + 10)::numeric, 2),
    (ARRAY['pending', 'processing', 'shipped', 'completed', 'cancelled'])[floor(random() * 5 + 1)],
    floor(random() * 1000) || ' Main Street, City, State ' || LPAD(floor(random() * 99999)::text, 5, '0'),
    (ARRAY['credit_card', 'paypal', 'debit_card', 'bank_transfer'])[floor(random() * 4 + 1)]
FROM generate_series(1, 500)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Analytics Schema: Page Views, Events, User Sessions
-- ============================================================================

-- Populate page_views table (1000 views)
INSERT INTO analytics.page_views (user_id, page_url, view_timestamp, session_id, referrer, device_type, user_agent)
SELECT 
    CASE WHEN random() < 0.7 THEN floor(random() * 100 + 1) ELSE NULL END,
    (ARRAY['/home', '/products', '/about', '/contact', '/cart', '/checkout'])[floor(random() * 6 + 1)],
    CURRENT_TIMESTAMP - (random() * INTERVAL '30 days'),
    'session-' || floor(random() * 500 + 1),
    CASE WHEN random() < 0.5 THEN 'https://google.com' ELSE NULL END,
    (ARRAY['desktop', 'mobile', 'tablet'])[floor(random() * 3 + 1)],
    'Mozilla/5.0 (compatible; TestBot/1.0)'
FROM generate_series(1, 1000)
ON CONFLICT DO NOTHING;

-- Populate events table (2000 events)
INSERT INTO analytics.events (event_type, user_id, event_timestamp, properties, session_id)
SELECT 
    (ARRAY['page_view', 'click', 'purchase', 'add_to_cart', 'remove_from_cart', 'search'])[floor(random() * 6 + 1)],
    CASE WHEN random() < 0.8 THEN floor(random() * 100 + 1) ELSE NULL END,
    CURRENT_TIMESTAMP - (random() * INTERVAL '30 days'),
    jsonb_build_object(
        'page', '/page' || floor(random() * 10 + 1),
        'value', round((random() * 100)::numeric, 2),
        'category', (ARRAY['electronics', 'clothing', 'books', 'food'])[floor(random() * 4 + 1)]
    ),
    'session-' || floor(random() * 500 + 1)
FROM generate_series(1, 2000)
ON CONFLICT DO NOTHING;

-- Populate user_sessions table (500 sessions)
INSERT INTO analytics.user_sessions (session_id, user_id, started_at, ended_at, duration_seconds, page_count)
SELECT 
    'session-' || generate_series,
    CASE WHEN random() < 0.8 THEN floor(random() * 100 + 1) ELSE NULL END,
    CURRENT_TIMESTAMP - (random() * INTERVAL '30 days'),
    CASE WHEN random() < 0.7 THEN CURRENT_TIMESTAMP - (random() * INTERVAL '30 days') + (random() * INTERVAL '2 hours') ELSE NULL END,
    CASE WHEN random() < 0.7 THEN floor(random() * 7200) ELSE NULL END,
    floor(random() * 20 + 1)
FROM generate_series(1, 500)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Inventory Schema: Warehouses, Stock Items, Movements
-- ============================================================================

-- Populate warehouses table (10 warehouses)
INSERT INTO inventory.warehouses (code, name, location, capacity, is_active)
SELECT 
    'WH-' || LPAD(generate_series::text, 3, '0'),
    'Warehouse ' || generate_series,
    'Location ' || generate_series || ', City, State',
    floor(random() * 10000 + 1000),
    random() < 0.9
FROM generate_series(1, 10)
ON CONFLICT DO NOTHING;

-- Populate stock_items table (500 stock items)
INSERT INTO inventory.stock_items (warehouse_id, product_id, quantity, reserved_quantity, last_updated, location_code)
SELECT 
    floor(random() * 10 + 1),
    floor(random() * 200 + 1),
    floor(random() * 1000),
    floor(random() * 100),
    CURRENT_TIMESTAMP - (random() * INTERVAL '7 days'),
    'A' || LPAD(floor(random() * 100)::text, 3, '0') || '-B' || LPAD(floor(random() * 50)::text, 2, '0')
FROM generate_series(1, 500)
ON CONFLICT DO NOTHING;

-- Populate movements table (1000 movements)
INSERT INTO inventory.movements (stock_id, movement_type, quantity, movement_date, reference_number, notes)
SELECT 
    floor(random() * 500 + 1),
    (ARRAY['in', 'out', 'transfer', 'adjustment', 'return'])[floor(random() * 5 + 1)],
    floor(random() * 100 + 1),
    CURRENT_TIMESTAMP - (random() * INTERVAL '90 days'),
    'REF-' || LPAD(floor(random() * 100000)::text, 6, '0'),
    'Movement note for reference ' || floor(random() * 1000)
FROM generate_series(1, 1000)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Reporting Schema: Daily Sales, Monthly Summaries
-- ============================================================================

-- Populate daily_sales table (365 days of data)
INSERT INTO reporting.daily_sales (report_date, product_id, total_sales, quantity_sold, average_price, created_at)
SELECT 
    CURRENT_DATE - generate_series,
    floor(random() * 200 + 1),
    round((random() * 10000 + 100)::numeric, 2),
    floor(random() * 100 + 1),
    round((random() * 500 + 10)::numeric, 2),
    CURRENT_TIMESTAMP - generate_series * INTERVAL '1 day'
FROM generate_series(0, 364)
ON CONFLICT DO NOTHING;

-- Populate monthly_summaries table (24 months)
INSERT INTO reporting.monthly_summaries (year, month, total_revenue, total_orders, unique_customers, created_at)
SELECT 
    EXTRACT(YEAR FROM CURRENT_DATE) - (generate_series / 12)::int,
    CASE 
        WHEN (generate_series % 12) = 0 THEN 12
        ELSE (generate_series % 12)
    END,
    round((random() * 1000000 + 10000)::numeric, 2),
    floor(random() * 10000 + 100),
    floor(random() * 5000 + 100),
    DATE_TRUNC('month', CURRENT_DATE) - (generate_series || ' months')::INTERVAL
FROM generate_series(1, 24)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Audit Schema: Audit Log, User Activity
-- ============================================================================

-- Populate audit_log table (2000 audit entries)
INSERT INTO audit.audit_log (table_name, record_id, action, changed_by, changed_at, old_values, new_values)
SELECT 
    (ARRAY['users', 'products', 'orders', 'categories', 'stock_items'])[floor(random() * 5 + 1)],
    floor(random() * 1000 + 1),
    (ARRAY['create', 'update', 'delete'])[floor(random() * 3 + 1)],
    floor(random() * 100 + 1),
    CURRENT_TIMESTAMP - (random() * INTERVAL '180 days'),
    jsonb_build_object('field1', 'old_value_' || floor(random() * 100), 'field2', floor(random() * 1000)),
    jsonb_build_object('field1', 'new_value_' || floor(random() * 100), 'field2', floor(random() * 1000))
FROM generate_series(1, 2000)
ON CONFLICT DO NOTHING;

-- Populate user_activity table (1500 activities)
INSERT INTO audit.user_activity (user_id, activity_type, activity_timestamp, ip_address, user_agent, details)
SELECT 
    floor(random() * 100 + 1),
    (ARRAY['login', 'logout', 'view', 'edit', 'delete', 'create'])[floor(random() * 6 + 1)],
    CURRENT_TIMESTAMP - (random() * INTERVAL '90 days'),
    ('192.168.' || floor(random() * 255) || '.' || floor(random() * 255))::inet,
    'Mozilla/5.0 (compatible; TestBot/1.0)',
    jsonb_build_object(
        'action', 'activity_' || floor(random() * 100),
        'resource', '/resource/' || floor(random() * 100),
        'success', random() < 0.9
    )
FROM generate_series(1, 1500)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Public Schema: Categories, Product Reviews, Payment Transactions, System Configurations
-- ============================================================================

-- Populate categories table (50 categories with hierarchy)
INSERT INTO public.categories (parent_category_id, name, slug, description, display_order, is_active, created_at)
SELECT 
    CASE WHEN generate_series > 10 THEN floor(random() * 10 + 1) ELSE NULL END,
    'Category ' || generate_series,
    'category-' || generate_series,
    'Description for category ' || generate_series,
    generate_series,
    random() < 0.9,
    CURRENT_TIMESTAMP - (random() * INTERVAL '180 days')
FROM generate_series(1, 50)
ON CONFLICT DO NOTHING;

-- Populate product_reviews table (300 reviews)
INSERT INTO public.product_reviews (product_id, user_id, rating, title, review_text, is_verified_purchase, is_moderated, helpful_count, created_at)
SELECT 
    floor(random() * 200 + 1),
    floor(random() * 100 + 1),
    floor(random() * 5 + 1),
    'Review title ' || generate_series,
    'This is a review text for product ' || floor(random() * 200 + 1) || '. ' || repeat('Lorem ipsum dolor sit amet. ', floor(random() * 5 + 1)),
    random() < 0.6,
    random() < 0.8,
    floor(random() * 50),
    CURRENT_TIMESTAMP - (random() * INTERVAL '60 days')
FROM generate_series(1, 300)
ON CONFLICT DO NOTHING;

-- Populate payment_transactions table (800 transactions)
INSERT INTO public.payment_transactions (order_id, user_id, payment_method, amount, currency, status, processor_response_code, transaction_date, refunded_amount, metadata)
SELECT 
    floor(random() * 500 + 1),
    floor(random() * 100 + 1),
    (ARRAY['credit_card', 'paypal', 'debit_card', 'bank_transfer', 'apple_pay'])[floor(random() * 5 + 1)],
    round((random() * 1000 + 10)::numeric, 2),
    (ARRAY['USD', 'EUR', 'GBP'])[floor(random() * 3 + 1)],
    (ARRAY['completed', 'pending', 'failed', 'refunded'])[floor(random() * 4 + 1)],
    CASE WHEN random() < 0.1 THEN 'ERROR' || floor(random() * 100) ELSE NULL END,
    CURRENT_TIMESTAMP - (random() * INTERVAL '90 days'),
    CASE WHEN random() < 0.1 THEN round((random() * 100)::numeric, 2) ELSE 0 END,
    jsonb_build_object(
        'processor', (ARRAY['stripe', 'paypal', 'square'])[floor(random() * 3 + 1)],
        'transaction_fee', round((random() * 5)::numeric, 2),
        'customer_id', 'cust_' || floor(random() * 10000)
    )
FROM generate_series(1, 800)
ON CONFLICT DO NOTHING;

-- Populate system_configurations table (30 configurations)
INSERT INTO public.system_configurations (config_key, config_value, config_type, is_encrypted, version, updated_by)
SELECT 
    'config.key.' || generate_series,
    'value_' || floor(random() * 1000),
    (ARRAY['string', 'integer', 'boolean', 'json'])[floor(random() * 4 + 1)],
    random() < 0.2,
    floor(random() * 5 + 1),
    floor(random() * 100 + 1)
FROM generate_series(1, 30)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Public Schema: Partitioned Tables - Transaction Log and Event Stream
-- ============================================================================

-- Populate transaction_log table (5000 entries across different partitions)
-- This will distribute data across the last 24 months
INSERT INTO public.transaction_log (
    transaction_id, transaction_type, entity_type, entity_id, action, 
    old_state, new_state, changed_by, change_reason, ip_address, 
    log_timestamp, processing_time_ms, status, error_message, metadata
)
SELECT 
    floor(random() * 10000 + 1),
    (ARRAY['payment', 'order', 'user', 'product', 'inventory'])[floor(random() * 5 + 1)],
    (ARRAY['users', 'products', 'orders', 'payments', 'inventory'])[floor(random() * 5 + 1)],
    floor(random() * 1000 + 1),
    (ARRAY['create', 'update', 'delete', 'view', 'process'])[floor(random() * 5 + 1)],
    jsonb_build_object('old_value', floor(random() * 1000)),
    jsonb_build_object('new_value', floor(random() * 1000)),
    floor(random() * 100 + 1),
    'Change reason ' || floor(random() * 100),
    ('192.168.' || floor(random() * 255) || '.' || floor(random() * 255))::inet,
    CURRENT_TIMESTAMP - (random() * INTERVAL '730 days'), -- Last 2 years
    floor(random() * 5000),
    (ARRAY['completed', 'pending', 'failed', 'processing'])[floor(random() * 4 + 1)],
    CASE WHEN random() < 0.1 THEN 'Error message ' || floor(random() * 100) ELSE NULL END,
    jsonb_build_object(
        'source', 'system',
        'module', (ARRAY['payment', 'order', 'user'])[floor(random() * 3 + 1)],
        'trace_id', 'trace-' || floor(random() * 1000000)
    )
FROM generate_series(1, 5000)
ON CONFLICT DO NOTHING;

-- Populate event_stream table (3000 events across different categories)
INSERT INTO public.event_stream (
    event_category, event_name, event_source, user_id, session_id,
    event_data, event_timestamp, processed, processing_attempts, error_details
)
SELECT 
    CASE 
        WHEN random() < 0.2 THEN 'user_login'
        WHEN random() < 0.4 THEN 'order_created'
        WHEN random() < 0.6 THEN 'payment_initiated'
        WHEN random() < 0.8 THEN 'product_viewed'
        ELSE 'system_error'
    END,
    (ARRAY['user_login', 'user_logout', 'order_created', 'order_updated', 'payment_success', 'product_viewed', 'product_purchased'])[floor(random() * 7 + 1)],
    (ARRAY['web', 'mobile', 'api', 'admin'])[floor(random() * 4 + 1)],
    CASE WHEN random() < 0.8 THEN floor(random() * 100 + 1) ELSE NULL END,
    'session-' || floor(random() * 500 + 1),
    jsonb_build_object(
        'event_id', generate_series,
        'properties', jsonb_build_object(
            'page', '/page' || floor(random() * 10),
            'value', round((random() * 100)::numeric, 2),
            'category', (ARRAY['electronics', 'clothing', 'books'])[floor(random() * 3 + 1)]
        )
    ),
    CURRENT_TIMESTAMP - (random() * INTERVAL '30 days'),
    random() < 0.9,
    CASE WHEN random() < 0.1 THEN floor(random() * 5 + 1) ELSE 0 END,
    CASE WHEN random() < 0.05 THEN 'Error details for event ' || generate_series ELSE NULL END
FROM generate_series(1, 3000)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Summary
-- ============================================================================
-- This script creates:
-- - 5 custom schemas: ecommerce, analytics, inventory, reporting, audit
-- - Public schema tables: categories, product_reviews, payment_transactions, system_configurations
-- - 2 partitioned tables in public schema:
--   * transaction_log (RANGE partitioned by month with 24+ partitions)
--   * event_stream (LIST partitioned by event category with 5+ partitions)
-- - Multiple tables in each schema with various index types:
--   * Primary keys (constraint indexes)
--   * Unique constraints (constraint indexes)
--   * Regular B-tree indexes
--   * Composite indexes (multi-column)
--   * Partial indexes (with WHERE clauses)
--   * Expression indexes (computed columns)
--   * GIN indexes (for JSONB columns)
--   * Indexes on partitioned tables (global and partition-local)
--
-- Total indexes created: ~150+ indexes across all schemas
-- This provides comprehensive test coverage for the reindexer tool including:
-- - Schema-level reindexing
-- - Table-level reindexing
-- - Partitioned table reindexing
-- - Multiple index types
-- - Complex index patterns

