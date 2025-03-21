-- Database Schema for Media Analytics Platform

-- Create database (run this as admin user if you haven't created it yet)
-- CREATE DATABASE media_analytics;

-- Connect to the database
-- \c media_analytics

-- Drop existing table if it exists to avoid conflicts
DROP TABLE IF EXISTS user_interactions CASCADE;

-- Create User Interactions Table with additional columns 
CREATE TABLE IF NOT EXISTS user_interactions (
    interaction_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    page_url TEXT NOT NULL,
    action VARCHAR(50) NOT NULL,
    device_type VARCHAR(50),
    referrer TEXT,
    event_date DATE NOT NULL,
    event_time TIME NOT NULL,
    event_hour INTEGER,
    event_day INTEGER,
    event_month INTEGER,
    event_year INTEGER,
    event_dayofweek INTEGER,
    is_weekend BOOLEAN,
    content_category VARCHAR(100),
    article_id VARCHAR(100),
    referrer_category VARCHAR(50),
    time_spent_seconds INTEGER,
    scroll_depth FLOAT,
    PRIMARY KEY (interaction_id, event_date)
) PARTITION BY RANGE (event_date);

-- Create initial partitions for March 2025
CREATE TABLE IF NOT EXISTS user_interactions_2025_03 PARTITION OF user_interactions
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

-- Create Users Dimension Table
CREATE TABLE IF NOT EXISTS dim_users (
    user_id VARCHAR(255) PRIMARY KEY,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    session_count INTEGER DEFAULT 0,
    total_interactions INTEGER DEFAULT 0,
    device_types VARCHAR(255)[], -- Array of device types used
    preferred_device VARCHAR(50),
    preferred_content_category VARCHAR(100)
);

-- Create Content Dimension Table
CREATE TABLE IF NOT EXISTS dim_content (
    article_id VARCHAR(100) PRIMARY KEY,
    content_category VARCHAR(100) NOT NULL,
    full_url TEXT NOT NULL,
    total_views INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    avg_time_spent NUMERIC(10,2) DEFAULT 0
);

-- Create Sessions Dimension Table
CREATE TABLE IF NOT EXISTS dim_sessions (
    session_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    page_count INTEGER DEFAULT 0,
    device_type VARCHAR(50),
    referrer_category VARCHAR(50),
    entry_page VARCHAR(255),
    exit_page VARCHAR(255)
);

-- Create Date Dimension Table for faster temporal analysis
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    quarter INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL
);

-- Create Time Dimension Table for time-based analysis
CREATE TABLE IF NOT EXISTS dim_time (
    time_id TIME PRIMARY KEY,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    hour_of_day VARCHAR(20) NOT NULL, -- Morning, Afternoon, Evening, Night
    is_business_hours BOOLEAN NOT NULL
);

-- Create aggregate table for daily user statistics
CREATE TABLE IF NOT EXISTS agg_daily_users (
    date_id DATE NOT NULL,
    total_users INTEGER NOT NULL,
    new_users INTEGER NOT NULL,
    returning_users INTEGER NOT NULL,
    total_sessions INTEGER NOT NULL,
    total_pageviews INTEGER NOT NULL,
    avg_session_duration NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (date_id)
);

-- Create aggregate table for article performance
CREATE TABLE IF NOT EXISTS agg_article_performance (
    article_id VARCHAR(100) NOT NULL,
    date_id DATE NOT NULL,
    total_views INTEGER NOT NULL,
    unique_visitors INTEGER NOT NULL,
    avg_time_spent NUMERIC(10,2) NOT NULL,
    social_referrals INTEGER NOT NULL,
    search_referrals INTEGER NOT NULL,
    direct_referrals INTEGER NOT NULL,
    PRIMARY KEY (article_id, date_id)
);

-- Create indexes for optimized querying
-- User Interactions Indexes (create on the parent table)
CREATE INDEX IF NOT EXISTS idx_user_interactions_user_id ON user_interactions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_content_category ON user_interactions(content_category);
CREATE INDEX IF NOT EXISTS idx_user_interactions_article_id ON user_interactions(article_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_action ON user_interactions(action);

-- Create indexes on the partition
CREATE INDEX IF NOT EXISTS idx_user_interactions_2025_03_user_id ON user_interactions_2025_03(user_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_2025_03_article_id ON user_interactions_2025_03(article_id);

-- Dimension table indexes
CREATE INDEX IF NOT EXISTS idx_dim_sessions_user_id ON dim_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_content_category ON dim_content(content_category);

-- Create partition function
CREATE OR REPLACE FUNCTION create_partition_and_insert() RETURNS TRIGGER AS $$
DECLARE
    partition_date TEXT;
    partition_name TEXT;
BEGIN
    partition_date := to_char(NEW.event_date, 'YYYY_MM');
    partition_name := 'user_interactions_' || partition_date;
    
    -- Check if partition exists, if not create it
    IF NOT EXISTS(SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF user_interactions 
                       FOR VALUES FROM (%L) TO (%L)',
                       partition_name,
                       date_trunc('month', NEW.event_date),
                       date_trunc('month', NEW.event_date) + interval '1 month');
                       
        -- Create indexes on the new partition
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_user_id ON %I(user_id)',
                      partition_name, partition_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_article_id ON %I(article_id)',
                      partition_name, partition_name);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for automatic partitioning
DROP TRIGGER IF EXISTS user_interactions_insert_trigger ON user_interactions;
CREATE TRIGGER user_interactions_insert_trigger
    BEFORE INSERT ON user_interactions
    FOR EACH ROW EXECUTE FUNCTION create_partition_and_insert();

-- Add table comments
COMMENT ON TABLE user_interactions IS 'Stores all user interaction events';
COMMENT ON TABLE dim_users IS 'User dimension table with aggregated user metrics';
COMMENT ON TABLE dim_content IS 'Content dimension table with article details and metrics';
COMMENT ON TABLE dim_sessions IS 'Session dimension table tracking user sessions';
COMMENT ON TABLE dim_date IS 'Date dimension table for temporal analysis';
COMMENT ON TABLE dim_time IS 'Time dimension table for time-based analysis';
COMMENT ON TABLE agg_daily_users IS 'Aggregate table for daily user metrics';
COMMENT ON TABLE agg_article_performance IS 'Aggregate table for article performance metrics';