import json
import os
import glob
import pandas as pd
from datetime import datetime
import re
import psycopg2
import logging
from dotenv import load_dotenv
import numpy as np

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("media_etl")

# Database connection parameters 
DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': 'media_analytics',
    'user': os.getenv('POSTGRES_USER'),
    'password':os.getenv('POSTGRES_PASSWORD') ,
    'port': os.getenv('POSTGRES_PORT') 
}

def create_engine():
    """Create postgres engine from DB parameters"""
    return f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"

def connect_to_db():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_PARAMS['host'],
            database=DB_PARAMS['database'],
            user=DB_PARAMS['user'],
            password=DB_PARAMS['password'],
            port=DB_PARAMS['port']
        )
        logger.info("Successfully connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise

def extract_data(data_dir):
    """
    Extract data from JSON files in the specified directory
    """
    logger.info(f"Starting data extraction from {data_dir}")
    all_data = []
    json_files = glob.glob(os.path.join(data_dir, "*.json"))
    
    if not json_files:
        logger.warning(f"No JSON files found in {data_dir}")
        return pd.DataFrame()
    
    for file_path in json_files:
        try:
            logger.info(f"Processing file: {file_path}")
            
            # If file contains multiple JSON objects (one per line)
            with open(file_path, 'r') as file:
                file_content = file.read()
                # Check if file contains array of JSON objects or line-delimited JSON
                if file_content.strip().startswith('['):
                    # Handle JSON array format
                    data_list = json.loads(file_content)
                else:
                    # Handle line-delimited JSON (one JSON object per line)
                    data_list = []
                    for line in file_content.strip().split('\n'):
                        if line.strip():
                            data_list.append(json.loads(line))
            
            all_data.extend(data_list)
            logger.info(f"Successfully processed {len(data_list)} records from {file_path}")
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    
    if not all_data:
        logger.warning("No data was extracted from any files")
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    logger.info(f"Extracted {len(df)} total records from {len(json_files)} files")
    return df

def transform_data(df):
    """
    Clean and transform the raw data
    """
    if df.empty:
        logger.warning("No data to transform")
        return df
    logger.info("Starting data transformation")
    
    # Make a copy 
    transformed_df = df.copy()
    
    try:
        #  Handle missing values
        for col in transformed_df.columns:
            missing_count = transformed_df[col].isna().sum()
            if missing_count > 0:
                logger.info(f"Column {col} has {missing_count} missing values")
        
        #  Convert timestamp to datetime
        transformed_df['timestamp'] = pd.to_datetime(transformed_df['timestamp'])
        
        #  Extract date and time components for analysis
        transformed_df['event_date'] = transformed_df['timestamp'].dt.date
        transformed_df['event_time'] = transformed_df['timestamp'].dt.time
        transformed_df['event_hour'] = transformed_df['timestamp'].dt.hour
        transformed_df['event_day'] = transformed_df['timestamp'].dt.day
        transformed_df['event_month'] = transformed_df['timestamp'].dt.month
        transformed_df['event_year'] = transformed_df['timestamp'].dt.year
        transformed_df['event_dayofweek'] = transformed_df['timestamp'].dt.dayofweek
        transformed_df['is_weekend'] = transformed_df['event_dayofweek'].isin([5, 6])
        
        # Extract content category and article ID from page_url
        transformed_df['content_category'] = transformed_df['page_url'].apply(
            lambda url: extract_category(url)
        )
        
        transformed_df['article_id'] = transformed_df['page_url'].apply(
            lambda url: extract_article_id(url)
        )
        
        #  Categorize referrer sources
        transformed_df['referrer_category'] = transformed_df['referrer'].apply(categorize_referrer)
        
        # Convert all string columns to lowercase for consistency
        for col in transformed_df.select_dtypes(include=['object']).columns:
            # Check if the column has any non-null values before applying str methods
            if transformed_df[col].notna().any():
                transformed_df[col] = transformed_df[col].astype(str).str.lower()
            else:
                # Handle columns that are all null
                logger.info(f"Column {col} contains all null values, skipping string conversion")
        
        #  Generate a unique interaction_id if it doesn't exist
        if 'interaction_id' not in transformed_df.columns:
            transformed_df['interaction_id'] = transformed_df.apply(
                lambda row: f"{row['user_id']}_{row['session_id']}_{int(row['timestamp'].timestamp())}", 
                axis=1
            )
        
        logger.info(f"Transformation complete. Shape after transformation: {transformed_df.shape}")
        return transformed_df
    
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise

def extract_category(url):
    """Extract content category from URL"""
    pattern = r'news\.example\.com/([^/]+)'
    match = re.search(pattern, url)
    return match.group(1) if match else 'unknown'

def extract_article_id(url):
    """Extract article ID from URL"""
    pattern = r'article-(\d+)'
    match = re.search(pattern, url)
    return match.group(1) if match else 'unknown'

def categorize_referrer(referrer):
    """Categorize referrer into source types"""
    if pd.isna(referrer) or referrer == '':
        return 'direct'
    elif 'google' in referrer:
        return 'search'
    elif 'facebook' in referrer or 'twitter' in referrer or 'instagram' in referrer or 'social' in referrer:
        return 'social'
    elif 'news' in referrer or 'nytimes' in referrer or 'cnn' in referrer:
        return 'news'
    elif 'email' in referrer or 'newsletter' in referrer:
        return 'email'
    else:
        return 'other'

def load_data(df, table_name='user_interactions'):
    """
    Load transformed data into PostgreSQL database using dict conversion
    """
    if df.empty:
        logger.warning("No data to load")
        return
    
    logger.info(f"Starting data load into {table_name} table")
    
    try:
        # Connect to database
        conn = connect_to_db()
        cursor = conn.cursor()    
        records = df.to_dict('records')
        
        # Get column names
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Create the SQL query with ON CONFLICT
        insert_query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (interaction_id, event_date) DO NOTHING
        """
        # Process in batches
        batch_size = 100
        inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            # Convert each record to a tuple of Python native types
            batch_values = []
            for record in batch:
                row_values = []
                for col in columns:
                    val = record[col]
                    # Explicitly convert NumPy types to Python native types
                    if 'numpy.int' in str(type(val)):
                        val = int(val)
                    elif 'numpy.float' in str(type(val)):
                        val = float(val)
                    elif 'numpy' in str(type(val)) and 'datetime' in str(type(val)):
                        val = val.strftime('%Y-%m-%d %H:%M:%S')
                    elif val is None or (isinstance(val, float) and np.isnan(val)):
                        val = None
                    row_values.append(val)
                batch_values.append(tuple(row_values))
            
            # Execute the batch
            cursor.executemany(insert_query, batch_values)
            inserted += cursor.rowcount
        
        # Commit the transaction
        conn.commit()
        logger.info(f"Successfully loaded {inserted} new records (skipped {len(records) - inserted} duplicates)")
        
    except Exception as e:
        logger.error(f"Error loading data into database: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def create_database_schema():
    """Create database tables if they don't exist"""
    logger.info("Creating database schema if not exists")
    
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        
        # Create user_interactions table with TEXT types for potentially problematic columns
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_interactions (
            interaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            session_id VARCHAR(255) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            page_url TEXT NOT NULL,
            action VARCHAR(50) NOT NULL,
            device_type VARCHAR(50),
            referrer TEXT,
            event_date DATE NOT NULL,
            event_time TIME NOT NULL,
            event_hour TEXT,
            event_day TEXT,
            event_month TEXT,
            event_year TEXT,
            event_dayofweek TEXT,
            is_weekend BOOLEAN,
            content_category VARCHAR(100),
            article_id VARCHAR(100),
            referrer_category VARCHAR(50),
            time_spent_seconds TEXT,
            scroll_depth TEXT
        );
        """)
        
        # Create indexes
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_interactions_user_id ON user_interactions(user_id);
        CREATE INDEX IF NOT EXISTS idx_user_interactions_event_date ON user_interactions(event_date);
        CREATE INDEX IF NOT EXISTS idx_user_interactions_content_category ON user_interactions(content_category);
        CREATE INDEX IF NOT EXISTS idx_user_interactions_article_id ON user_interactions(article_id);
        """)
        
        # Commit changes
        conn.commit()
        logger.info("Database schema created successfully")
        
    except Exception as e:
        logger.error(f"Error creating database schema: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


def run_etl_pipeline(data_dir):
    """
    Execute the complete ETL pipeline
    
    Args:
        data_dir: Directory containing JSON files
    """
    logger.info("Starting ETL pipeline")
    
    try:
        # Create a new connection for table checks
        conn = connect_to_db()
        cursor = conn.cursor()
        
        # Check if we need to create partitions before loading data
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM pg_tables 
            WHERE tablename = 'user_interactions_2025_03'
        );
        """)
        partition_exists = cursor.fetchone()[0]
        
        if not partition_exists:
            logger.info("Creating necessary table partitions")
            # Create partitions for March 2025 (adjust as needed)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_interactions_2025_03 
            PARTITION OF user_interactions
            FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
            """)
            conn.commit()
            
    
        cursor.close()
        conn.close()
        
        # Create database schema
        create_database_schema()
        
        # Extract data from JSON files
        raw_data = extract_data(data_dir)
        if not raw_data.empty:
            # Transform the data
            transformed_data = transform_data(raw_data)
        
            # Load data into database
            load_data(transformed_data)
            
            logger.info("ETL pipeline completed successfully")
        else:
            logger.warning("ETL pipeline completed with no data processed")
            
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    DATA_DIRECTORY = "./data"
    
    # Run the ETL pipeline
    run_etl_pipeline(DATA_DIRECTORY)