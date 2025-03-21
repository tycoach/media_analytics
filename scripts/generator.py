import json
import random
import uuid
from datetime import datetime, timedelta
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Configuration
NUM_USERS = 100
NUM_SESSIONS_PER_USER = 5
NUM_INTERACTIONS_PER_SESSION = 10
OUTPUT_DIR = "./data"
START_DATE = datetime(2025, 3, 1)
END_DATE = datetime(2025, 3, 31)

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Generate random content categories and article IDs
content_categories = ["politics", "sports", "technology", "business", "entertainment", "health", "science", "travel"]
article_ids = list(range(1, 201))  # 200 different articles
# Generate random device types
device_types = ["mobile", "desktop", "tablet"]

# Generate random actions
actions = ["read", "video_play", "comment", "share", "like", "bookmark"]

# Generate random referrers
referrers = [
    "https://google.com",
    "https://facebook.com",
    "https://twitter.com",
    "https://instagram.com",
    "https://news.example.com",
    "https://email.newsletter.com",
    ""  # Direct traffic
]

def random_date_between(start_date, end_date):
    """Generate a random datetime between start_date and end_date"""
    delta = end_date - start_date
    random_days = random.randrange(delta.days + 1)
    random_seconds = random.randrange(86400)  # Seconds in a day
    return start_date + timedelta(days=random_days, seconds=random_seconds)

def generate_interaction(user_id, session_id, timestamp):
    """Generate a single interaction event"""
    category = random.choice(content_categories)
    article_id = random.choice(article_ids)
    action = random.choice(actions)
    
    return {
        "user_id": user_id,
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "page_url": f"https://news.example.com/{category}/article-{article_id}",
        "action": action,
        "device_type": random.choice(device_types),
        "referrer": random.choice(referrers),
        "session_id": session_id,
        "time_spent_seconds": random.randint(5, 300) if action == "read" else None,
        "scroll_depth": random.uniform(0.1, 1.0) if action in ["read", "video_play"] else None
    }

def generate_data():
    """Generate sample user interaction data"""
    data = []
    
    for user_index in range(NUM_USERS):
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        
        for session_index in range(NUM_SESSIONS_PER_USER):
            session_id = f"session_{uuid.uuid4().hex[:10]}"
            
            # Base timestamp 
            session_start = random_date_between(START_DATE, END_DATE)
            
            for interaction_index in range(NUM_INTERACTIONS_PER_SESSION):
                # Add some time between interactions (1-60 seconds)
                interaction_time = session_start + timedelta(seconds=interaction_index * random.randint(1, 60))
                
                # Generate interaction
                interaction = generate_interaction(user_id, session_id, interaction_time)
                data.append(interaction)
    
    return data

def save_data_to_files(data, num_files=5):
    """Split data into multiple files and save"""
    # Shuffle data to distribute interactions
    random.shuffle(data)
    
    # Calculate items per file
    items_per_file = len(data) // num_files
    
    for i in range(num_files):
        start_idx = i * items_per_file
        end_idx = start_idx + items_per_file if i < num_files - 1 else len(data)
        
        file_data = data[start_idx:end_idx]
        file_path = os.path.join(OUTPUT_DIR, f"interactions_{i+1}.json")
        
        with open(file_path, 'w') as f:
            json.dump(file_data, f, indent=2)
        
        print(f"Generated {len(file_data)} interactions in {file_path}")

if __name__ == "__main__":
    print(f"Generating sample data: {NUM_USERS} users with {NUM_SESSIONS_PER_USER} sessions each")
    data = generate_data()
    print(f"Generated {len(data)} total interactions")
    save_data_to_files(data)
    print("Sample data generation complete")