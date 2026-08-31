import os

from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
from datetime import datetime
import time
import random
import mysql.connector
import pandas as pd
import warnings
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

#Load environment variables
load_dotenv()

#Disable warnings pandas to cleaner output
warnings.filterwarnings('ignore')

CASSANDRA_HOST = 'localhost'
CASSANDRA_PORT = 9042

MYSQL_HOST = 'localhost'
MYSQL_PORT = 3307
MYSQL_DB = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

print(f"Using MySQL: {MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")

CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
#DATA SCENATIOS (create fake data):

PUBLISHER_SCENARIOS = { 
    # Facebook: Cheap (1-5), Low Quality, High Traffic
    1: {'name': 'Facebook',  'bid_range': (1, 5),    'quality': 'LOW',  'traffic': 'HIGH'}, 
    # LinkedIn: Expensive (20-50), High Quality, Low Traffic
    2: {'name': 'LinkedIn',  'bid_range': (20, 50),  'quality': 'HIGH', 'traffic': 'LOW'},  
    # TikTok: Very Cheap (1-3), Low Quality, High Traffic
    4: {'name': 'ITviec',    'bid_range': (1, 3),    'quality': 'LOW',  'traffic': 'HIGH'}, 
    # TopCV: Medium (10-20), Medium Quality
    5: {'name': 'TopCV',     'bid_range': (10, 20),  'quality': 'MED',  'traffic': 'MED'},  
}

#CONNECTORS
def get_cassandra_connection():
    """Connect to Cassandra database"""
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    return cluster.connect(CASSANDRA_KEYSPACE)

def get_mysql_connection():
    "Connect to MySQL database"
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

def get_master_data():
    "Fetch real Job and Publisher IDs from MySQL database"
    try:
        cnx = get_mysql_connection()

        #Get Jobs
        query_jobs = "SELECT id as job_id, campaign_id, group_id, company_id FROM job"
        jobs_df = pd.read_sql(query_jobs, cnx)

        #Get Publishers
        query_publishers = "SELECT DISTINCT(id) AS publisher_id from master_publisher"
        pubs_df = pd.read_sql(query_publishers, cnx)

        cnx.close()
        return jobs_df, pubs_df
    except Exception as e:
        print(f"Error fetching master data: {e}")
        return pd.DataFrame(), pd.DataFrame()

#SMART DATA GENERATION
def decide_event_type(pub_quality, job_quality):
    """
    Decides if a click becomes a conversion based on quality.
    Returns: click, conversion, qualified, unqualified
    """

    #Base conversion chance (0-100)
    conversion_chance = 10 #xac suat click

    #1. Impact of Publisher Quality
    if pub_quality == 'HIGH': conversion_chance += 30 #LinkedIn users convert more
    elif pub_quality == 'LOW': conversion_chance -= 5 #ITviec users convert more

    # 2. Impact of Job Quality
    if job_quality == 'HOT': conversion_chance += 40   # Good jobs get more apps
    elif job_quality == 'DEAD': conversion_chance = 1  # Bad jobs get ignored

    #Roll the dice for Conversion
    if random.randint(0,100) <= conversion_chance:
        #If converted, is it Qualified?
        qualify_threshold = 70 if pub_quality == 'HIGH' else (20 if pub_quality == 'LOW' else 50)

        if random.randint(0,100) <= qualify_threshold:
            return 'qualified'
        else:
            return 'unqualified'
    return 'click'

def generate_smart_data(session, n_records, jobs_df, pubs_df):
    """
    Generate a batch of events based on scenarios
    """
    if jobs_df.empty or pubs_df.empty:
        print("No master data found. Skipping generation.")
        return
    jobs_list = jobs_df.to_dict('records')
    pubs_list = pubs_df['publisher_id'].to_list()

    print(f"--- Generating {n_records} events ---")

    for _ in range(n_records):
        #1. Pick Publisher (Weighted by traffic)
        weighted_pubs = []
        for pid in pubs_list:
            scenario = PUBLISHER_SCENARIOS.get(pid,{'traffic':'MED'})
            weight = 10 if scenario['traffic'] == 'HIGH' else (2 if scenario['traffic'] == 'LOW' else 5)
            weighted_pubs.extend([pid] * weight)
        publisher_id = random.choice(weighted_pubs)
        pub_scenario = PUBLISHER_SCENARIOS.get(publisher_id, {'name': 'Unknown','bid_range': (5, 10), 'quality': 'MED'})

        #2. Pick Job (Random)
        job = random.choice(jobs_list)
        job_id = job['job_id']
        #Simulate Job Quality: ID divisible by 3 is HOT, by 2 is DEAD
        job_quality = 'HOT' if job_id % 3 == 0 else ('DEAD' if job_id % 2 == 0 else 'MED')

        #3. Decide Event Type
        final_state = decide_event_type(pub_scenario['quality'], job_quality)

        #Flatten the funnel logic for simple data generation
        # We mostly want 'clicks', sometimes 'conversion'

        if final_state != 'click':
            if random.random() < 0.8:
                custom_track = 'click'
            else:
                if final_state == 'qualified':
                    custom_track = random.choice(['conversion', 'qualified'])
                else:
                    custom_track  = final_state
        else:
            custom_track = 'click'

        #4. Generate Bid (Integer)
        #FIX: Using randint instead of uniform because Cassandra column is INT
        bid = random.randint(*pub_scenario['bid_range'])

        #5. Generate Timestamp
        created_at_uuid = datetime.now()
        created_at = datetime.now()
        create_time = str(uuid_from_time(created_at_uuid))

        #6. Get other metadata
        campaign_id = job['campaign_id'] if pd.notnull(job['campaign_id']) else random.randint(1, 10)
        group_id = int(job['group_id']) if pd.notnull(job['group_id']) else 0

        # 7. Insert to Cassandra
        sql = f"""INSERT INTO tracking (create_time,created_at,bid,campaign_id,custom_track,group_id,job_id,publisher_id)
        VALUES ('{create_time}','{created_at}',{bid},{campaign_id},'{custom_track}',{group_id},{job_id},{publisher_id})"""
        print("========== SQL ==========")
        print(sql)
        print("==========================")
        session.execute(sql)

if __name__ == "__main__":
    print("Smart Data Generator Started...")
    
    # Connect to DBs
    try:
        session = get_cassandra_connection()

        jobs_df, pubs_df = get_master_data()
        
        print(f"Loaded {len(jobs_df)} jobs and {len(pubs_df)} publishers.")

        while True:
            # Randomize batch size
            n = random.randint(5, 20)
            
            generate_smart_data(session, n, jobs_df, pubs_df)
            
            # Randomize sleep time for natural feel
            sleep_time = random.randint(5, 10)
            print(f"Sleeping {sleep_time}s...") 
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\n🛑 Generator Stopped.")
    except Exception as e:
        print(f"\n❌ Critical Error: {e}")
    finally:
        if 'session' in locals():
            session.shutdown()
                
    














