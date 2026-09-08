import time
import json
from kafka import KafkaProducer
from cassandra.cluster import Cluster
from datetime import datetime


# --- Configuration ---
# Use your VM IP so it matches the docker network
CASSANDRA_IPS = ['localhost'] 
KAFKA_SERVER = ['localhost:9092']
TOPIC_NAME = 'tracking_topic'

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    raise TypeError(
        f"Type {type(obj)} is not JSON serializable"
    )

def main():
        #1. Connect
        cluster = Cluster(CASSANDRA_IPS)
        session = cluster.connect('recruitment_startup')
        
        producer = KafkaProducer(
            bootstrap_servers = KAFKA_SERVER,
            value_serializer = lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
        )
        
        #2. Set starting point in the past to pick up existing data
        last_processed_time = datetime.strptime('2022-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
   
        print(f"Producer Loop Started. Press Ctrl+C to stop")
        print(f"Looking for data newer than: {last_processed_time}")
          
        try:
            #3. INFINITE LOOP 
            while True:
                #Query only data NEWER than what we saw last time
                query = f"SELECT * FROM tracking WHERE created_at > '{last_processed_time}' ALLOW FILTERING"
                rows = session.execute(query)
                
                new_rows_count = 0
                current_batch_max_time = last_processed_time
                
                for row in rows:
                    #Filter
                    if row.custom_track is None:
                        continue
                    data = {
                        'created_at': row.created_at,
                        'job_id': row.job_id,
                        'custom_track': row.custom_track,
                        'bid': row.bid,
                        'campaign_id': row.campaign_id,
                        'group_id': row.group_id,
                        'publisher_id': row.publisher_id     
                    }
                    
                    producer.send(TOPIC_NAME, data)
                    new_rows_count += 1
                                
                    if row.created_at > current_batch_max_time:
                        current_batch_max_time = row.created_at
                
                # If we found new data, update the checkpoint and print status:
                if new_rows_count > 0:
                    print(f"Sent {new_rows_count} new events. Updated time to: {current_batch_max_time}")
                    last_processed_time = current_batch_max_time
                    producer.flush()
                    
                # 4. SLEEP 
                time.sleep(5)    
                    
        except KeyboardInterrupt:
            print("Producer stopped by user.")
        finally:
            producer.close()
            session.shutdown()
            cluster.shutdown()      
if __name__ == "__main__":
    main()
    
    