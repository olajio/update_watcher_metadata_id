from elasticsearch import Elasticsearch, helpers
import json

# Elasticsearch Connection
ELASTICSEARCH_HOST = "http://localhost:9200"  # Change this if needed
es = Elasticsearch(ELASTICSEARCH_HOST)

# File containing watcher IDs
WATCHER_IDS_FILE = "watcher_ids.txt"

# Read Watcher IDs from file
def load_watcher_ids(file_path):
    with open(file_path, "r") as file:
        return [line.strip() for line in file.readlines() if line.strip()]

# Fetch a single watcher
def get_watcher(watch_id):
    try:
        response = es.get(index=".watches", id=watch_id)
        return response["_source"]
    except Exception as e:
        print(f"Error fetching watcher {watch_id}: {e}")
        return None

# Update watcher if metadata.id is missing or incorrect
def update_watcher(watch_id, watch_data):
    if "metadata" not in watch_data:
        watch_data["metadata"] = {}

    current_metadata_id = watch_data["metadata"].get("id", None)

    # Check if metadata.id is missing or incorrect
    if current_metadata_id is None or current_metadata_id != watch_id:
        watch_data["metadata"]["id"] = watch_id
        try:
            # Update the watcher
            es.index(index=".watches", id=watch_id, body=watch_data)
            print(f"Updated watcher {watch_id} - metadata.id corrected.")
        except Exception as e:
            print(f"Error updating watcher {watch_id}: {e}")
    else:
        print(f"Watcher {watch_id} is already correct. No update needed.")

# Main function
def main():
    watcher_ids = load_watcher_ids(WATCHER_IDS_FILE)
    print(f"Loaded {len(watcher_ids)} watcher IDs.")

    for watch_id in watcher_ids:
        watch_data = get_watcher(watch_id)
        if watch_data:
            update_watcher(watch_id, watch_data)

if __name__ == "__main__":
    main()
