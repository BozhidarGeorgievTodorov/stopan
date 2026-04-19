from __future__ import annotations

import os


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
REPO_STORE_DIR = os.getenv("REPO_STORE_DIR", "node_store")
NODE_ID_FILE = os.getenv("NODE_ID_FILE", os.path.join(REPO_STORE_DIR, "node_id.txt"))
DEFAULT_PROTECTION_RF = int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3")))
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
