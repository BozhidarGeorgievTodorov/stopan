from __future__ import annotations

import os


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
REPO_STORE_DIR = os.getenv("REPO_STORE_DIR", "node_store")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
DEFAULT_SEED = os.getenv("MEMBERSHIP_SEED", "localhost:50051")
DEFAULT_RF = int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3")))
DEFAULT_RPC_TIMEOUT_S = float(os.getenv("RESTORE_RPC_TIMEOUT_S", "5.0"))
GRPC_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))
DEFAULT_BATCH_TARGET_PARALLELISM = int(os.getenv("RESTORE_BATCH_TARGET_PARALLELISM", "4"))
DEFAULT_PREFETCH_WINDOW = int(os.getenv("RESTORE_PREFETCH_WINDOW", "32"))
