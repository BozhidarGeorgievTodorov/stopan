from __future__ import annotations

import os


DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
DEFAULT_SEED = os.getenv("MEMBERSHIP_SEED", "localhost:50051")
DEFAULT_PROBE_TIMEOUT_S = float(os.getenv("VERIFIER_PROBE_TIMEOUT_S", "10.0"))
DEFAULT_TARGET_PARALLELISM = int(os.getenv("VERIFIER_TARGET_PARALLELISM", "4"))
DEFAULT_PROBE_BATCH_HASHES = int(os.getenv("VERIFIER_PROBE_BATCH_HASHES", "2048"))
DEFAULT_MAX_MESSAGE_BYTES = int(os.getenv("GRPC_MAX_MESSAGE_BYTES", str(8 * 1024 * 1024)))
