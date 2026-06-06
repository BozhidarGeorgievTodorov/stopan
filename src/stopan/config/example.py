from __future__ import annotations


EXAMPLE_CONFIG = """# Stopan node configuration
#
# This file is suitable both for a real machine and for Docker.
# CLI arguments may override these values for one-off commands.

node:
  bind_addr: "0.0.0.0:50051"
  advertise_addr: "192.168.1.42:50051"
  repo_store_dir: "/var/lib/stopan/node_store"
  local_shard_dir: "/var/lib/stopan/local_cas"
  db_file: "/var/lib/stopan/backup_index.db"

cluster:
  token: "oficina-demo"
  seeds:
    - "192.168.1.42:50051"
    - "192.168.1.43:50051"
    - "192.168.1.44:50051"

protection:
  remote_copies: 3
  strict_remote_copies: true
  ec_pack_size_bytes: 8388608

backup:
  workers: 4

grpc:
  max_message_bytes: 8388608

storage:
  rpc_workers: 64
  commit_workers: 16
  commit_queue_items: 256
  max_chunk_size: 8388608

replication:
  target_parallelism: 4
  probe_batch_hashes: 2048
  stream_inflight: 64
  probe_timeout_s: 10.0
  stream_timeout_s: 60.0
  commit_every: 100

verify:
  target_parallelism: 4
  probe_batch_hashes: 2048
  probe_timeout_s: 10.0

restore:
  batch_target_parallelism: 4
  prefetch_window: 32
  rpc_timeout_s: 5.0

membership:
  protocol_period_s: 1.0
  ping_timeout_s: 0.25
  rpc_timeout_s: 2.0
  suspect_timeout_s: 6.0
  indirect_ping_fanout: 3
  max_gossip_events: 20
  gossip_ttl_s: 60.0
metadata:
  # Secret used to encrypt/decrypt the local metadata vault and packs.
  passphrase_file: "/etc/stopan/metadata.passphrase"

  # Public identity used to discover distributed metadata packs.
  # If owner_id is empty, Stopan can read it from identity_file.
  owner_id: ""
  identity_file: "/etc/stopan/metadata_identity.json"

  # Incremental, deduplicated encrypted metadata vault.
  object_graph_auto_export: false
  object_store_dir: "/var/lib/stopan/metadata_object_store"
  object_graph_include_protection: true
  object_graph_auto_pack: false
  object_pack_dir: ""

  # Local storage for encrypted metadata packs received from / prepared for P2P.
  distributed_pack_store_dir: "/var/lib/stopan/metadata_distributed_packs"
  pack_copies: 3
  strict_pack_copies: false
  pack_discovery_max_candidates: 10
  pack_target_parallelism: 4
  pack_rpc_timeout_s: 60.0
  cli_warning_limit: 10
  max_distributed_pack_bytes: 67108864
  max_distributed_packs_per_owner: 8
  max_distributed_pack_bytes_per_owner: 536870912
  max_distributed_pack_store_bytes: 10737418240
  # 0 disables age-based pruning. Set e.g. 90 for periodic maintenance.
  scrypt_n: 32768
  scrypt_r: 8
  scrypt_p: 1
  key_length: 32
gc:
  # CAS generado localmente por backup/push.
  generated_chunk_grace_hours: 48.0

  # CAS recibido por el nodo desde otros peers.
  # 0 desactiva poda por edad. Usar con cuidado: puede degradar protección remota.
  received_chunk_max_age_days: 0

  # Shards EC generados localmente.
  generated_ec_grace_hours: 48.0

  # Shards EC recibidos por el nodo desde otros peers.
  # 0 desactiva poda por edad. Usar con cuidado: puede degradar protección remota.
  received_ec_max_age_days: 0

  # Metadata object graph generado localmente.
  generated_metadata_graph_grace_hours: 48.0

  # .stopanmetapack generados localmente desde el object graph.
  generated_metadata_pack_grace_hours: 48.0

  # Metadata packs recibidos en el distributed metadata pack store.
  # 0 desactiva poda por edad.
  received_metadata_pack_max_age_days: 0

  # Metadata packs descargados por metadata pack recover.
  # 0 desactiva poda por edad.
  recovered_metadata_pack_max_age_days: 0
"""
