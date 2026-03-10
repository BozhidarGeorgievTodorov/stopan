class ChunkPlanner:
    """
    Decide si un chunk debe escribirse en el CAS local o puede reutilizarse.
    """

    __slots__ = (
        "repo",
        "db",
        "fast_path_enabled",
        "safe_mode",
        "index",
        "allow_remote_only",
    )

    def __init__(self, repo, db, *, fast_path_enabled=False, safe_mode=False, index=None, allow_remote_only=True):
        if index is None:
            raise ValueError("ChunkPlanner requires a ChunkIndex instance.")

        self.repo = repo
        self.db = db
        self.fast_path_enabled = bool(fast_path_enabled)
        self.safe_mode = bool(safe_mode)
        self.index = index
        self.allow_remote_only = bool(allow_remote_only)

    def decide(self, chunk_hash):
        if self.safe_mode or not self.fast_path_enabled:
            return "process"

        cached_local = self.index.local_exists.get(chunk_hash)
        if cached_local is True:
            return "skip_local"

        if cached_local is None:
            exists_local = self.repo.exists_local(chunk_hash)
            self.index.local_exists.set(chunk_hash, exists_local)
            if exists_local:
                return "skip_local"

        if self.allow_remote_only:
            cached_synced = self.index.synced.get(chunk_hash)
            if cached_synced is True:
                return "skip_synced"

            if cached_synced is None:
                is_synced = self.db.is_chunk_synced(chunk_hash)
                self.index.synced.set(chunk_hash, is_synced)
                if is_synced:
                    return "skip_synced"

        return "process"
