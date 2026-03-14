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
        "allow_remote_protected_skip",
        "desired_rf",
        "current_placement_epoch",
    )

    def __init__(
        self,
        repo,
        db,
        *,
        fast_path_enabled=False,
        safe_mode=False,
        index=None,
        allow_remote_protected_skip=False,
        desired_rf=3,
        current_placement_epoch=None,
    ):
        if index is None:
            raise ValueError("ChunkPlanner requires a ChunkIndex instance.")

        self.repo = repo
        self.db = db
        self.fast_path_enabled = bool(fast_path_enabled)
        self.safe_mode = bool(safe_mode)
        self.index = index
        self.allow_remote_protected_skip = bool(allow_remote_protected_skip)
        self.desired_rf = max(int(desired_rf), 1)
        self.current_placement_epoch = current_placement_epoch

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

        if self.allow_remote_protected_skip:
            cached_protected = self.index.synced.get(chunk_hash)
            if cached_protected is True:
                return "skip_synced"

            if cached_protected is None:
                is_protected = self.db.is_chunk_remotely_protected(
                    chunk_hash,
                    required_rf=self.desired_rf,
                    current_epoch=self.current_placement_epoch,
                )
                self.index.synced.set(chunk_hash, is_protected)
                if is_protected:
                    return "skip_synced"

        return "process"
