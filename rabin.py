ROLLING_PRIME = 31
ROLLING_MOD = 1_000_000_009


class RabinKarpRollingHash:
    """Rolling hash sencillo para buscar cortes de CDC."""

    def __init__(self, window_size=48):
        self.window_size = window_size
        self.power_term = pow(ROLLING_PRIME, window_size, ROLLING_MOD)
        self.current_hash = 0
        self.window = bytearray(window_size)
        self.cursor = 0

    def update(self, new_byte):
        old_byte = self.window[self.cursor]
        self.window[self.cursor] = new_byte
        self.cursor = (self.cursor + 1) % self.window_size

        self.current_hash = (self.current_hash - old_byte * self.power_term) % ROLLING_MOD
        self.current_hash = (self.current_hash * ROLLING_PRIME + new_byte) % ROLLING_MOD

        return self.current_hash
