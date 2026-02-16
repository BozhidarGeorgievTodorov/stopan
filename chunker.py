import os
import hashlib

class FileChunker:
    """
    Implementación base del algoritmo de Content-Defined Chunking (CDC)
    Utiliza un rolling hash simple para encontrar límites de bloque basados en el contenido del archivo.
    """
    def __init__(self, avg_chunk_size=1024, min_chunk_size=512, max_chunk_size=2048):
        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.mask = avg_chunk_size - 1 

    def chunk_file(self, file_path):
        """
        Lee el archivo de forma secuencial y emite fragmentos de tamaño variable.
        El corte del bloque se decide cuando el rolling hash de la ventana actual
        coincide con la máscara de bits, asegurando el tamaño medio esperado.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'rb') as f:
            buffer = bytearray()
            
            while True:
                byte = f.read(1)
                if not byte:
                    break
                
                buffer.append(byte[0])
                current_len = len(buffer)

                if current_len < self.min_chunk_size:
                    continue
                
                if current_len >= self.max_chunk_size:
                    yield self._create_chunk(buffer)
                    buffer = bytearray()
                    continue

                window_size = 48
                if current_len >= window_size:
                    window = buffer[-window_size:] 
                    rolling_val = sum(b * i for i, b in enumerate(window))
                    
                    if (rolling_val & self.mask) == 0:
                        yield self._create_chunk(buffer)
                        buffer = bytearray()

            if buffer:
                yield self._create_chunk(buffer)

    def _create_chunk(self, buffer_data):
        """Calcula el SHA-256 del bloque finalizado y devuelve ambos."""
        data = bytes(buffer_data)
        sha256 = hashlib.sha256(data).hexdigest()
        return sha256, data