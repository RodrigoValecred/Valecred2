import time
import os
import concurrent.futures

# Mocking external dependencies
class MockResponse:
    def __init__(self, url):
        self.url = url

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size=8192):
        # Simulate network delay for content transfer
        time.sleep(1.0)
        yield b'content' * 100

def mock_requests_get(url, stream=True, timeout=60, headers=None):
    # Simulate network latency for connection establishment
    time.sleep(0.5)
    return MockResponse(url)

class MockFS:
    def mkdirs(self, path):
        pass
    def mv(self, src, dst):
        pass

class MockMsSparkUtils:
    fs = MockFS()

mssparkutils = MockMsSparkUtils()
requests_get = mock_requests_get

urls = {
    "empresas": "https://data.brasil.io/dataset/socios-brasil/empresas.csv.gz",
    "holdings": "https://data.brasil.io/dataset/socios-brasil/holdings.csv.gz",
    "socios": "https://data.brasil.io/dataset/socios-brasil/socios.csv.gz"
}

# --- Shared Config ---
local_download_path = "/tmp/brasil_io_benchmark_data"
os.makedirs(local_download_path, exist_ok=True)
lakehouse_dir = "Files/brasil_io_data"

def sequential_download():
    print("Starting sequential download...")
    lakehouse_files = {}
    start_time = time.time()
    # Sequential approach creates dir inside loop potentially or just once,
    # but original code had it inside loop.
    for name, url in urls.items():
        file_name = f"{name}.csv.gz"
        local_file_path = os.path.join(local_download_path, file_name)

        # print(f"Baixando {url} para {local_file_path}...")
        headers = {
            "User-Agent": "ValeCred-Data-Pipeline/1.0 (contact: admin@valecred.com.br)"
        }
        response = requests_get(url, stream=True, timeout=60, headers=headers)
        response.raise_for_status()

        with open(local_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        lakehouse_file_path = os.path.join(lakehouse_dir, file_name)
        mssparkutils.fs.mkdirs(lakehouse_dir)
        mssparkutils.fs.mv(f"file:{local_file_path}", lakehouse_file_path)
        lakehouse_files[name] = lakehouse_file_path

    end_time = time.time()
    return end_time - start_time

def parallel_download():
    print("Starting parallel download...")
    lakehouse_files = {}
    start_time = time.time()

    # Optimized: mkdirs called once before loop
    mssparkutils.fs.mkdirs(lakehouse_dir)

    def process_url(item):
        name, url = item
        file_name = f"{name}.csv.gz"
        local_file_path = os.path.join(local_download_path, file_name)

        headers = {
            "User-Agent": "ValeCred-Data-Pipeline/1.0 (contact: admin@valecred.com.br)"
        }
        response = requests_get(url, stream=True, timeout=60, headers=headers)
        response.raise_for_status()

        with open(local_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        lakehouse_file_path = os.path.join(lakehouse_dir, file_name)

        mssparkutils.fs.mv(f"file:{local_file_path}", lakehouse_file_path)
        return name, lakehouse_file_path

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(process_url, item): item for item in urls.items()}
        for future in concurrent.futures.as_completed(future_to_url):
            name, path = future.result()
            lakehouse_files[name] = path

    end_time = time.time()
    return end_time - start_time

if __name__ == "__main__":
    seq_time = sequential_download()
    par_time = parallel_download()

    print(f"\nBenchmark Results:")
    print(f"Sequential Time: {seq_time:.2f}s")
    print(f"Parallel Time:   {par_time:.2f}s")
    print(f"Speedup:         {seq_time / par_time:.2f}x")
