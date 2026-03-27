import time
import requests
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os

FILE_SIZE = 50 * 1024 * 1024  # 50 MB
TEST_FILE = 'test_50mb.bin'

# Create a 50MB dummy file
with open(TEST_FILE, 'wb') as f:
    f.write(os.urandom(FILE_SIZE))

def start_server():
    server = HTTPServer(('localhost', 8080), SimpleHTTPRequestHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server

def download_file(chunk_size):
    url = "http://localhost:8080/" + TEST_FILE
    start_time = time.time()
    response = requests.get(url, stream=True)
    with open('downloaded.bin', 'wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            f.write(chunk)
    end_time = time.time()
    return end_time - start_time

if __name__ == "__main__":
    server = start_server()
    time.sleep(1) # espera o servidor iniciar

    # Aquecimento
    download_file(8192)

    # Mede chunk size de 8KB
    time_8k = download_file(8192)
    print(f"Time with 8KB chunk size: {time_8k:.4f} seconds")

    # Mede chunk size de 1MB
    time_1m = download_file(1048576)
    print(f"Time with 1MB chunk size: {time_1m:.4f} seconds")

    print(f"Improvement: {(time_8k - time_1m) / time_8k * 100:.2f}%")

    # Limpeza
    server.shutdown()
    os.remove(TEST_FILE)
    os.remove('downloaded.bin')
