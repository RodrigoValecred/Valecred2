import time
import requests
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import ssl
import os
import subprocess

FILE_SIZE = 50 * 1024 * 1024  # 50 MB
TEST_FILE = 'test_50mb.bin'
CERT_FILE = 'cert_temp.pem'
KEY_FILE = 'key_temp.pem'

def generate_temp_cert():
    # 🔒 Security: Programmatically generate temporary self-signed certificate for the local session
    print("Generating temporary certificate for HTTPS benchmark...")
    subprocess.run([
        'openssl', 'req', '-x509', '-newkey', 'rsa:4096', '-keyout', KEY_FILE,
        '-out', CERT_FILE, '-sha256', '-days', '1', '-nodes', '-subj', '/CN=localhost'
    ], check=True, capture_output=True)

# Cria um arquivo mock de 50MB
with open(TEST_FILE, 'wb') as f:
    f.write(os.urandom(FILE_SIZE))

def start_server():
    server = HTTPServer(('localhost', 8080), SimpleHTTPRequestHandler)

    # 🔒 Security: Configure SSL/TLS for local benchmark server using the temporary certificate
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    server.socket = context.wrap_socket(server.socket, server_side=True)

    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server

def download_file(chunk_size):
    # 🔒 Security: Use HTTPS for local requests and specify timeout to prevent hanging (DoS)
    url = "https://localhost:8080/" + TEST_FILE
    start_time = time.time()

    # 🔒 Security: verify with the temporary cert to ensure secure connection even on localhost
    response = requests.get(url, stream=True, verify=CERT_FILE, timeout=30)

    with open('downloaded.bin', 'wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            f.write(chunk)
    end_time = time.time()
    return end_time - start_time

if __name__ == "__main__":
    generate_temp_cert()
    server = start_server()
    time.sleep(1) # espera o servidor iniciar

    try:
        # Aquecimento
        download_file(8192)

        # Mede chunk size de 8KB
        time_8k = download_file(8192)
        print(f"Time with 8KB chunk size: {time_8k:.4f} seconds")

        # Mede chunk size de 1MB
        time_1m = download_file(1048576)
        print(f"Time with 1MB chunk size: {time_1m:.4f} seconds")

        print(f"Improvement: {(time_8k - time_1m) / time_8k * 100:.2f}%")

    finally:
        # Limpeza
        server.shutdown()

        # 🔒 Security: Cleanup all sensitive and temporary files
        for f in [TEST_FILE, 'downloaded.bin', CERT_FILE, KEY_FILE]:
            if os.path.exists(f):
                os.remove(f)
        print("Temporary files cleaned up.")
