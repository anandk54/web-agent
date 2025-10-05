import ftplib
import os
import concurrent.futures
import time
import threading
from collections import defaultdict

def get_files_with_sizes(ip, port, user, password, directory):
    """Connects to FTP, lists files in a directory, and gets their sizes."""
    print("Connecting to get file list and sizes...")
    files_with_sizes = []
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(host=ip, port=port, timeout=30)
            ftp.login(user=user, passwd=password)
            ftp.cwd(directory)
            
            filenames = ftp.nlst()
            print(f"Found {len(filenames)} total files. Fetching sizes...")
            
            for filename in filenames:
                try:
                    size = ftp.size(filename)
                    if size > 0:
                        files_with_sizes.append((filename, size))
                except ftplib.error_perm:
                    # This can happen if the entry is a directory, skip it.
                    print(f"Could not get size for '{filename}', likely a directory. Skipping.")
                    continue
        print("Successfully retrieved file list.")
        return files_with_sizes
    except Exception as e:
        print(f"An error occurred while getting file list: {e}")
        return None

def download_chunk_worker(ip, port, user, password, directory, filename, local_path, start_byte, chunk_size):
    """Downloads a specific chunk of a file and writes it to the correct position."""
    thread_id = threading.get_ident()
    # print(f" Starting chunk for {filename} at byte {start_byte}")
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(host=ip, port=port, timeout=60)
            ftp.login(user=user, passwd=password)
            ftp.cwd(directory)
            
            # Use 'r+b' to open the existing file for reading and writing in binary mode.
            with open(local_path, 'r+b') as f:
                f.seek(start_byte)
                # Use transfercmd to get a socket, allowing us to control the read size.
                # The 'rest' parameter tells the server where to start the transfer from.
                with ftp.transfercmd(f'RETR {filename}', rest=start_byte) as conn:
                    bytes_to_read = chunk_size
                    while bytes_to_read > 0:
                        # Read a block of data, ensuring we don't read past the chunk boundary.
                        block = conn.recv(min(8192, bytes_to_read))
                        if not block:
                            break # End of file reached
                        f.write(block)
                        bytes_to_read -= len(block)
        # print(f" Finished chunk for {filename} at byte {start_byte}")
        return local_path, True
    except Exception as e:
        print(f" Error downloading chunk for {filename} at {start_byte}: {e}")
        return local_path, False

def main():
    # --- Configuration ---
    IP_ADDRESS = "10.20.3.108"
    PORT = 2221
    USERNAME = "android"
    PASSWORD = "android"
    REMOTE_DOWNLOAD_DIR = "/Download/"
    LOCAL_DOWNLOAD_DIR = "downloaded_images_chunked"
    
    # --- Tuning Parameters ---
    # Number of parallel workers (chunks to download at once)
    MAX_WORKERS = 16 
    # Number of chunks to split each file into
    NUM_CHUNKS_PER_FILE = 8

    if not os.path.exists(LOCAL_DOWNLOAD_DIR):
        os.makedirs(LOCAL_DOWNLOAD_DIR)

    # 1. Get a list of all files and their sizes
    all_files = get_files_with_sizes(IP_ADDRESS, PORT, USERNAME, PASSWORD, REMOTE_DOWNLOAD_DIR)
    if not all_files:
        print("No files found or could not connect.")
        return

    # 2. Filter for the files you want to download
    file_extensions_to_download = ('.jpg', '.jpeg', '.png')
    files_to_download = [f for f in all_files if f.lower().endswith(file_extensions_to_download)]

    if not files_to_download:
        print(f"No files with extensions {file_extensions_to_download} found.")
        return

    print(f"\nPreparing to download {len(files_to_download)} files using chunking parallelism.")
    
    # 3. Pre-allocate local files and create a master list of all chunk tasks
    chunk_tasks = []
    download_progress = defaultdict(lambda: {"total": NUM_CHUNKS_PER_FILE, "completed": 0})

    for filename, total_size in files_to_download:
        local_filepath = os.path.join(LOCAL_DOWNLOAD_DIR, filename)
        
        # Create an empty file of the correct size on disk
        with open(local_filepath, 'wb') as f:
            f.seek(total_size - 1)
            f.write(b'\0')
            
        chunk_size = total_size // NUM_CHUNKS_PER_FILE
        for i in range(NUM_CHUNKS_PER_FILE):
            start_byte = i * chunk_size
            # For the last chunk, ensure it reads to the very end of the file
            current_chunk_size = total_size - start_byte if i == NUM_CHUNKS_PER_FILE - 1 else chunk_size
            
            # Add all required info for the worker to the task list
            chunk_tasks.append((IP_ADDRESS, PORT, USERNAME, PASSWORD, REMOTE_DOWNLOAD_DIR, filename, local_filepath, start_byte, current_chunk_size))

    # 4. Execute all chunk downloads in parallel
    start_time = time.time()
    total_bytes_to_download = sum(size for _, size in files_to_download)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        print(f"\nSubmitting {len(chunk_tasks)} total chunks to {MAX_WORKERS} workers...")
        
        future_to_chunk = {executor.submit(*task): task for task in chunk_tasks}

        for future in concurrent.futures.as_completed(future_to_chunk):
            local_path, success = future.result()
            if success:
                progress = download_progress[local_path]
                progress["completed"] += 1
                if progress["completed"] == progress["total"]:
                    print(f"✅ Completed: {os.path.basename(local_path)}")

    end_time = time.time()
    duration = end_time - start_time
    speed_mbps = (total_bytes_to_download / (1024*1024)) / duration if duration > 0 else 0

    print("\nAll download tasks finished.")
    print(f"Total time taken: {duration:.2f} seconds")
    print(f"Average download speed: {speed_mbps:.2f} MB/s")

if __name__ == "__main__":
    main()