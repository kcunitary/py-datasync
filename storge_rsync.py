import os
import hashlib
import zlib
import json
from multiprocessing import Pool, cpu_count

def hash_chunk(chunk):
    md5_hash = hashlib.md5()
    md5_hash.update(chunk)
    adler32_hash = zlib.adler32(chunk)
    return md5_hash.hexdigest(), adler32_hash

def split_file(file_path, size=40*1024*1024):
    try:
        file_num = 0
        with open(file_path, 'rb') as f:
            chunk = f.read(size)
            while chunk:
                file_num += 1
                start_byte = (file_num - 1) * size
                end_byte = start_byte + len(chunk) - 1
                md5_hash, adler32_hash = hash_chunk(chunk)
                yield file_num, md5_hash, adler32_hash, start_byte, end_byte
                chunk = f.read(size)
    except Exception as e:
        print(f'Error processing file {file_path}: {e}')

def process_file(file_path):
    result = []
    for file_num, md5_hash, adler32_hash, start_byte, end_byte in split_file(file_path):
        result.append({
            'File': file_path,
            'Slice': file_num,
            'MD5 Hash': md5_hash,
            'Adler-32 Hash': adler32_hash,
            'StartByte': start_byte,
            'EndByte': end_byte,
            'Size': os.path.getsize(file_path),
            'Modified Time': os.path.getmtime(file_path)
        })
    return result

def get_file_list(directory):
    files = []
    for foldername, subfolders, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(foldername, filename)
            files.append(file_path)
    return files

def process_file_list(files):
    pool = Pool(cpu_count())
    results = pool.map(process_file, files)
    pool.close()
    pool.join()
    return [item for sublist in results for item in sublist]

def scan_directory(directory):
    files = get_file_list(directory)
    return process_file_list(files)

def save_results(results, filename):
    with open(filename, 'w') as f:
        json.dump(results, f)

def load_results(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def update_results(filename,directory):
    old_results = load_results(filename)
    old_file_list = map(lambda x:x['File'],old_results)
    files_to_update = []
    new_results = []

    new_file_list = get_file_list(directory)
    in_old_resuls = [f for f in new_file_list if f in old_file_list]
    for root, dirs, files in os.walk(directory):
        for name in files:
            path = os.path.join(root, name)
            if path in old_results:
                size, mtime, _, _ = old_results[path]
                file_info = os.stat(path)
                if size == file_info.st_size and mtime == file_info.st_mtime:
                    continue
            file_paths.append(path)
    for old_result in old_results:
        if old_result['Size'] != os.path.getsize(old_result['File']) or old_result['Modified Time'] != os.path.getmtime(old_result['File']):
            files_to_update.append(old_result['File'])
    new_results = process_file_list(files_to_update)
    
    if new_results:
        print(f'Found {len(new_results)} updated files.')
        save_results(new_results + [result for result in old_results if result['File'] not in [new_result['File'] for new_result in new_results]], filename)
    else:
        print('No updates found.')

# 使用方法：update_results('你的目录路径', '你的结果文件名')
#r = scan_directory("/opt/company-tomcat-app")
#save_results(r,"r.json")




