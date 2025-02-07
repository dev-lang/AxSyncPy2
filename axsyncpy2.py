import os
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import argparse

# ----------------------------
# Funciones comunes
# ----------------------------

def check_or_create_directory(path):
    if not os.path.exists(path):
        print(f"La carpeta de destino '{path}' no existe.")
        respuesta = input("¿Deseas crearla? (Y/N): ").strip().lower()
        if respuesta == 'y':
            os.makedirs(path)
            print(f"Carpeta '{path}' creada.")
            return True
        else:
            print("Descarga cancelada.")
            return False
    return True

def get_filename_from_url(url):
    return os.path.basename(urlparse(url).path)

# ----------------------------
# Descarga con Multithreading
# ----------------------------

def download_chunk(url, start, end, file_name, part_number):
    headers = {
        'Range': f'bytes={start}-{end}',
        'Accept-Encoding': 'identity'  # Evitar compresión gzip
    }
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    
    part_file = f"{file_name}.part{part_number}"
    with open(part_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return part_file, part_number

def combine_parts(file_name, parts):
    with open(file_name, 'wb') as f:
        for part in parts:
            with open(part, 'rb') as p:
                f.write(p.read())
            os.remove(part)

def download_file_multithread(url, file_path, num_threads=8):
    response = requests.head(url, headers={'Accept-Encoding': 'identity'})
    total_size = int(response.headers.get('content-length', 0))
    
    chunk_size = total_size // num_threads
    ranges = [(i * chunk_size, (i + 1) * chunk_size - 1) for i in range(num_threads)]
    ranges[-1] = (ranges[-1][0], total_size - 1)

    parts = [None] * num_threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i, (start, end) in enumerate(ranges):
            futures.append(executor.submit(download_chunk, url, start, end, file_path, i))
        
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(file_path)) as pbar:
            for future in as_completed(futures):
                part_file, part_number = future.result()
                parts[part_number] = part_file
                pbar.update(os.path.getsize(part_file))

    combine_parts(file_path, parts)
    print(f"{os.path.basename(file_path)} descargado con éxito.")

# ----------------------------
# Descarga Secuencial (Sin Multithreading)
# ----------------------------

def download_file_single_thread(url, file_path):
    response = requests.get(url, headers={'Accept-Encoding': 'identity'}, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    
    with open(file_path, 'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(file_path)) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))

# ----------------------------
# Lógica Principal
# ----------------------------

def download_file(url, file_path, num_threads=8):
    response = requests.head(url, headers={'Accept-Encoding': 'identity'})
    total_size = int(response.headers.get('content-length', 0))
    
    if total_size < 1024 * 1024:  # Si el archivo es menor a 1MB
        print(f"El archivo {os.path.basename(file_path)} es pequeño. Descargando en un solo hilo...")
        download_file_single_thread(url, file_path)
    else:
        print(f"El archivo {os.path.basename(file_path)} es grande. Descargando con {num_threads} hilos...")
        download_file_multithread(url, file_path, num_threads)

def download_directory(base_url, destination_folder, num_threads=8):
    if not check_or_create_directory(destination_folder):
        return
    
    response = requests.get(base_url, headers={'Accept-Encoding': 'identity'})
    soup = BeautifulSoup(response.text, 'html.parser')

    for link in soup.find_all('a'):
        file_name = link.get('href')
        if not file_name or file_name.startswith('?') or file_name.endswith('/') or file_name == '../':
            continue
        
        file_url = base_url + file_name
        file_path = os.path.join(destination_folder, file_name)
        
        try:
            download_file(file_url, file_path, num_threads)
        except Exception as e:
            print(f"Error al descargar {file_name}: {e}")

def process_urls(input_file, base_destination, num_threads=8):
    if not check_or_create_directory(base_destination):
        return
    
    with open(input_file, 'r') as f:
        urls = f.read().splitlines()
    
    for url in urls:
        parsed_url = urlparse(url)
        folder_name = parsed_url.path.strip('/').split('/')[-1]
        destination_folder = os.path.join(base_destination, folder_name)
        
        if not check_or_create_directory(destination_folder):
            continue
        
        print(f"\nDescargando carpeta: {folder_name}")
        print(f"URL: {url}")
        print(f"Destino: {destination_folder}")
        
        download_directory(url, destination_folder, num_threads)

# ----------------------------
# Configuración de Argumentos
# ----------------------------

def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("El número de hilos debe ser mayor que 0.")
    return ivalue

def parse_arguments():
    parser = argparse.ArgumentParser(description="Descargar archivos desde URLs FTP.")
    parser.add_argument("--input", "-i", help="Archivo de texto con URLs de carpetas.")
    parser.add_argument("--output", "-o", required=True, help="Carpeta base de destino.")
    parser.add_argument(
        "--threads", "-t", 
        type=positive_int,  # Validación personalizada
        default=8, 
        help="Número de hilos (debe ser mayor que 0, default: 8)."
    )
    parser.add_argument("--url", "-u", help="URL de un directorio para descargar todos sus archivos.")
    return parser.parse_args()

# ----------------------------
# Punto de Entrada
# ----------------------------

def print_configuration(args):
    print("\nConfiguración de descarga:")
    print(f"- Carpeta de destino: {args.output}")
    print(f"- Multithreading: Activado ({args.threads} hilos)")
    if args.url:
        print(f"- URL del directorio: {args.url}")
    elif args.input:
        print(f"- Archivo de URLs: {args.input}")
    print("-" * 40)

if __name__ == "__main__":
    # Parsear argumentos de la línea de comandos
    args = parse_arguments()
    
    # Mostrar configuración de descarga
    print_configuration(args)
    
    if args.url:
        # Descargar todos los archivos de la URL del directorio
        parsed_url = urlparse(args.url)
        folder_name = parsed_url.path.strip('/').split('/')[-1]
        destination_folder = os.path.join(args.output, folder_name)
        
        print(f"\nDescargando carpeta: {folder_name}")
        print(f"URL: {args.url}")
        print(f"Destino: {destination_folder}")
        
        download_directory(args.url, destination_folder, args.threads)
    elif args.input:
        # Procesar el archivo de texto con URLs
        process_urls(args.input, args.output, args.threads)
    else:
        print("Error: Debes proporcionar --input o --url.")
        exit(1)
    
    print("\n¡Descarga completada!")
