import requests
import re
import os
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from typing import List, Optional
import argparse

class BinanceDataDownloader:
    def __init__(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
        data_type: str = "futures/um",
        frequency: str = "monthly",
        years_filter: Optional[List[int]] = None,
        months_filter: Optional[List[int]] = None,
        download_dir: str = "downloads",
        output_dir: str = "parquet_data"
    ):
        self.symbol = symbol
        self.interval = interval
        self.data_type = data_type
        self.frequency = frequency
        self.years_filter = years_filter
        self.months_filter = months_filter
        self.download_dir = download_dir
        self.output_dir = output_dir
        
        # URL base corretto
        self.base_url = "https://data.binance.vision/"
        
        # Crea directory
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
    
    def get_zip_links_fallback(self) -> List[str]:
        """
        Costruisci URL manualmente basandoti su anni/mesi.
        Questo metodo FUNZIONA perché Binance ha uno schema di URL prevedibile.
        """
        print("🔗 Costruzione URL manuale...")
        
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Range di anni
        if self.years_filter:
            years = sorted(self.years_filter)
        else:
            # Dati disponibili su Binance (dato che fallback funziona, usiamo range ampio)
            years = list(range(2020, current_year + 2))  # +2 per possibili dati futuri
        
        if self.months_filter:
            months = sorted(self.months_filter)
        else:
            months = list(range(1, 13))
        
        generated_links = []
        
        for year in years:
            for month in months:
                # Formatta mese a 2 cifre
                month_str = f"{month:02d}"
                
                # Costruisci URL DIRETTO al file zip
                # Formato: https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip
                filename = f"{self.symbol}-{self.interval}-{year}-{month_str}.zip"
                full_url = (
                    f"{self.base_url}data/{self.data_type}/{self.frequency}/"
                    f"klines/{self.symbol}/{self.interval}/{filename}"
                )
                
                generated_links.append(full_url)
        
        print(f"📁 Generati {len(generated_links)} URL per il download")
        return generated_links
    
    def check_file_exists(self, url: str) -> bool:
        """Verifica rapida se un file esiste"""
        try:
            response = requests.head(url, timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_zip_links(self) -> List[str]:
        """
        Metodo principale: costruisce URL e verifica quali esistono.
        """
        print("🔍 Verifica file disponibili...")
        
        # Costruisci tutti gli URL possibili
        all_urls = self.get_zip_links_fallback()
        
        if not all_urls:
            return []
        
        # Verifica in parallelo quali URL esistono realmente
        valid_urls = []
        print(f"🔎 Verifico {len(all_urls)} URL...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {
                executor.submit(self.check_file_exists, url): url 
                for url in all_urls
            }
            
            for future in tqdm(
                as_completed(future_to_url),
                total=len(future_to_url),
                desc="Verifica esistenza file",
                unit="file"
            ):
                url = future_to_url[future]
                if future.result():
                    valid_urls.append(url)
        
        print(f"✅ File disponibili: {len(valid_urls)}/{len(all_urls)}")
        return valid_urls
    
    def download_file(self, url: str) -> Optional[str]:
        """Scarica un singolo file"""
        try:
            filename = os.path.basename(urlparse(url).path)
            filepath = os.path.join(self.download_dir, filename)
            
            # Verifica se già esiste
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                if file_size > 1024:  # Almeno 1KB
                    print(f"⏭️  Saltato (esiste): {filename}")
                    return filepath
            
            print(f"⬇️  Scaricando: {filename}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f, tqdm(
                desc=f"📥 {filename[:20]}",
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                bar_format='{l_bar}{bar:30}{r_bar}{bar:-30b}'
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
            
            # Verifica integrità
            final_size = os.path.getsize(filepath)
            if final_size < 1024:
                print(f"⚠️  File troppo piccolo, eliminato: {filename}")
                os.remove(filepath)
                return None
            
            print(f"✅ Completato: {filename} ({final_size/1024/1024:.1f} MB)")
            return filepath
            
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout per: {filename}")
            return None
        except Exception as e:
            print(f"❌ Errore: {e}")
            return None
    
    def download_all(self, max_workers: int = 5) -> List[str]:
        """Scarica tutti i file"""
        zip_urls = self.get_zip_links()
        
        if not zip_urls:
            print("⚠️  Nessun file disponibile per il download!")
            return []
        
        print(f"\n🚀 Avvio download di {len(zip_urls)} file...")
        print(f"   Thread: {max_workers}")
        
        downloaded_files = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.download_file, url): url for url in zip_urls}
            
            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="📊 Progresso download",
                unit="file",
                bar_format='{l_bar}{bar:30}{r_bar}{bar:-30b}'
            ):
                result = future.result()
                if result:
                    downloaded_files.append(result)
        
        # Statistiche
        total_size = sum(os.path.getsize(f) for f in downloaded_files if os.path.exists(f))
        
        print(f"\n{'='*50}")
        print("📊 DOWNLOAD COMPLETATO")
        print(f"{'='*50}")
        print(f"✅ File scaricati: {len(downloaded_files)}/{len(zip_urls)}")
        print(f"💾 Dimensione totale: {total_size/1024/1024:.1f} MB")
        print(f"📁 Directory: {os.path.abspath(self.download_dir)}")
        print(f"{'='*50}")
        
        return downloaded_files
    
    
    def extract_to_parquet(self, delete_zip: bool = False):
        """Versione che VERIFICA correttamente se c'è header"""
        
        zip_files = sorted([f for f in os.listdir(self.download_dir) if f.endswith('.zip')])
        
        if not zip_files:
            print("⚠️  Nessun file ZIP trovato!")
            return
        
        print(f"\n🔄 Conversione {len(zip_files)} file in Parquet...")
        
        success_count = 0
        error_count = 0
        
        for zip_filename in tqdm(zip_files, desc="Conversione"):
            zip_path = os.path.join(self.download_dir, zip_filename)
            parquet_path = os.path.join(self.output_dir, zip_filename.replace('.zip', '.parquet'))
            
            try:
                # APRI il file ZIP e leggi le prime righe MANUALMENTE
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    csv_name = [f for f in zf.namelist() if f.endswith('.csv')][0]
                    
                    with zf.open(csv_name) as f:
                        # Leggi la PRIMA riga
                        first_line = f.readline().decode('utf-8').strip()
                        
                        # Torna all'inizio del file
                        f.seek(0)
                        
                        # VERIFICA REALE: la prima riga è un timestamp o un header?
                        is_header = False
                        first_value = first_line.split(',')[0] if first_line else ""
                        
                        try:
                            # Se possiamo convertire a float, è probabilmente un timestamp
                            float(first_value)
                            is_header = False  # È un numero → NO HEADER
                        except ValueError:
                            # Se non è convertibile, probabilmente è un header
                            is_header = True   # È testo → HEADER
                
                # Ora leggi il CSV con la giusta impostazione
                if is_header:
                    df = pd.read_csv(zip_path, compression='zip', header=0)
                    print(f"✓ {zip_filename}: VERO HEADER - colonne: {list(df.columns)}")
                    
                    # Normalizza nomi colonne
                    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
                    
                    # Rinomina se necessario
                    rename_map = {}
                    if 'open_time' in df.columns:
                        rename_map['open_time'] = 'timestamp'

                    # Do not rename these columns
                    # if 'count' in df.columns:
                    #     rename_map['count'] = 'trades'
                    # if 'taker_buy_volume' in df.columns:
                    #     rename_map['taker_buy_volume'] = 'taker_buy_base'
                    # if 'taker_buy_quote_volume' in df.columns:
                    #     rename_map['taker_buy_quote_volume'] = 'taker_buy_quote'
                    
                    if rename_map:
                        df.rename(columns=rename_map, inplace=True)
                        
                else:
                    df = pd.read_csv(zip_path, compression='zip', header=None)
                    print(f"✓ {zip_filename}: NO HEADER - {len(df.columns)} colonne")
                    
                    # Assegna nomi standard
                    if len(df.columns) >= 12:
                        df = df.iloc[:, :12]  # Prendi prime 12 colonne
                        df.columns = [
                            'timestamp', 'open', 'high', 'low', 'close', 'volume',
                            'close_time', 'quote_volume', 'count', 'taker_buy_volume',
                            'taker_buy_quote_volume', 'ignore'
                        ]
                    else:
                        print(f"⚠️  {zip_filename}: Solo {len(df.columns)} colonne")
                
                # CONVERTI TIMESTAMP
                # Assicurati che la colonna timestamp esista
                if 'timestamp' not in df.columns:
                    # Cerca qualsiasi colonna che contenga 'time'
                    for col in df.columns:
                        if 'time' in col.lower() and col != 'close_time':
                            df.rename(columns={col: 'timestamp'}, inplace=True)
                            break
                
                # Converti
                df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
                df = df[df['timestamp'].notna()].copy()
                
                if len(df) == 0:
                    print(f"✗ {zip_filename}: Nessun dato valido dopo pulizia")
                    error_count += 1
                    continue
                
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('datetime', inplace=True)
                df = df.sort_index()
                
                # Salva
                df.to_parquet(parquet_path, compression='snappy')
                success_count += 1
                
                # Dimensione
                file_size = os.path.getsize(parquet_path) / 1024 / 1024
                print(f"   ✅ Salvato ({file_size:.2f} MB, {len(df):,} righe)")
                
                if delete_zip:
                    os.remove(zip_path)
                    
            except Exception as e:
                print(f"✗ {zip_filename}: Errore - {str(e)[:80]}")
                import traceback
                traceback.print_exc()
                error_count += 1
        
        # Statistiche
        print(f"\n{'='*60}")
        print(f"✅ Successo: {success_count}/{len(zip_files)}")
        print(f"❌ Errori: {error_count}/{len(zip_files)}")
        print(f"{'='*60}")


def main():
    """Funzione principale con interfaccia da riga di comando"""
    parser = argparse.ArgumentParser(
        description='Scarica dati storici da Binance e convertili in Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Scarica tutto
  python script.py
  
  # Scarica solo 2024 e 2025
  python script.py --years 2024 2025
  
  # Scarica solo gennaio e febbraio
  python script.py --months 1 2
  
  # Scarica ETHUSDT 5m
  python script.py --symbol ETHUSDT --interval 5m --years 2024
  
  # Download veloce con 10 thread
  python script.py --years 2024 2025 --workers 10
  
  # Solo download, senza conversione
  python script.py --no-convert
  
  # Elimina file ZIP dopo conversione
  python script.py --delete-zip
        """
    )
    
    parser.add_argument('--symbol', default='BTCUSDT',
                       help='Coppia di trading (default: BTCUSDT)')
    parser.add_argument('--interval', default='1m',
                       help='Intervallo temporale (default: 1m)')
    parser.add_argument('--data-type', default='futures/um',
                       choices=['futures/um', 'spot', 'futures/cm'],
                       help='Tipo di dati (default: futures/um)')
    parser.add_argument('--frequency', default='monthly',
                       choices=['monthly', 'daily'],
                       help='Frequenza (default: monthly)')
    parser.add_argument('--years', type=int, nargs='+',
                       help='Anni da scaricare (es: 2023 2024)')
    parser.add_argument('--months', type=int, nargs='+', choices=range(1, 13),
                       help='Mesi da scaricare (1-12)')
    parser.add_argument('--workers', type=int, default=5,
                       help='Thread per download (default: 5)')
    parser.add_argument('--no-convert', action='store_true',
                       help='Non convertire in Parquet')
    parser.add_argument('--delete-zip', action='store_true',
                       help='Elimina file ZIP dopo conversione')
    parser.add_argument('--output-dir', default='binance_data',
                       help='Directory di output (default: binance_data)')
    
    args = parser.parse_args()
    
    print(f"""
    ╔══════════════════════════════════════════════╗
    ║          BINANCE DATA DOWNLOADER             ║
    ╠══════════════════════════════════════════════╣
    ║ Simbolo:    {args.symbol:>30} ║
    ║ Intervallo: {args.interval:>30}   ║
    ║ Tipo:       {args.data_type:>30}  ║
    ║ Frequenza:  {args.frequency:>30}  ║
    ║ Anni:       {str(args.years if args.years else 'Tutti'):>30}  ║
    ║ Mesi:       {str(args.months if args.months else 'Tutti'):>30}    ║
    ║ Thread:     {args.workers:>30}    ║
    ╚══════════════════════════════════════════════╝
    """)
    
    # Crea downloader
    downloader = BinanceDataDownloader(
        symbol=args.symbol,
        interval=args.interval,
        data_type=args.data_type,
        frequency=args.frequency,
        years_filter=args.years,
        months_filter=args.months,
        download_dir=os.path.join(args.output_dir, "zips"),
        output_dir=os.path.join(args.output_dir, "parquet")
    )
    
    # Download
    downloaded = downloader.download_all(max_workers=args.workers)
    
    if downloaded and not args.no_convert:
        # Conversione in Parquet
        downloader.extract_to_parquet(delete_zip=args.delete_zip)
    
    print("\n🎉 OPERAZIONE COMPLETATA!")


if __name__ == "__main__":
    main()