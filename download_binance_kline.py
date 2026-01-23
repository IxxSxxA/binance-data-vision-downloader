# download_binance_kline.py

import requests
import re
import os
import sys
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
import argparse
import json
import time
from pathlib import Path
import shutil


class BinanceDataDownloader:
    def __init__(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
        data_type: str = "futures/um",
        frequency: str = "monthly",
        years_filter: Optional[List[int]] = None,
        months_filter: Optional[List[int]] = None,
        days_filter: Optional[List[int]] = None,
        download_dir: str = "downloads",
        output_dir: str = "parquet_data",
        config_file: str = "binance_config.json",
    ):
        self.symbol = symbol
        self.interval = interval
        self.data_type = data_type
        self.frequency = frequency
        self.years_filter = years_filter
        self.months_filter = months_filter
        self.days_filter = days_filter
        self.download_dir = download_dir
        self.output_dir = output_dir
        self.config_file = config_file

        # URL base
        self.base_url = "https://data.binance.vision/"

        # Cache per evitare richieste ripetute
        self.symbol_start_year = None

        # Configurazione utente
        self.config = self.load_config()

        # Crea directory
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

    def load_config(self) -> Dict:
        """Carica configurazione da file o crea default"""
        default_config = {
            "delete_zip_after_conversion": False,
            "always_ask_delete_zip": True,
            "max_download_workers": 5,
            "download_retries": 3,
            "preferred_frequency": "monthly",
            "last_download": {},
            "skip_existing_files": True,
            "smart_year_detection": True,
            "symbol_start_dates": {},
        }

        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r") as f:
                    config = json.load(f)
                    # Merge con default per chiavi mancanti
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
        except:
            pass

        return default_config

    def save_config(self):
        """Salva configurazione su file"""
        try:
            with open(self.config_file, "w") as f:
                json.dump(self.config, f, indent=2)
        except:
            print(f"⚠️  Impossibile salvare configurazione su {self.config_file}")

    def get_symbol_start_year(self) -> Optional[int]:
        """
        Determina l'anno di inizio del trading per questo simbolo.
        Usa un approccio binario per trovare il primo anno con dati disponibili.
        """
        # Controlla cache nella configurazione
        cache_key = f"{self.symbol}_{self.data_type}"
        if cache_key in self.config.get("symbol_start_dates", {}):
            cached_year = self.config["symbol_start_dates"][cache_key]
            if cached_year:
                return int(cached_year)

        # Se disabilitato o simbolo noto (BTC, ETH), usa valori predefiniti
        if not self.config.get("smart_year_detection", True):
            return 2020

        known_early_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT"]
        if self.symbol in known_early_symbols:
            return 2020

        print(f"🔍 Ricerca anno di inizio per {self.symbol}...")

        # Prova anni recenti prima (approccio più efficiente)
        current_year = datetime.now().year
        test_years = list(
            range(current_year, 2019, -1)
        )  # Dal più recente al più vecchio

        for year in test_years:
            # Prova con un URL di esempio (primo mese)
            test_url = (
                f"{self.base_url}data/{self.data_type}/monthly/"
                f"klines/{self.symbol}/{self.interval}/{self.symbol}-{self.interval}-{year}-01.zip"
            )

            if self.check_file_exists(test_url):
                print(f"✅ {self.symbol} disponibile dal {year}")
                # Salva in cache
                if "symbol_start_dates" not in self.config:
                    self.config["symbol_start_dates"] = {}
                self.config["symbol_start_dates"][cache_key] = year
                self.save_config()
                return year

        # Se non trova nulla, prova l'anno corrente
        print(
            f"⚠️  Anno di inizio non determinato per {self.symbol}, uso {current_year}"
        )
        return current_year

    def get_zip_links_fallback(self) -> List[str]:
        """
        Costruisci URL manualmente basandoti su anni/mesi/giorni.
        Supporta sia monthly che daily.
        """
        print("🔗 Costruzione URL manuale...")

        current_year = datetime.now().year
        current_month = datetime.now().month
        current_day = datetime.now().day

        # Determina anno di inizio SMART
        start_year = self.get_symbol_start_year()

        # Range di anni
        if self.years_filter:
            years = sorted(self.years_filter)
            # Filtra per anni successivi all'inizio
            years = [y for y in years if y >= start_year]
        else:
            # Usa solo anni da start_year in poi
            years = list(range(start_year, current_year + 1))

        if not years:
            print(f"⚠️  Nessun anno valido per {self.symbol} (inizio: {start_year})")
            return []

        generated_links = []

        if self.frequency == "monthly":
            if self.months_filter:
                months = sorted(self.months_filter)
            else:
                months = list(range(1, 13))

            for year in years:
                for month in months:
                    # Non generare URL per mesi futuri
                    if year == current_year and month > current_month:
                        continue

                    # Per anni precedenti, tutti i mesi sono validi
                    month_str = f"{month:02d}"
                    filename = f"{self.symbol}-{self.interval}-{year}-{month_str}.zip"
                    full_url = (
                        f"{self.base_url}data/{self.data_type}/{self.frequency}/"
                        f"klines/{self.symbol}/{self.interval}/{filename}"
                    )
                    generated_links.append(full_url)

        elif self.frequency == "daily":
            if self.months_filter:
                months = sorted(self.months_filter)
            else:
                months = list(range(1, 13))

            if self.days_filter:
                days = sorted(self.days_filter)
            else:
                days = list(range(1, 32))

            for year in years:
                # Per daily, considera solo l'anno corrente per simboli nuovi
                # (per evitare troppi URL)
                if year < current_year and start_year == current_year:
                    continue

                for month in months:
                    # Non generare URL per mesi futuri
                    if year == current_year and month > current_month:
                        continue

                    for day in days:
                        # Controlla se il giorno è valido per il mese
                        try:
                            datetime(year, month, day)
                        except ValueError:
                            continue

                        # Non generare URL per giorni futuri
                        if (
                            year == current_year
                            and month == current_month
                            and day > current_day
                        ):
                            continue

                        # Per simboli molto nuovi, limitati ai primi mesi
                        if start_year == current_year and month < current_month - 3:
                            continue

                        month_str = f"{month:02d}"
                        day_str = f"{day:02d}"
                        filename = f"{self.symbol}-{self.interval}-{year}-{month_str}-{day_str}.zip"
                        full_url = (
                            f"{self.base_url}data/{self.data_type}/{self.frequency}/"
                            f"klines/{self.symbol}/{self.interval}/{filename}"
                        )
                        generated_links.append(full_url)

        print(
            f"📁 Generati {len(generated_links)} URL per il download ({self.frequency})"
        )
        print(f"   Anni: {years[0]}-{years[-1]}")

        if self.frequency == "daily" and len(generated_links) > 100:
            print(
                f"💡 Suggerimento: {len(generated_links)} URL sono tanti per il daily."
            )
            print(
                "   Considera di usare --frequency monthly o filtrare per mesi/giorni specifici."
            )

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

        # Se ci sono troppi URL, chiedi conferma
        if len(all_urls) > 100 and self.frequency == "daily":
            print(f"\n⚠️  ATTENZIONE: {len(all_urls)} URL da verificare.")
            print("   Questo potrebbe richiedere del tempo.")

            if not self.config.get("skip_verification_prompt", False):
                response = input("   Continuare? (s/n/always): ").lower().strip()
                if response == "n":
                    return []
                elif response == "always":
                    self.config["skip_verification_prompt"] = True
                    self.save_config()

        # Se skip_existing_files è True, controlla prima localmente
        existing_files = []
        if self.config.get("skip_existing_files", True):
            existing_files = self.get_existing_files()
            if existing_files:
                print(f"📊 {len(existing_files)} file già presenti localmente")

        # Verifica in parallelo quali URL esistono realmente
        valid_urls = []

        # Se abbiamo troppi URL, usa un approccio più intelligente
        if len(all_urls) > 500:
            print(f"🔎 Verifica intelligente di {len(all_urls)} URL...")

            # Prima verifica un campione per determinare pattern
            sample_size = min(50, len(all_urls))
            sample_urls = all_urls[:sample_size]

            with ThreadPoolExecutor(max_workers=10) as executor:
                sample_results = list(executor.map(self.check_file_exists, sample_urls))

            # Se nessuno del campione esiste, probabilmente il simbolo non ha dati per quel periodo
            if not any(sample_results):
                print(
                    f"⚠️  Nessun dato trovato nel campione. Simbolo potrebbe non avere dati per questo periodo."
                )

                # Prova un approccio diverso: cerca il file più recente
                print("🔄 Tentativo ricerca file più recente...")
                recent_urls = all_urls[-100:]  # Ultimi 100 URL (più recenti)

                with ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_url = {
                        executor.submit(self.check_file_exists, url): url
                        for url in recent_urls
                    }

                    for future in tqdm(
                        as_completed(future_to_url),
                        total=len(future_to_url),
                        desc="Verifica file recenti",
                        unit="file",
                    ):
                        url = future_to_url[future]
                        if future.result():
                            valid_urls.append(url)

                if valid_urls:
                    print(f"✅ Trovati {len(valid_urls)} file recenti")
                    return valid_urls
                else:
                    return []

        print(f"🔎 Verifico {len(all_urls)} URL...")

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {}
            for url in all_urls:
                # Verifica se il file esiste già localmente
                filename = os.path.basename(urlparse(url).path)
                if filename in existing_files:
                    print(f"⏭️  Saltato (esiste): {filename}")
                    continue

                future = executor.submit(self.check_file_exists, url)
                future_to_url[future] = url

            for future in tqdm(
                as_completed(future_to_url),
                total=len(future_to_url),
                desc="Verifica esistenza file",
                unit="file",
                bar_format="{l_bar}{bar:30}{r_bar}{bar:-30b}",
            ):
                url = future_to_url[future]
                if future.result():
                    valid_urls.append(url)

        print(f"✅ File disponibili per download: {len(valid_urls)}/{len(all_urls)}")
        return valid_urls

    def get_existing_files(self) -> List[str]:
        """Ottieni lista di file già scaricati"""
        existing = []
        if os.path.exists(self.download_dir):
            for f in os.listdir(self.download_dir):
                if f.endswith(".zip"):
                    # Verifica dimensione minima
                    filepath = os.path.join(self.download_dir, f)
                    if os.path.getsize(filepath) > 1024:  # Almeno 1KB
                        existing.append(f)
        return existing

    def download_file_with_retry(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Scarica un singolo file con retry"""
        filename = os.path.basename(urlparse(url).path)

        for attempt in range(max_retries):
            try:
                result = self.download_file(url, attempt + 1)
                if result:
                    return result
                elif attempt < max_retries - 1:
                    print(f"🔄 Tentativo {attempt + 2}/{max_retries} per {filename}")
                    time.sleep(2**attempt)  # Exponential backoff
            except Exception as e:
                print(f"❌ Errore tentativo {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)

        return None

    def download_file(self, url: str, attempt: int = 1) -> Optional[str]:
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

            print(f"⬇️  Scaricando (tentativo {attempt}): {filename}")

            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36"
            }

            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            # Crea una directory temporanea per il download
            temp_filepath = filepath + ".tmp"

            with open(temp_filepath, "wb") as f, tqdm(
                desc=f"📥 {filename[:20]}",
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                bar_format="{l_bar}{bar:30}{r_bar}{bar:-30b}",
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            # Rinomina temp a file finale
            os.rename(temp_filepath, filepath)

            # Verifica integrità
            final_size = os.path.getsize(filepath)
            if final_size < 1024:
                print(f"⚠️  File troppo piccolo, eliminato: {filename}")
                os.remove(filepath)
                return None

            # Aggiorna configurazione
            self.config["last_download"][filename] = datetime.now().isoformat()
            self.save_config()

            print(f"✅ Completato: {filename} ({final_size/1024/1024:.1f} MB)")
            return filepath

        except requests.exceptions.Timeout:
            print(f"⏰ Timeout per: {filename}")
            return None
        except Exception as e:
            print(f"❌ Errore: {e}")
            return None

    def download_all(self, max_workers: int = None) -> List[str]:
        """Scarica tutti i file"""
        if max_workers is None:
            max_workers = self.config.get("max_download_workers", 5)

        zip_urls = self.get_zip_links()

        if not zip_urls:
            print("⚠️  Nessun file disponibile per il download!")
            return []

        print(f"\n🚀 Avvio download di {len(zip_urls)} file...")
        print(f"   Thread: {max_workers}")
        print(f"   Tentativi per file: {self.config.get('download_retries', 3)}")

        downloaded_files = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for url in zip_urls:
                future = executor.submit(
                    self.download_file_with_retry,
                    url,
                    self.config.get("download_retries", 3),
                )
                futures[future] = url

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="📊 Progresso download",
                unit="file",
                bar_format="{l_bar}{bar:30}{r_bar}{bar:-30b}",
            ):
                result = future.result()
                if result:
                    downloaded_files.append(result)

        # Statistiche
        total_size = sum(
            os.path.getsize(f) for f in downloaded_files if os.path.exists(f)
        )

        print(f"\n{'='*50}")
        print("📊 DOWNLOAD COMPLETATO")
        print(f"{'='*50}")
        print(f"✅ File scaricati: {len(downloaded_files)}/{len(zip_urls)}")
        print(f"💾 Dimensione totale: {total_size/1024/1024:.1f} MB")
        print(f"📁 Directory: {os.path.abspath(self.download_dir)}")
        print(f"{'='*50}")

        return downloaded_files

    def ask_delete_zip_files(self) -> bool:
        """Chiede all'utente se eliminare i file ZIP"""
        if not self.config.get("always_ask_delete_zip", True):
            return self.config.get("delete_zip_after_conversion", False)

        zip_files = [f for f in os.listdir(self.download_dir) if f.endswith(".zip")]
        if not zip_files:
            return False

        total_size = (
            sum(os.path.getsize(os.path.join(self.download_dir, f)) for f in zip_files)
            / 1024
            / 1024
        )

        print(f"\n📦 Hai {len(zip_files)} file ZIP ({total_size:.1f} MB)")

        while True:
            response = (
                input("🗑️  Vuoi eliminare i file ZIP dopo la conversione? (s/n/auto): ")
                .lower()
                .strip()
            )

            if response == "s":
                # Salva preferenza
                self.config["delete_zip_after_conversion"] = True
                self.config["always_ask_delete_zip"] = False
                self.save_config()
                return True

            elif response == "n":
                # Salva preferenza
                self.config["delete_zip_after_conversion"] = False
                self.config["always_ask_delete_zip"] = False
                self.save_config()
                return False

            elif response == "auto":
                # Usa impostazione automatica basata su spazio disco
                import shutil

                total, used, free = shutil.disk_usage(self.download_dir)
                free_gb = free / (1024**3)

                if free_gb < 5:  # Meno di 5GB liberi
                    print(
                        f"⚠️  Spazio disco limitato ({free_gb:.1f} GB liberi). Elimino automaticamente."
                    )
                    self.config["delete_zip_after_conversion"] = True
                    self.config["always_ask_delete_zip"] = (
                        True  # Chiedi ancora in futuro
                    )
                    self.save_config()
                    return True
                else:
                    print(
                        f"✅ Spazio disco sufficiente ({free_gb:.1f} GB liberi). Mantengo i file."
                    )
                    self.config["delete_zip_after_conversion"] = False
                    self.config["always_ask_delete_zip"] = (
                        True  # Chiedi ancora in futuro
                    )
                    self.save_config()
                    return False

            else:
                print("⚠️  Risposta non valida. Usa 's', 'n' o 'auto'")

    def extract_to_parquet(self, delete_zip: bool = None):
        """Versione che VERIFICA correttamente se c'è header"""

        zip_files = sorted(
            [f for f in os.listdir(self.download_dir) if f.endswith(".zip")]
        )

        if not zip_files:
            print("⚠️  Nessun file ZIP trovato!")
            return

        # Chiedi all'utente se eliminare i file ZIP
        if delete_zip is None:
            delete_zip = self.ask_delete_zip_files()
        else:
            # Forza l'eliminazione se specificato
            if delete_zip:
                print("🗑️  Eliminazione file ZIP abilitata (forzata da parametro)")

        print(f"\n🔄 Conversione {len(zip_files)} file in Parquet...")

        success_count = 0
        error_count = 0
        skipped_count = 0

        for zip_filename in tqdm(zip_files, desc="Conversione"):
            zip_path = os.path.join(self.download_dir, zip_filename)
            parquet_filename = zip_filename.replace(".zip", ".parquet")
            parquet_path = os.path.join(self.output_dir, parquet_filename)

            # Verifica se il file Parquet esiste già
            if os.path.exists(parquet_path):
                parquet_size = os.path.getsize(parquet_path)
                zip_size = os.path.getsize(zip_path)

                # Se il Parquet è significativamente più piccolo dello ZIP,
                # probabilmente è corrotto o incompleto
                if (
                    parquet_size > zip_size * 0.1
                ):  # Almeno 10% della dimensione originale
                    print(f"⏭️  Saltato (Parquet esiste): {parquet_filename}")
                    if delete_zip:
                        os.remove(zip_path)
                        print(f"🗑️  Eliminato ZIP: {zip_filename}")
                    skipped_count += 1
                    continue

            try:
                # APRI il file ZIP e leggi le prime righe MANUALMENTE
                with zipfile.ZipFile(zip_path, "r") as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
                    if not csv_files:
                        print(f"⚠️  Nessun file CSV in {zip_filename}")
                        error_count += 1
                        continue

                    csv_name = csv_files[0]

                    with zf.open(csv_name) as f:
                        # Leggi la PRIMA riga
                        first_line = f.readline().decode("utf-8").strip()

                        # Torna all'inizio del file
                        f.seek(0)

                        # VERIFICA REALE: la prima riga è un timestamp o un header?
                        is_header = False
                        first_value = first_line.split(",")[0] if first_line else ""

                        try:
                            # Se possiamo convertire a float, è probabilmente un timestamp
                            float(first_value)
                            is_header = False  # È un numero → NO HEADER
                        except ValueError:
                            # Se non è convertibile, probabilmente è un header
                            is_header = True  # È testo → HEADER

                # Ora leggi il CSV con la giusta impostazione
                if is_header:
                    df = pd.read_csv(zip_path, compression="zip", header=0)

                    # Normalizza nomi colonne
                    df.columns = [
                        col.strip().lower().replace(" ", "_") for col in df.columns
                    ]

                    # Rinomina se necessario
                    rename_map = {}
                    if "open_time" in df.columns:
                        rename_map["open_time"] = "timestamp"

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
                    df = pd.read_csv(zip_path, compression="zip", header=None)

                    # Assegna nomi standard
                    if len(df.columns) >= 12:
                        df = df.iloc[:, :12]  # Prendi prime 12 colonne
                        df.columns = [
                            "timestamp",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "close_time",
                            "quote_volume",
                            "count",
                            "taker_buy_volume",
                            "taker_buy_quote_volume",
                            "ignore",
                        ]
                    else:
                        print(f"⚠️  {zip_filename}: Solo {len(df.columns)} colonne")

                # CONVERTI TIMESTAMP
                # Assicurati che la colonna timestamp esista
                if "timestamp" not in df.columns:
                    # Cerca qualsiasi colonna che contenga 'time'
                    for col in df.columns:
                        if "time" in col.lower() and col != "close_time":
                            df.rename(columns={col: "timestamp"}, inplace=True)
                            break

                # Converti
                df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
                df = df[df["timestamp"].notna()].copy()

                if len(df) == 0:
                    print(f"✗ {zip_filename}: Nessun dato valido dopo pulizia")
                    error_count += 1
                    continue

                df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
                df.set_index("datetime", inplace=True)
                df = df.sort_index()

                # Salva
                df.to_parquet(parquet_path, compression="snappy")
                success_count += 1

                # Dimensione
                file_size = os.path.getsize(parquet_path) / 1024 / 1024
                print(
                    f"   ✅ {parquet_filename} ({file_size:.2f} MB, {len(df):,} righe)"
                )

                if delete_zip:
                    os.remove(zip_path)
                    print(f"   🗑️  Eliminato ZIP: {zip_filename}")

            except Exception as e:
                print(f"✗ {zip_filename}: Errore - {str(e)[:80]}")
                error_count += 1

        # Statistiche
        print(f"\n{'='*60}")
        print(f"📊 STATISTICHE CONVERSIONE")
        print(f"{'='*60}")
        print(f"✅ Successo: {success_count}/{len(zip_files)}")
        print(f"⏭️  Saltati: {skipped_count}/{len(zip_files)}")
        print(f"❌ Errori: {error_count}/{len(zip_files)}")
        print(f"{'='*60}")

        # Suggerimento per l'utente
        if success_count > 0 and delete_zip:
            print("\n💡 Suggerimento: I file ZIP sono stati eliminati.")
            print("   Per ricrearli, devi ridiscercare i dati.")
        elif success_count > 0 and not delete_zip:
            total_zip_size = (
                sum(
                    os.path.getsize(os.path.join(self.download_dir, f))
                    for f in zip_files
                )
                / 1024
                / 1024
                if zip_files
                else 0
            )
            print(
                f"\n💡 Suggerimento: Hai ancora {len(zip_files)} file ZIP ({total_zip_size:.1f} MB)"
            )
            print("   Puoi eliminarli manualmente o eseguire con --delete-zip")

    def interactive_mode(self):
        """Modalità interattiva per utenti meno esperti"""
        print("\n🎮 MODALITÀ INTERATTIVA")
        print("=" * 40)

        # Simbolo
        symbol = input(f"Simbolo (default: {self.symbol}): ").strip().upper()
        if symbol:
            self.symbol = symbol

        # Intervallo
        interval = input(f"Intervallo (default: {self.interval}): ").strip()
        if interval:
            self.interval = interval

        # Tipo dati
        print("\nTipo di dati:")
        print("1. Futures USD-M (futures/um)")
        print("2. Spot (spot)")
        print("3. Futures COIN-M (futures/cm)")
        data_type_choice = input(f"Scegli (default: {self.data_type}): ").strip()
        if data_type_choice == "1":
            self.data_type = "futures/um"
        elif data_type_choice == "2":
            self.data_type = "spot"
        elif data_type_choice == "3":
            self.data_type = "futures/cm"
        elif data_type_choice:
            self.data_type = data_type_choice

        # Frequenza
        print("\nFrequenza:")
        print("1. Mensile (monthly) - File più grandi, meno download")
        print("2. Giornaliera (daily) - File più piccoli, download più frequenti")
        freq_choice = input(f"Scegli (default: {self.frequency}): ").strip()
        if freq_choice == "1":
            self.frequency = "monthly"
            self.months_filter = None
            self.days_filter = None
        elif freq_choice == "2":
            self.frequency = "daily"

            # Chiedi giorni se daily
            days_input = input(
                "Giorni specifici? (es: 1 15 30 o lascia vuoto per tutti): "
            ).strip()
            if days_input:
                self.days_filter = [int(d) for d in days_input.split()]

        # Anni
        years_input = input("\nAnni (es: 2023 2024 o lascia vuoto per tutti): ").strip()
        if years_input:
            self.years_filter = [int(y) for y in years_input.split()]

        # Mesi (solo se monthly o se non è daily)
        if self.frequency == "monthly":
            months_input = input(
                "Mesi (1-12, es: 1 6 12 o lascia vuoto per tutti): "
            ).strip()
            if months_input:
                self.months_filter = [int(m) for m in months_input.split()]

        # Thread
        workers_input = input(
            f"\nThread per download (default: {self.config.get('max_download_workers', 5)}): "
        ).strip()
        if workers_input:
            self.config["max_download_workers"] = int(workers_input)
            self.save_config()

        # Download
        print(f"\n{'='*40}")
        print(f"RIEPILOGO:")
        print(f"Simbolo: {self.symbol}")
        print(f"Intervallo: {self.interval}")
        print(f"Tipo: {self.data_type}")
        print(f"Frequenza: {self.frequency}")
        if self.years_filter:
            print(f"Anni: {self.years_filter}")
        if self.months_filter:
            print(f"Mesi: {self.months_filter}")
        if self.days_filter:
            print(f"Giorni: {self.days_filter}")
        print(f"Thread: {self.config.get('max_download_workers', 5)}")
        print(f"{'='*40}")

        confirm = input("\nConfermi il download? (s/n): ").strip().lower()
        if confirm != "s":
            print("❌ Download annullato")
            return

        # Esegui download
        downloaded = self.download_all()

        if downloaded:
            convert = input("\nVuoi convertire in Parquet? (s/n): ").strip().lower()
            if convert == "s":
                self.extract_to_parquet()


def main():
    """Funzione principale con interfaccia da riga di comando"""
    parser = argparse.ArgumentParser(
        description="Scarica dati storici da Binance e convertili in Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Modalità interattiva
  python script.py --interactive
  
  # Scarica tutto (con rilevamento intelligente dell'anno di inizio)
  python script.py
  
  # Dati giornalieri per un nuovo simbolo
  python script.py --symbol XPLUSDT --frequency daily
  
  # Scarica solo 2025
  python script.py --years 2025
  
  # Scarica solo agosto e settembre 2025
  python script.py --months 8 9 --years 2025
  
  # Scarica ETHUSDT 5m (rileva automaticamente dal 2020)
  python script.py --symbol ETHUSDT --interval 5m
  
  # Download veloce con 10 thread
  python script.py --workers 10
  
  # Solo download, senza conversione
  python script.py --no-convert
  
  # Elimina file ZIP dopo conversione (chiede conferma)
  python script.py --delete-zip
  
  # Disabilita rilevamento intelligente dell'anno
  python script.py --no-smart-year
        """,
    )

    parser.add_argument(
        "--symbol", default="BTCUSDT", help="Coppia di trading (default: BTCUSDT)"
    )
    parser.add_argument(
        "--interval", default="1m", help="Intervallo temporale (default: 1m)"
    )
    parser.add_argument(
        "--data-type",
        default="futures/um",
        choices=["futures/um", "spot", "futures/cm"],
        help="Tipo di dati (default: futures/um)",
    )
    parser.add_argument(
        "--frequency",
        default="monthly",
        choices=["monthly", "daily"],
        help="Frequenza (default: monthly)",
    )
    parser.add_argument(
        "--years", type=int, nargs="+", help="Anni da scaricare (es: 2023 2024)"
    )
    parser.add_argument(
        "--months",
        type=int,
        nargs="+",
        choices=range(1, 13),
        help="Mesi da scaricare (1-12)",
    )
    parser.add_argument(
        "--days",
        type=int,
        nargs="+",
        choices=range(1, 32),
        help="Giorni da scaricare (1-31, solo per frequency=daily)",
    )
    parser.add_argument("--workers", type=int, help="Thread per download (default: 5)")
    parser.add_argument(
        "--no-convert", action="store_true", help="Non convertire in Parquet"
    )
    parser.add_argument(
        "--delete-zip",
        action="store_true",
        help="Elimina file ZIP dopo conversione (chiede conferma)",
    )
    parser.add_argument(
        "--force-delete-zip",
        action="store_true",
        help="Forza eliminazione ZIP senza chiedere",
    )
    parser.add_argument(
        "--keep-zip", action="store_true", help="Mantieni sempre i file ZIP"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Disabilita salto file esistenti",
    )
    parser.add_argument(
        "--no-smart-year",
        action="store_true",
        help="Disabilita rilevamento intelligente anno di inizio",
    )
    parser.add_argument(
        "--interactive", "-i", action="store_true", help="Avvia modalità interattiva"
    )
    parser.add_argument(
        "--output-dir",
        default="binance_data",
        help="Directory di output (default: binance_data)",
    )

    args = parser.parse_args()

    # Se modalità interattiva, esegui e termina
    if args.interactive:
        downloader = BinanceDataDownloader(
            symbol=args.symbol,
            interval=args.interval,
            data_type=args.data_type,
            frequency=args.frequency,
            years_filter=args.years,
            months_filter=args.months,
            days_filter=args.days,
            download_dir=os.path.join(args.output_dir, "zips"),
            output_dir=os.path.join(args.output_dir, "parquet"),
        )
        downloader.interactive_mode()
        return

    print(
        f"""
    ╔══════════════════════════════════════════════╗
    ║          BINANCE DATA DOWNLOADER             ║
    ╠══════════════════════════════════════════════╣
    ║ Simbolo:    {args.symbol:>30} ║
    ║ Intervallo: {args.interval:>30}   ║
    ║ Tipo:       {args.data_type:>30}  ║
    ║ Frequenza:  {args.frequency:>30}  ║
    ║ Anni:       {str(args.years if args.years else 'Tutti'):>30}  ║
    ║ Mesi:       {str(args.months if args.months else 'Tutti'):>30}    ║
    ║ Giorni:     {str(args.days if args.days else 'Tutti'):>30}    ║
    ║ Thread:     {str(args.workers if args.workers else 'Auto'):>30}    ║
    ║ Smart Year: {'No' if args.no_smart_year else 'Sì':>30}    ║
    ╚══════════════════════════════════════════════╝
    """
    )

    # Crea downloader
    downloader = BinanceDataDownloader(
        symbol=args.symbol,
        interval=args.interval,
        data_type=args.data_type,
        frequency=args.frequency,
        years_filter=args.years,
        months_filter=args.months,
        days_filter=args.days,
        download_dir=os.path.join(args.output_dir, "zips"),
        output_dir=os.path.join(args.output_dir, "parquet"),
    )

    # Imposta configurazione da parametri
    if args.no_skip_existing:
        downloader.config["skip_existing_files"] = False

    if args.no_smart_year:
        downloader.config["smart_year_detection"] = False

    if args.keep_zip:
        downloader.config["delete_zip_after_conversion"] = False
        downloader.config["always_ask_delete_zip"] = False

    if args.force_delete_zip:
        downloader.config["delete_zip_after_conversion"] = True
        downloader.config["always_ask_delete_zip"] = False

    downloader.save_config()

    # Download
    downloaded = downloader.download_all(
        max_workers=args.workers if args.workers else None
    )

    if downloaded and not args.no_convert:
        # Determina se eliminare i file ZIP
        delete_zip = None
        if args.delete_zip or args.force_delete_zip:
            delete_zip = True
        elif args.keep_zip:
            delete_zip = False

        # Conversione in Parquet
        downloader.extract_to_parquet(delete_zip=delete_zip)

    print("\n🎉 OPERAZIONE COMPLETATA!")


if __name__ == "__main__":
    main()
