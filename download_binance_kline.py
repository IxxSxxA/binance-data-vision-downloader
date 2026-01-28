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
import numpy as np


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
        base_dir: str = "binance_data",
        config_file: str = "binance_config.json",
    ):
        self.symbol = symbol
        self.interval = interval
        self.data_type = data_type
        self.frequency = frequency
        self.years_filter = years_filter
        self.months_filter = months_filter
        self.days_filter = days_filter
        self.base_dir = base_dir

        # Crea directory specifiche per il simbolo
        self.download_dir = os.path.join(base_dir, symbol, "zip_raw")
        self.output_dir = os.path.join(base_dir, symbol, "parquet_raw")
        self.consolidated_dir = os.path.join(base_dir, symbol, "parquet_consolidated")

        self.config_file = config_file

        # URL base
        self.base_url = "https://data.binance.vision/"

        # Cache per evitare richieste ripetute
        self.symbol_start_year = None

        # Configurazione utente
        self.config = self.load_config()

        # Crea directory
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.consolidated_dir, exist_ok=True)

    def load_config(self) -> Dict:
        """Carica configurazione da file o crea default"""
        default_config = {
            "delete_zip_after_conversion": False,
            "delete_raw_parquet_after_consolidation": False,
            "always_ask_deletion": True,
            "max_download_workers": 5,
            "download_retries": 3,
            "preferred_frequency": "monthly",
            "last_download": {},
            "skip_existing_files": True,
            "smart_year_detection": True,
            "symbol_start_dates": {},
            "auto_consolidate": True,
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
        Usa un approccio intelligente che cerca dati a partire da oggi all'indietro.
        """
        # Controlla cache nella configurazione
        cache_key = f"{self.symbol}_{self.data_type}"
        if cache_key in self.config.get("symbol_start_dates", {}):
            cached_year = self.config["symbol_start_dates"][cache_key]
            if cached_year:
                return int(cached_year)

        # Se disabilitato, usa valori predefiniti
        if not self.config.get("smart_year_detection", True):
            return 2020

        # Simboli conosciuti che hanno dati dal 2020
        known_early_symbols = [
            "BTCUSDT",
            "ETHUSDT",
            "BNBUSDT",
            "XRPUSDT",
            "ADAUSDT",
            "DOGEUSDT",
            "DOTUSDT",
            "SOLUSDT",
        ]
        if self.symbol in known_early_symbols:
            return 2020

        print(f"🔍 Ricerca intelligente anno di inizio per {self.symbol}...")

        current_year = datetime.now().year
        current_month = datetime.now().month

        # Strategia: partiamo dal mese corrente e andiamo all'indietro
        # Prima cerca dati mensili recenti, poi espandi la ricerca

        # Fase 1: Cerca nell'ultimo anno
        print(f"   Fase 1: Cerca dati recenti (ultimi 12 mesi)...")

        found_year = None
        found_month = None

        # Cerca per 12 mesi all'indietro dal corrente
        for month_offset in range(12):
            test_date = datetime.now() - timedelta(days=30 * month_offset)
            test_year = test_date.year
            test_month = test_date.month

            # Prova con URL mensile
            test_url = (
                f"{self.base_url}data/{self.data_type}/monthly/"
                f"klines/{self.symbol}/{self.interval}/{self.symbol}-{self.interval}-{test_year}-{test_month:02d}.zip"
            )

            if self.check_file_exists(test_url):
                print(f"   ✅ Trovati dati in {test_year}-{test_month:02d}")
                found_year = test_year
                found_month = test_month
                break

        if found_year:
            # Ora espandi la ricerca all'indietro dal primo mese trovato
            print(
                f"   Fase 2: Espandi ricerca all'indietro da {found_year}-{found_month:02d}..."
            )

            start_year = found_year
            for year in range(found_year - 1, 2016, -1):  # Cerca fino al 2017
                # Prova il primo mese dell'anno
                test_url = (
                    f"{self.base_url}data/{self.data_type}/monthly/"
                    f"klines/{self.symbol}/{self.interval}/{self.symbol}-{self.interval}-{year}-01.zip"
                )

                if not self.check_file_exists(test_url):
                    # Se non trovato, prova l'ultimo mese dell'anno
                    test_url = (
                        f"{self.base_url}data/{self.data_type}/monthly/"
                        f"klines/{self.symbol}/{self.interval}/{self.symbol}-{self.interval}-{year}-12.zip"
                    )

                    if not self.check_file_exists(test_url):
                        # Anno senza dati, fermati qui
                        print(f"   ⏹️  Nessun dato per {year}, fermo a {start_year}")
                        break

                start_year = year

            print(f"✅ {self.symbol} disponibile dal {start_year}")

            # Salva in cache
            if "symbol_start_dates" not in self.config:
                self.config["symbol_start_dates"] = {}
            self.config["symbol_start_dates"][cache_key] = start_year
            self.save_config()
            return start_year

        else:
            # Nessun dato trovato negli ultimi 12 mesi
            print(f"⚠️  Nessun dato trovato per {self.symbol} negli ultimi 12 mesi")

            # Prova una ricerca più aggressiva
            print(f"   Fase 3: Ricerca completa (2017-{current_year})...")

            for year in range(current_year, 2016, -1):
                # Prova un mese a caso (metà anno)
                test_url = (
                    f"{self.base_url}data/{self.data_type}/monthly/"
                    f"klines/{self.symbol}/{self.interval}/{self.symbol}-{self.interval}-{year}-06.zip"
                )

                if self.check_file_exists(test_url):
                    print(f"✅ Trovati dati in {year}")

                    # Salva in cache
                    if "symbol_start_dates" not in self.config:
                        self.config["symbol_start_dates"] = {}
                    self.config["symbol_start_dates"][cache_key] = year
                    self.save_config()
                    return year

            # Se tutto fallisce, usa l'anno corrente
            print(f"⚠️  Nessun dato trovato, uso {current_year} come fallback")
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

        # Determina anno di inizio SMART (migliorato)
        start_year = self.get_symbol_start_year()

        # Per simboli nuovi, assicurati di iniziare dall'anno giusto
        if start_year > current_year:
            start_year = current_year - 1 if current_year > 2020 else 2020
            print(f"   ⚠️  Anno di inizio corretto a {start_year}")

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

                    # Per gli anni precedenti a start_year, salta
                    if year < start_year:
                        continue

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

                # Salta anni precedenti a start_year
                if year < start_year:
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
        print(f"   Anno di inizio rilevato: {start_year}")

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

    def ask_deletion_preferences(
        self, zip_count: int, parquet_count: int
    ) -> Tuple[bool, bool]:
        """Chiede all'utente le preferenze di eliminazione"""
        if not self.config.get("always_ask_deletion", True):
            return (
                self.config.get("delete_zip_after_conversion", False),
                self.config.get("delete_raw_parquet_after_consolidation", False),
            )

        zip_size = 0
        if os.path.exists(self.download_dir):
            zip_files = [f for f in os.listdir(self.download_dir) if f.endswith(".zip")]
            zip_size = (
                sum(
                    os.path.getsize(os.path.join(self.download_dir, f))
                    for f in zip_files
                )
                / 1024
                / 1024
            )

        parquet_size = 0
        if os.path.exists(self.output_dir):
            parquet_files = [
                f for f in os.listdir(self.output_dir) if f.endswith(".parquet")
            ]
            parquet_size = (
                sum(
                    os.path.getsize(os.path.join(self.output_dir, f))
                    for f in parquet_files
                )
                / 1024
                / 1024
            )

        print(f"\n📦 Hai:")
        print(f"   {zip_count} file ZIP ({zip_size:.1f} MB)")
        print(f"   {parquet_count} file Parquet raw ({parquet_size:.1f} MB)")

        while True:
            print("\n🗑️  Opzioni di eliminazione:")
            print("   1. Mantieni tutti i file (ZIP + Parquet raw)")
            print("   2. Elimina solo ZIP dopo conversione")
            print("   3. Elimina tutto (ZIP + Parquet raw) dopo consolidamento")
            print("   4. Usa impostazioni automatiche basate su spazio disco")

            response = input("\n👉 Scegli un'opzione (1-4): ").strip()

            if response == "1":
                self.config["delete_zip_after_conversion"] = False
                self.config["delete_raw_parquet_after_consolidation"] = False
                self.config["always_ask_deletion"] = False
                self.save_config()
                return False, False

            elif response == "2":
                self.config["delete_zip_after_conversion"] = True
                self.config["delete_raw_parquet_after_consolidation"] = False
                self.config["always_ask_deletion"] = False
                self.save_config()
                return True, False

            elif response == "3":
                self.config["delete_zip_after_conversion"] = True
                self.config["delete_raw_parquet_after_consolidation"] = True
                self.config["always_ask_deletion"] = False
                self.save_config()
                return True, True

            elif response == "4":
                import shutil

                total, used, free = shutil.disk_usage(self.base_dir)
                free_gb = free / (1024**3)

                if free_gb < 10:  # Meno di 10GB liberi
                    print(
                        f"⚠️  Spazio disco limitato ({free_gb:.1f} GB liberi). Elimino automaticamente tutto."
                    )
                    self.config["delete_zip_after_conversion"] = True
                    self.config["delete_raw_parquet_after_consolidation"] = True
                    self.config["always_ask_deletion"] = True  # Chiedi ancora in futuro
                    self.save_config()
                    return True, True
                elif free_gb < 50:  # Tra 10GB e 50GB
                    print(
                        f"⚠️  Spazio disco moderato ({free_gb:.1f} GB liberi). Elimino solo ZIP."
                    )
                    self.config["delete_zip_after_conversion"] = True
                    self.config["delete_raw_parquet_after_consolidation"] = False
                    self.config["always_ask_deletion"] = True  # Chiedi ancora in futuro
                    self.save_config()
                    return True, False
                else:
                    print(
                        f"✅ Spazio disco sufficiente ({free_gb:.1f} GB liberi). Mantengo tutti i file."
                    )
                    self.config["delete_zip_after_conversion"] = False
                    self.config["delete_raw_parquet_after_consolidation"] = False
                    self.config["always_ask_deletion"] = True  # Chiedi ancora in futuro
                    self.save_config()
                    return False, False

            else:
                print("⚠️  Risposta non valida. Scegli un'opzione da 1 a 4")

    def extract_to_parquet(self, delete_zip: bool = None):
        """Versione che VERIFICA correttamente se c'è header"""

        zip_files = sorted(
            [f for f in os.listdir(self.download_dir) if f.endswith(".zip")]
        )

        if not zip_files:
            print("⚠️  Nessun file ZIP trovato!")
            return 0

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

        return success_count

    def analyze_parquet_files(self):
        """Analizza tutti i file Parquet e mostra statistiche utili"""

        parquet_files = sorted(
            [f for f in os.listdir(self.output_dir) if f.endswith(".parquet")]
        )

        if not parquet_files:
            print("⚠️  Nessun file Parquet trovato!")
            return []

        print(f"📁 Trovati {len(parquet_files)} file Parquet")
        print("=" * 60)

        all_data = []
        total_rows = 0
        total_size_mb = 0
        schemas_consistent = True

        # Analizza ogni file
        for i, filename in enumerate(parquet_files, 1):
            filepath = os.path.join(self.output_dir, filename)

            try:
                # Leggi il file
                df = pd.read_parquet(filepath)

                # Estrai informazioni
                file_info = {
                    "filename": filename,
                    "rows": len(df),
                    "size_mb": os.path.getsize(filepath) / 1024 / 1024,
                    "start_date": df.index.min(),
                    "end_date": df.index.max(),
                    "columns": list(df.columns),
                    "dtypes": dict(df.dtypes),
                    "index_type": type(df.index).__name__,
                }

                all_data.append(file_info)
                total_rows += file_info["rows"]
                total_size_mb += file_info["size_mb"]

                # Stampa info file
                print(f"{i:3d}. {filename}")
                print(f"     📊 Righe: {file_info['rows']:>7,}")
                print(f"     💾 Dimensione: {file_info['size_mb']:>6.2f} MB")
                print(
                    f"     📅 Periodo: {file_info['start_date'].date()} - {file_info['end_date'].date()}"
                )

                # Controllo qualità dati
                missing_values = df.isnull().sum().sum()
                if missing_values > 0:
                    print(f"     ⚠️  Valori mancanti: {missing_values}")

            except Exception as e:
                print(f"{i:3d}. {filename} - ❌ ERRORE: {str(e)[:80]}")
                all_data.append({"filename": filename, "error": str(e)})

        # ANALISI GENERALE
        print(f"\n{'='*60}")
        print("📊 ANALISI COMPLESSIVA")
        print(f"{'='*60}")

        if all_data and "rows" in all_data[0]:
            # Statistiche generali
            print(f"📈 Statistiche totali:")
            print(f"   Totale file: {len([d for d in all_data if 'rows' in d])}")
            print(f"   Totale righe: {total_rows:,}")
            print(f"   Dimensione totale: {total_size_mb:.2f} MB")
            print(f"   Media righe/file: {total_rows/len(all_data):,.0f}")
            print(f"   Media dimensione/file: {total_size_mb/len(all_data):.2f} MB")

            # Range date
            all_starts = [d["start_date"] for d in all_data if "start_date" in d]
            all_ends = [d["end_date"] for d in all_data if "end_date" in d]

            if all_starts and all_ends:
                overall_start = min(all_starts)
                overall_end = max(all_ends)
                print(f"\n📅 Copertura temporale complessiva:")
                print(f"   Da: {overall_start}")
                print(f"   A: {overall_end}")
                print(f"   Giorni totali: {(overall_end - overall_start).days:,}")

            # Verifica consistenza schema
            print(f"\n🔍 Verifica consistenza schema:")
            first_schema = all_data[0]
            inconsistencies = []

            for data in all_data[1:]:
                if "columns" in data:
                    if data["columns"] != first_schema["columns"]:
                        inconsistencies.append(f"Colonne diverse in {data['filename']}")
                    if data["index_type"] != first_schema["index_type"]:
                        inconsistencies.append(
                            f"Tipo indice diverso in {data['filename']}"
                        )

            if not inconsistencies:
                print("   ✅ Tutti i file hanno schema CONSISTENTE!")
                print(f"   🗂️  Schema standard:")
                print(f"      Indice: {first_schema['index_type']}")
                print(
                    f"      Colonne ({len(first_schema['columns'])}): {', '.join(first_schema['columns'])}"
                )
            else:
                print(f"   ❌ Trovate {len(inconsistencies)} inconsistenze!")
                for inc in inconsistencies[:3]:
                    print(f"      - {inc}")

            # Analisi buchi temporali
            print(f"\n🔎 Analisi continuità temporale:")

            # Crea lista di periodi ordinati
            periods = [
                (d["start_date"], d["end_date"], d["filename"])
                for d in all_data
                if "start_date" in d
            ]
            periods.sort(key=lambda x: x[0])  # Ordina per data inizio

            gaps = []
            for i in range(len(periods) - 1):
                current_end = periods[i][1]
                next_start = periods[i + 1][0]

                # Se c'è un gap maggiore di 2 minuti (120 secondi per dati 1m)
                gap_hours = (next_start - current_end).total_seconds() / 3600
                if gap_hours > 2 / 60:  # Più di 2 minuti
                    gaps.append(
                        {
                            "gap_hours": gap_hours,
                            "between": f"{periods[i][2]} e {periods[i+1][2]}",
                            "from": current_end,
                            "to": next_start,
                        }
                    )

            if gaps:
                print(f"   ⚠️  Trovati {len(gaps)} buchi temporali:")
                for gap in gaps[:5]:  # Mostra solo primi 5 buchi
                    print(f"      - {gap['gap_hours']:.2f} ore tra {gap['between']}")
                    print(f"        ({gap['from']} → {gap['to']})")
            else:
                print("   ✅ Nessun buco temporale significativo trovato!")

            # Statistiche per mese/anno
            print(f"\n📅 Distribuzione per anno:")
            years_data = {}
            for data in all_data:
                if "start_date" in data:
                    year = data["start_date"].year
                    if year not in years_data:
                        years_data[year] = {"files": 0, "rows": 0, "size_mb": 0}
                    years_data[year]["files"] += 1
                    years_data[year]["rows"] += data["rows"]
                    years_data[year]["size_mb"] += data["size_mb"]

            for year in sorted(years_data.keys()):
                print(
                    f"   {year}: {years_data[year]['files']:2d} file, "
                    f"{years_data[year]['rows']:8,} righe, "
                    f"{years_data[year]['size_mb']:6.1f} MB"
                )

        return all_data

    def create_master_file(self, output_filename="master_data.parquet"):
        """Crea un unico file Parquet con tutti i dati"""

        parquet_files = sorted(
            [f for f in os.listdir(self.output_dir) if f.endswith(".parquet")]
        )

        if not parquet_files:
            print("Nessun file da unire!")
            return None

        print(f"\n🔗 Unione di {len(parquet_files)} file in {output_filename}...")

        all_dfs = []
        for i, filename in enumerate(parquet_files, 1):
            filepath = os.path.join(self.output_dir, filename)
            try:
                df = pd.read_parquet(filepath)
                all_dfs.append(df)
                print(f"  {i:3d}. {filename} - {len(df):,} righe")
            except Exception as e:
                print(f"  {i:3d}. {filename} - ❌ Errore: {str(e)[:50]}")

        if not all_dfs:
            print("❌ Nessun DataFrame valido da unire!")
            return None

        master_df = pd.concat(all_dfs).sort_index()
        output_path = os.path.join(self.consolidated_dir, output_filename)
        master_df.to_parquet(output_path, compression="snappy")

        print(f"\n✅ File master creato: {output_path}")
        print(f"   Righe totali: {len(master_df):,}")
        print(f"   Periodo: {master_df.index.min()} - {master_df.index.max()}")
        print(f"   Dimensione: {os.path.getsize(output_path)/1024/1024:.2f} MB")

        return master_df

    def consolidate_data(self, delete_raw_parquet: bool = False):
        """Consolida tutti i dati in un unico file e chiede se eliminare i file raw"""

        print(f"\n🔗 CONSOLIDAMENTO DATI")
        print(f"=" * 60)

        # Analizza i file esistenti
        analysis = self.analyze_parquet_files()

        if not analysis:
            print("❌ Nessun dato da consolidare!")
            return

        # Chiedi conferma per il consolidamento
        if not self.config.get("auto_consolidate", True):
            confirm = (
                input("\nVuoi consolidare i dati in un unico file? (s/n): ")
                .strip()
                .lower()
            )
            if confirm != "s":
                print("❌ Consolidamento annullato")
                return

        # Crea il file master
        master_df = self.create_master_file()

        if master_df is not None:
            # Chiedi all'utente se eliminare i file raw
            if delete_raw_parquet:
                self.delete_raw_files()
            elif self.config.get("always_ask_deletion", True):
                print(f"\n📦 Hai {len(analysis)} file Parquet raw")
                raw_size = sum([d.get("size_mb", 0) for d in analysis])
                print(f"   Dimensione totale: {raw_size:.1f} MB")

                delete_raw = (
                    input("\n🗑️  Vuoi eliminare i file Parquet raw? (s/n): ")
                    .strip()
                    .lower()
                )
                if delete_raw == "s":
                    self.delete_raw_files()
                else:
                    print("✅ File raw mantenuti")

        return master_df

    def delete_raw_files(self):
        """Elimina tutti i file raw (ZIP e Parquet)"""

        # Elimina file ZIP
        zip_count = 0
        if os.path.exists(self.download_dir):
            zip_files = [f for f in os.listdir(self.download_dir) if f.endswith(".zip")]
            for zip_file in zip_files:
                os.remove(os.path.join(self.download_dir, zip_file))
                zip_count += 1

        # Elimina file Parquet raw
        parquet_count = 0
        if os.path.exists(self.output_dir):
            parquet_files = [
                f for f in os.listdir(self.output_dir) if f.endswith(".parquet")
            ]
            for parquet_file in parquet_files:
                os.remove(os.path.join(self.output_dir, parquet_file))
                parquet_count += 1

        print(f"\n🗑️  File eliminati:")
        print(f"   ZIP: {zip_count} file")
        print(f"   Parquet raw: {parquet_count} file")
        print(f"✅ Solo il file consolidato rimane in: {self.consolidated_dir}")

    def interactive_mode(self):
        """Modalità interattiva per utenti meno esperti"""
        print("\n🎮 MODALITÀ INTERATTIVA")
        print("=" * 40)

        # Simbolo
        symbol = input(f"Simbolo (default: {self.symbol}): ").strip().upper()
        if symbol:
            self.symbol = symbol

        # Aggiorna directory con nuovo simbolo
        self.download_dir = os.path.join(self.base_dir, self.symbol, "zip_raw")
        self.output_dir = os.path.join(self.base_dir, self.symbol, "parquet_raw")
        self.consolidated_dir = os.path.join(
            self.base_dir, self.symbol, "parquet_consolidated"
        )

        # Crea directory se necessario
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.consolidated_dir, exist_ok=True)

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
        print(f"Directory download: {self.download_dir}")
        print(f"{'='*40}")

        confirm = input("\nConfermi il download? (s/n): ").strip().lower()
        if confirm != "s":
            print("❌ Download annullato")
            return

        # Esegui download
        downloaded = self.download_all()

        if downloaded:
            # Conversione in Parquet
            delete_zip, delete_raw = self.ask_deletion_preferences(len(downloaded), 0)
            success_count = self.extract_to_parquet(delete_zip)

            if success_count > 0:
                # Consolida i dati
                self.consolidate_data(delete_raw)

        print("\n🎉 OPERAZIONE COMPLETATA!")


def main():
    """Funzione principale"""

    print(
        f"""
    ╔══════════════════════════════════════════════╗
    ║          BINANCE DATA DOWNLOADER             ║
    ║           (Modalità Interattiva)             ║
    ╚══════════════════════════════════════════════╝
    """
    )

    # Avvia sempre in modalità interattiva
    downloader = BinanceDataDownloader(
        symbol="BTCUSDT",
        interval="1m",
        data_type="futures/um",
        frequency="monthly",
        base_dir="binance_data",
    )

    downloader.interactive_mode()


if __name__ == "__main__":
    main()
