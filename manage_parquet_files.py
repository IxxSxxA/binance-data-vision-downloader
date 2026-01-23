# check_parquet_consistency.py
import os
import pandas as pd
import glob
from datetime import datetime
import numpy as np

# Set here the path to check
FOLDER_TO_CHECK = 'binance_data/parquet'


def analyze_parquet_files(folder_path):
    """Analizza tutti i file Parquet e mostra statistiche utili"""
    
    parquet_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.parquet')])
    
    if not parquet_files:
        print("⚠️  Nessun file Parquet trovato!")
        return
    
    print(f"📁 Trovati {len(parquet_files)} file Parquet")
    print("="*60)
    
    all_data = []
    total_rows = 0
    total_size_mb = 0
    schemas_consistent = True
    
    # Analizza ogni file
    for i, filename in enumerate(parquet_files, 1):
        filepath = os.path.join(folder_path, filename)
        
        try:
            # Leggi il file
            df = pd.read_parquet(filepath)
            
            # Estrai informazioni
            file_info = {
                'filename': filename,
                'rows': len(df),
                'size_mb': os.path.getsize(filepath) / 1024 / 1024,
                'start_date': df.index.min(),
                'end_date': df.index.max(),
                'columns': list(df.columns),
                'dtypes': dict(df.dtypes),
                'index_type': type(df.index).__name__
            }
            
            all_data.append(file_info)
            total_rows += file_info['rows']
            total_size_mb += file_info['size_mb']
            
            # Stampa info file
            print(f"{i:3d}. {filename}")
            print(f"     📊 Righe: {file_info['rows']:>7,}")
            print(f"     💾 Dimensione: {file_info['size_mb']:>6.2f} MB")
            print(f"     📅 Periodo: {file_info['start_date'].date()} - {file_info['end_date'].date()}")
            
            # Controllo qualità dati
            missing_values = df.isnull().sum().sum()
            if missing_values > 0:
                print(f"     ⚠️  Valori mancanti: {missing_values}")
            
        except Exception as e:
            print(f"{i:3d}. {filename} - ❌ ERRORE: {str(e)[:80]}")
            all_data.append({
                'filename': filename,
                'error': str(e)
            })
    
    # ANALISI GENERALE
    print(f"\n{'='*60}")
    print("📊 ANALISI COMPLESSIVA")
    print(f"{'='*60}")
    
    if all_data and 'rows' in all_data[0]:
        # Statistiche generali
        print(f"📈 Statistiche totali:")
        print(f"   Totale file: {len([d for d in all_data if 'rows' in d])}")
        print(f"   Totale righe: {total_rows:,}")
        print(f"   Dimensione totale: {total_size_mb:.2f} MB")
        print(f"   Media righe/file: {total_rows/len(all_data):,.0f}")
        print(f"   Media dimensione/file: {total_size_mb/len(all_data):.2f} MB")
        
        # Range date
        all_starts = [d['start_date'] for d in all_data if 'start_date' in d]
        all_ends = [d['end_date'] for d in all_data if 'end_date' in d]
        
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
            if 'columns' in data:
                if data['columns'] != first_schema['columns']:
                    inconsistencies.append(f"Colonne diverse in {data['filename']}")
                if data['index_type'] != first_schema['index_type']:
                    inconsistencies.append(f"Tipo indice diverso in {data['filename']}")
        
        if not inconsistencies:
            print("   ✅ Tutti i file hanno schema CONSISTENTE!")
            print(f"   🗂️  Schema standard:")
            print(f"      Indice: {first_schema['index_type']}")
            print(f"      Colonne ({len(first_schema['columns'])}): {', '.join(first_schema['columns'])}")
        else:
            print(f"   ❌ Trovate {len(inconsistencies)} inconsistenze!")
            for inc in inconsistencies[:3]:
                print(f"      - {inc}")
        
        # Analisi buchi temporali
        print(f"\n🔎 Analisi continuità temporale:")
        
        # Crea lista di periodi ordinati
        periods = [(d['start_date'], d['end_date'], d['filename']) 
                  for d in all_data if 'start_date' in d]
        periods.sort(key=lambda x: x[0])  # Ordina per data inizio
        
        gaps = []
        for i in range(len(periods) - 1):
            current_end = periods[i][1]
            next_start = periods[i+1][0]
            
            # Se c'è un gap maggiore di 2 minuti (120 secondi per dati 1m)
            gap_hours = (next_start - current_end).total_seconds() / 3600
            if gap_hours > 2/60:  # Più di 2 minuti
                gaps.append({
                    'gap_hours': gap_hours,
                    'between': f"{periods[i][2]} e {periods[i+1][2]}",
                    'from': current_end,
                    'to': next_start
                })
        
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
            if 'start_date' in data:
                year = data['start_date'].year
                if year not in years_data:
                    years_data[year] = {'files': 0, 'rows': 0, 'size_mb': 0}
                years_data[year]['files'] += 1
                years_data[year]['rows'] += data['rows']
                years_data[year]['size_mb'] += data['size_mb']
        
        for year in sorted(years_data.keys()):
            print(f"   {year}: {years_data[year]['files']:2d} file, "
                  f"{years_data[year]['rows']:8,} righe, "
                  f"{years_data[year]['size_mb']:6.1f} MB")
    
    # Suggerimenti
    print(f"\n💡 SUGGERIMENTI:")
    print("   1. Usa pd.concat() per unire tutti i file in un unico DataFrame")
    print("   2. Salva come singolo file .parquet per query più veloci")
    print("   3. Considera di eliminare colonne non necessarie (es: 'ignore')")
    
    return all_data


def create_master_file(folder_path, output_filename="master_data.parquet"):
    """Crea un unico file Parquet con tutti i dati"""
    
    parquet_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.parquet')])
    
    if not parquet_files:
        print("Nessun file da unire!")
        return
    
    print(f"\n🔗 Unione di {len(parquet_files)} file in {output_filename}...")
    
    all_dfs = []
    for i, filename in enumerate(parquet_files, 1):
        filepath = os.path.join(folder_path, filename)
        try:
            df = pd.read_parquet(filepath)
            all_dfs.append(df)
            print(f"  {i:3d}. {filename} - {len(df):,} righe")
        except Exception as e:
            print(f"  {i:3d}. {filename} - ❌ Errore: {str(e)[:50]}")
    
    if all_dfs:
        master_df = pd.concat(all_dfs).sort_index()
        output_path = os.path.join(folder_path, output_filename)
        master_df.to_parquet(output_path, compression='snappy')
        
        print(f"\n✅ File master creato: {output_path}")
        print(f"   Righe totali: {len(master_df):,}")
        print(f"   Periodo: {master_df.index.min()} - {master_df.index.max()}")
        print(f"   Dimensione: {os.path.getsize(output_path)/1024/1024:.2f} MB")
        
        return master_df


def check_specific_file(folder_path, filename):
    """Controlla un file specifico in dettaglio"""
    
    filepath = os.path.join(folder_path, filename)
    
    if not os.path.exists(filepath):
        print(f"❌ File non trovato: {filepath}")
        return
    
    print(f"🔍 Analisi dettagliata di: {filename}")
    print("="*60)
    
    try:
        df = pd.read_parquet(filepath)
        
        print(f"📊 Statistiche generali:")
        print(f"   Righe: {len(df):,}")
        print(f"   Colonne: {len(df.columns)}")
        print(f"   Dimensione file: {os.path.getsize(filepath)/1024/1024:.2f} MB")
        print(f"   Periodo: {df.index.min()} - {df.index.max()}")
        print(f"   Giorni coperti: {(df.index.max() - df.index.min()).days + 1}")
        
        print(f"\n🗂️  Schema:")
        print(f"   Tipo indice: {type(df.index).__name__}")
        print(f"   Colonne e tipi:")
        for col, dtype in df.dtypes.items():
            print(f"     - {col}: {dtype}")
        
        print(f"\n📈 Statistiche numeriche:")
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols[:5]:  # Solo prime 5 colonne numeriche
            print(f"   {col}:")
            print(f"     Min: {df[col].min():.2f}")
            print(f"     Max: {df[col].max():.2f}")
            print(f"     Media: {df[col].mean():.2f}")
            print(f"     Valori mancanti: {df[col].isnull().sum()}")
        
        print(f"\n🔎 Campione dati (prime 3 righe):")
        print(df.head(3))
        
        print(f"\n🕒 Frequenza temporale:")
        # Calcola intervalli tra le righe
        if len(df) > 1:
            time_diffs = df.index.to_series().diff().dropna()
            most_common_diff = time_diffs.mode()[0] if not time_diffs.empty else None
            if most_common_diff:
                print(f"   Intervallo più comune: {most_common_diff}")
                print(f"   Min intervallo: {time_diffs.min()}")
                print(f"   Max intervallo: {time_diffs.max()}")
        
    except Exception as e:
        print(f"❌ Errore lettura file: {e}")


def main():
    
    
    while True:

        """Menu principale"""
    
        print("""
        ╔══════════════════════════════════════════════╗
        ║         ANALIZZATORE DATI PARQUET            ║
        ╠══════════════════════════════════════════════╣
        ║ 1. Analizza tutti i file                     ║
        ║ 2. Crea file master unico                    ║
        ║ 3. Controlla file specifico                  ║
        ║ 4. Esci                                      ║
        ╚══════════════════════════════════════════════╝
        """)
        
        choice = input("\n👉 Scegli un'opzione (1-4): ").strip()
        
        if choice == '1':
            print("\n" + "="*60)
            analyze_parquet_files(FOLDER_TO_CHECK)
            print("="*60)
            
        elif choice == '2':
            output_name = input("Nome file master (default: master_data.parquet): ").strip()
            if not output_name:
                output_name = "master_data.parquet"
            create_master_file(FOLDER_TO_CHECK, output_name)
            
        elif choice == '3':
            # Lista file disponibili
            files = sorted([f for f in os.listdir(FOLDER_TO_CHECK) if f.endswith('.parquet')])
            if files:
                print("\nFile disponibili:")
                for i, f in enumerate(files[:100], 1):  # Mostra solo primi 100
                    print(f"  {i:2d}. {f}")
                
                if len(files) > 20:
                    print(f"  ... e altri {len(files) - 100} file")
                
                file_num = input("\nNumero file da analizzare (0 per tornare): ").strip()
                if file_num.isdigit():
                    idx = int(file_num) - 1
                    if 0 <= idx < len(files):
                        check_specific_file(FOLDER_TO_CHECK, files[idx])
            else:
                print("Nessun file trovato!")
                
        elif choice == '4':
            print("Arrivederci!")
            break
            
        else:
            print("Scelta non valida!")


if __name__ == "__main__":
    # Avvia il programma
    if os.path.exists(FOLDER_TO_CHECK):
        main()
    else:
        print(f"❌ Directory non trovata: {FOLDER_TO_CHECK}")
        print(f"   Crea la directory o modifica FOLDER_TO_CHECK nello script")