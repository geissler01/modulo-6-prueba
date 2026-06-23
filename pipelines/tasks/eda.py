from pathlib import Path

SOURCES_DIR = Path("/opt/airflow/sources")
DOCS_DIR = Path("/opt/airflow/docs")

def generate_eda_profiles(**kwargs):
    """
    Escanea la carpeta de fuentes (sources), encuentra todos los archivos CSV
    descargados y genera un reporte HTML exploratorio (EDA) usando ydata-profiling.
    """
    import pandas as pd
    from ydata_profiling import ProfileReport
    
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    csv_files = list(SOURCES_DIR.rglob("*.csv"))
    
    if not csv_files:
        print("No se encontraron archivos CSV para perfilar en /opt/airflow/sources")
        return
        
    for csv_file in csv_files:
        print(f"Generando EDA para: {csv_file.name}...")
        
        # Leemos el CSV (latin1 es común en estos datasets de Kaggle)
        df = pd.read_csv(csv_file, low_memory=False, encoding='latin1')
        
        # Generamos el reporte (minimal=True acelera mucho para archivos pesados)
        profile = ProfileReport(df, title=f"EDA DataMart - {csv_file.name}", minimal=True)
        
        output_file = DOCS_DIR / f"{csv_file.stem}_eda_report.html"
        profile.to_file(output_file)
        
        print(f"Reporte HTML guardado exitosamente en: {output_file}")
