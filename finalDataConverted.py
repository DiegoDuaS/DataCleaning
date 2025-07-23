import pandas as pd
import os

# Columnas esperadas
columnas = [
    "CODIGO", "DISTRITO", "DEPARTAMENTO", "MUNICIPIO", "ESTABLECIMIENTO", "DIRECCION",
    "TELEFONO", "SUPERVISOR", "DIRECTOR", "NIVEL", "SECTOR", "AREA", "STATUS",
    "MODALIDAD", "JORNADA", "PLAN", "DEPARTAMENTAL"
]

# Carpeta donde están los archivos
carpeta = "./raw"  # Cambia esta ruta según tu caso

print("Archivos encontrados en la carpeta:", os.listdir(carpeta))

# Lista para guardar los DataFrames válidos
dataframes = []

# Iterar sobre los archivos en la carpeta
for archivo in os.listdir(carpeta):
    if archivo.endswith(".csv"):
        ruta = os.path.join(carpeta, archivo)
        try:
            df = pd.read_csv(ruta, encoding="latin1", dtype=str)
            if all(col in df.columns for col in columnas):
                df = df[columnas]  # Ordenar columnas
                dataframes.append(df)
            else:
                print(f"⚠️ Archivo omitido por columnas faltantes: {archivo}")
        except Exception as e:
            print(f"❌ Error leyendo {archivo}: {e}")

# Unir todos los DataFrames
if dataframes:
    df_final = pd.concat(dataframes, ignore_index=True)
    df_final.to_csv("republica.csv", index=False, encoding="utf-8")
    print("✅ Archivos combinados exitosamente en 'republica.csv'")
else:
    print("⚠️ No se encontraron archivos válidos para combinar.")
