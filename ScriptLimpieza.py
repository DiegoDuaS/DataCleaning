# pipeline_republica.py
import pandas as pd
import re

def normalize_text(s: str) -> str:
    if pd.isnull(s):
        return s
    s = str(s).strip()
    s = re.sub(r'\s+', ' ', s)
    s = s.upper()
    s = re.sub(r'[ÁÉÍÓÚÜÑ]',
               lambda m: {'Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U','Ü':'U','Ñ':'N'}[m.group()],
               s)
    return s

def clean_republica_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ── 1. Normalización de texto ----------------------------------------------
    # Limpia espacios, acentos y pasa a mayúsculas en columnas textuales.
    text_cols = ['ESTABLECIMIENTO','DIRECCION','MUNICIPIO','DEPARTAMENTO']
    df[text_cols] = df[text_cols].apply(lambda x: x.map(normalize_text))

    # ── 2. Consistencia geográfica --------------------------------------------
    # Une DEPARTAMENTO y MUNICIPIO en una llave sintética para facilitar filtros.
    mun_map = {'LA-TINTA':'LA TINTA'}
    df['MUNICIPIO'] = df['MUNICIPIO'].replace(mun_map)
    df['DEPT_MUN'] = df['DEPARTAMENTO'] + '_' + df['MUNICIPIO']

    # ── 3. Limpieza de teléfonos -----------------------------------------------
    # TELEFONO_CLEAN: solo dígitos (limpia paréntesis, guiones, espacios).
    # TELEFONO_FLAG: True si la longitud NO está entre 8-12 dígitos.
    df['TELEFONO_CLEAN'] = df['TELEFONO'].astype(str).str.replace(r'[^0-9]', '', regex=True)
    df['TELEFONO_FLAG'] = ~df['TELEFONO_CLEAN'].str.fullmatch(r'\d{8,12}')

    # ── 4. Anomalía: dirección vacía en área urbana ----------------------------
    # True cuando no hay dirección y el establecimiento está en zona urbana.
    df['ADDR_MISSING_URBAN'] = df['DIRECCION'].isna() & (df['AREA'] == 'URBANA')

    # ── 5. One-hot de jornadas -------------------------------------------------
    # Crea columnas JORNADA_MATUTINA, JORNADA_VESPERTINA, etc. (1 = presente).
    jornada_flags = df['JORNADA'].str.upper().str.get_dummies(sep=' Y ')
    df = pd.concat([df, jornada_flags.add_prefix('JORNADA_')], axis=1)

    # ── 6. Modalidad agrupada (MOD_BUCKET) -------------------------------------
    # presencial, semi, distance según la modalidad original.
    modalidad_upper = df['MODALIDAD'].str.upper()
    df['MOD_BUCKET'] = pd.cut(
        modalidad_upper.map({
            'DIARIO(REGULAR)':0,'FIN DE SEMANA':0,
            'SEMIPRESENCIAL':1,'A DISTANCIA':2,'VIRTUAL A DISTANCIA':2
        }),
        bins=[-1,0.5,1.5,2.5], labels=['presencial','semi','distance']
    )

    # ── 7. Banderas booleanas de calidad ---------------------------------------
    df['IS_BILINGUE'] = df['MODALIDAD'].str.contains('BILINGUE', case=False, na=False)
    df['STATUS_NOT_ABIERTA'] = df['STATUS'] != 'ABIERTA'
    df['OFICIAL_NO_PHONE']   = (df['SECTOR']=='OFICIAL') & (df['TELEFONO_CLEAN']=='')
    df['NACIONAL_TAG_MISTAKE'] = (
        df['ESTABLECIMIENTO'].str.contains('NACIONAL', na=False) & (df['SECTOR']!='OFICIAL')
    )

    return df

# --------------------------------------------------------------------------
# Ejecución
# --------------------------------------------------------------------------
if __name__ == '__main__':
    df_raw = pd.read_csv('republica.csv')
    df_clean = clean_republica_pipeline(df_raw)

    # Detectar y exportar duplicados reales por CODIGO
    dup_mask = df_clean.duplicated(subset=['CODIGO'], keep=False)
    df_duplicados = df_clean[dup_mask].sort_values(['CODIGO'])
    df_duplicados.to_csv('codigos_duplicados.csv', index=False)

    # Dataset final sin duplicados
    df_final = df_clean.drop_duplicates(subset=['CODIGO'])
    df_final.to_csv('republica_limpia_sin_duplicados.csv', index=False)

    # Reporte breve
    report = {
        'filas_totales': len(df_clean),
        'codigos_unicos': df_clean['CODIGO'].nunique(),
        'filas_duplicadas': len(df_duplicados),
        'municipios': df_clean['MUNICIPIO'].nunique()
    }
    print("📊 RESUMEN")
    for k, v in report.items():
        print(f"{k}: {v}")