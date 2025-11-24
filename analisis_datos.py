"""
Script para generar una tabla tipo "Tract Majority Race" como en los
reportes regulatorios (ejemplo PNC - Los Angeles MSA).

Qué hace este script:
----------------------
1. Obtiene, vía API HMDA Data Browser:
   - El LEI del lender a partir de su nombre (/view/filers)
   - Todos los LAR del lender en un MSA/MD o CBSA y año dados (/view/csv)
   - Todos los LAR de todos los lenders en ese MSA/MD o CBSA y año (/view/csv)
2. Cruza esos LAR con un fichero demográfico local por tracto censal
   (census_tract) que debe contener, al menos:
     - census_tract (str)
     - tract_majority_race  (White, Black, Hispanic, Asian, etc.)
     - is_majority_minority (0/1)
     - is_majority_black_hisp (0/1)
3. Calcula, para cada categoría de Tract Majority Race:
     - # de tracts
     - Lender Apps y Peers Apps
     - % de TTL (por lender y peers)
     - Difference (Lender % - Peers %)
     - z-statistic, p-value (test de diferencia de proporciones)
     - Statistically Significant? (Yes/No, alpha=0.05)
     - Gap (apps esperadas - reales del lender, redondeado)
4. Añade filas de Total, Majority Minority Tracts y Majority Black/Hisp. Tracts.
5. Exporta un CSV cuyo layout replica la tabla del informe.

IMPORTANTE (CBSA vs MD)
-----------------------
- Si Config.msamd es un CBSA que aparece en CBSA_TO_MD, el script:
    * Llama a la API usando todos los MD de ese CBSA (msamds=MD1,MD2,...)
    * Filtra la demografía a esos MD
    * Agrega los resultados para todos los MD → tabla a nivel CBSA
- Si Config.msamd NO está en CBSA_TO_MD, se trata como un MSA/MD normal.

Requisitos:
    pip install requests pandas scipy openpyxl
"""

import io
import math
import sys
from dataclasses import dataclass, field
from typing import Optional, Tuple, List

from pathlib import Path

import numpy as np
import pandas as pd
import requests
from scipy.stats import norm

BASE_URL = "https://ffiec.cfpb.gov/v2/data-browser-api"

# --- CONFIGURACIÓN DEL PROXY ---
# ATENCIÓN: Reemplaza con los datos de tu proxy real.
# Ejemplo de formato: "http://USUARIO:CONTRASEÑA@DIRECCIÓN_IP:PUERTO"
PROXY_URL = "http://67.43.236.203:209"  # Ejemplo de proxy sin autenticación
# Si tu proxy usa autenticación:
# PROXY_URL = "http://miusuario:micontrasena@203.0.113.44:8080" 

proxies = {
    # El proxy a usar para solicitudes HTTP
    "http": PROXY_URL,
}
# ------------------------------

# ---------------------------------------------------------------------------
# Mapeo CBSA -> lista de Metropolitan Division Codes (MDs)
# ---------------------------------------------------------------------------

CBSA_TO_MD = {
    # Los que detectaste con más de un MD
    "12060": ["12054", "31924"],
    "14460": ["14454", "15764", "40484"],
    "16980": ["16984", "20994", "29404", "29414"],
    "19100": ["19124", "23104"],
    "19820": ["19804", "47664"],
    "31080": ["11244", "31084"],  # Los Angeles-Long Beach-Anaheim, CA
    "33100": ["22744", "33124", "48424"],
    "35620": ["29484", "35004", "35084", "35614"],
    "37980": ["15804", "33874", "37964", "48864"],
    "41860": ["36084", "41884", "42034"],
    "42660": ["21794", "42644", "45104"],
    "45300": ["41304", "45294"],
    "47900": ["11694", "23224", "47764"],
}

# Mapping año → (flatfile, diccionario)
CENSUS_FILES_SUFFIX = {
    2022: ("26AUG22"),
    2023: ("28SEP23"),
    2024: ("16JULY24"),
    2025: ("10JULY25"),
}

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

@dataclass
class Config:
    year: int = 2023
    # Puede ser un MD (ej. '11244') o un CBSA (ej. '31080')
    msamd: str = "31080"
    lender_name: str = "PNC BANK, NATIONAL ASSOCIATION"
    # Filtro típico de acciones para análisis de originación / denegación
    actions_taken: str = "1,2,3"  # 1=originated, 2=approved not accepted, 3=denied

    # Demografía: fichero ya procesado (tabla por tract) que usará el script
    demographics_csv: str = "demographics_tract_majority_race.csv"

    # Ficheros fuente para construir el CSV demográfico si no existe
    census_flatfile_csv: str = f"CensusFlatFile{year}.csv"
    census_dictionary_xlsx: str = f"FFIEC_Census_File_Definitions_{CENSUS_FILES_SUFFIX[year]}.xlsx"

    output_csv: str = "tabla_tract_majority_race.csv"


# ---------------------------------------------------------------------------
# HMDA API
# ---------------------------------------------------------------------------

def get_filers(config: Config) -> pd.DataFrame:
    """
    Descarga la lista de filers HMDA para el año indicado, SIN filtrar por MSA/MD.

    Sólo usamos esto para obtener el LEI del lender (que es único a nivel nacional),
    así que no hace falta filtrar por msamd y evitamos timeouts cuando hay varios MD.
    """
    params = {
        "years": str(config.year),
    }
    url = f"{BASE_URL}/view/filers"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=60)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print("Error llamando a /view/filers:", e, file=sys.stderr)
        print("URL efectiva:", resp.url, file=sys.stderr)
        print("Status code:", resp.status_code, file=sys.stderr)
        print("Texto de respuesta (inicio):", resp.text[:500], file=sys.stderr)
        raise

    data = resp.json()
    institutions = data.get("institutions", [])
    if not institutions:
        print("Respuesta de /filers sin 'institutions' o vacía.", file=sys.stderr)

    df = pd.DataFrame(institutions)
    return df


def find_lei_for_lender(df_filers: pd.DataFrame, lender_name: str) -> Optional[str]:
    """Busca el LEI correspondiente al nombre de la entidad (case-insensitive)."""

    if "name" not in df_filers.columns:
        print(
            "La respuesta de /filers no contiene columna 'name'. Columnas reales:",
            list(df_filers.columns),
            file=sys.stderr,
        )
        return None

    # Búsqueda exacta case-insensitive
    mask_exact = df_filers["name"].str.upper() == lender_name.upper()
    matches = df_filers[mask_exact]

    # Si no hay match exacto, probamos búsqueda por 'contiene'
    if matches.empty:
        mask_contains = df_filers["name"].str.upper().str.contains(
            lender_name.upper()
        )
        matches = df_filers[mask_contains]

    if matches.empty:
        print(
            f"No se ha encontrado el lender '{lender_name}' en la lista de filers.",
            file=sys.stderr,
        )
        print(
            "Ejemplos de names devueltos:",
            df_filers["name"].head(10).to_list(),
            file=sys.stderr,
        )
        return None

    if len(matches) > 1:
        print(
            f"Atención: hay {len(matches)} instituciones que coinciden con '{lender_name}'. "
            f"Se usará la primera.",
            file=sys.stderr,
        )

    row = matches.iloc[0]
    lei = row["lei"]
    print(f"Se usará LEI={lei} para '{row['name']}'")
    return lei


def download_hmda_csv(
    year: int, msamds: str, actions_taken: str, lei: Optional[str] = None
) -> pd.DataFrame:
    """
    Descarga los LAR HMDA en CSV utilizando un proxy para enmascarar la IP del servidor.
    """
    params = {
        "years": str(year),
        "msamds": msamds,
        "actions_taken": actions_taken,
    }
    if lei is not None:
        params["leis"] = lei

    url = f"{BASE_URL}/view/csv"

    # Se mantienen las cabeceras para seguir simulando un navegador
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv, */*; q=0.5",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Origin": "https://ffiec.cfpb.gov",
        "Referer": "https://ffiec.cfpb.gov/data-browser/data",
        "DNT": "1",
    }

    resp = requests.get(
        url,
        params=params,
        headers=headers,
        proxies=proxies,  # <-- ¡Aquí se añade el proxy!
        timeout=120
    )
    
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print("Error llamando a /view/csv (Con Proxy):", e, file=sys.stderr)
        print("Status code:", resp.status_code, file=sys.stderr)
        # El resto de tu logging (URL efectiva, cabeceras)
        raise

    csv_bytes = resp.content
    # Asegúrate de que pandas.read_csv puede manejar el contenido si está comprimido (aunque quitamos Accept-Encoding)
    df = pd.read_csv(io.BytesIO(csv_bytes))
    return df


# ---------------------------------------------------------------------------
# DEMOGRAFÍA DESDE CENSUS FLAT FILE
# ---------------------------------------------------------------------------

def _load_dictionary(config) -> pd.DataFrame:
    """
    Carga la hoja 'Data Dictionary' del Excel de definiciones FFIEC
    y devuelve el DataFrame completo (con al menos Index y Description).
    """

    dictionary_path = getattr(config, "census_dictionary_xlsx", None)
    if dictionary_path is None:
        dictionary_path = getattr(config, "census_dictionary", None)

    if dictionary_path is None:
        raise AttributeError(
            "Config no tiene ni 'census_dictionary_xlsx' ni 'census_dictionary'."
        )

    xls = pd.ExcelFile(dictionary_path)

    SHEET = "Data Dictionary"
    if SHEET not in xls.sheet_names:
        raise ValueError(
            f"No se ha encontrado la hoja '{SHEET}' en "
            f"{dictionary_path}. Hojas disponibles: {xls.sheet_names}"
        )

    df = xls.parse(SHEET)

    # Limpiar nombres de columnas
    df.columns = [str(c).strip() for c in df.columns]

    if "Index" not in df.columns or "Description" not in df.columns:
        raise ValueError(
            "No se han podido localizar las columnas de 'Index' y 'Description' "
            f"en {dictionary_path}. Columnas: {list(df.columns)}"
        )

    dict_df = df[["Index", "Description"]].copy()
    dict_df["Index"] = pd.to_numeric(dict_df["Index"], errors="coerce")
    dict_df = dict_df.dropna(subset=["Index"])
    dict_df["Index"] = dict_df["Index"].astype(int)
    dict_df["Description"] = dict_df["Description"].astype(str)

    return dict_df


def _find_index_by_field_substring(dict_df: pd.DataFrame, substring: str) -> int:
    """
    Busca la primera fila cuyo Description contenga `substring` (case-insensitive)
    y devuelve el índice de columna CERO-BASED para el flat file.
    (En el Excel Index es 1-based, por eso restamos 1).
    """
    mask = dict_df["Description"].str.contains(substring, case=False, na=False)
    matches = dict_df.loc[mask, "Index"]

    if matches.empty:
        raise ValueError(
            f"No se ha encontrado ningún campo en el diccionario que contenga: "
            f"'{substring}'"
        )

    # Index en el Excel es 1-based; las columnas del CSV son 0-based
    return int(matches.iloc[0]) - 1


def _find_index(dict_df: pd.DataFrame, substrings) -> int:
    """
    Intenta varias descripciones alternativas hasta encontrar una que exista.
    1) Primero intenta coincidencia EXACTA de la descripción.
    2) Si no la encuentra, cae a búsqueda por 'contains' (como antes).
    """
    if isinstance(substrings, str):
        substrings = [substrings]

    last_err = None
    for s in substrings:
        # 1) Intentar coincidencia exacta (ignorando mayúsculas/minúsculas y espacios)
        desc_norm = s.strip().casefold()
        mask_exact = (
            dict_df["Description"]
            .astype(str)
            .str.strip()
            .str.casefold()
            == desc_norm
        )
        matches_exact = dict_df.loc[mask_exact, "Index"]

        if not matches_exact.empty:
            # Index en Excel es 1-based -> columnas 0-based
            return int(matches_exact.iloc[0]) - 1

        # 2) Si no hay exacta, usar búsqueda por 'contains' (comportamiento antiguo)
        try:
            return _find_index_by_field_substring(dict_df, s)
        except ValueError as e:
            last_err = e

    # Si ninguna de las alternativas funciona, relanzar el último error
    raise last_err


def build_demographics_from_flatfile(config, flatfile_path: str) -> pd.DataFrame:
    """
    Lee el CensusFlatFile20XX (sin cabeceras) y construye un DataFrame
    con información de composición racial por tracto y una columna
    de tract_majority_race (White, Black, Hispanic, Asian, Native American,
    Hawaiian, Other, MultiRace, No Majority Race, No Population), así como
    indicadores de majority minority y majority black/hispanic.
    """
    dict_df = _load_dictionary(config)

    # --- Campos clave (índices 1–5 del Excel) ---------------------------
    YEAR_COL   = _find_index(dict_df, "HMDA/CRA collection year")
    MSA_COL    = _find_index(dict_df, "MSA/MD Code")
    STATE_COL  = _find_index(dict_df, "FIPS state code")
    COUNTY_COL = _find_index(dict_df, "FIPS county code")
    TRACT_COL  = _find_index(dict_df, "Census tract. Implied decimal")

    # --- Población total y minoría -------------------------------------
    TOTAL_POP_COL = _find_index(dict_df, "Total persons")
    MINORITY_PCT_COL = _find_index(
        dict_df,
        "Minority population as percent of tract population"
    )

    # --- Composición racial / étnica -----------------------------------

    # Total población hispana
    HISP_POP_COL = _find_index(
        dict_df,
        [
            "Total population Hispanic only",
            "Total Hispanic population",
        ],
    )

    # No hispano White
    WHITE_POP_COL = _find_index(
        dict_df,
        [
            "Total population non-Hispanic White",
            "Total White alone population",
        ],
    )

    # No hispano Black
    BLACK_POP_COL = _find_index(
        dict_df,
        [
            "Total population non-Hispanic Black",
            "Total Black alone population",
        ],
    )

    # No hispano Asian
    ASIAN_POP_COL = _find_index(
        dict_df,
        [
            "Total population non-Hispanic Asian",
            "Total population Asian",
        ],
    )

    # No hispano Native Hawaiian / Pacific Islander
    NHPI_POP_COL = _find_index(
        dict_df,
        "Total population non-Hispanic native Hawaiian or other Pacific Islander",
    )

    # No hispano American Indian / Alaska Native
    AIAN_POP_COL = _find_index(
        dict_df,
        [
            "Total population non-Hispanic American Indian or Alaska Native",
            "Total non-Hispanic American Indian or Alaska Native",
        ],
    )

    # No hispano "some other race"
    OTHER1_POP_COL = _find_index(
        dict_df,
        "Total population non-Hispanic some other race",
    )

    # No hispano "two or more races"
    OTHER2_POP_COL = _find_index(
       dict_df,
        "Total population non-Hispanic two or more races",
    )

    usecols = sorted(
        {
            YEAR_COL,
            MSA_COL,
            STATE_COL,
            COUNTY_COL,
            TRACT_COL,
            TOTAL_POP_COL,
            MINORITY_PCT_COL,
            HISP_POP_COL,
            WHITE_POP_COL,
            BLACK_POP_COL,
            ASIAN_POP_COL,
            NHPI_POP_COL,
            AIAN_POP_COL,
            OTHER1_POP_COL,
            OTHER2_POP_COL,
        }
    )

    df = pd.read_csv(
        flatfile_path,
        header=None,
        dtype=str,
        usecols=usecols,
    )

    rename_map = {
        YEAR_COL: "year",
        MSA_COL: "msa_md",
        STATE_COL: "state_fips",
        COUNTY_COL: "county_fips",
        TRACT_COL: "tract",
        TOTAL_POP_COL: "total_pop",
        MINORITY_PCT_COL: "pct_minority_raw",
        HISP_POP_COL: "pop_hispanic",
        WHITE_POP_COL: "pop_nh_white",
        BLACK_POP_COL: "pop_nh_black",
        ASIAN_POP_COL: "pop_nh_asian",
        NHPI_POP_COL: "pop_nh_nhpi",
        AIAN_POP_COL: "pop_nh_aian",
        OTHER1_POP_COL: "pop_nh_other1",
        OTHER2_POP_COL: "pop_nh_other2",
    }
    df = df.rename(columns=rename_map)

    # Padding FIPS / tract
    df["state_fips"] = df["state_fips"].str.zfill(2)
    df["county_fips"] = df["county_fips"].str.zfill(3)
    df["tract"] = df["tract"].str.zfill(6)

    df["tract_fips"] = df["state_fips"] + df["county_fips"] + df["tract"]
    df["census_tract"] = df["tract_fips"]

    # Convertir a numérico las columnas de población y pct minority
    pop_cols = [
        "total_pop",
        "pop_hispanic",
        "pop_nh_white",
        "pop_nh_black",
        "pop_nh_asian",
        "pop_nh_nhpi",
        "pop_nh_aian",
        "pop_nh_other1",
        "pop_nh_other2",
    ]

    for c in pop_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["pct_minority_raw"] = pd.to_numeric(df["pct_minority_raw"], errors="coerce")

    # Aseguramos total_pop > 0 donde tenga sentido
    df["total_pop"] = df["total_pop"].replace(0, np.nan)

    # Por si hay valores negativos raros, recortamos
    for c in pop_cols:
        df[c] = df[c].clip(lower=0)

    # Porcentaje por grupo (0–100)
    groups = [
        ("White", "pop_nh_white"),
        ("Black", "pop_nh_black"),
        ("Hispanic", "pop_hispanic"),
        ("Asian", "pop_nh_asian"),
        ("Native American", "pop_nh_aian"),
        ("Hawaiian", "pop_nh_nhpi"),
        ("Other", "pop_nh_other1"),
        ("MultiRace", "pop_nh_other2"),
    ]

    pct_cols = []
    for label, pop_col in groups:
        slug = label.lower().replace(" ", "_")
        colname = f"pct_{slug}"
        pct_cols.append(colname)
        df[colname] = (
            (df[pop_col] / df["total_pop"]) * 100.0
        ).fillna(np.nan)

    # Determinar grupo mayoritario
    vals = df[pct_cols].to_numpy(dtype=float)
    vals_filled = np.where(np.isnan(vals), -1, vals)  # evitar NaN

    max_idx = vals_filled.argmax(axis=1)
    max_pct = vals_filled[np.arange(len(df)), max_idx]

    labels = np.array([g[0] for g in groups])
    no_pop_mask = df["total_pop"].isna()

    majority_race = np.where(
        no_pop_mask,
        "No Population",
        np.where(
            max_pct < 50.0,
            "No Majority Race",
            labels[max_idx],
        ),
    )

    df["tract_majority_race"] = majority_race

    # Majority minority: pct_minority >= 50
    df["is_majority_minority"] = (
        (df["pct_minority_raw"] >= 50.0) & (~no_pop_mask)
    ).astype(int)

    # Majority Black/Hisp: pct_black + pct_hisp >= 50
    df["is_majority_black_hisp"] = (
        (df["pct_black"].fillna(0) + df["pct_hispanic"].fillna(0) >= 50.0)
        & (~no_pop_mask)
    ).astype(int)

    out_cols = [
        "year",
        "msa_md",
        "state_fips",
        "county_fips",
        "tract",
        "tract_fips",
        "census_tract",
        "total_pop",
        "pct_minority_raw",
        "tract_majority_race",
        "is_majority_minority",
        "is_majority_black_hisp",
    ]
    out_cols.extend(pct_cols)

    return df[out_cols]


def load_demographics(
    demographics_csv: str,
    config,
    force_rebuild_demo: bool = False,
) -> pd.DataFrame:
    """
    Carga el CSV demográfico ya preprocesado. Si no existe, o si
    force_rebuild_demo=True, lo construye a partir del flatfile del censo.
    """
    p = Path(demographics_csv)

    if p.exists() and not force_rebuild_demo:
        print(f"Cargando fichero demográfico existente: {p}")
        return pd.read_csv(p, dtype={"census_tract": str, "msa_md": str})

    # Localizar el flatfile del censo en el config
    census_flatfile = getattr(config, "census_flatfile", None)
    if census_flatfile is None:
        census_flatfile = getattr(config, "census_flatfile_csv", None)

    if census_flatfile is None:
        raise AttributeError(
            "Config no tiene ni 'census_flatfile' ni 'census_flatfile_csv'. "
            "Añade uno de esos atributos con la ruta a CensusFlatFile2023.csv"
        )

    print(
        f"No se encuentra '{p.name}' o force_rebuild_demo={force_rebuild_demo}. "
        f"Se va a construir a partir de '{census_flatfile}' y "
        f"'{config.census_dictionary_xlsx}'..."
    )

    df = build_demographics_from_flatfile(config, census_flatfile)

    df.to_csv(p, index=False)
    print(f"Fichero demográfico guardado en: {p}")

    return df


# ---------------------------------------------------------------------------
# Normalización de tracto (clave robusta)
# ---------------------------------------------------------------------------

def _normalize_tract(series: pd.Series) -> pd.Series:
    """
    Normaliza un tracto a una clave de 11 dígitos:
    - Extrae solo dígitos
    - Se queda con los últimos 11
    - Rellena con ceros a la izquierda si hace falta
    """
    s = series.astype(str)
    s = s.str.extract(r"(\d+)", expand=False)  # sólo dígitos
    s = s.fillna("")
    s = s.str[-11:].str.zfill(11)
    s = s.replace("", np.nan)
    return s


# ---------------------------------------------------------------------------
# Anotar LAR con demografía (Lender + Peers)
# ---------------------------------------------------------------------------

def annotate_lars_with_demo(
    lender_df: pd.DataFrame, all_df: pd.DataFrame, demo_df_msa: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Añade tract_majority_race e indicadores de mayoría a los LAR del lender
    y al conjunto total, y separa lender vs peers.

    Aquí demo_df_msa ya está filtrado al MSA/MD (o conjunto de MD) en main().
    """

    demo_key = demo_df_msa[
        [
            "census_tract",
            "tract_majority_race",
            "is_majority_minority",
            "is_majority_black_hisp",
        ]
    ].drop_duplicates()

    demo_key["ct_norm"] = _normalize_tract(demo_key["census_tract"])

    # Normalizamos claves en HMDA
    lender_df = lender_df.copy()
    all_df = all_df.copy()

    lender_df["ct_norm"] = _normalize_tract(lender_df["census_tract"])
    all_df["ct_norm"] = _normalize_tract(all_df["census_tract"])

    print("Tracts únicos en lender (HMDA):", lender_df["ct_norm"].nunique())
    print("Tracts únicos en peers (HMDA):", all_df["ct_norm"].nunique())

    # Merge lender y todos los LAR con la demografía
    lender = lender_df.merge(
        demo_key,
        on="ct_norm",
        how="left",
        suffixes=("", "_demo"),
    )

    all_with_demo = all_df.merge(
        demo_key,
        on="ct_norm",
        how="left",
        suffixes=("", "_demo"),
    )

    # Sobrescribimos census_tract de HMDA con el estándar del demo cuando exista
    for df in (lender, all_with_demo):
        if "census_tract_demo" in df.columns:
            df["census_tract"] = df["census_tract_demo"].fillna(df["census_tract"])

    # Separar lender vs peers por LEI
    lender_lei = lender_df.iloc[0]["lei"]
    peers = all_with_demo[all_with_demo["lei"] != lender_lei].copy()

    # Mensajes de control
    print(
        "Registros lender con tract_majority_race no nulo:",
        lender["tract_majority_race"].notna().sum(),
    )
    print(
        "Registros peers con tract_majority_race no nulo:",
        peers["tract_majority_race"].notna().sum(),
    )

    return lender, peers


# ---------------------------------------------------------------------------
# Cálculo de tabla por Tract Majority Race
# ---------------------------------------------------------------------------

def compute_race_table(
    lender: pd.DataFrame, peers: pd.DataFrame, demo_df_msa: pd.DataFrame
) -> Tuple[pd.DataFrame, int, int]:
    """Construye la tabla por Tract Majority Race con estadísticos."""

    ttl_lender_apps = len(lender)
    ttl_peers_apps = len(peers)

    # Conteo de tracts por majority race (desde el demográfico del MSA/MD agregado)
    tracts_per_race = (
        demo_df_msa.groupby("tract_majority_race")["census_tract"]
        .nunique()
        .rename("Year # Tracts")
        .reset_index()
    )

    # Apps por race para lender y peers
    lender_by_race = (
        lender.groupby("tract_majority_race")["census_tract"]
        .count()
        .rename("Lender Apps")
        .reset_index()
    )
    peers_by_race = (
        peers.groupby("tract_majority_race")["census_tract"]
        .count()
        .rename("Peers Apps")
        .reset_index()
    )

    df = tracts_per_race.merge(
        lender_by_race, on="tract_majority_race", how="left"
    ).merge(
        peers_by_race, on="tract_majority_race", how="left"
    )

    df["Lender Apps"] = df["Lender Apps"].fillna(0).astype(int)
    df["Peers Apps"] = df["Peers Apps"].fillna(0).astype(int)

    # Porcentajes de TTL
    if ttl_lender_apps > 0:
        df["Lender % of TTL"] = df["Lender Apps"] / ttl_lender_apps * 100
    else:
        df["Lender % of TTL"] = 0.0

    if ttl_peers_apps > 0:
        df["Peers % of TTL"] = df["Peers Apps"] / ttl_peers_apps * 100
    else:
        df["Peers % of TTL"] = 0.0

    # Difference (puntos porcentuales)
    df["Difference"] = df["Lender % of TTL"] - df["Peers % of TTL"]

    # z-statistic y p-value (test de diferencia de proporciones)
    def z_test(row):
        n1 = ttl_lender_apps
        n2 = ttl_peers_apps
        if n1 == 0 or n2 == 0:
            return math.nan, math.nan
        p1 = row["Lender Apps"] / n1
        p2 = row["Peers Apps"] / n2
        p_comb = (row["Lender Apps"] + row["Peers Apps"]) / (n1 + n2)
        se = math.sqrt(p_comb * (1 - p_comb) * (1 / n1 + 1 / n2))
        if se == 0:
            return math.nan, math.nan
        z = (p1 - p2) / se
        p_val = 2 * (1 - norm.cdf(abs(z)))
        return z, p_val

    zs, ps = [], []
    for _, r in df.iterrows():
        z, p = z_test(r)
        zs.append(z)
        ps.append(p)

    df["z-statistic"] = zs
    df["p-value"] = ps
    df["Statistically Significant?"] = df["p-value"].apply(
        lambda p: "Yes" if (not math.isnan(p) and p <= 0.05) else "No"
    )

    # Gap = Expected - Actual (para el lender)
    df["Expected Lender Apps"] = ttl_lender_apps * df["Peers % of TTL"] / 100
    df["Gap"] = (df["Expected Lender Apps"] - df["Lender Apps"]).round().astype(int)

    # ---------------------------
    # Fila "Unknown Tract / Not in Demo"
    # ---------------------------
    known_lender = df["Lender Apps"].sum()
    known_peers = df["Peers Apps"].sum()

    unknown_lender = ttl_lender_apps - known_lender
    unknown_peers = ttl_peers_apps - known_peers

    if unknown_lender != 0 or unknown_peers != 0:
        lender_pct_u = (
            unknown_lender / ttl_lender_apps * 100 if ttl_lender_apps > 0 else 0.0
        )
        peers_pct_u = (
            unknown_peers / ttl_peers_apps * 100 if ttl_peers_apps > 0 else 0.0
        )

        if ttl_lender_apps == 0 or ttl_peers_apps == 0:
            z_u = math.nan
            p_u = math.nan
        else:
            p1 = unknown_lender / ttl_lender_apps
            p2 = unknown_peers / ttl_peers_apps
            p_comb = (unknown_lender + unknown_peers) / (ttl_lender_apps + ttl_peers_apps)
            se = math.sqrt(p_comb * (1 - p_comb) * (1 / ttl_lender_apps + 1 / ttl_peers_apps))
            if se == 0:
                z_u = math.nan
                p_u = math.nan
            else:
                z_u = (p1 - p2) / se
                p_u = 2 * (1 - norm.cdf(abs(z_u)))

        expected_lender_apps_u = ttl_lender_apps * peers_pct_u / 100
        gap_u = round(expected_lender_apps_u - unknown_lender)

        row_unknown = {
            "tract_majority_race": "Unknown Tract / Not in Demo",
            "Year # Tracts": 0,
            "Lender Apps": int(unknown_lender),
            "Lender % of TTL": lender_pct_u,
            "Peers Apps": int(unknown_peers),
            "Peers % of TTL": peers_pct_u,
            "Difference": lender_pct_u - peers_pct_u,
            "z-statistic": z_u,
            "p-value": p_u,
            "Statistically Significant?": "Yes"
            if (not math.isnan(p_u) and p_u <= 0.05)
            else "No",
            "Gap": int(gap_u),
        }

        df = pd.concat([df, pd.DataFrame([row_unknown])], ignore_index=True)

    # Orden de categorías
    order = [
        "White",
        "Black",
        "Hispanic",
        "Asian",
        "Native American",
        "Hawaiian",
        "Other",
        "MultiRace",
        "No Majority Race",
        "No Population",
        "Unknown Tract / Not in Demo",
    ]
    df["__order"] = df["tract_majority_race"].apply(
        lambda x: order.index(x) if x in order else len(order)
    )
    df = df.sort_values("__order").drop(columns=["__order", "Expected Lender Apps"])

    df = df.rename(columns={"tract_majority_race": "Tract Majority Race"})

    return df, ttl_lender_apps, ttl_peers_apps


# ---------------------------------------------------------------------------
# Filas especiales y Total
# ---------------------------------------------------------------------------

def aggregate_by_tract_mask(
    lender: pd.DataFrame,
    peers: pd.DataFrame,
    demo_df_msa: pd.DataFrame,
    mask_col: str,
    mask_val: int,
) -> dict:
    """Agrega métricas para todos los tracts que cumplen un cierto mask demográfico."""

    tracts = (
        demo_df_msa.loc[demo_df_msa[mask_col] == mask_val, "census_tract"]
        .astype(str)
        .str.zfill(11)
        .unique()
    )
    num_tracts = len(tracts)

    lender_sub = lender[lender["census_tract"].isin(tracts)]
    peers_sub = peers[peers["census_tract"].isin(tracts)]

    ttl_lender_apps = len(lender)
    ttl_peers_apps = len(peers)

    lender_apps = len(lender_sub)
    peers_apps = len(peers_sub)

    lender_pct = lender_apps / ttl_lender_apps * 100 if ttl_lender_apps > 0 else 0.0
    peers_pct = peers_apps / ttl_peers_apps * 100 if ttl_peers_apps > 0 else 0.0

    if ttl_lender_apps == 0 or ttl_peers_apps == 0:
        z = math.nan
        p_val = math.nan
    else:
        p1 = lender_apps / ttl_lender_apps
        p2 = peers_apps / ttl_peers_apps
        p_comb = (lender_apps + peers_apps) / (ttl_lender_apps + ttl_peers_apps)
        se = math.sqrt(p_comb * (1 - p_comb) * (1 / ttl_lender_apps + 1 / ttl_peers_apps))
        if se == 0:
            z = math.nan
            p_val = math.nan
        else:
            z = (p1 - p2) / se
            p_val = 2 * (1 - norm.cdf(abs(z)))

    expected_lender_apps = ttl_lender_apps * peers_pct / 100
    gap = round(expected_lender_apps - lender_apps)

    row = {
        "Tract Majority Race": "",
        "Year # Tracts": num_tracts,
        "Lender Apps": lender_apps,
        "Lender % of TTL": lender_pct,
        "Peers Apps": peers_apps,
        "Peers % of TTL": peers_pct,
        "Difference": lender_pct - peers_pct,
        "z-statistic": z,
        "p-value": p_val,
        "Statistically Significant?": "Yes" if (not math.isnan(p_val) and p_val <= 0.05) else "No",
        "Gap": gap,
    }
    return row


def add_total_and_special_rows(
    base_df: pd.DataFrame,
    lender: pd.DataFrame,
    peers: pd.DataFrame,
    demo_df_msa: pd.DataFrame,
    ttl_lender_apps: int,
    ttl_peers_apps: int,
) -> pd.DataFrame:
    """
    Añade fila Total, Majority Minority Tracts y Majority Black/Hisp. Tracts.
    """
    df = base_df.copy()

    # Fila TOTAL
    total_row = {
        "Tract Majority Race": "Total",
        "Year # Tracts": demo_df_msa["census_tract"].nunique(),
        "Lender Apps": ttl_lender_apps,
        "Lender % of TTL": 100.0 if ttl_lender_apps > 0 else 0.0,
        "Peers Apps": ttl_peers_apps,
        "Peers % of TTL": 100.0 if ttl_peers_apps > 0 else 0.0,
        "Difference": 0.0,
        "z-statistic": math.nan,
        "p-value": math.nan,
        "Statistically Significant?": "No",
        "Gap": 0,
    }

    row_mm = aggregate_by_tract_mask(
        lender, peers, demo_df_msa, mask_col="is_majority_minority", mask_val=1
    )
    row_mm["Tract Majority Race"] = "Majority Minority Tracts"

    row_bh = aggregate_by_tract_mask(
        lender, peers, demo_df_msa, mask_col="is_majority_black_hisp", mask_val=1
    )
    row_bh["Tract Majority Race"] = "Majority Black/Hisp. Tracts"

    df = pd.concat(
        [
            df,
            pd.DataFrame([total_row]),
            pd.DataFrame([row_mm]),
            pd.DataFrame([row_bh]),
        ],
        ignore_index=True,
    )

    return df

def run_tract_majority_race(year: int, msamd: str, lender_name: str, actions_taken: str) -> pd.DataFrame:
    """
    Ejecuta todo el pipeline y devuelve la tabla final (full_df) como DataFrame,
    sin imprimir ni escribir CSV.

    year: año HMDA (2022–2025, según tengas archivos censales)
    msamd: puede ser un MD (11244, 31084, etc.) o un CBSA (31080, 12060, etc.)
    lender_name: nombre tal como aparece en /view/filers (ej. 'PNC BANK, NATIONAL ASSOCIATION')
    actions_taken: string tipo '1,2,3'
    """

    # ---------------------------
    # 1) Construir Config dinámicamente
    # ---------------------------
    cfg = Config()
    cfg.year = int(year)
    cfg.msamd = str(msamd)
    cfg.lender_name = lender_name
    cfg.actions_taken = actions_taken

    # Ajustar los ficheros censales según el año
    if cfg.year not in CENSUS_FILES_SUFFIX:
        raise ValueError(f"No hay definición de ficheros censales para el año {cfg.year}")

    suffix = CENSUS_FILES_SUFFIX[cfg.year]
    cfg.census_flatfile_csv = f"CensusFlatFile{cfg.year}.csv"
    cfg.census_dictionary_xlsx = f"FFIEC_Census_File_Definitions_{suffix}.xlsx"

    # ---------------------------
    # 2) Resolver si msamd es CBSA o MD
    # ---------------------------
    msamd_input = str(cfg.msamd)
    msamds_to_query = CBSA_TO_MD.get(msamd_input, [msamd_input])
    msamds_param = ",".join(msamds_to_query)

    # ---------------------------
    # 3) LEI del lender
    # ---------------------------
    filers_df = get_filers(cfg)
    lei = find_lei_for_lender(filers_df, cfg.lender_name)
    if lei is None:
        raise RuntimeError(f"No se ha encontrado LEI para el lender '{cfg.lender_name}'")

    # ---------------------------
    # 4) Descargar LAR lender + peers
    # ---------------------------
    lender_df = download_hmda_csv(cfg.year, msamds_param, cfg.actions_taken, lei=lei)
    all_df = download_hmda_csv(cfg.year, msamds_param, cfg.actions_taken, lei=None)

    # ---------------------------
    # 5) Demografía
    # ---------------------------
    #demo_full = load_demographics(cfg.demographics_csv, cfg)

    demo_filename = f"demographics_tract_majority_race_{cfg.year}.csv"
    demo_full = load_demographics(demo_filename, cfg)

    # Ojo: ahora filtramos por todos los MD implicados en el CBSA
    demo_msa = demo_full[demo_full["msa_md"].isin(msamds_to_query)].copy()

    # ---------------------------
    # 6) Anotar LAR con demografía
    # ---------------------------
    lender, peers = annotate_lars_with_demo(lender_df, all_df, demo_msa)

    # ---------------------------
    # 7) Tabla base + filas especiales
    # ---------------------------
    base_df, ttl_lender_apps, ttl_peers_apps = compute_race_table(lender, peers, demo_msa)
    full_df = add_total_and_special_rows(
        base_df, lender, peers, demo_msa, ttl_lender_apps, ttl_peers_apps
    )

    # Renombrar columna de tracts con el año
    year_label = f"{cfg.year} # Tracts"
    if "Year # Tracts" in full_df.columns:
        full_df = full_df.rename(columns={"Year # Tracts": year_label})

    cols_order = [
        "Tract Majority Race",
        year_label,
        "Lender Apps",
        "Lender % of TTL",
        "Peers Apps",
        "Peers % of TTL",
        "Difference",
        "z-statistic",
        "p-value",
        "Statistically Significant?",
        "Gap",
    ]
    # Por si en el futuro cambian columnas, hacemos intersección
    cols_order = [c for c in cols_order if c in full_df.columns]
    full_df = full_df[cols_order]

    return full_df


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    cfg = Config()

    # Determinar si cfg.msamd es un CBSA o un MD directo
    msamd_input = str(cfg.msamd)
    md_list = CBSA_TO_MD.get(msamd_input, [msamd_input])
    msamds_param = ",".join(md_list)

    if msamd_input in CBSA_TO_MD:
        print(f"Año: {cfg.year}, CBSA: {msamd_input} → MDs usados: {md_list}")
    else:
        print(f"Año: {cfg.year}, MSA/MD: {cfg.msamd}")

    # 1) Obtener LEI sin filtrar por MSA/MD (evita timeouts)
    print("Descargando filers (sin filtrar por MSA/MD)...")
    filers_df = get_filers(cfg)
    print(f"Filers recuperados: {len(filers_df)}")

    lei = find_lei_for_lender(filers_df, cfg.lender_name)
    if lei is None:
        sys.exit(1)

    # 2) LAR del lender
    print(f"Descargando LAR del lender para msamds={msamds_param}...")
    lender_df = download_hmda_csv(cfg.year, msamds_param, cfg.actions_taken, lei=lei)
    print(f"Registros lender: {len(lender_df)}")

    # 3) LAR de todos los lenders (lender + peers)
    print(f"Descargando LAR de todos los lenders para msamds={msamds_param}...")
    all_df = download_hmda_csv(cfg.year, msamds_param, cfg.actions_taken, lei=None)
    print(f"Registros totales: {len(all_df)}")

    # 4) Demografía
    print(f"Cargando/creando fichero demográfico: {cfg.demographics_csv}")
    demo_full = load_demographics(cfg.demographics_csv, cfg)
    print(
        f"Tracts únicos en demografía (toda la tabla): "
        f"{demo_full['census_tract'].nunique()}"
    )

    # Filtrar demografía a los MD usados (pueden ser uno o varios)
    demo_msa = demo_full[demo_full["msa_md"].isin(md_list)].copy()
    print(
        f"Tracts únicos en demografía (msa_md in {md_list}): "
        f"{demo_msa['census_tract'].nunique()}"
    )

    # 5) Anotar LAR con demo
    print("Anotando LAR con datos demográficos...")
    lender, peers = annotate_lars_with_demo(lender_df, all_df, demo_msa)
    print(f"LAR lender anotados: {len(lender)}, LAR peers anotados: {len(peers)}")

    # 6) Tabla base
    print("Calculando tabla base por Tract Majority Race...")
    base_df, ttl_lender_apps, ttl_peers_apps = compute_race_table(lender, peers, demo_msa)

    # 7) Filas especiales
    print("Añadiendo filas Total y especiales...")
    full_df = add_total_and_special_rows(
        base_df, lender, peers, demo_msa, ttl_lender_apps, ttl_peers_apps
    )

    # Renombramos la columna sólo al final para mostrar en el output
    year_label = f"{cfg.year} # Tracts"
    full_df = full_df.rename(columns={"Year # Tracts": year_label})

    cols_order = [
        "Tract Majority Race",
        year_label,
        "Lender Apps",
        "Lender % of TTL",
        "Peers Apps",
        "Peers % of TTL",
        "Difference",
        "z-statistic",
        "p-value",
        "Statistically Significant?",
        "Gap",
    ]
    full_df = full_df[cols_order]

    pd.set_option("display.float_format", lambda x: f"{x:0.4f}")
    print("\n=== Tabla Tract Majority Race ===")
    print(full_df.to_string(index=False))

    full_df.to_csv(cfg.output_csv, index=False)
    print(f"\nTabla guardada en '{cfg.output_csv}'.")


if __name__ == "__main__":
    main()
