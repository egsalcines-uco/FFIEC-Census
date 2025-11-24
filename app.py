from flask import Flask, render_template, request, jsonify
import math
import requests

from analisis_datos import run_tract_majority_race, CBSA_TO_MD

app = Flask(__name__)

BASE_URL = "https://ffiec.cfpb.gov/v2/data-browser-api"

# Valores por defecto / opciones de ejemplo
AVAILABLE_YEARS = [2022, 2023, 2024, 2025]

# Usamos las claves del mapping + algunos MD sueltos (apto para demo)
AVAILABLE_MSAMD = sorted(
    set(CBSA_TO_MD.keys()) | {md for mds in CBSA_TO_MD.values() for md in mds}
)

AVAILABLE_LENDERS = [
    "PNC BANK, NATIONAL ASSOCIATION",
    "ALLY BANK",
    "BANK OF AMERICA, NATIONAL ASSOCIATION",
    "BANK OF NEW YORK MELLON, THE",
    "BMO BANK NATIONAL ASSOCIATION",
    "CHARLES SCHWAB BANK, SSB",
    "CITIBANK, N.A.",
    "CITIZENS BANK, NATIONAL ASSOCIATION",
    "FIFTH THIRD BANK, NATIONAL ASSOCIATION",
    "FIRST-CITIZENS BANK & TRUST COMPANY",
    "GOLDMAN SACHS BANK USA",
    "HSBC BANK USA, NATIONAL ASSOCIATION",
    "HUNTINGTON NATIONAL BANK, THE",
    "JP MORGAN CHASE BANK, NATIONAL ASSOCIATION",
    "KEYBANK NATIONAL ASSOCIATION",
    "MANUFACTURERS AND TRADERS TRUST COMPANY",
    "MORGAN STANLEY BANK, N.A.",
    "MORGAN STANLEY PRIVATE BANK, NATIONAL ASSOCIATION",
    "NORTHERN TRUST COMPANY, THE",
    "REGIONS BANK",
    "TD BANK, NATIONAL ASSOCIATION",
    "TRUIST BANK",
    "UBS BANK USA",
    "U.S. BANK NATIONAL ASSOCIATION",
    "WELLS FARGO BANK, NATIONAL ASSOCIATION",
]

AVAILABLE_ACTIONS = [
    "1,2,3",
    "1,2",
    "1,3",
    "1",
    "2",
    "3",
]


@app.route("/")
def index():
    return render_template(
        "index.html",
        years=AVAILABLE_YEARS,
        msamds=AVAILABLE_MSAMD,
        lenders=AVAILABLE_LENDERS,
        actions_list=AVAILABLE_ACTIONS,
    )


@app.route("/api/run", methods=["POST"])
def api_run():
    try:
        data = request.get_json(force=True)

        year = int(data.get("year"))
        msamd = str(data.get("msamd"))
        lender_name = data.get("lender_name")
        actions_taken = data.get("actions_taken")

        if not lender_name:
            return jsonify({"success": False, "error": "Falta lender_name"}), 400

        # Llamamos a tu lógica pesada de analisis_datos
        df = run_tract_majority_race(year, msamd, lender_name, actions_taken)

        # Convertimos a formato JSON amigable
        # (reemplazamos NaN/inf por None para que sea JSON válido)
        def clean_value(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v

        rows = [
            {col: clean_value(val) for col, val in row.items()}
            for row in df.to_dict(orient="records")
        ]

        return jsonify(
            {
                "success": True,
                "columns": list(df.columns),
                "rows": rows,
            }
        )

    except Exception as e:
        # Para depuración, imprime el error en la consola de Flask
        import traceback

        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/ffiec-test", methods=["GET"])
def ffiec_test():
    """
    Prueba de llamada a /view/filers de FFIEC hecha DESDE EL SERVIDOR.
    El navegador solo ve esta ruta /api/ffiec-test, que es mismo origen.
    """
    try:
        params = {
            "years": "2023",
            "msamds": "11244",
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
        resp.raise_for_status()
        data = resp.json()

        return jsonify({
            "success": True,
            "url": resp.url,
            "status": resp.status_code,
            "institutions": data.get("institutions", []),
        })

    except requests.RequestException as e:
        # Aquí verás si realmente hay un 403, 5xx, etc.
        return jsonify({
            "success": False,
            "error": f"Error al llamar a FFIEC: {str(e)}",
        }), 500


if __name__ == "__main__":
    app.run(debug=True)
