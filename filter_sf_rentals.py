"""
Filtra archivos CSV (o .csv.gz) de rental prices de California
para quedarse solo con los zipcodes de San Francisco.

USO:
    python filter_sf_rentals.py --input_dir ./data --output_dir ./sf_filtered
    python filter_sf_rentals.py --input_dir ./data --output_dir ./sf_filtered --consolidated

    --input_dir    : carpeta con los CSVs/GZs originales
    --output_dir   : carpeta donde se guardarán los archivos filtrados
    --zipcode_col  : nombre de la columna de zipcode (default: "ZIP")
    --consolidated : genera un único CSV en vez de uno por archivo
"""

import argparse
import os
import glob
import pandas as pd

# Zipcodes residenciales/estándar de San Francisco
SF_ZIPCODES = {
    94102, 94103, 94104, 94105, 94107, 94108, 94109, 94110,
    94111, 94112, 94114, 94115, 94116, 94117, 94118, 94121,
    94122, 94123, 94124, 94127, 94128, 94129, 94130, 94131,
    94132, 94133, 94134, 94158, 94188,
}


def filter_file(filepath: str, zipcode_col: str) -> pd.DataFrame:
    """Lee un CSV/GZ y devuelve solo las filas de San Francisco."""
    compression = "gzip" if filepath.endswith(".gz") else "infer"
    df = pd.read_csv(filepath, compression=compression, low_memory=False)

    # Limpiar zipcode: convertir a string, quitar decimales, quedarse con 5 dígitos
    df[zipcode_col] = (
        df[zipcode_col]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str[:5]
    )

    # Filtrar por zipcodes de SF
    mask = df[zipcode_col].apply(lambda z: z.isdigit() and int(z) in SF_ZIPCODES)
    return df[mask]


def main():
    parser = argparse.ArgumentParser(
        description="Filtra archivos de rental prices para quedarse con SF zipcodes."
    )
    parser.add_argument("--input_dir", required=True, help="Carpeta con archivos originales")
    parser.add_argument("--output_dir", required=True, help="Carpeta de salida")
    parser.add_argument("--zipcode_col", default="ZIP", help="Nombre de la columna de zipcode (default: ZIP)")
    parser.add_argument("--consolidated", action="store_true", help="Generar un único CSV consolidado")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Buscar .csv, .csv.gz y _csv.gz (tu formato)
    csv_files = sorted(set(
        glob.glob(os.path.join(args.input_dir, "*.csv"))
        + glob.glob(os.path.join(args.input_dir, "*.csv.gz"))
        + glob.glob(os.path.join(args.input_dir, "*_csv.gz"))
    ))

    if not csv_files:
        print(f"No se encontraron archivos CSV/GZ en {args.input_dir}")
        return

    print(f"Encontrados {len(csv_files)} archivos.\n")

    all_frames = []
    total_rows_original = 0
    total_rows_filtered = 0

    for filepath in csv_files:
        filename = os.path.basename(filepath)
        try:
            compression = "gzip" if filepath.endswith(".gz") else "infer"
            n_original = len(pd.read_csv(filepath, compression=compression, usecols=[0], low_memory=False))

            df_filtered = filter_file(filepath, args.zipcode_col)
            n_filtered = len(df_filtered)

            total_rows_original += n_original
            total_rows_filtered += n_filtered

            # Truncar nombre para la consola
            display_name = filename if len(filename) <= 55 else filename[:52] + "..."
            print(f"  {display_name}: {n_original:,} -> {n_filtered:,} (SF)")

            if args.consolidated:
                df_filtered = df_filtered.copy()
                df_filtered["source_file"] = filename
                all_frames.append(df_filtered)
            else:
                out_name = "sf_" + filename.replace("_csv.gz", ".csv").replace(".csv.gz", ".csv")
                output_path = os.path.join(args.output_dir, out_name)
                df_filtered.to_csv(output_path, index=False)

        except KeyError:
            print(f"  {filename}: columna '{args.zipcode_col}' no encontrada. Saltando.")
        except Exception as e:
            print(f"  {filename}: Error: {e}")

    if args.consolidated and all_frames:
        consolidated_df = pd.concat(all_frames, ignore_index=True)
        output_path = os.path.join(args.output_dir, "sf_rentals_consolidated.csv")
        consolidated_df.to_csv(output_path, index=False)
        print(f"\nConsolidado guardado: {output_path}")
        print(f"  -> {len(consolidated_df):,} filas, {consolidated_df[args.zipcode_col].nunique()} zipcodes SF")

    print(f"\n{'='*50}")
    print(f"RESUMEN: {total_rows_filtered:,} filas SF de {total_rows_original:,} totales")
    print(f"Archivos en: {args.output_dir}")


if __name__ == "__main__":
    main()
