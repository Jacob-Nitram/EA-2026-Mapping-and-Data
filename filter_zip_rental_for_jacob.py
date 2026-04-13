"""
Filters CSV (or .csv.gz) rental price files from California
to keep only San Francisco zipcodes.

USAGE:
    python filter_sf_rentals.py --input_dir ./data --output_dir ./sf_filtered
    python filter_sf_rentals.py --input_dir ./data --output_dir ./sf_filtered --consolidated

    --input_dir    : folder containing the original CSVs/GZs
    --output_dir   : folder where filtered files will be saved
    --zipcode_col  : name of the zipcode column (default: "ZIP")
    --consolidated : generates a single CSV instead of one per file
"""

import argparse
import os
import glob
import pandas as pd

# Residential/standard San Francisco zipcodes
SF_ZIPCODES = {
    94102, 94103, 94104, 94105, 94107, 94108, 94109, 94110,
    94111, 94112, 94114, 94115, 94116, 94117, 94118, 94121,
    94122, 94123, 94124, 94127, 94128, 94129, 94130, 94131,
    94132, 94133, 94134, 94158, 94188,
}


def filter_file(filepath: str, zipcode_col: str) -> pd.DataFrame:
    """Reads a CSV/GZ and returns only San Francisco rows."""
    compression = "gzip" if filepath.endswith(".gz") else "infer"
    df = pd.read_csv(filepath, compression=compression, low_memory=False)

    # Clean zipcode: convert to string, remove decimals, keep 5 digits
    df[zipcode_col] = (
        df[zipcode_col]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str[:5]
    )

    # Filter by SF zipcodes
    mask = df[zipcode_col].apply(lambda z: z.isdigit() and int(z) in SF_ZIPCODES)
    return df[mask]


def main():
    parser = argparse.ArgumentParser(
        description="Filters rental price files to keep only SF zipcodes."
    )
    parser.add_argument("--input_dir", required=True, help="Folder with original files")
    parser.add_argument("--output_dir", required=True, help="Output folder")
    parser.add_argument("--zipcode_col", default="ZIP", help="Name of the zipcode column (default: ZIP)")
    parser.add_argument("--consolidated", action="store_true", help="Generate a single consolidated CSV")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Search for .csv, .csv.gz, and _csv.gz (your format)
    csv_files = sorted(set(
        glob.glob(os.path.join(args.input_dir, "*.csv"))
        + glob.glob(os.path.join(args.input_dir, "*.csv.gz"))
        + glob.glob(os.path.join(args.input_dir, "*_csv.gz"))
    ))

    if not csv_files:
        print(f"No CSV/GZ files found in {args.input_dir}")
        return

    print(f"Found {len(csv_files)} files.\n")

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

            # Truncate name for console display
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
            print(f"  {filename}: column '{args.zipcode_col}' not found. Skipping.")
        except Exception as e:
            print(f"  {filename}: Error: {e}")

    if args.consolidated and all_frames:
        consolidated_df = pd.concat(all_frames, ignore_index=True)
        output_path = os.path.join(args.output_dir, "sf_rentals_consolidated.csv")
        consolidated_df.to_csv(output_path, index=False)
        print(f"\nConsolidated file saved: {output_path}")
        print(f"  -> {len(consolidated_df):,} rows, {consolidated_df[args.zipcode_col].nunique()} SF zipcodes")

    print(f"\n{'='*50}")
    print(f"SUMMARY: {total_rows_filtered:,} SF rows out of {total_rows_original:,} total")
    print(f"Files saved to: {args.output_dir}")


if __name__ == "__main__":
    main()
