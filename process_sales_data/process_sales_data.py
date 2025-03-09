import polars as pl
import argparse
import psycopg2
import os
from sqlalchemy import create_engine


# Function to filter and validate data
def process_data(csv_path, filter_column=None, filter_value=None):
    """
    Loads and processes data from a CSV file, performing data quality checks and optional filtering.

    This function reads a CSV file into a DataFrame, checks for missing values, duplicates, and data types.
    If a filter column and value are provided, it filters the data accordingly.

    Args:
        csv_path (str): Path to the CSV file.
        filter_column (str, optional): Column name to apply filtering on.
        filter_value (str, optional): Value to filter the specified column by.

    Returns:

    print(f"Loading data from {csv_path}...")
        DataFrame: Processed data.
    """
    df = pl.read_csv(csv_path, infer_schema_length=0)
    ### Checking if original data has duplicates
    ### but not takign any action on transformation
    print("###### START DATA QUALITY CHECKS AND TRANSFORMATIONS #####\n")
    print(f"checking for duplicates... : {df.is_duplicated().sum()}")
    ### Checking for missing values on original dataframe
    print("checking for missing values... (Shows each record with any nulls) \n")
    df_nulls = df.filter(pl.any_horizontal(pl.all().is_null()))
    print(df_nulls)
    has_duplicated_saleIDs = (
        df.select(pl.col("SaleID").is_duplicated()).to_series().any()
    )
    print(
        f"\nhas duplicated saleIDs? : {has_duplicated_saleIDs}\n"
    )  # True if there are duplicates, False otherwise
    ### Transformations and formats for all columns
    ### strings : on having empty or null sets value to UNKNOWN
    ### integers : on having empty or null sets value to 0
    ### date : on having empty or null sets value to 1970-01-01
    string_columns_transform = [
        "Brand",
        "ProductName",
        "Category",
        "RetailerName",
        "Channel",
    ]
    int_columns_transform = ["ProductID", "RetailerID", "Quantity"]
    float_columns_transform = ["Price"]
    df = df.with_columns(
        [
            (
                pl.when(pl.col(col).is_null() | (pl.col(col) == ""))
                .then(pl.lit("Unknown"))
                .otherwise(pl.col(col))
                .alias(col)
            )
            for col in string_columns_transform
        ]
    )
    df = df.with_columns(
        [
            (pl.col(col).cast(pl.Int8, strict=False).fill_null(0)).alias(col)
            for col in int_columns_transform
        ]
    )
    df = df.with_columns(
        [
            (pl.col(col).cast(pl.Float32, strict=False).fill_null(0.0)).alias(col)
            for col in float_columns_transform
        ]
    )
    df = df.with_columns(
        [
            pl.col("Date")
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .fill_null("1970-01-01")
            .alias("Date")
        ]
    )
    if filter_column and filter_value:
        print(f"filtering data where {filter_column} == {filter_value}... \n")
        df = df.filter(df[filter_column] == filter_value)
    print("###### END DATA QUALITY CHECKS AND TRANSFORMATIONS #####\n")
    return df


def copy_to_postgres(df, replace_table, table_name, db_url):
    """
    Copies the given DataFrame to a PostgreSQL database using the COPY command.

    Args:
        df (DataFrame): DataFrame to copy.
        table_name (str): Name of the PostgreSQL table to copy to.
        db_url (str): URL of the PostgreSQL database to connect to.

    Returns:
        None
    """
    print("###### START LOAD / COPY DATA TO POSTGRES #####\n")
    print(f"Total records to copy: {len(df)}")
    if not df.is_empty():
        temp_csv = "filtered_data.csv"
        df.write_csv(temp_csv)  # Save to CSV for COPY command

        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        print(f"Replaces table {table_name}? [true/false] : {replace_table}")
        if replace_table:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            conn.commit()

        try:
            # Generate CREATE TABLE SQL based on DataFrame columns and types
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                SaleID int8 not null,
                ProductID int8,
                ProductName text,
                Brand text,
                Category text,
                RetailerID int8,
                RetailerName text,
                Channel text,
                Location text,
                Quantity int8,
                Price numeric,
                Date date not null,
                PRIMARY KEY (SaleID)
            )
            """
            # Execute CREATE TABLE
            cursor.execute(create_table_sql)
        except psycopg2.errors.DuplicateTable:
            print(f"Table {table_name} already exists. Skipping CREATE TABLE.")
        conn.commit()

        try:
            # Generate CREATE INDEX SQL based on DataFrame columns
            create_index_sql = f"""
            CREATE INDEX idx_{table_name}_product_id ON {table_name} (ProductID);
            CREATE INDEX idx_{table_name}_retailer_id ON {table_name} (RetailerID);
            CREATE INDEX idx_{table_name}_date ON {table_name} (Date);
            """
            # Execute CREATE INDEX
            cursor.execute(create_index_sql)
        except psycopg2.errors.DuplicateTable:
            print(f"Index already exists. Skipping CREATE INDEX.")
        conn.commit()

        with open(temp_csv, "r") as f:
            next(f)  # Skip header row
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

        cursor.close()
        conn.commit()
        conn.close()
        print(f"Data successfully copied to {table_name}!")
        os.remove(temp_csv)
    else:
        print("No data to copy.")
    print("\n###### END LOAD / COPY DATA TO POSTGRES #####\n")


def main():
    """
    Main function to parse command-line arguments and execute the data loading workflow.

    This function parses input arguments for a CSV file and PostgreSQL connection details,
    performs data validation and cleaning on the CSV file, and loads the cleaned data into
    the specified PostgreSQL table.

    Arguments:
        --csv (str): Path to the CSV file.
        --table (str): Name of the PostgreSQL table.
        --host (str): PostgreSQL host (default: localhost).
        --port (str): PostgreSQL port (default: 5432).
        --db (str): PostgreSQL database name.
        --user (str): PostgreSQL username.
        --password (str): PostgreSQL password.

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description="Load sales CSV file into postgres with data quality checks and filtering."
    )
    parser.add_argument("--csv", required=True, help="CSV file")
    parser.add_argument("--table", required=True, help="postgres table name")
    parser.add_argument(
        "--host", default="localhost", help="postgres host (default: localhost)"
    )
    parser.add_argument("--port", default="5432", help="postgres port (default: 5432)")
    parser.add_argument("--db", default="sales", help="postgres database name")
    parser.add_argument("--user", default="sales", help="postgres username")
    parser.add_argument("--password", default="sales", help="postgres password")
    parser.add_argument(
        "--replace_table",
        action="store_true",
        help="define if it drop or not the table",
    )
    parser.add_argument("--filter_column", help="Column to filter on (optional)")
    parser.add_argument("--filter_value", help="Value to filter on (optional)")
    args = parser.parse_args()
    db_url = (
        f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}"
    )
    df = process_data(args.csv, args.filter_column, args.filter_value)
    copy_to_postgres(df, args.replace_table, args.table, db_url)


if __name__ == "__main__":
    main()
