import polars as pl
import time

# to enrich the examples in this quickstart with dates
from datetime import datetime, timedelta 
# to generate data for the examples
#import numpy as np 

def save_dataframes():
    # with a tuple
    series = pl.Series("a", [1, 2, 3, 4, 5])

    print(series)


    dataframe=pl.DataFrame({"integer":[1,2,3],
                            "date":[
                                (datetime(2022,1,1)),
                                (datetime(2022,1,2)),
                                (datetime(2022,1,3))
                            ],
                            "float":[4.0,5.0,6.0]})

    print(dataframe)

    # Write CSV file
    dataframe.write_csv("output.csv")
    df_csv=pl.read_csv("output.csv")
    print(df_csv)

    # Write parquet file
    dataframe.write_parquet("output.parquet")
    df_parquet=pl.read_parquet("output.parquet")
    print(df_parquet)


def make_aggregations_fits_memory():
    file_name="69M_reddit_accounts.csv"
    dataset = pl.read_csv(file_name, n_rows=10)
    dataset.describe()

    # Create a query but do not execute
    q1=(
        pl.scan_csv(file_name)
        .with_columns(pl.col("name").str.to_uppercase())
        .filter(pl.col("comment_karma")>0)
    )

    # Show non-optimized query plan
    print(q1.explain())

    # `describe_plan` is deprecated
    #print(q1.describe_plan())

    # Save start time
    start=time.time()

    # Execute the query on the full dataset and show results
    print(q1.collect())

    # Save end time
    end=time.time()

    # Get duration
    print(f"Duration: {end-start}")


def make_aggregations_does_not_fit_memory():
    file_name="69M_reddit_accounts.csv"
    dataset = pl.read_csv(file_name, n_rows=10)
    dataset.describe()

    # Create a query but do not execute
    q1=(
        pl.scan_csv(file_name)
        .with_columns(pl.col("name").str.to_uppercase())
        .filter(pl.col("comment_karma")>0)
    )

    # Show non-optimized query plan
    print(q1.explain())

    # Save start time
    start=time.time()

    # Execute the query and show results using streaming,
    # this is useful when dataset is bigger than available memory
    print(q1.collect(streaming=True))


    # Save end time
    end=time.time()

    # Get duration
    print(f"Duration: {end-start}")

def save_dataset_does_not_fit_memory():
    file_name="69M_reddit_accounts.csv"
    dataset = pl.read_csv(file_name, n_rows=10)
    dataset.describe()

    # Create a query but do not execute
    q1=(
        pl.scan_csv(file_name)
        .with_columns(pl.col("name").str.to_uppercase())
        .filter(pl.col("comment_karma")>0)
    )

    q1.collect().write_parquet("reddit.parquet")

def save_dataset_streaming():
    file_name="69M_reddit_accounts.csv"
    dataset = pl.read_csv(file_name, n_rows=10)
    dataset.describe()

    # Create a query but do not execute
    q1=(
        pl.scan_csv(file_name)
        .with_columns(pl.col("name").str.to_uppercase())
        .filter(pl.col("comment_karma")>0)
    )

    q1.collect(streaming=True).write_parquet("reddit_streaming.parquet")

if __name__=="__main__":
    #save_dataframes();
    #make_aggregations_fits_memory()
    #make_aggregations_does_not_fit_memory()
    #save_dataset_does_not_fit_memory()
    save_dataset_streaming()