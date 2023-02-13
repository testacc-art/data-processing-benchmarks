from datetime import datetime

import polars as pl

from data_processing_benchmarks.const import crime_path, reddit_path


def do_polars_test():
    # the data:
    # 1. Read in `Crime_Data_from_2020_to_Present.csv` to a frame, converting all datetime to native datetime format: `Date Rptd`, `DATE OCC`
    df_crime = pl.scan_csv(crime_path, parse_dates=True)
    df_reddit = pl.scan_csv(reddit_path, parse_dates=True)

    distinct_ids = df_crime.select([pl.col("DR_NO").unique()]).collect()

    q0 = (
        # 3. Read in the `reddit_account_data.csv` to a frame, converting all datetime to native datetime format: `created_utc`, `updated_on`
        df_reddit
        # 4. Filter this frame such that:
        #    1. None of the account names end with a number
        .filter(~pl.col("name").str.contains("abc")).filter(~pl.col("name").str.ends_with("7"))
        # 5. Create a column in the `account_data` frame, and assign each of the users a `group` value, uniformly, from the `crime_data.DR_NO` column
        .with_columns(
            [
                pl.col("name").apply(lambda _: distinct_ids.sample(seed=0, with_replacement=True)[0]).alias("DR_NO"),
                pl.from_epoch(pl.col("created_utc"), unit="s").alias("created_utc"),
            ]
        )
    )
    print(q0.collect())

    q1 = (
        df_crime
        # 2. Filter this frame such that:
        #    1. Column `Vict Age` > 15
        .filter(pl.col("Vict Age") > 15)
        #    2. Use only records in the quantiles 0.05 -> 0.95 for `LAT` and `LON`
        .filter(pl.col("LAT").quantile(0.05) < pl.col("LAT"))
        .filter(pl.col("LAT").quantile(0.95) > pl.col("LAT"))
        .filter(pl.col("LON").quantile(0.05) < pl.col("LON"))
        .filter(pl.col("LON").quantile(0.95) > pl.col("LON"))
        # 6. Join these two frames together on `account_data.Group == crime_data.Status`
        .join(q0, on="DR_NO")
        # 7. Select only those who are NOT part of the following groups:
        #     1. `CC`
        #     2. `AA`
        .filter(~pl.col("Status").is_in(("CC", "AA")))
        # 8. Group these by the lower case first letter of their name and calculate:
        .with_columns(
            [
                pl.col("name").apply(lambda name: name[0].lower()).alias("grp_by"),
                (pl.col("comment_karma") + pl.col("link_karma")).alias("karma_sum"),
            ]
        )
        .groupby("grp_by")
        .agg(
            [
                #    1. The average time since reporting the crime as `mean_time_since_reports`
                pl.col("Date Rptd")
                .drop_nulls()
                .apply(lambda x: x - datetime.now())
                .mean()
                .alias("mean_time_since_reports"),
                #    2. The number of members in the group as `count_members`
                pl.col("name").count().alias("count_members"),
                #    3. The user with the highest & lowest Karma scores as `highest_karma` and `lowest_karma`
                pl.col("karma_sum").max().alias("highest_karma"),
                pl.col("karma_sum").min().alias("lowest_karma"),
                #    4. The average time since creating an account as `mean_time_since_account_open`
                pl.col("created_utc")
                .drop_nulls()
                .apply(lambda x: x - datetime.now())
                .mean()
                .alias("mean_time_since_account_open"),
            ]
        )
    )
    return q1.collect()
