import numpy as np
import pandas as pd

from data_processing_benchmarks.const import crime_path, reddit_path


def do_pandas_test():
    # Pandas
    # 1. Read in `Crime_Data_from_2020_to_Present.csv` to a frame, converting all datetime to native datetime format: `Date Rptd`, `DATE OCC`
    df_crime = pd.read_csv(
        crime_path, parse_dates=["Date Rptd", "DATE OCC"], dtype={"Vict Age": int, "LAT": float, "LON": float}
    )

    # 2. Filter this frame such that:
    #    1. Column `Vict Age` > 15
    #    2. Use only records in the quantiles 0.05 -> 0.95 for `LAT` and `LON`
    df_crime = df_crime[df_crime["Vict Age"] > 15]
    df_crime = df_crime[df_crime["LAT"].quantile(0.05) < df_crime["LAT"]]
    df_crime = df_crime[df_crime["LAT"] < df_crime["LAT"].quantile(0.95)]
    df_crime = df_crime[df_crime["LON"].quantile(0.05) < df_crime["LON"]]
    df_crime = df_crime[df_crime["LON"] < df_crime["LON"].quantile(0.95)]

    # 3. Read in the `reddit_account_data.csv` to a frame, converting all datetime to native datetime format: `created_utc`, `updated_on`
    df_reddit = pd.read_csv(reddit_path)
    df_reddit["created_utc"] = pd.to_datetime(df_reddit["created_utc"], unit="s")
    df_reddit["updated_on"] = pd.to_datetime(df_reddit["updated_on"], unit="s")

    # 4. Filter this frame such that:
    #    1. None of the account names end in the number 7
    #    2. None of the account names have the string abc in them
    df_reddit = df_reddit[df_reddit["name"].str.contains("abc") == False]
    df_reddit = df_reddit[df_reddit["name"].str.endswith("7") == False]
    # df_reddit = df_reddit[df_reddit["name"].str.match("(.*abc.*|.*7$)") == False]

    # 5. Create a column in the `account_data` frame, and assign each of the users a `group` value, uniformly, from the `crime_data.DR_NO` column
    rng = np.random.default_rng()
    unique = df_crime["DR_NO"].unique()
    df_reddit["group"] = rng.choice(unique, len(df_reddit))

    # 6. Join these two frames together on `account_data.group == crime_data.DR_NO`
    df_merge = df_reddit.merge(df_crime, how="inner", right_on="DR_NO", left_on="group")

    # 7. Select only those who are NOT part of the following Statuses:
    #     1. `CC`
    #     2. `AA`
    df_merge = df_merge[~df_merge["Status"].isin(("CC", "AA"))]

    df_merge["grp_by"] = [s[0].lower() for s in df_merge["name"]]
    df_merge["karma_sum"] = df_merge["comment_karma"] + df_merge["link_karma"]
    df_merge["since_rptd"] = df_merge["Date Rptd"] - pd.Timestamp.now()
    df_merge["since_open"] = df_merge["created_utc"] - pd.Timestamp.now()
    groups = df_merge.groupby("grp_by")

    # 8. Group these by the lower case first letter of their name and calculate:
    res = groups.agg(
        #    1. The average time since reporting the crime as `mean_time_since_reports`
        mean_time_since_reports=pd.NamedAgg(column="since_rptd", aggfunc=np.mean),
        #    2. The number of members in the group as `count_members`
        count_members=pd.NamedAgg(column="id", aggfunc="count"),
        #    3. The user with the highest & lowest Karma scores as `highest_karma` and `lowest_karma`
        highest_karma=pd.NamedAgg(column="karma_sum", aggfunc=np.max),
        lowest_karma=pd.NamedAgg(column="karma_sum", aggfunc=np.min),
        #    4. The average time since creating an account as `mean_time_since_account_open`
        mean_time_since_account_open=pd.NamedAgg(column="since_open", aggfunc=np.mean),
    )
    return res
