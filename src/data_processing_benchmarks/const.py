from pathlib import Path

_data_root = Path(__file__).parent.parent.parent / "data"
crime_path = _data_root / "Crime_Data_from_2020_to_Present.csv"
reddit_path = _data_root / "reddit_account_data.csv"

print(crime_path)
print(reddit_path)
