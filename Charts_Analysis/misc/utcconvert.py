import pandas as pd
import datetime

# Convert timestamp from your DB
print(datetime.datetime.utcfromtimestamp(1750113420000/ 1000))  # --> 2025-06-17 16:30:00

print(int(pd.to_datetime("2025-06-20 00:04:00").value // 10**6))