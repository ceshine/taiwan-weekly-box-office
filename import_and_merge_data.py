from pathlib import Path
from datetime import datetime
from typing import List

import pandas as pd

from download_data import TARGET_FOLDER

OUTPUT_FOLDER = Path("output/")
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
COLUMNS_IN = [
    '國別地區', '中文片名', '上映日期', '申請人',
    '出品', '上映院數', '銷售票數', '銷售金額', '累計銷售票數', '累計銷售金額'
]

COLUMNS_OUT = [
    'country', 'name', 'release_date',
    'agent', 'publisher', 'theaters',
    'tickets', 'revenue',
    'total_tickets', 'total_revenue'
]


def main():
    buffer: List[pd.DataFrame] = []
    for filename in TARGET_FOLDER.iterdir():
        if filename.suffix == ".xlsx":
            df = pd.read_excel(str(filename), thousands=',')
            if "申請人" not in df.columns:
                print(f"申請人 missing in week {filename.stem}")
                # missing data
                df["申請人"] = "N/A"
            if "累計票數" in df.columns:
                df.rename(columns={"累計票數": "累計銷售票數"}, inplace=True)
            if "累計金額" in df.columns:
                df.rename(columns={"累計金額": "累計銷售金額"}, inplace=True)
            df = df[COLUMNS_IN]
            df.columns = COLUMNS_OUT
            df["week"] = datetime.strptime(filename.stem, "%Y%m%d")
            df = df[["week"] + COLUMNS_OUT]
            buffer.append(df)
    df_final = pd.concat(buffer).sort_values("week")
    df_final = df_final[~df_final.name.isnull()]
    for col in ("theaters", "tickets", "revenue", "total_tickets", "total_revenue"):
        df_final[col] = df_final[col].astype("int")
    # Normalize the release date
    df_final["release_date"] = df_final["release_date"].apply(lambda x: str(x).split(" ")[0].replace("/", "-"))
    print(
        "# of moveis with only year of release:",
        df_final[df_final.release_date.str.len() == 4].shape[0]
    )
    df_final.loc[df_final.release_date.str.len() == 4, "release_date"] += "-01-01"
    assert df_final.loc[df_final.release_date.str.len() == 4].shape[0] == 0
    df_final.to_csv(OUTPUT_FOLDER / "box_office.csv", index=False)


if __name__ == "__main__":
    main()
