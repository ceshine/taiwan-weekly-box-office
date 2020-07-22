import time
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import typer
import requests

URL_PATTERN = "https://www.tfi.org.tw/Content/TFI/PublicInfo/全國電影票房%s年%s-%s統計資訊.xlsx"
URL_PATTERN_ALT = "https://www.tfi.org.tw/Content/TFI/PublicInfo/全國電影票房%s年%s-%s年%s統計資訊.xlsx"
REFERENCE_START_DATE = datetime(2020, 7, 6)
TARGET_FOLDER = Path("cache/")
TARGET_FOLDER.mkdir(exist_ok=True, parents=True)


def main(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    if start_date is None:
        start_date = datetime(2017, 10, 2)
    if end_date is None:
        end_date = datetime.now()
    delta = (start_date - REFERENCE_START_DATE).days
    if delta % 7 != 0:
        raise ValueError("start-date must be a Monday!")
    print(start_date, end_date)
    while start_date < end_date:
        end_of_the_week = start_date + timedelta(days=6)
        if start_date.year == end_of_the_week.year:
            url = URL_PATTERN % (
                start_date.year, start_date.strftime("%m%d"),  end_of_the_week.strftime("%m%d")
            )
        else:
            url = URL_PATTERN_ALT % (
                start_date.year, start_date.strftime("%m%d"),
                end_of_the_week.year, end_of_the_week.strftime("%m%d")
            )
        if start_date >= datetime(2018, 8, 20) and start_date < datetime(2019, 7, 29):
            # The chaos in naming...
            url = url.replace("全國電影票房", "全國票房")
        res = requests.get(url)
        if res.status_code != 200:
            print(f"Error {res.status_code} getting {start_date.strftime('%Y%m%d')}")
            break
        target_file = f"{start_date.strftime('%Y%m%d')}.xlsx"
        with open(TARGET_FOLDER / target_file, 'wb') as f:
            f.write(res.content)
        print(f"Wrote {target_file}")
        start_date += timedelta(days=7)
        time.sleep(random.random()*2)


if __name__ == "__main__":
    typer.run(main)
