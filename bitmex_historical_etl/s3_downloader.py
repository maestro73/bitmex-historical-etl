import gzip
import os
from tempfile import NamedTemporaryFile

import httpx
import pandas as pd

from .utils import get_bitmex_s3_data_url


class S3Downloader:
    def download(self, date):
        temp_file = NamedTemporaryFile()
        filename = temp_file.name
        with open(filename, "wb+") as temp:
            url = get_bitmex_s3_data_url(date)
            # Streaming gave many EOFErrors.
            response = httpx.get(url)
            if response.status_code == 200:
                temp.write(response.content)
                size = os.path.getsize(filename)
                if size > 0:
                    return self._extract(date, filename)
                else:
                    print(f"No data: {url}")
            else:
                print(f"No data: {url}")

    def _extract(self, date, filename):
        try:
            data_frame = pd.read_csv(filename, engine="python", compression="gzip")
        except EOFError:
            date_string = date.isoformat()
            print(f"EOFError: {date_string}")
            data_frame = self._extract_force(filename)
        return data_frame

    def _extract_force(self, filename):
        lines = []
        try:
            with gzip.open(filename, "rt") as f:
                for line in f:
                    lines.append(line)
        except EOFError:
            data = [line.strip().split(",") for line in lines]
            data_frame = pd.DataFrame(data[1:], columns=data[0])
        return data_frame
