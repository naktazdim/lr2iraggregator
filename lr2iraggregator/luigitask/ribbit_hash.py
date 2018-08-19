"""
Ribbit! さんの難易度表まとめデータから LR2IR のハッシュ表を生成する。
"""

import argparse
import requests
import sys

from lr2irscraper import get_bms_table
from lr2iraggregator.logic.bms_tables import merge_bms_tables, convert_lr2irscraper_bms_table_to_bmsirt_format
from lr2iraggregator.logic.lr2_hash import make_hash_table_from_bms_table
from lr2iraggregator.util.luigiutil import *


class LoadBMSTableIgnoringFailureTask(WorkDirDFCSVTask):
    """
    URL から難易度表ひとつのデータを読み込む。読み込めなかった場合も、空のファイルを出力し、タスクは成功したものとみなす。

    Args:
        url: 難易度表の URL

    Returns:
        (type, lr2_id, lr2_hash, level, title)
    """

    name = "bms_table"
    dtypes = {"lr2_id": str}

    url = luigi.Parameter()

    def run(self):
        try:
            df = get_bms_table(self.url)
            df = convert_lr2irscraper_bms_table_to_bmsirt_format(df)
        except Exception as e:
            sys.stderr.write(str(e.args))
            df = pd.DataFrame(columns=["type", "lr2_id", "lr2_hash", "level", "title"])

        self.save(df, index=False)


class MakeHashTableFromRibbitData(WorkDirJSONTask):
    name = "ribbit_hash_table"

    url = luigi.Parameter()

    def run(self):
        ribbit_json = requests.get(self.url).json()
        tasks = [LoadBMSTableIgnoringFailureTask(url=table_data["url"], work_dir=self.work_dir)
                 for table_data in ribbit_json
                 if "//www.ribbit.xyz" in table_data["url"]]
        yield tasks
        bms_tables = [pd.read_csv(task.output().path, dtype={"lr2_id": str}) for task in tasks]
        bms_table_dict = dict(enumerate(bms_tables))
        merged_bms_tables = merge_bms_tables(bms_table_dict)
        self.save(make_hash_table_from_bms_table(merged_bms_tables))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("output_path", type=str)
    p.add_argument("work_dir", type=str)
    p.add_argument("--ribbit-json-url", type=str, default="http://www.ribbit.xyz/bms/tables/table_info.json")
    args = p.parse_args()

    task = MakeHashTableFromRibbitData(
        url=args.ribbit_json_url,
        work_dir=args.work_dir
    )
    luigi.build([task], local_scheduler=True, workers=4)
    if task.complete():
        shutil.copy(task.output().path, args.output_path)


if __name__ == "__main__":
    main()
