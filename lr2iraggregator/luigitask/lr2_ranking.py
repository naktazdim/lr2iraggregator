import time

import lr2irscraper
import luigi

from lr2iraggregator.util.luigiutil import WorkDirDFCSVTask
from lr2iraggregator.luigitask.bms_tables import MakeItemCSVTask
from lr2iraggregator.logic.ranking import *


class GetRankingTask(WorkDirDFCSVTask):
    """
    GetRanking"s"Task の内部で呼ばれるタスク。
    1 項目分のランキングデータを取得する。
    """
    name = "ranking"

    type = luigi.Parameter()
    lr2_id = luigi.Parameter()
    lr2_hash = luigi.Parameter(default="", significant=False)
    wait = luigi.FloatParameter(default=1.0, significant=False)

    resources = {"lr2ir_access": 1}  # LR2IR にアクセスするタスクが同時に実行されないようにする

    def run(self):
        # nan が luigi.Parameter() として渡されると "nan" (str) になるらしい
        if self.lr2_hash == "" or self.lr2_hash is None or self.lr2_hash == "nan":
            ranking = (
                lr2irscraper
                .get_ranking_data_detail(int(self.lr2_id), str(self.type) + "id")
                .assign(clear=lambda df: df["clear"].cat.codes.clip(1, 5))
                .assign(notes=lambda df: df["max_score"] // 2)
            )
        else:
            ranking = lr2irscraper.get_ranking_data(self.lr2_hash)

        time.sleep(self.wait)  # LR2IR のサーバに負荷をかけないようにウェイト

        df = (
            ranking
            [["id", "name", "clear", "pg", "gr", "minbp", "notes"]]
            .rename(columns={"id": "player_lr2_id", "name": "player_name", "notes": "num_notes"})
            .assign(item_type=self.type)
            .assign(item_lr2_id=self.lr2_id)
        )  # (item_type, item_lr2_id, player_lr2_id, player_name, clear, pg, gr, minbp, num_notes)

        self.save(df, index=False)


class GetRankingsTask(WorkDirDFCSVTask):
    """
    item.csv 内にある全てのランキングデータを取得する。
    """
    name = "rankings"

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    WAIT = 1.0  # 1 項目取得する度に挟むインターバル (秒)。いずれ外部から指定できるようにする

    def requires(self):
        return MakeItemCSVTask(bms_table_list_csv=self.bms_table_list_csv,
                               hash_table_json=self.hash_table_json,
                               work_dir=self.work_dir)

    def run(self):
        item = self.load_requires()

        # item_df の各行に対応する項目のランキングを取得
        tasks = []
        for i, (_, row) in enumerate(item.iterrows()):
            tasks.append(GetRankingTask(type=row["type"],
                                        lr2_id=row["lr2_id"],
                                        lr2_hash=row["lr2_hash"],
                                        wait=self.WAIT,
                                        work_dir=self.work_dir))
        yield tasks

        df = pd.concat([task.load() for task in tasks])
        self.save(df, index=False)


class MakeItemCSVWithNumNotesTask(WorkDirDFCSVTask):
    """
    item.csv に num_notes を付加する。
    """
    name = "item_with_num_notes"

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return (
            GetRankingsTask(bms_table_list_csv=self.bms_table_list_csv,
                            hash_table_json=self.hash_table_json,
                            work_dir=self.work_dir),
            MakeItemCSVTask(bms_table_list_csv=self.bms_table_list_csv,
                            hash_table_json=self.hash_table_json,
                            work_dir=self.work_dir)
        )

    def run(self):
        rankings, item = self.load_requires()
        self.save(append_num_notes(item, extract_num_notes(rankings)), index=False)


class MakePlayerCSVTask(WorkDirDFCSVTask):
    """
    プレイヤの一覧を抽出する。
    """
    name = "player_name"

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return GetRankingsTask(bms_table_list_csv=self.bms_table_list_csv,
                               hash_table_json=self.hash_table_json,
                               work_dir=self.work_dir)

    def run(self):
        self.save(extract_players(self.load_requires()), index=False)


class MakeRecordCSVTask(WorkDirDFCSVTask):
    """
    プレイ記録 ((譜面, プレイヤ) の組 ごとのクリアランプやミスカウントなど) を抽出する。
    """
    name = "record"

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return GetRankingsTask(bms_table_list_csv=self.bms_table_list_csv,
                               hash_table_json=self.hash_table_json,
                               work_dir=self.work_dir)

    def run(self):
        self.save(
            self.load_requires()
            [["item_type", "item_lr2_id", "player_lr2_id", "clear", "pg", "gr", "minbp"]],
            index=False
        )
