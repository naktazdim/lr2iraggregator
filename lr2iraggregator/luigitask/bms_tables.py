from lr2irscraper import get_bms_table

from lr2iraggregator.logic.bms_tables import *
from lr2iraggregator.logic.lr2_hash import fill_ids, fill_hashes
from lr2iraggregator.util.luigiutil import *
from lr2iraggregator.luigitask.lr2_hash import RegisterNewHashesToHashTableTask


class LoadBMSTableTask(WorkDirDFCSVTask):
    """
    難易度表ひとつのデータを読み込む。URL、またはローカルの csv ファイルへのパスを入力に取る。

    Args:
        bms_table_path: 難易度表の URL またはパスを表す文字列

    Returns:
        (type, lr2_id, lr2_hash, level, title)
    """

    name = "bms_table"
    dtypes = {"lr2_id": str}

    bms_table_path = luigi.Parameter()

    def run(self):

        if is_url(self.bms_table_path):
            df = get_bms_table(self.bms_table_path)
            df = convert_lr2irscraper_bms_table_to_bmsirt_format(df)
        else:
            df = (
                pd.read_csv(self.bms_table_path, header=0, dtype=str)
                  .astype({"type": CategoricalDtype(categories=["bms", "course"])})
            )

        self.save(df, index=False)


class LoadBMSTableListTask(WorkDirDFCSVTask):
    """
    難易度表のリストを読み込む。
    リスト内の path で指定されている相対パスはすべて絶対パスに直す。

    Args:
        bms_table_list_csv: 難易度表リスト

    Returns:
        (name, path, is_dan)
    """
    name = "bms_table_list"
    dtypes = {"is_dan": bool}

    bms_table_list_csv = luigi.Parameter()

    def run(self):
        self.save(load_bms_table_list_csv(self.bms_table_list_csv), index=False)


class LoadBMSTablesWithIDUnFilledTask(WorkDirDFCSVTask):
    """
    難易度表のリストから難易度表をすべて読み込み、その内容を一つのテーブルに結合する。
    この段階では LR2ID に抜けがある (ことがある)。あとでハッシュテーブルを用いて埋める。

    Args:
        bms_table_list_csv: 難易度表リスト (name, path)

    Returns:
        (type, lr2_id, bms_table_name, lr2_hash, level, title)
    """

    name = "merged_bms_tables_without_ids"
    dtypes = {"lr2_id": str}

    bms_table_list_csv = luigi.Parameter()

    def requires(self):
        return LoadBMSTableListTask(
            bms_table_list_csv=self.bms_table_list_csv,
            work_dir=self.work_dir
        )

    def run(self):
        bms_table_list = self.load_requires()
        bms_table_tasks = {
            row["name"]: LoadBMSTableTask(
                bms_table_path=row["path"],
                work_dir=self.work_dir
            )
            for _, row in bms_table_list.iterrows()
        }
        yield bms_table_tasks.values()

        df = merge_bms_tables({key: task.load() for key, task in bms_table_tasks.items()})
        self.save(df, index=False)


class UpdateHashTableTask(WorkDirJSONTask):
    """
    bms_table_list 中の難易度表にある項目から、IDが未知のものを抽出し、
    そのIDをLR2IRにアクセスすることで調べ、ハッシュテーブルに登録する。
    (正確には、「既知のハッシュテーブルに新規のハッシュを追加したハッシュテーブル」を新たに生成する)
    """
    name = "hash_table_updated"

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return LoadBMSTablesWithIDUnFilledTask(
                bms_table_list_csv=self.bms_table_list_csv,
                work_dir=self.work_dir
            )

    def run(self):
        hash_table_task = RegisterNewHashesToHashTableTask(
            list_csv=self.requires().output().path,
            hash_table_json=self.hash_table_json,
            work_dir=self.work_dir
        )
        yield hash_table_task
        self.save(hash_table_task.load())


class LoadBMSTablesTask(WorkDirDFCSVTask):
    """
    ハッシュテーブルを用いて LoadBMSTablesTaskWithIDUnfilled の出力のIDの欠損を埋め、返す。

    Args:
        bms_table_list_csv: 難易度表リスト (name, path)
        hash_table_json: ハッシュテーブル {lr2_hash: (type, lr2_id)}

    Returns:
        (type, lr2_id, bms_table_name, lr2_hash, level, title)

    """
    name = "bms_tables"

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return [
            LoadBMSTablesWithIDUnFilledTask(
                bms_table_list_csv=self.bms_table_list_csv,
                work_dir=self.work_dir
            ),
            UpdateHashTableTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            )
        ]

    def run(self):
        bms_tables_csv_without_ids, hash_table = self.load_requires()
        bms_tables_csv = fill_ids(bms_tables_csv_without_ids, hash_table)
        bms_tables_csv = fill_hashes(bms_tables_csv, hash_table)
        self.save(bms_tables_csv, index=False)


class MakeItemCSVTask(WorkDirDFCSVTask):
    """
    LoadBMSTablesTask の出力から item.csv を作成する。

    Args:
        bms_table_list_csv: 難易度表リスト (name, path)
        hash_table_json: ハッシュテーブル {lr2_hash: (type, lr2_id)}

    Returns:
        (type, lr2_id, lr2_hash, title)
    """

    name = "item"
    dtypes = {"lr2_id": str}

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return LoadBMSTablesTask(
            bms_table_list_csv=self.bms_table_list_csv,
            hash_table_json = self.hash_table_json,
            work_dir=self.work_dir
        )

    def run(self):
        self.save(make_item_df(self.load_requires()), index=False)


class MakeLevelCSVTask(WorkDirDFCSVTask):
    """
    LoadBMSTablesTask の出力から level.csv を作成する。

    Args:
        bms_table_list_csv: 難易度表リスト (name, path)
        hash_table_json: ハッシュテーブル {lr2_hash: (type, lr2_id)}


    Returns:
        (type, lr2_id, bms_table_name, level)
    """

    name = "level"
    dtypes = {"lr2_id": str}

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return LoadBMSTablesTask(
            bms_table_list_csv=self.bms_table_list_csv,
            hash_table_json=self.hash_table_json,
            work_dir=self.work_dir
        )

    def run(self):
        self.save(make_level_df(self.load_requires()), index=False)
