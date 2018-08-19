from time import sleep

from lr2irscraper.hash_util import hash_to_id

from lr2iraggregator.logic.lr2_hash import *
from lr2iraggregator.util.luigiutil import *


class MakeHashTableFromBMSTableListTask(WorkDirJSONTask):
    """
    list_csv から lr2_id と lr2_hash の対応を抽出する。

    Args:
        list_csv: (type, lr2_id, lr2_hash, ...)

    Returns:
        (type, lr2_id, lr2_hash)
    """

    name = "hash_table"
    dtypes = {"lr2_id": str}

    list_csv = luigi.Parameter()

    def run(self):
        self.save(make_hash_table_from_bms_table(pd.read_csv(self.list_csv, dtype=object)))


class ExtractNewHashesTask(WorkDirJSONTask):
    """
    list_csv から、ハッシュテーブルに載っていないハッシュを抽出する。

    Args:
        list_csv: (type, lr2_id, lr2_hash, ...)
        hash_table_json: ハッシュテーブル {lr2_hash: (type, lr2_id)}

    Returns:
        lr2_hash たち

    """

    name = "new_hashes"

    list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def run(self):
        item_csv = pd.read_csv(self.list_csv, dtype=object)
        hash_table = json.load(open(self.hash_table_json))
        self.save(list(extract_new_hashes(extract_hashes(item_csv), hash_table)))


class GetTypeAndIDFromHashTask(WorkDirJSONTask):
    """
    ハッシュからtype ("bms" または "course") とIDを得る。
    MakeHashTableFromNewHashesTask から呼ばれるだけのサブタスク。

    Args:
        lr2_hash: ハッシュ

    Returns:
        (type, lr2_id)

    """
    name = "lr2id"
    resources = {"lr2ir_access": 1}  # LR2IR にアクセスするタスクが同時に実行されないようにする

    lr2_hash = luigi.Parameter()

    def run(self):
        sleep(1.0)
        item_type, item_lr2_id = hash_to_id(self.lr2_hash)
        self.save([item_type, str(item_lr2_id)])


class MakeHashTableFromNewHashesTask(WorkDirJSONTask):
    """
    list_csv からハッシュテーブルに載っていないハッシュを抽出し、LR2IRにアクセスすることでIDを取得してハッシュテーブルを作る。

    Args:
        list_csv: (type, lr2_id, lr2_hash, ...)
        hash_table_json: ハッシュテーブル {lr2_hash: (type, lr2_id)}

    Returns:
        lr2_hash たち

    """
    name = "hash_table_from_new_hashes"

    list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return ExtractNewHashesTask(
            list_csv=self.list_csv,
            hash_table_json=self.hash_table_json,
            work_dir=self.work_dir
        )

    def run(self):
        task_dict = {new_hash: GetTypeAndIDFromHashTask(
            lr2_hash=new_hash,
            work_dir=self.work_dir
        ) for new_hash in self.load_requires()}
        yield task_dict.values()
        hash_table = {key: task.load() for key, task in task_dict.items()}
        self.save(hash_table)


class RegisterNewHashesToHashTableTask(WorkDirJSONTask):
    """
    list_csv に出現したハッシュから、IDが未知のものを抽出し、そのIDをLR2IRにアクセスすることで調べ、ハッシュテーブルに登録する。
    (正確には、「既知のハッシュテーブルに新規のハッシュを追加したハッシュテーブル」を新たに生成する)
    """
    name = "rnhthtt"

    list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return MakeHashTableFromNewHashesTask(
            list_csv=self.list_csv,
            hash_table_json=self.hash_table_json,
            work_dir=self.work_dir
        )

    def run(self):
        hash_table = merge_hash_tables(
            json.load(open(self.hash_table_json)),
            self.load_requires()
        )
        self.save(hash_table)
