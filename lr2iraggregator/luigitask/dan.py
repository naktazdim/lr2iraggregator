import luigi

from lr2iraggregator.util.luigiutil import WorkDirDFCSVTask
from lr2iraggregator.logic.dan import *
from lr2iraggregator.luigitask.bms_tables import LoadBMSTableListTask, MakeLevelCSVTask
from lr2iraggregator.luigitask.lr2_ranking import MakeRecordCSVTask


class MakeDanCSVTask(WorkDirDFCSVTask):
    """
    各プレイヤの段位を計算する。
    """
    name = "dan"

    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()

    def requires(self):
        return [
            LoadBMSTableListTask(
                bms_table_list_csv=self.bms_table_list_csv,
                work_dir=self.work_dir
            ), MakeLevelCSVTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            ), MakeRecordCSVTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            )
        ]

    def run(self):
        bms_table_list, level, record = self.load_requires()
        self.save(calculate_dans(bms_table_list, level, record), index=False)
