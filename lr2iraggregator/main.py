import argparse
import os

import luigi

from lr2iraggregator.luigitask.bms_tables import UpdateHashTableTask, MakeLevelCSVTask
from lr2iraggregator.luigitask.lr2_ranking import MakeItemCSVWithNumNotesTask, MakePlayerCSVTask, MakeRecordCSVTask
from lr2iraggregator.luigitask.dan import MakeDanCSVTask


class MainTask(luigi.WrapperTask):
    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()
    output_dir = luigi.Parameter()
    work_dir = luigi.Parameter()

    _complete = False

    def requires(self):
        return [
            UpdateHashTableTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            ),
            MakeLevelCSVTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            ),
            MakeItemCSVWithNumNotesTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            ),
            MakePlayerCSVTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            ),
            MakeRecordCSVTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            ),
            MakeDanCSVTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            )

        ]

    def complete(self):
        return self._complete

    def run(self):
        tasks = self.requires()

        hash_task = tasks.pop(0)
        hash_task.copy_output_to(self.hash_table_json)  # 上書き

        file_names = ["level.csv", "item.csv", "player.csv", "record.csv", "dan.csv"]
        for task, file_name in zip(tasks, file_names):
            task.copy_output_to(os.path.join(self.output_dir, file_name))

        self._complete = True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("bms_table_list_csv", type=str)
    p.add_argument("hash_table_json", type=str)
    p.add_argument("work_dir", type=str)
    p.add_argument("output_dir", type=str)
    args = p.parse_args()

    task = MainTask(
        bms_table_list_csv=args.bms_table_list_csv,
        hash_table_json=args.hash_table_json,
        work_dir=args.work_dir,
        output_dir=args.output_dir
    )
    luigi.build([task], local_scheduler=True, workers=4)


if __name__ == "__main__":
    main()
