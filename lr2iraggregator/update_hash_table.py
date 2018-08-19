import argparse
import json
import os

import luigi

from lr2iraggregator.luigitask.bms_tables import UpdateHashTableTask


class MainTask(luigi.WrapperTask):
    bms_table_list_csv = luigi.Parameter()
    hash_table_json = luigi.Parameter()
    work_dir = luigi.Parameter()

    _complete = False

    def requires(self):
        return UpdateHashTableTask(
                bms_table_list_csv=self.bms_table_list_csv,
                hash_table_json=self.hash_table_json,
                work_dir=self.work_dir
            )

    def complete(self):
        return self._complete

    def run(self):
        self.requires().copy_output_to(os.path.join(self.hash_table_json))
        self._complete = True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("params_json", type=argparse.FileType("r"))
    args = p.parse_args()

    task = MainTask(**json.load(args.params_json))
    luigi.build([task], local_scheduler=True, workers=4)


if __name__ == "__main__":
    main()
