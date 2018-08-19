# -*- coding: utf-8 -*-
import unittest

from pandas.testing import assert_frame_equal

from lr2iraggregator.logic.bms_tables import *
from util import *


class TestLogicBMSTables(unittest.TestCase):
    def test_load_bms_table(self):
        res_dir = "load_bms_table"
        assert_frame_equal(
            load_bms_table_list_csv(
                resource_path(res_dir, "bms_table_rel.csv"),
                base_path="/base/dir"
            ),
            pd.read_csv(resource_path(res_dir, "bms_table_abs.csv"))
        )

    def test_convert_lr2irscraper_bms_table_to_bmsirt_format(self):
        res_dir = "convert_lr2irscraper_bms_table_to_bmsirt_format"
        for name in ["insane", "overjoy", "second_insane"]:
            with self.subTest(name):
                lr2ir_bms_table = pd.read_csv(
                    resource_path(res_dir, "lr2irscraper_{}.csv".format(name)),
                    header=0, dtype=object)
                bmsirt_bms_table = pd.read_csv(
                    resource_path(res_dir, "{}.csv".format(name)),
                    header=0, dtype=object)
                assert_frame_equal(
                    convert_lr2irscraper_bms_table_to_bmsirt_format(lr2ir_bms_table),
                    bmsirt_bms_table,
                    check_dtype=False,
                    check_categorical=False
                )

    def test_merge_bms_tables(self):
        res_dir = "merge_bms_tables"
        files = {"0": "0_insane.csv", "1": "1_overjoy.csv", "2": "2_second_insane.csv", "3": "3_dan.csv"}
        bms_tables = {bms_table_id: pd.read_csv(resource_path(res_dir, file), header=0, dtype=object)
                      for bms_table_id, file in files.items()}
        merged = merge_bms_tables(bms_tables)
        merged_expected = pd.read_csv(resource_path(res_dir, "merged.csv"), header=0, dtype=object)

        assert_frame_equal(merged.fillna(""),
                           merged_expected.fillna(""))

    def test_make_item_df(self):
        res_dir = "make_item_df"
        merged_bms_table = pd.read_csv(resource_path(res_dir, "merged.csv"), header=0, dtype=object)
        item = make_item_df(merged_bms_table)
        expected = pd.read_csv(resource_path(res_dir, "item.csv"), header=0, dtype=object)

        assert_frame_equal(item.fillna(""),
                           expected.fillna(""))

    def test_make_level_df(self):
        res_dir = "make_level_df"
        merged_bms_table = pd.read_csv(resource_path(res_dir, "merged.csv"), header=0, dtype=object)
        level = make_level_df(merged_bms_table)
        expected = pd.read_csv(resource_path(res_dir, "level.csv"), header=0, dtype=object)

        assert_frame_equal(level.fillna(""),
                           expected.fillna(""))


if __name__ == '__main__':
    unittest.main()
