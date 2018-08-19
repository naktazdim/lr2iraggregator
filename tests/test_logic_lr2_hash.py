# -*- coding: utf-8 -*-
import unittest
import json

import numpy as np
from pandas.testing import assert_frame_equal, assert_series_equal

from lr2iraggregator.logic.lr2_hash import *
from util import *


def load_hash_table(path):
    """
    Dict[str, List[str, str]] を Dict[str, Tuple[str, str]] にしている
    json にすると tuple が list になってしまう、かつその違いを無視してテストする簡便な方法がなさそうだったので

    Args:
        path: パス

    Returns:

    """
    hash_table = json.load(open(path))
    return {key: tuple(value) for key, value in hash_table.items()}


class TestLogicLR2Hash(unittest.TestCase):
    def test_is_empty(self):
        assert_frame_equal(
            is_empty(pd.DataFrame([[0, "0", ""], [np.nan, 1, "2"]])),
            pd.DataFrame([[True, True, True], [True, False, False]])
        )
        assert_series_equal(
            is_empty(pd.Series([0, "0", "", np.nan, 1, "2"])),
            pd.Series([True, True, True, True, False, False])
        )

    def test_make_hash_table_from_bms_table(self):
        res_dir = "make_hash_table_from_bms_table"
        bms_table = pd.read_csv(resource_path(res_dir, "bms_table.csv"), header=0, dtype=object)
        hash_table = load_hash_table(resource_path(res_dir, "hash_table.json"))
        self.assertDictEqual(make_hash_table_from_bms_table(bms_table),
                             hash_table)

    def test_extract_hashes(self):
        res_dir = "extract_hashes"
        bms_table = pd.read_csv(resource_path(res_dir, "bms_table.csv"), header=0, dtype=object)
        hashes = set(json.loads(resource(res_dir, "hashes.json")))
        self.assertSetEqual(extract_hashes(bms_table),
                            hashes)

    def test_extract_new_hashes(self):
        res_dir = "extract_new_hashes"
        hashes = set(json.loads(resource(res_dir, "hashes.json")))
        hash_table = load_hash_table(resource_path(res_dir, "hash_table.json"))
        new_hashes = set(json.loads(resource(res_dir, "new_hashes.json")))
        self.assertSetEqual(extract_new_hashes(hashes, hash_table),
                            new_hashes)

    def test_merge_hash_tables(self):
        res_dir = "merge_hash_tables"
        hash_table1 = load_hash_table(resource_path(res_dir, "hash_table1.json"))
        hash_table2 = load_hash_table(resource_path(res_dir, "hash_table2.json"))
        hash_table_merged = load_hash_table(resource_path(res_dir, "hash_table_merged.json"))

        self.assertDictEqual(merge_hash_tables(hash_table1, hash_table2),
                             hash_table_merged)

    def test_fill_ids(self):
        res_dir = "fill_ids"
        bms_table = pd.read_csv(resource_path(res_dir, "bms_table.csv"), header=0, dtype=object)
        hash_table = load_hash_table(resource_path(res_dir, "hash_table.json"))
        bms_table_filled = pd.read_csv(resource_path(res_dir, "bms_table_filled.csv"), header=0, dtype=object)
        bms_table_filled_drop_duplicated_ids = \
            pd.read_csv(resource_path(res_dir, "bms_table_filled_drop_duplicated_ids.csv"), header=0, dtype=object)

        assert_frame_equal(fill_ids(bms_table, hash_table, drop_duplicates=False),
                           bms_table_filled)

        assert_frame_equal(fill_ids(bms_table, hash_table, drop_duplicates=True),
                           bms_table_filled_drop_duplicated_ids)

    def test_fill_hashes(self):
        res_dir = "fill_hashes"
        bms_table = pd.read_csv(resource_path(res_dir, "bms_table.csv"), header=0, dtype=object)
        hash_table = load_hash_table(resource_path(res_dir, "hash_table.json"))
        bms_table_filled = pd.read_csv(resource_path(res_dir, "bms_table_filled.csv"), header=0, dtype=object)

        assert_frame_equal(fill_hashes(bms_table, hash_table), bms_table_filled)


if __name__ == '__main__':
    unittest.main()
