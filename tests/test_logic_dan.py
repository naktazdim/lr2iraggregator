# -*- coding: utf-8 -*-
import unittest

from pandas.testing import assert_frame_equal

from lr2iraggregator.logic.dan import *
from util import *


class TestLogicDan(unittest.TestCase):
    def test_calculate_dan(self):
        res_dir = "calculate_dan"
        record = pd.read_csv(resource_path(res_dir, "record.csv"), header=0, dtype=object)
        dan = pd.read_csv(resource_path(res_dir, "dan.csv"), header=0, dtype=object)
        player_dan = pd.read_csv(resource_path(res_dir, "player_dan.csv"), header=0, dtype=object)

        player_dan_calculated = calculate_dan(record, dan)
        assert_frame_equal(player_dan_calculated, player_dan)

    def test_calculate_dans(self):
        res_dir = "calculate_dans"
        bms_table_list = pd.read_csv(resource_path(res_dir, "list.csv"), header=0, dtype={"is_dan": bool})
        record = pd.read_csv(resource_path(res_dir, "record.csv"), header=0, dtype=object)
        level = pd.read_csv(resource_path(res_dir, "level.csv"), header=0, dtype=object)
        player_dan = pd.read_csv(resource_path(res_dir, "player_dan.csv"), header=0, dtype=object)

        player_dan_calculated = calculate_dans(bms_table_list, level, record)
        assert_frame_equal(player_dan_calculated, player_dan)


if __name__ == '__main__':
    unittest.main()
