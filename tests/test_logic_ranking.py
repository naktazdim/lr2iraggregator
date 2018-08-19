# -*- coding: utf-8 -*-
import unittest

from pandas.testing import assert_frame_equal

from lr2iraggregator.logic.ranking import *
from util import *


class TestLogicRanking(unittest.TestCase):
    def test_extract_num_notes(self):
        res_dir = "extract_num_notes"
        rankings = pd.read_csv(resource_path(res_dir, "rankings.csv"), header=0, dtype=object)
        num_notes = pd.read_csv(resource_path(res_dir, "num_notes.csv"), header=0, dtype=object)

        assert_frame_equal(extract_num_notes(rankings),
                           num_notes)

    def test_append_num_notes(self):
        res_dir = "append_num_notes"
        item = pd.read_csv(resource_path(res_dir, "item.csv"), header=0, dtype=object)
        num_notes = pd.read_csv(resource_path(res_dir, "num_notes.csv"), header=0, dtype=object)
        item2 = pd.read_csv(resource_path(res_dir, "item2.csv"), header=0, dtype=object)

        assert_frame_equal(append_num_notes(item, num_notes),
                           item2)

    def test_extract_players(self):
        res_dir = "extract_players"
        rankings = pd.read_csv(resource_path(res_dir, "rankings.csv"), header=0, dtype=object)
        player = pd.read_csv(resource_path(res_dir, "player.csv"), header=0, dtype=object)

        assert_frame_equal(extract_players(rankings),
                           player)


if __name__ == '__main__':
    unittest.main()
