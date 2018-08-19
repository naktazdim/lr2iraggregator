from typing import Dict
import os

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from lr2iraggregator.util.util import *


def load_bms_table_list_csv(csv_path: str, base_path: str=None) -> pd.DataFrame:
    """
    以下のような形式の難易度表表 (?) を読み込む。

    |---:|:---|
    | name | 難易度表の名前 (好きな名前) |
    | path | 難易度表のパス/URL |
    | is_dan | 段位認定であれば 1, そうでなければ 0 を指定 |

    csvファイル側ではパスに相対パスを指定できる。読み込んだ際には絶対パスに直す。

    Args:
        csv_path: csv のパス
        base_path: 相対パスの基底パス。指定しない場合は csv が格納されているディレクトリのパス

    Returns:
        (name, path, is_dan)

    """
    if base_path is None:
        base_path = os.path.dirname(csv_path)

    def abs_path(path: str):
        if is_url(path):
            return path
        else:
            return os.path.normpath(os.path.join(base_path, path))

    bms_table_list = pd.read_csv(csv_path, header=0, index_col=None)
    bms_table_list["path"] = bms_table_list["path"].map(abs_path)
    return bms_table_list


def convert_lr2irscraper_bms_table_to_bmsirt_format(bms_table: pd.DataFrame) -> pd.DataFrame:
    """
    lr2irscraper の get_bms_table() の返り値を lr2iraggregator の入力ファイルの形式に合わせる。

    Args:
        bms_table: get_bms_table() の返り値

    Returns:
        (type, lr2_id, lr2_hash, level, title)

    """
    bms_table["type"] = "bms"
    bms_table["type"] = bms_table["type"].astype(CategoricalDtype(categories=["bms", "course"]))
    bms_table["lr2_id"] = bms_table["lr2_bmsid"] if "lr2_bmsid" in bms_table else None
    bms_table["lr2_hash"] = bms_table["md5"] if "md5" in bms_table else None
    return bms_table[["type", "lr2_id", "lr2_hash", "level", "title"]]


def merge_bms_tables(bms_tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    load_bms_table() で読み込んだ複数の bms_table を結合して一つの DataFrame を作成する。
    "item" と "bms_table_data" のもとになる。

    Args:
        bms_tables: bms_table_id をキー、bms_table を値とする dict
            各要素は (type, lr2_id, lr2_hash, level, title) の DataFrame

    Returns:
        (type, lr2_id, lr2_hash, bms_table_name, level, title)

    """
    return (
        pd.concat([bms_table.assign(bms_table_name=bms_table_name)
                   .fillna("")
                   .astype({"lr2_id": str})
                   for bms_table_name, bms_table in bms_tables.items()])
          .reset_index(drop=True)
    )


def make_item_df(merged_bms_tables: pd.DataFrame) -> pd.DataFrame:
    """
    item を生成する。

    Args:
        merged_bms_tables: merge_bms_tables() で得た DataFrame

    Returns: (type, lr2_id, lr2_hash, title)

    """
    return (
        merged_bms_tables
        .iloc[merged_bms_tables["lr2_hash"].isin(["", None, np.nan]).argsort()]  # ハッシュが空の行を下に持ってきている
        .drop_duplicates(["type", "lr2_id"], keep="first")  # ハッシュが空でないものを優先して残している
        .sort_values(by=["type", "lr2_id"])
        [["type", "lr2_id", "lr2_hash", "title"]]
        .reset_index(drop=True)
    )


def make_level_df(merged_bms_tables: pd.DataFrame) -> pd.DataFrame:
    """
    難易度表のデータ (を結合したもの) から、表内でのレベルの一覧を作成する。

    Args:
        merged_bms_tables: concatenate_bms_tables() の出力
            (type, lr2_id, lr2_hash, level, title, bms_table_name)


    Returns: (type, lr2_id, bms_table_name, level)

    """
    return merged_bms_tables[["type", "lr2_id", "bms_table_name", "level"]]

