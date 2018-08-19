from typing import Dict, Set, Tuple, Iterable, TypeVar

import pandas as pd


HashTable = Dict[str, Tuple[str, str]]
DataFrame_or_Series = TypeVar("DataFrame_or_Series", pd.DataFrame, pd.Series)


def is_empty(df: DataFrame_or_Series, empty_value=("", 0, "0")) -> DataFrame_or_Series:
    """
    df　の各要素が「空」であるかを返す。
    null, NA および引数 empty_value で指定されたものと一致した際に「空である」とみなす。

    Args:
        df: DataFrame (または Series)
        empty_value: 空であるとみなす値

    Returns: df の各要素が空であるか。

    """
    return df.isnull() | df.isna() | df.isin(empty_value)


def make_hash_table_from_bms_table(bms_table: pd.DataFrame) -> HashTable:
    """
    難易度表データ (など) から「lr2_id　と lr2_hash がともにわかっているもの」を抽出し、その対応を dict で返す。

    Args:
        bms_table: merge_bms_tables() で作った難易度表データ
            (type, lr2_id, lr2_hash, 任意)

    Returns: {lr2_hash: (type, lr2_id)}

    """
    hash_table = bms_table[["type", "lr2_id", "lr2_hash"]]  # type, lr2_id, lr2_hash のカラムを抜き出し、
    hash_table = hash_table[(~is_empty(hash_table)).all(axis=1)]  # type, lr2_id, lr2_hash が全て分かっている項目を抜き出し、
    return dict(zip(
            hash_table["lr2_hash"].values,  # キーがハッシュで
            zip(hash_table["type"].values, hash_table["lr2_id"].values)  # 値が (type, lr2_id) のタプル
        ))  # の辞書を作って返す


def extract_hashes(bms_table: pd.DataFrame) -> Set[str]:
    """
    難易度表データ (など) から lr2_hash を抽出し、set で返す。

    Args:
        bms_table: (type, lr2_id, lr2_hash, 任意)

    Returns: lr2_hash たち

    """
    hash_col = bms_table["lr2_hash"]

    return set(hash_col[~is_empty(hash_col)])


def extract_new_hashes(hashes: Iterable[str], hash_table: HashTable) -> Set[str]:
    """
    lr2_hash たちから、対応表に載っていないもののみを抽出し、set で返す。

    Args:
        hashes: lr2_hash たち
        hash_table: 対応表 {lr2_hash: (type, lr2_id)}

    Returns: lr2_hash たち

    """
    return set(hashes) - set(hash_table.keys())


def merge_hash_tables(hash_table1: HashTable, hash_table2: HashTable) -> HashTable:
    """
    2つの対応表をマージする。

    Args:
        hash_table1: 対応表1 {lr2_hash: (type, lr2_id)}
        hash_table2: 対応表2 {lr2_hash: (type, lr2_id)}

    Returns: 対応表

    """
    return {**hash_table1, **hash_table2}


def fill_ids(bms_table: pd.DataFrame, hash_table: HashTable, drop_duplicates=False) -> pd.DataFrame:
    """
    ハッシュテーブルにもとづいて表の type, lr2_id を埋める。

    Args:
        bms_table: (type, lr2_id, lr2_hash, 任意)
        hash_table: {lr2_hash: (type, lr2_id)}
        drop_duplicates: True を指定すると、埋めたあとに (type, lr2_id) の重複があれば除去する。

    Returns: 欠損値を埋めた難易度表データ

    """
    for index, row in bms_table.iterrows():
        if row["lr2_hash"] in hash_table:
            bms_table.loc[index, "type"], bms_table.loc[index, "lr2_id"] = hash_table[row["lr2_hash"]]

    if drop_duplicates:
        bms_table = bms_table.drop_duplicates(["type", "lr2_id"]).reset_index(drop=True)

    return bms_table


def fill_hashes(bms_table: pd.DataFrame, hash_table: HashTable) -> pd.DataFrame:
    """
    ハッシュテーブルにもとづいて表の lr2_hash を埋める。

    Args:
        bms_table: (type, lr2_id, lr2_hash, 任意)
        hash_table: {lr2_hash: (type, lr2_id)}

    Returns: 欠損値を埋めた難易度表データ

    """
    reverse_hash_table = {(item_type, lr2_id): lr2_hash for lr2_hash, (item_type, lr2_id) in hash_table.items()}

    for index, row in bms_table.iterrows():
        type_and_id = (row["type"], row["lr2_id"])
        if type_and_id in reverse_hash_table:
            bms_table.loc[index, "lr2_hash"] = reverse_hash_table[type_and_id]

    return bms_table
