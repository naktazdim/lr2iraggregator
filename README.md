[LR2IR (LunaticRave2 Internet Ranking)](http://www.dream-pro.info/~lavalse/LR2IR/search.cgi) からデータを収集する。

# usage


```
$ python lr2iraggregator/main.py list_csv hash_table_json work_dir output_dir
```

## 入力ファイル
### list.csv
以下のような csv ファイル   

|カラム|値|
|---:|:---|
| name | 難易度表の名前 (好きな名前) |
| path | 難易度表のURL。旧・新形式両対応。ローカルファイルも指定可 (後述) |
| is_dan | 段位認定であれば 1, そうでなければ 0 を指定 |
```
(例)
name,path,is_dan
発狂難易度表,http://nekokan.dyndns.info/~lobsak/genocide/insane.html,0
第2発狂難易度表,http://bmsnormal2.syuriken.jp/table_insane.html,0
GENOSIDE 2018 段位認定,./sp_dan.csv,1
GENOSIDE 2018 DP段位認定,./dp_dan.csv,1
```

### 難易度表ローカルファイル (あれば)
以下のような csv ファイル  
lr2_id と lr2_hash はどちらか片方がわかっていればよい

|カラム|値|
|---------:|:---|
|     type | "bms" または "course" のいずれか |
|   lr2_id | bmsid/courseid |
| lr2_hash | md5hash |
|    level | レベル (好きな文字列。段位の場合は数値で、段位順に並んでいること) |
|    title | 譜面名/コース名 |

```
(例)
type,lr2_id,lr2_hash,level,title
course,11099,,12,"GENOSIDE 2018 段位認定 Overjoy"
course,11100,,11,"GENOSIDE 2018 段位認定 発狂皆伝"
course,11101,,10,"GENOSIDE 2018 段位認定 発狂十段"
course,11102,,9,"GENOSIDE 2018 段位認定 発狂九段"
course,11103,,8,"GENOSIDE 2018 段位認定 発狂八段"
course,11104,,7,"GENOSIDE 2018 段位認定 発狂七段"
course,11105,,6,"GENOSIDE 2018 段位認定 発狂六段"
course,11106,,5,"GENOSIDE 2018 段位認定 発狂五段"
course,11107,,4,"GENOSIDE 2018 段位認定 発狂四段"
course,11108,,3,"GENOSIDE 2018 段位認定 発狂三段"
course,11109,,2,"GENOSIDE 2018 段位認定 発狂二段"
course,11110,,1,"GENOSIDE 2018 段位認定 発狂初段"
```

### hash_table_json
以下のような JSON ファイル  
「ハッシュがわかっているとデータの取得が速くなる」 (LR2本体が使っているAPIを叩けるため) というだけなので、中身が空でも動作はする  
集計を実行する際にこのファイルの情報が更新される (上書き)
```
{
    "bbe56a302c1cefd8cd61cbd440354361": ["bms", "234264"],
    "69d9fa0655fd12c8cfb3c736e9461c14": ["bms", "234475"],
    ...
} 
```

## 出力ファイル
item.csv, player.csv, record.csv, level.csv, dan.csv の 5 ファイル  
すべて引数で指定した出力先 (output_dir) 以下に出力される。

### item.csv
集計対象の (＝bms_table_list_csv で指定した難易度表に登場する、以下同じ) 譜面・コースの一覧  

|カラム|値|
|----------:|:---|
|      type | "bms" または "course" のいずれか |
|    lr2_id | bmsid/courseid |
|  lr2_hash | md5hash |
|     title | 譜面名/コース名 |
| num_notes | ノート数 |

### player.csv
集計対象の譜面・コースをプレイしているプレイヤーの一覧  

|カラム|値|
|-------:|:---|
| lr2_id | playerid |
|   name | プレイヤ名 |

### record.csv
集計対象の譜面・コースのプレイ記録の一覧

item_type,item_lr2_id,player_lr2_id,clear,pg,gr,minbp

|カラム|値|
|---------------:|:---|
| item_type | "bms" または "course" のいずれか |
| item_lr2_id | bmsid/courseid |
| player_lr2_id | playerid |
| clear | クリアランプ。1-5 の数値 (FAILED, EASY, NORMAL, HARD, FULLCOMBO)|
| pg | PGREAT 数|
| gr | GREAT 数|
| minbp | 最小 BP 数|

### level.csv
集計対象の譜面・コースの難易度の一覧  

|カラム|値|
|---------------:|:---|
|           type | "bms" または "course" のいずれか |
|         lr2_id | bmsid/courseid |
| bms_table_name | 難易度表名 (bms_table_list_csv で指定したもの) |
|          level | その表でのレベル |

### dan.csv
各プレイヤの段位の一覧

|カラム|値|
|---------------:|:---|
|   lr2_id | playerid |
| dan_name | 段位認定の名前 (bms_table_list_csv で指定したもの) |
|      dan | 段位に対応する数値 (上記ローカルファイルで指定したもの) |

# 備考
work_dir に途中経過がキャッシュされているので、
ネットワーク切断などで途中で失敗しても、単に再実行すれば続きから処理が始まる。  
work_dir の中身を空にすれば初めからになる。