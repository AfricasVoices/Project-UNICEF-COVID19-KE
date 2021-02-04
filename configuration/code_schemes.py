import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    S01E01 = _open_scheme("s01e01.json")
    S01E02 = _open_scheme("s01e02.json")
    S01E03 = _open_scheme("s01e03.json")
    S01E04 = _open_scheme("s01e04.json")
    S01E05 = _open_scheme("s01e05.json")
    S01E06 = _open_scheme("s01e06.json")
    S01E07 = _open_scheme("s01e07.json")
    S01E08 = _open_scheme("s01e08.json")
    S01E09 = _open_scheme("s01e09.json")
    S01E10 = _open_scheme("s01e10.json")
    S01_CLOSE_OUT = _open_scheme("s01_close_out.json")

    KENYA_CONSTITUENCY = _open_scheme("kenya_constituency.json")
    KENYA_COUNTY = _open_scheme("kenya_county.json")
    GENDER = _open_scheme("gender.json")
    AGE = _open_scheme("age.json")
    AGE_CATEGORY = _open_scheme("age_category.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")
