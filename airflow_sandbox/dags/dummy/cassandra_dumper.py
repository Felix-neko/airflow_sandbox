from typing import Tuple
import numpy as np
from pyarrow import Table


STRING_FIELD_LENGTH = 10
PARQUET_CHUNK_SIZE = 50_000


def create_test_arrow_table(event_ts_between: Tuple[np.datetime64, np.datetime64],
                            n_rows: int = 5, n_string_cols: int = 5, n_float_cols: int = 5,
                            n_unique_ids: int = 2, sorted_event_ts: bool = True) -> Table:

    min_datetime, max_datetime = event_ts_between

    assert min_datetime <= max_datetime
    min_datetime_int64, max_datetime_int64 = np.asarray(min_datetime, np.int64), np.asarray(max_datetime, np.int64)

    temp = np.asarray(np.random.uniform(min_datetime_int64, max_datetime_int64, n_rows), np.uint64)
    if sorted_event_ts:
        temp = np.sort(temp)
    event_ts_array = np.asarray(temp, dtype="<M8[ns]")
    event_date_str_array = event_ts_array.astype('datetime64[D]').astype('O')

    created_ts_array = event_ts_array + np.timedelta64(1, "D")

    string_cols = []
    string_col_names = []
    allowed_chars = list(b"0123456789abcdef")
    for i in range(n_string_cols):
        string_col_names.append(f"str_FLD_{i}")

        temp_str = np.asarray(np.random.choice(
            allowed_chars, STRING_FIELD_LENGTH * n_rows), np.uint8).tobytes().decode("utf8")
        string_cols.append([temp_str[j:j + STRING_FIELD_LENGTH] for j in range(n_rows)])

    float_cols = []
    float_col_names = []
    min_float_val, max_float_val = 0, 1
    for i in range(n_float_cols):
        float_col_names.append(f"float64_fld_{i}")
        float_cols.append(np.random.uniform(min_float_val, max_float_val, size=n_rows))

    uids = np.random.randint(0, n_unique_ids - 1, dtype=np.int32, size=n_rows)

    temp_table = Table.from_arrays(
        [uids, event_date_str_array] + string_cols + float_cols,
        ["user_id", "event_date_str"] + string_col_names + float_col_names)

    return temp_table


if __name__ == "__main__":
    event_ts_between = np.datetime64("2020-01-02 20:01:00.000000000"), np.datetime64("2021-01-02 20:01:00.000000000")
    arrow_table = create_test_arrow_table(event_ts_between, n_string_cols=100, n_float_cols=100, n_unique_ids=100_000,
                                          n_rows=30_000)
    print(arrow_table)
