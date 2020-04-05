def update_sequence(data_frame):
    # Synthetic ID.
    _ids = []
    _id = 0
    for idx, row in data_frame.iterrows():
        if idx > 0:
            last_row = data_frame.loc[idx - 1]
            is_equal_symbol = last_row.symbol == row.symbol
            is_equal_timestamp = last_row.timestamp == row.timestamp
            is_equal_tick = last_row.tickRule == row.tickRule
            if not is_equal_symbol:
                _id = 0
            elif not is_equal_timestamp or not is_equal_tick:
                _id += 1
        _ids.append(_id)
    data_frame["sequence"] = _ids
    if "date" not in data_frame.columns:
        data_frame.insert(1, "date", data_frame["timestamp"].dt.date)
    return data_frame
