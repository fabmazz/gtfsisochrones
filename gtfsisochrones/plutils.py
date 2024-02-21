def check_equal(df1,df2):
    all_values_same = True
    if df1.shape != df2.shape:
        all_values_same = False
    else:
        for col_name in df1.columns:
            if col_name not in df2.columns:
                all_values_same = False
                break
            if not df1[col_name].equals(df2[col_name]):
                all_values_same = False
                break
    return all_values_same