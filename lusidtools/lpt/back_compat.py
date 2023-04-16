# Make backwards compatibility adjustments
# When reading old files
def convert(frame_type, df):

    # Security is now known as instrument_id
    if "security_uid" in df.columns.values:
        df.rename(columns={"security_uid": "instrument_uid"}, inplace=True)

    # Some older dividend files may not have the pay date column
    # In this case we duplicate the record date
    if frame_type == "div":
        if "payment_date" not in df.columns.values:
            df["payment_date"] = df["record_date"]

    # Some older pricing files call the instr column 'sec'
    if frame_type == "prc":
        if "instr" not in df.columns.values:
            df["sec"] = df["instr"]

    return df
