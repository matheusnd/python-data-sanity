from input_data import tables
import validation_rules


if __name__ == "__main__":
    MAIN_DF = validation_rules.join_dataframes(
        left_df_properties={"df": tables.df_players, "key": "team_id", "suffix": "_player"},
        right_df_properties={"df": tables.df_teams, "key": "id", "suffix": "_team"},
        join_type="left"
    )

    validation_list = [
        {"output_column_name": "position_value_checked", "args": {"column_name": "position", "column_values": ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "P", "DH"]},
         "function": lambda row, args: validation_rules.check_column_values(row, args)},

        {"output_column_name": "league_player_value_checked", "args": {"column_name": "league_player", "column_values": ["AL", "NL"]},
         "function": lambda row, args: validation_rules.check_column_values(row, args)},

        {"output_column_name": "league_compare_value_checked", "args": {"columns_to_compare": ["league_player", "league_team"]},
         "function": lambda row, args: validation_rules.compare_columns(row, args)},

        {"output_column_name": "id_team_null_value_checked", "args": {"column_name": "id_team"},
         "function": lambda row, args: validation_rules.check_null_values(row, args)},

        {"output_column_name": "player_first_name_ascii_checked", "args": {"column_name": "first_name"},
         "function": lambda row, args: validation_rules.check_is_ascii(row, args)},
    ]

    check_column_filter = []
    for validation in validation_list:
        args = validation["args"]
        output_column_name = validation["output_column_name"]
        function = validation["function"]
        check_column_filter.append(output_column_name)
        MAIN_DF[output_column_name] = MAIN_DF.apply(lambda row: function(row, args), axis=1)

    MAIN_DF = validation_rules.filter_check_fail(MAIN_DF, check_column_filter)
    MAIN_DF.to_csv("validation_job/output_data/dataframe_player_validation.csv", header=True, index=False)
    print(MAIN_DF)
