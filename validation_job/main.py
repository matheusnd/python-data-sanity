from input_data import tables
import pandas as pd


def join_dataframes(left_df_properties, right_df_properties, join_type):
    """
    Description:
        merge two dataframes according to the properties
    :param join_type: type of the join that will be applied
    :param right_df_properties: right dataframe's properties
        df: dataframe object
        key: join key
        suffix: suffix that will be applied to columns with the same name
    :param left_df_properties: left dataframe's properties
        df: dataframe object
        key: join key
        suffix: suffix that will be applied to columns with the same name
    :return: dataframe with the result of the merge
    """
    left_df = left_df_properties["df"]
    left_key = left_df_properties["key"]
    left_suffix = left_df_properties["suffix"]

    right_df = right_df_properties["df"]
    right_key = right_df_properties["key"]
    right_suffix = right_df_properties["suffix"]

    return pd.merge(left_df, right_df, how=join_type, left_on=left_key, right_on=right_key, suffixes=(left_suffix, right_suffix))


def check_column_values(row, args):
    """
    Description:
        validate if a column has only the expected values
    :param args: dict with the args of the function
        column_values: list of values that will be checked
        column_name: column's name to be checked
    :param row: dataframe's row to to be validated
    :return:
        True if the value in the column is in the list column values
        True if the value in the column is not in the list column values
    """
    column_values = args["column_values"]
    column_name = args["column_name"]

    if row[column_name] in column_values:
        return True
    else:
        return False


def compare_columns(row, args):
    """
    Description:
        compare the values of two columns
    :param args: dict with the args of the function
        columns_to_compare: list with the columns that will be compared
    :param row: dataframe's row to to be validated
    :return:
        True if the value in the first column's value is the same of the second column's value
        False if the value in the first column's value is not the same of the second column's value
    """
    column_names = args["columns_to_compare"]

    if row[column_names[0]] == row[column_names[1]]:
        return True
    else:
        return False


def check_null_values(row, args):
    """
    Description:
        check if a column has null values
    :param args: dict with the args of the function
        column_name: columns that will be checked
    :param row: dataframe's row to to be validated
    :return:
        True if the value in the column is null
        False if the value in the column is not null
    """
    column_name = args["column_name"]
    return pd.notnull(row[column_name])


def check_is_ascii(row, args):
    """
    Description:
        check if a column has all characters as ASCII.
    :param args: dict with the args of the function
        column_name: columns that will be checked
    :param row: dataframe's row to to be validated
    :return:
        True if the column has only ASCII characters.
        False if the column hasn't only ASCII characters.
    """
    column_name = args["column_name"]
    return all(ord(c) < 128 for c in row[column_name])


def filter_check_fail(df, column_names):
    filter_string = ""
    for column in column_names:
        filter_string = f"{column} == False | {filter_string}"

    return df.query(filter_string.strip()[:-1])


if __name__ == "__main__":
    MAIN_DF = join_dataframes(
        left_df_properties={"df": tables.df_players, "key": "team_id", "suffix": "_player"},
        right_df_properties={"df": tables.df_teams, "key": "id", "suffix": "_team"},
        join_type="left"
    )

    validation_list = [
        {"output_column_name": "position_value_checked", "args": {"column_name": "position", "column_values": ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "P", "DH"]},
         "function": lambda row, args: check_column_values(row, args)},

        {"output_column_name": "league_player_value_checked", "args": {"column_name": "league_player", "column_values": ["AL", "NL"]},
         "function": lambda row, args: check_column_values(row, args)},

        {"output_column_name": "league_compare_value_checked", "args": {"columns_to_compare": ["league_player", "league_team"]},
         "function": lambda row, args: compare_columns(row, args)},

        {"output_column_name": "id_team_null_value_checked", "args": {"column_name": "id_team"},
         "function": lambda row, args: check_null_values(row, args)},

        {"output_column_name": "player_first_name_ascii_checked", "args": {"column_name": "first_name"},
         "function": lambda row, args: check_is_ascii(row, args)},
    ]

    check_column_filter = []
    for validation in validation_list:
        args = validation["args"]
        output_column_name = validation["output_column_name"]
        function = validation["function"]
        check_column_filter.append(output_column_name)
        MAIN_DF[output_column_name] = MAIN_DF.apply(lambda row: function(row, args), axis=1)

    MAIN_DF = filter_check_fail(MAIN_DF, check_column_filter)
    MAIN_DF.to_csv("validation_job/output_data/dataframe_validation.csv", header=True, index=False)
    print(MAIN_DF)
