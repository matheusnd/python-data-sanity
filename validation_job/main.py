from input_data import tables
import pandas as pd


def join_dataframes(left_df_properties, right_df_properties, join_type):
    """
    Description:
        merge the dataframes according to the properties
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


def check_column_values(row, column_name, column_values):
    """
    Description:
        validate if a column has only the columns expected
    :param column_values: list of values that will be checked
    :param column_name: column's name to be checked
    :param row: dataframe's row to with the column position to be validated
    :return:
        True if the value in the column is in the list column values
        True if the value in the column is not in the list column values
    """
    if row[column_name] in column_values:
        return True
    else:
        return False


def compare_columns(row, column_names):
    """
    Description:
        compare the values of two columns
    :param column_names: columns that will be compared
    :param row: dataframe's row to with the column position to be validated
    :return:
        True if the value in the first column's value is the same of the second column's value
        False if the value in the first column's value is not the same of the second column's value
    """
    if row[column_names[0]] == row[column_names[1]]:
        return True
    else:
        return False


def check_null_values(row, column_name):
    """
    Description:
        check if a column has null values
    :param column_name: columns that will be checked
    :param row: dataframe's row to with the column position to be validated
    :return:
        True if the value in the column is null
        False if the value in the column is not null
    """
    return pd.notnull(row[column_name])


if __name__ == "__main__":
    MAIN_DF = join_dataframes(
        left_df_properties={"df": tables.df_players, "key": "team_id", "suffix": "_player"},
        right_df_properties={"df": tables.df_teams, "key": "id", "suffix": "_team"},
        join_type="left"
    )

    check_values_columns = [
        {"column_name": "position", "output_column_name": "position_value_checked", "values": ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "P", "DH"]},
        {"column_name": "league_player", "output_column_name": "league_player_value_checked", "values": ["AL", "NL"]}
    ]

    for check in check_values_columns:
        column_name = check["column_name"]
        values = check["values"]
        output_column_name = check["output_column_name"]
        MAIN_DF[output_column_name] = MAIN_DF.apply(lambda row: check_column_values(row, column_name, values), axis=1)

    check_compare_columns = [
        {"column_name": "league", "output_column_name": "league_compare_value_checked", "columns_to_compare": ["league_player", "league_team"]},
    ]

    for check in check_compare_columns:
        column_name = check["column_name"]
        columns_to_compare = check["columns_to_compare"]
        output_column_name = check["output_column_name"]
        MAIN_DF[output_column_name] = MAIN_DF.apply(lambda row: compare_columns(row, columns_to_compare), axis=1)

    check_null_value = [
        {"column_name": "id_team", "output_column_name": "id_team_null_value_checked"},
    ]

    for check in check_null_value:
        column_name = check["column_name"]
        output_column_name = check["output_column_name"]
        MAIN_DF[output_column_name] = MAIN_DF.apply(lambda row: check_null_values(row, column_name), axis=1)

    # df_players_team = df_players_team[(~df_players_team["position_check"]) | (~df_players_team["league_check"])]

    MAIN_DF.to_csv("validation_job/output_data/dataframe_validation.csv", header=True, index=False)
    print(MAIN_DF)
