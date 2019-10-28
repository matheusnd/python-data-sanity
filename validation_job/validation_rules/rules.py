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
    """
    Description:
        filter the validation that have failed
    :param df: dataframe that will be filtered
    :param column_names: list of validation columns that will be filtered
    :return: dataframe with only the row that have failed in the tests
    """
    filter_string = ""
    for column in column_names:
        filter_string = f"{column} == False | {filter_string}"

    return df.query(filter_string.strip()[:-1])
