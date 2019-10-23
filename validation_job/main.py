# Write your code here
# You are welcome to implement any data structure or algorithm of your choice.
# You may also add additional files/directories if you find it helpful.

# As your data source are pandas dataframes, you are welcome to use the pandas library to the extent of your choice.

# If you use any additional libraries, please include them in requirements.txt
# If you have any questions, please feel free to ask. The requirements of this technical test are intentionally vague
# to allow you the freedom to implement the solution of your choice. However, they are not vague to confuse you.
from data import tables
import pandas as pd


def join_players_team(df_players, df_teams):
    """
    Description:
        merge the dataframes df_players and df_teams
    :param df_teams: dataframe with the teams' data
    :param df_players: dataframe with the players' data
    :return: dataframe with the result of the merge. The columns with the same name will be with the suffixes
        _player or _team
    """
    return pd.merge(df_players, df_teams, how="inner", left_on="team_id", right_on="id", suffixes=("_player", "_team"))


def check_position_values(row):
    """
    Description:
        validate if column position contain the values in the list position_values
    :param row: dataframe's row to with the column position to be validated
    :return:
        True if the value in the column position is in the list position_values
        False if the value in the column position is not in the list position_values
    """
    position_values = ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "P", "DH"]

    if row["position"] in position_values:
        return True
    else:
        return False


def check_league_values(row):
    """
    Description:
        validate if column position contain the values in the list position_values
    :param row: dataframe's row to with the column position to be validated
    :return:
        True if the value in the column league_player is the same of the column league_team
        False if the value in the column league_player is not the same of the column league_team
    """
    if row["league_player"] == row["league_team"]:
        return True
    else:
        return False


if __name__ == "__main__":
    df_players_team = join_players_team(tables.df_players, tables.df_teams)
    df_players_team["position_check"] = df_players_team.apply(lambda row: check_position_values(row), axis=1)
    df_players_team["league_check"] = df_players_team.apply(lambda row: check_league_values(row), axis=1)

    df_players_team = df_players_team[(~df_players_team["position_check"]) | (~df_players_team["league_check"])]

    df_players_team.to_csv("validation_job/data/dataframe_validation.csv", header=True, index=False)
