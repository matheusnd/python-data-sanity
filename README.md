## Data Architecture Technical Test: Coding

As a member of the Data Architecture team, you will have a lot of freedom to implement solutions that you deem fit.
However, once complete, your code will likely be used, if not completely inherited, by another team entirely.
As such, it is important for your code to not only be stable but readable and scalable as well.

## Assignment:
Write a data sanity checker for a data source.
Your data sources will be pandas dataframes -- they have been provided for you for this test.

This is an instance where **the person who writes the code will not be the one who runs it**.
In addition to correctness, we're looking for code that is easy for others to contribute to.

## Requirements
 - Written in Python. If you use libraries, please include them in the requirements file. 
 - The code must implement and correctly execute the provided test cases.
 - Upon submission, we will add additional test cases. It should be easy and seamless to add new test cases. This will include running tests on new tables and columns -- your code should be able to accommodate these new data sources without necessitating a code refactor. 
 - You have a lot of freedom in how you approach this project, but this task should take a couple hours at most. If you would like any clarifications, please feel free to reach out with questions.

## Test Cases
Identify any instances in which a rule is not followed.
1. df_players' column "position" should only ever have one of values: 
      ["C", "1B", "2B", "3B", "SS", "LF", "CF", RF", "P", "DH"]

2. df_players' column "league" should always match with it's teams' league.
    df_players.team_id corresponds with _teams.id

3. ??? 
