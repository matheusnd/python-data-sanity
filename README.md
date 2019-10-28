# Python Data Sanity

This code will validate pandas dataframes and create new columns with the result of the validation.

## Result
The result will be written to a CSV file in the folder:

**validation_job/output_data/daframe_validation.csv**

## Executing the job
```
make run
```

## Adding new validations
There is a list with all validations that will be performed by the job:

```python
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
``` 
- **output_column_name**: name of the column that will store the validation result
- **args**: arguments of the validation function
- **function**: function responsible for execute the data validation  
