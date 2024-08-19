import json
import os
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType
from typing import Any, Dict, List

class JsonMaintenance:
    """
    This class handles with maintenance with json files that gives parameters to dlt control table.
    Basically i have a path with the json files that creates the dlt parameters table with an 'Create Table' statement, this class compare all
    these files with a json_template that have the same keys of the json files in path, adding or editing them. After all changes i need
    to commit the changes to change the parameters

    Attributes:
    -----------
    json_template : str
        The path to the JSON template file, that is used as an intermediate file between the transactions.
    folder_path : str
        The path to the folder containing JSON files that creates the control file of dlt.
    """

    def __init__(self, json_template: str, folder_path: str) -> None:
        """
        Initializes the JsonMaintenance object with the paths for the JSON template and folder.

        Parameters:
        -----------
        json_template : str
            Path to the JSON template file.
        folder_path : str
            Path to the folder containing JSON files.
        """
        self.json_template = json_template
        self.folder_path = folder_path  

    def _read_json(self) -> Dict[str, Any]:
        """Reads and returns the contents of the JSON template."""
        with open(self.json_template, 'r') as file:
            return json.load(file)

    def _write_json(self, data: Dict[str, Any]) -> None:
        """Writes the provided data to the JSON template."""
        with open(self.json_template, 'w') as file:
            json.dump(data, file, indent=4)

    def add_key(self, key: str, default_value: Any = "") -> None:
        """
        Adds a key with a default value to the JSON template.

        Parameters:
        -----------
        key : str
            The key to add.
        default_value : Any, optional
            The default value for the key. Defaults to an empty string.
        """
        json_data = self._read_json()
        json_data[key] = default_value
        self._write_json(json_data)

    def edit_key(self, actual_key: str, new_key: str) -> None:
        """
        Renames a key in the JSON template.

        Parameters:
        -----------
        actual_key : str
            The current key name.
        new_key : str
            The new key name to replace the actual key.
        """
        json_data = self._read_json()
        if actual_key in json_data:
            json_data[new_key] = json_data.pop(actual_key)
            self._write_json(json_data)
            print(f"The key '{actual_key}' has been changed to '{new_key}'")
        else:
            print(f"Key '{actual_key}' not found.")

    def remove_key(self, key_to_remove: str) -> None:
        """
        Removes a key from the JSON template.

        Parameters:
        -----------
        key_to_remove : str
            The key to remove.
        """
        json_data = self._read_json()
        if key_to_remove in json_data:
            del json_data[key_to_remove]
            self._write_json(json_data)
        else:
            print(f"Key '{key_to_remove}' not found.")

    def commit_json(self, target_subfolder: str) -> None:
        """
        Commits changes to JSON files in a specific subfolder based on the template.

        Parameters:
        -----------
        target_subfolder : str
            The target subfolder within `folder_path` where the changes will be committed.
        """
        folder_path = self.folder_path
        template_data = self._read_json()
        template_keys = set(template_data.keys())

        changes = {}
        target_folder = os.path.join(folder_path, target_subfolder)

        for root, _, files in os.walk(target_folder):
            for json_file in files:
                if json_file.endswith('.json'):
                    json_file_path = os.path.join(root, json_file)
                    with open(json_file_path, 'r') as current_json_file:
                        current_data = json.load(current_json_file)
                        current_keys = set(current_data.keys())

                    if current_keys == template_keys:
                        changes[json_file] = "no changes"
                    else:
                        differences = current_keys.symmetric_difference(template_keys)
                        changes[json_file] = f"Differences: {', '.join(differences)}"
        #logging changes in the files compared with template
        for json_file, change_info in changes.items():
            print(f"JSON file: {json_file}, changes: {change_info}")

        confirmation = input("Do you want to proceed with the changes? (y/n): ").strip().lower()

        if confirmation == 'y':
            for json_file in files:
                system_name = json_file.split('_')[0]
                json_file_path = os.path.join(target_folder, f"json_{system_name}", json_file)

                current_data = json.load(open(json_file_path, 'r'))

                for key in template_data:
                    if key not in current_data:
                        current_data[key] = template_data[key]

                keys_to_remove = [key for key in current_data if key not in template_data]
                for key in keys_to_remove:
                    del current_data[key]

                with open(json_file_path, 'w') as current_json_file:
                    json.dump(current_data, current_json_file, indent=4)
                    print(f"{json_file} modified.")
        else:
            print("No JSON files were modified.")

    def generate_schema(self, json_schema: Dict[str, Any]) -> StructType:
        """
        Generates a PySpark schema from a given JSON schema.

        Parameters:
        -----------
        json_schema : dict
            A dictionary representing the JSON schema.

        Returns:
        --------
        StructType
            A PySpark StructType schema.
        """
        spark_schema = []
        
        for key, value in json_schema.items():
            if isinstance(value, int):
                spark_type = LongType()
            elif isinstance(value, bool):
                spark_type = BooleanType()
            else:
                spark_type = StringType()
            
            field = StructField(key, spark_type, True)
            spark_schema.append(field)
            
        return StructType(spark_schema)

    def commit_control(self) -> None:
        """
        Commits control based on the JSON template and schema, and compares it with the data frame.
        """
        json_template = self._read_json()
        schema = self.generate_schema(json_template)
        path = f"file:{self.folder_path}"

        json_df = spark.read.option("multiline", "true").schema(schema).json(path)
        
        with open(self.json_template, 'r') as file:
            json_data = json.load(file)

        df_columns = set(json_df.columns)
        json_keys = set(json_data.keys())
        divergences = json_keys - df_columns
        display(json_df)

        if not divergences:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            json_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable('dev.control.processing_test')
            print('The table has been updated')
        else:
            print(f'Found divergences {divergences} between the JSON and the DataFrame')

    def modify_json(self, key_to_modify: str, new_value: Any, system: str = '*', schema: str = '*', kind: str = '*', table: str = '*') -> None:
        """
        Modifies a specific key across multiple JSON files based on filtering criteria.

        Parameters:
        -----------
        key_to_modify : str
            The key to modify in the JSON files.
        new_value : Any
            The new value to set for the specified key.
        system : str, optional
            Filter based on system source. Defaults to '*' (no filter).
        schema : str, optional
            Filter based on schema source. Defaults to '*' (no filter).
        kind : str, optional
            Filter based on kind. Defaults to '*' (no filter).
        table : str, optional
            Filter based on table name. Defaults to '*' (no filter).
        """
        json_file_names_set = set()

        for sub_folder in os.listdir(self.folder_path):
            sub_folder_path = os.path.join(self.folder_path, sub_folder)

            if os.path.isdir(sub_folder_path):
                where_clause = ""

                if system != '*':
                    where_clause += f"system_source like '%{system}%' AND "

                if schema != '*':
                    where_clause += f"schema_source like '%{schema}%' AND "

                if kind != '*':
                    where_clause += f"kind like '%{kind}%' AND "

                if table != '*':
                    where_clause += f"table_name_source like '%{table}%' AND "

                if where_clause.endswith("AND "):
                    where_clause = where_clause[:-4]

                sql_query = f"""
                SELECT DISTINCT
                CONCAT(system_source, '_', kind, '_', schema_source, '_', table_name_source, '.json') AS filename
                FROM dev.control.processing
                """

                if where_clause:
                    sql_query += f"WHERE {where_clause}"

                return_query = spark.sql(sql_query)
                file_names = [row.filename for row in return_query.collect()]
                json_file_names_set.update(file_names)

        json_file_names = list(json_file_names_set)

        for json_filename in json_file_names:
            system_name = json_filename.split("_")[0]
            json_filepath = os.path.join(self.folder_path, f'json_{system_name}', json_filename)

            if os.path.exists(json_filepath):
                try:
                    with open(json_filepath, 'r') as json_file:
                        data = json.load(json_file)

                        if key_to_modify in data:
                            data[key_to_modify] = new_value
                            with open(json_filepath, 'w') as json_file:
                                json.dump(data, json_file, indent=4)
                                print(f"Modified {json_filename}: Set '{key_to_modify}' to '{new_value}'")
                        else:
                            print(f"Key '{key_to_modify}' not found in {json_filename}")
                except Exception as e:
                    print(f"Error in {json_filename}: {str(e)}")
            else:
                print(f"JSON file not found: {json_filename}")
