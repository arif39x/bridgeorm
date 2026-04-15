import os
import sys


def main():
    print("BridgeORM Schema Designer ")
    print("==================================================")
    print("Welcome to the interactive schema designer.")

    models = []

    while True:
        model_name = input("Enter model name (or 'done' to finish): ").strip()
        if model_name.lower() == "done":
            break

        fields = []
        while True:
            field_name = input(
                f"  Enter field name for {model_name} (or 'done' to finish): "
            ).strip()
            if field_name.lower() == "done":
                break
            field_type = input(
                f"    Enter type for {field_name} (str, int, datetime, uuid): "
            ).strip()
            fields.append((field_name, field_type))

        models.append({"name": model_name, "fields": fields})

    if not models:
        print("No models defined. Exiting.")
        return

    output_file = "models.py"
    with open(output_file, "w") as f:
        f.write("from bridge_orm import BaseModel\n")
        f.write("from typing import Optional, List\n")
        f.write("from datetime import datetime\n\n")

        for model in models:
            f.write(f"class {model['name']}(BaseModel):\n")
            f.write(f'    table = "{model["name"].lower()}s"\n')
            field_names = [field[0] for field in model["fields"]]
            f.write(f"    _fields = {field_names}\n\n")
            for field in model["fields"]:
                f.write(f"    {field[0]}: {field[1]}\n")
            f.write("\n")

    print(f"Schema saved to {output_file}")


if __name__ == "__main__":
    main()
