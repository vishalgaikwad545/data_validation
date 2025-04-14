# data_validation
```
import os

def create_file_structure(base_dir="data_validation_poc"):
    structure = {
        "": ["app.py", "requirements.txt", "config.py"],
        "agents": [
            "__init__.py",
            "supervisor.py",
            "code_validator.py",
            "zipcode_validator.py",
            "reporting.py"
        ],
        "utils": [
            "__init__.py",
            "data_loader.py",
            "sql_helpers.py"
        ],
        "data": [".gitkeep"]
    }

    for folder, files in structure.items():
        folder_path = os.path.join(base_dir, folder)
        os.makedirs(folder_path, exist_ok=True)
        for file in files:
            file_path = os.path.join(folder_path, file)
            with open(file_path, "w") as f:
                f.write("")  # Create an empty file

    print(f"Project structure created under '{base_dir}'.")

if __name__ == "__main__":
    create_file_structure()
```
