
# Metadata and Logging Exporter

This project is a Python application designed to scan a local directory, extract metadata from files based on a specific naming convention, and export this data to a CSV file. It also includes a robust, singleton logger for centralized application logging.

---

## 🚀 Getting Started

### Prerequisites
* Python 3.9+
* `pandas` library

### Installation
1. Clone this repository to your local machine.
2. Navigate to the project root directory.
3. Install the required dependencies:
```bash
   pip install pandas
```

### Configuration

Update the `config/settings.json` file with your desired paths:

```json
{
    "files_metadata_exporter": {
        "path_to_scan": "/path/to/your/files",
        "output_csv": "files_details.csv"
    },
    "loader_to_df": {
        "source_file": "files_details.csv"
    }
}
```

-----

## 🛠 Usage

### 1\. Extracting Metadata

To scan a directory and export file metadata to a CSV:

```bash
python src/files_metadata_exporter.py
```

This will create a `files_details.csv` file in the project's root directory, containing the extracted information.

### 2\. Loading Data

To load the exported CSV file into a pandas DataFrame:

```bash
python src/loader_to_df.py
```

This script will read the `files_details.csv` file and print the head of the DataFrame to the console.

-----

## 📂 Project Structure

```
.
├── config/
│   ├── config.py
│   └── settings.json
├── logs/
│   └── log_<timestamp>.log
├── src/
│   ├── files_metadata_exporter.py
│   └── loader_to_df.py
├── logger.py
├── README.md
└── files_details.csv
```

-----

## 📝 Key Components

  * **`logger.py`**: A **singleton** logger module for consistent, centralized logging. It supports both file and console handlers with configurable log levels.
  * **`config/config.py`**: Handles loading project settings from the `settings.json` file, providing a single source of truth for all configuration variables.
  * **`src/files_metadata_exporter.py`**: The core script that scans a directory, applies a regex pattern to file names, and exports the extracted data to a CSV file.
  * **`src/loader_to_df.py`**: A utility class to load data from the exported CSV into a pandas DataFrame for further analysis or processing.