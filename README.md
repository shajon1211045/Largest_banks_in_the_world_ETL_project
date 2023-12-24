# Largest Banks ETL Project

This Python project focuses on extracting, transforming, and loading (ETL) data related to the largest banks' market capitalization from a Wikipedia page. The data is then saved both to a CSV file and a SQLite database. Additionally, the project logs progress messages along with timestamps.

## Requirements

- `Python 3.x`  

- ### Libraries:
  - `requests`
  - `pandas`
  - `numpy`
  - `BeautifulSoup`
  - `sqlite3`
  - `datetime`

Install the required libraries using the following command:

```bash
pip install pandas numpy beautifulsoup4
```  
## Usage  
### Clone the repository  
```
git clone https://github.com/shajon1211045/Largest_banks_in_the_world_ETL_project.git      
cd Largest_banks_in_the_world_ETL_project  
```
### Run the ETL script  
```
python Etl.py  
```
### Project Structure

- `Etl.py`: Main Python script for ETL operations.  
- `Largest_banks_data.csv`: CSV file containing the transformed data.  
- `Banks.db`: SQLite database containing the transformed data.  
- `code_log.txt`: Text file containg the runtime details.

## ETL Process Steps
`Extract`: The script extracts data from the specified URL, getting the 10 largest banks.

`Transform`: Data transformation includes adding three new colums.

`Load`: The transformed data is loaded into both a CSV file and an SQLite database.
