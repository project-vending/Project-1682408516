 
import os

# Define project name
project_name = "WebScraperAnalyzer"

# Define project folder structure
folders = [
    "src",
    "data",
    "aws_lambda",
    "aws_glue",
    "aws_athena",
    "streamlit_dashboard",
]

# Create project directory if it doesn't exit
if not os.path.exists(project_name):
    os.mkdir(project_name)

# Create project subdirectories
for folder in folders:
    os.makedirs(os.path.join(project_name, folder))
    
# Create empty files in each folder
src_files = ["app.py"]
data_files = ["raw_data.csv"]
aws_lambda_files = ["scraper_lambda.py"]
aws_glue_files = ["aws_glue_script.py"]
aws_athena_files = ["aws_athena_queries.sql"]
streamlit_files = ["streamlit_app.py"]

for file in src_files:
    open(os.path.join(project_name, "src", file), "w").close()
    
for file in data_files:
    open(os.path.join(project_name, "data", file), "w").close()
    
for file in aws_lambda_files:
    open(os.path.join(project_name, "aws_lambda", file), "w").close()
    
for file in aws_glue_files:
    open(os.path.join(project_name, "aws_glue", file), "w").close()
    
for file in aws_athena_files:
    open(os.path.join(project_name, "aws_athena", file), "w").close()
    
for file in streamlit_files:
    open(os.path.join(project_name, "streamlit_dashboard", file), "w").close()
