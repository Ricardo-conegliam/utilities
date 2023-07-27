# Databricks notebook source
# INCLUDE_HEADER_FALSE
# INCLUDE_FOOTER_FALSE

# COMMAND ----------

# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

from dbacademy_helper import DBAcademyHelper, Paths

# The following attributes are externalized to make them easy
# for content developers to update with every new course.
helper_arguments = {
    "course_code" : "ec",             # The abreviated version of the course
    "course_name" : "example-course", # The full name of the course, hyphenated
    "data_source_name" : "example-course", # Should be the same as the course
    "data_source_version" : "v01",    # New courses would start with 01
    "enable_streaming_support": True, # This couse uses stream and thus needs checkpoint directories
    "install_min_time" : "3 min",     # The minimum amount of time to install the datasets (e.g. from Oregon)
    "install_max_time" : "10 min",    # The maximum amount of time to install the datasets (e.g. from India)
    "remote_files": remote_files,     # The enumerated list of files in the datasets
}
