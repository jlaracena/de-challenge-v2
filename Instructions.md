# Before Deploy
- Create a Service Account in your GCP proyect and save it in the deploy/terraform folder.
- Set your proyect id, region and service account name in terraform.tfvars.
- Set variables in variables.tf (specially storage name because there are unique).
- Set de variables in src/composer/airflow with your information.

# Deploy
- Set bucket landing in deploy.sh
- Run bash script deploy.sh   

# Google Cloud Platform
- To run the ETL you should run the dag in Composer/airflow manually.
- The ETL will create Raw tables for the data and the reports in Bigquery.  Also it will create the CVS reports files in the sending bucket.