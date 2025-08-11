include .env
export

init-db:
	@echo "Initializing database..."
	airflow db init

create-user:
	@echo "Creating user..."
	airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}"

upgrade-db:
	@echo "Upgrading database..."
	airflow db upgrade