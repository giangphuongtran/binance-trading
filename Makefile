AIRFLOW_ENV=airflow.env

.PHONY: fernet

fernet:
	@if [ ! -f $(AIRFLOW_ENV) ]; then \
		echo "$(AIRFLOW_ENV) not found"; \
		exit 1; \
	fi; \
	if grep -q '^AIRFLOW__CORE__FERNET_KEY=' $(AIRFLOW_ENV) && \
	   [ -n "$$(grep '^AIRFLOW__CORE__FERNET_KEY=' $(AIRFLOW_ENV) | cut -d '=' -f2)" ]; then \
		echo "Fernet key already set in $(AIRFLOW_ENV)"; \
	else \
		echo "Generating Fernet key..."; \
		KEY=$$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"); \
		if grep -q '^AIRFLOW__CORE__FERNET_KEY=' $(AIRFLOW_ENV); then \
			sed -i "s|^AIRFLOW__CORE__FERNET_KEY=.*|AIRFLOW__CORE__FERNET_KEY=$$KEY|" $(AIRFLOW_ENV); \
		else \
			echo "AIRFLOW__CORE__FERNET_KEY=$$KEY" >> $(AIRFLOW_ENV); \
		fi; \
		echo "Fernet key written to $(AIRFLOW_ENV)"; \
	fi
