# Don't change, project scripts
SRC_DIR := src

run:  venv  ## Run Fake Data Generator & Insert Data to DB.
	. $(SRC_DIR)/.venv/bin/activate \
	&& python src/run.py --n_orders 1000000 --n_cust 1000

## 🏃 Create and Run Postgres Image
run_img:
	docker run --name=online_store_db --rm -it \
	-e POSTGRES_USER=root \
	-e POSTGRES_PASSWORD=root \
	-e POSTGRES_DB=online_store_data -v /home/ezzaldin/online_store_pipeline/fake-data-generator/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres:13

## stop Postgres Image
stop_img:
	docker stop online_store_db

test: venv  ## Unit tests
	. $(SRC_DIR)/.venv/bin/activate \
	&& pytest -v

# clean and remove virtual environment..
clean:  ## Clean up project
	rm -rf $(SRC_DIR)/.venv
	rm -rf $(SRC_DIR)/app/__pycache__
	rm -rf $(SRC_DIR)/utils/__pycache__
	rm -rf $(SRC_DIR)/logs
	rm -rf ./ny_taxi_postgres_data
#	rm -rf $(SRC_DIR)/app/tests/__pycache__
	rm -rf .pytest_cache
	rm -rf $(SRC_DIR)/.pytest_cache

#=====================================================
# create virtual environment..

venv: $(SRC_DIR)/.venv/touchfile

$(SRC_DIR)/.venv/touchfile: $(SRC_DIR)/requirements.txt
	python3 -m venv $(SRC_DIR)/.venv
	. $(SRC_DIR)/.venv/bin/activate; pip install -Ur $(SRC_DIR)/requirements.txt
	touch $(SRC_DIR)/.venv/touchfile

