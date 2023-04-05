
# run the pipeline flow.
run: venv
	. ./.venv/bin/activate && python ./flows/parent_flow.py

# start prefect UI
prefect_ui: venv
	. ./.venv/bin/activate && prefect orion start

# create prefect deployment
prefect_deploy: venv
	. ./.venv/bin/activate && \
	prefect deployment build --name "online_store_data" ./flows/parent_flow.py:parent_flow

#apply prefect deployment
prefect_deploy_apply: venv
	. ./.venv/bin/activate && \
	prefect deployment apply parent_flow-deployment.yaml

# prefect docker deployment
prefect_docker_deploy: venv
	. ./.venv/bin/activate && python ./flows/online_store_docker_deploy.py


# clean and remove virtual environment..
clean:  ## Clean up project
	rm -rf ./.venv
	rm -rf ./flows/__pycache__
#	rm -rf ./utils/__pycache__

#=====================================================
# create virtual environment..

venv: ./.venv/touchfile

./.venv/touchfile: ./requirements.txt
	python3 -m venv ./.venv
	. ./.venv/bin/activate; pip install -Ur ./requirements.txt
	touch ./.venv/touchfile