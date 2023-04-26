.PHONY: deploy

deploy:
	poetry run python src/deployment.py

agent:
	poetry run prefect agent start -q 'test'

run-deployment-locally:
	poetry run prefect deployment run log-flow/log-simple

list-deployments:
	poetry run prefect deployment ls