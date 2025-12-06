.PHONY: dev run test demo docker-build docker-up docker-down

dev:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

run:
	python run.py --loop

test:
	pytest

demo:
	python -m demo.load_sample_data --reset

docker-build:
	docker build -t alert-service .

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down
