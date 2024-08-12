venv:
	python -m venv pyenv
	chmod +x pyenv/Scripts/activate

install:
	pip install -r requirements.txt

build-start:
	docker compose build
	docker compose up

start:
	docker compose up

stop:
	docker compose down -v