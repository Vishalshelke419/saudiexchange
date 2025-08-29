build:
	docker compose build

daily:
	docker compose run --rm daily

shell:
	docker compose run --rm daily bash
