DB_URL=postgres://resilient:resilient@localhost:5432/resilient_event_processor?sslmode=disable

.PHONY: up down logs migrate-up migrate-down

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

migrate-up:
	docker run --rm -v $(PWD)/migrations:/migrations --network host migrate/migrate \
		-path=/migrations -database "$(DB_URL)" up

migrate-down:
	docker run --rm -v $(PWD)/migrations:/migrations --network host migrate/migrate \
		-path=/migrations -database "$(DB_URL)" down 1
