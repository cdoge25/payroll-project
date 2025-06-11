.PHONY: up down destroy restart fresh-restart help

up:
	docker compose up -d

recreate:
	docker compose up -d --force-recreate --build

down:
	docker compose down

destroy:
	docker compose down -v

restart:
	down up

fresh-restart:
	destroy up

spark-app:
	cd docker/spark-app && docker build -t spark-app .

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  up                		Start containers"
	@echo "  down              		Stop and remove containers"
	@echo "  destroy             	Stop and remove containers, as well as storage volumes"
	@echo "  restart           		Restart containers with existing data"
	@echo "  fresh-restart     		Restart containers without existing data"
	@echo "  help              		Show this help message"
	@echo ""