image = data_generator
shared = true

build:
	docker build -t tosha/$(image)_image:latest -f docker/$(image)/Dockerfile services/$(image)

up: down delete_trash
    ifeq ($(shared), true)
		docker-compose -f=docker/docker-compose-shared.yaml -p 1 up -d
    else
		docker-compose -f=docker/docker-compose.yaml -p 1 up -d
    endif

down:
	docker-compose -p 1 down

enter:
	docker exec -it $(shell docker ps -aq -f "ancestor=tosha/$(image)_image") sh

delete_trash:
	@docker volume prune -f
	@docker system prune -f
	@docker volume rm $(shell docker volume ls -q) || true