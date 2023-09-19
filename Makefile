image = result_viewer
shared = true

build:
	docker build -t tosha/$(image)_image:latest -f docker/$(image)/Dockerfile .

up: down delete_trash
    ifeq ($(shared), true)
		docker-compose -f=docker/docker-compose-shared.yaml -p 1 up -d
    else
		docker-compose -f=docker/docker-compose.yaml -p 1 up -d
    endif

down:
	docker-compose -p 1 down

enter:
	docker exec -it $(image) sh

logs:
	@docker logs $(image)

exec:
    ifeq ($(image), 'pyflink')
		@docker exec pyflink sh -c "python processor.py"
    else
		@docker exec data_generator sh -c "python generator.py"
    endif

delete_trash:
	@docker volume prune -f
	@docker system prune -f
	@docker volume rm $(shell docker volume ls -q) || true