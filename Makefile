IMAGE_LIST = data_generator pyflink result_viewer

image = result_viewer
shared = true

build:
	docker build -t tosha/$(image)_image:latest -f docker/$(image)/Dockerfile .

full_build:
	@$(foreach image_name, $(IMAGE_LIST), \
		docker build -t tosha/$(image_name)_image:latest -f docker/$(image_name)/Dockerfile .;)

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

exec:
    ifeq ($(image), pyflink)
		@docker exec pyflink sh -c "python service/processor.py"
    endif
    ifeq ($(image), data_generator)
		@docker exec data_generator sh -c "python service/generator.py"
    endif
    ifeq ($(image), result_viewer)
		@docker exec result_viewer sh -c "python service/viewer.py"
    endif
    ifeq ($(image), gp)
		@docker exec result_viewer sh -c "python service/greenplum.py"
    endif

delete_trash:
	@docker volume prune -f
	@docker system prune -f