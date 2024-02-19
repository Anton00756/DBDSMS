IMAGE_LIST = data_generator orchestrator pyflink result_viewer

image = result_viewer
shared = true
only_up = true

build:
	@docker build -t tosha/$(image)_image:latest -f docker_files/$(image)/Dockerfile .

full_build:
	@$(foreach image_name, $(IMAGE_LIST), \
		docker build -t tosha/$(image_name)_image:latest -f docker_files/$(image_name)/Dockerfile .;)

up: down delete_trash
    ifeq ($(shared), true)
		@docker-compose --env-file .env -f=docker_files/docker-compose-shared.yaml -p cdaps up -d
    else
		@docker-compose --env-file .env -f=docker_files/docker-compose.yaml -p cdaps up -d
    endif
    ifeq ($(only_up), false)
		@docker exec pyflink /opt/flink/bin/flink run -py /work_dir/service/processor.py
    endif

run:
	@docker exec pyflink /opt/flink/bin/flink run -py /work_dir/service/processor.py

generator:
	@docker restart data_generator

down:
	@docker-compose -p cdaps down

enter:
	@docker exec -it $(image) sh

exec:
    ifeq ($(image), result_viewer)
		@docker exec result_viewer sh -c "python service/viewer.py"
    endif
    ifeq ($(image), gp)
		@docker exec result_viewer sh -c "python service/greenplum.py"
    endif

delete_trash:
	@docker volume rm -f $(shell docker volume ls -q | grep -v "grafana") || true
	@docker system prune -f