Дирекория с протобуфами. Главный протобуф - `tasks.proto`. Его генерация ещё не налажена. 

Интрукция такая:

1. Запустить виртуальную python-среду с помощью `source /path/to/venv/bin/activate`. 
2. Из директории `proto` запустить скрипт, указанный в `regenerate_protos_cmd`. Сгенерируются `.py` файлы.
3. В `tasks_pb2_grpc.py` поменять `import tasks_pb2 as tasks__pb2` на `import proto.tasks_pb2 as tasks__pb2`. 