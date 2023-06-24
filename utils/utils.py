import os


def sql_file_reader(project_name: str, query_filename: str, encoding: str = 'utf-8', f_string: bool = True, **kwargs) -> str:
    """
    Читаем sql файл из репозитория и возвращаем его содержимое

    :param project_name: название падпапки с проектом
    :param query_filename: название sql файла в репозитории в папке sql_queries, формат обязательно .sql
    :param debug: выполнение команды print для загруженного запроса (по умолчанию False)
    :param encoding: кодировка
    :param f_string: использование f-строки для форматирования запроса (по умолчанию True)
    :param kwargs: переменные для замены значений в запросе
    """
    file_path = os.path.join('/opt/airflow/sql_queris/', project_name, query_filename)
    if os.path.exists(file_path):
        with open(file_path, encoding=encoding) as sql_file:
            rows = sql_file.read()
            if f_string:
                return rows.format(**kwargs)
            else:
                return rows
    else:
        raise FileNotFoundError(
            'Такого файла с запросом не существует! Проверь имя проекта и название файла с запросом!')