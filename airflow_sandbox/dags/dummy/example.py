from pathlib import Path
from datetime import date

from fs_etl.etl_repo import EtlRepo

if __name__ == "__main__":
    # По этому пути должен быть ETL-репозитори: директория с settings.yaml и Python-файлами с наследниками EtlTaskBase
    repo_path = Path(__file__).parent.absolute()

    etl_repo = EtlRepo(repo_path)

    print(etl_repo.etl_class_names)  # Посмотреть, какие классы ETL-задач корректно определены в репозитории

    # ExampleEtlTask: этот класс должен наследовать EtlTaskBase должен быть определён в одном из Python-файлов репозитория
    etl_task = etl_repo.get_etl_task('SimpleEtlTask')

    # Запуск расчёта ETL-скрипта на определённую дату дату, например на 2022-01-01 (остальные параметры берутся из файла настроек)
    calc_date = date(2021, 2, 1)
    etl_task.run(calc_date)