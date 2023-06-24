insert into {{ params.project_name }}_{{ params.dimension }}
select
    *,
    now() as create_datetime
from s3(
    'https://storage.yandexcloud.net/{{ params.s3_bucket }}/my_target/{{ params.project_name }}/{{ params.project_name }}_{{ params.dimension }}.csv',
    '{{ params.s3_key }}', '{{ params.s3_secret }}', CSVWithNames
    );