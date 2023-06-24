insert into {{ params.project_name }}_{{ params.mt_type_of_data }}_{{ params.metric }}
select
    *,
    toYYYYMMDD(toDate(c2)) as partition,
    now() as create_datetime
from s3(
    'https://storage.yandexcloud.net/{{ params.s3_bucket }}/my_target/{{ params.project_name }}/{{ params.project_name }}_{{ params.mt_type_of_data }}_{{ params.metric }}_{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}_{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}.csv',
    '{{ params.s3_key }}', '{{ params.s3_secret }}', CSV
    )
where c1 != 'id'
settings max_partitions_per_insert_block = 1000;