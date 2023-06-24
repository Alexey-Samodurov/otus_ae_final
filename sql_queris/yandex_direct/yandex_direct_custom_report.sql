insert into {table}
    select
        *,
        toYYYYMMDD(toDate(Date)) as partition,
        now() as create_datetime
from s3(
    'https://storage.yandexcloud.net/{s3_bucket}/yandex_direct/{project}/{report_type}_{start_date}_{end_date}.tsv',
    '{s3_access_key}', '{s3_secret_key}', TSVWithNames
)