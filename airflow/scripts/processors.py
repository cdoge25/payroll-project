import os

import duckdb


class S3CsvFileProcessor:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.conn = duckdb.connect()
        self.conn.execute(
            f"""
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{self.aws_access_key_id}',
                SECRET '{self.aws_secret_access_key}',
                REGION 'ap-southeast-2'
            )
            """
        )
        self.table_cleaning_rules = {
            "time_off_in_lieu": self._process_time_off_in_lieu,
            "timesheet": self._process_timesheet,
            "roster": self._process_roster,
            "dim_pay_period": self._process_dim_pay_period,
            "bonus": self._process_bonus,
            "employee_details": self._process_employee_details,
            "allowance": self._process_allowance,
            "employee_leave": self._process_employee_leave,
            "contract_details": self._process_contract_details,
            "junior_pay_rates": self._process_junior_pay_rates,
            "pay_rate_adjustments": self._process_pay_rate_adjustments,
            "minimum_pay_rates": self._process_minimum_pay_rates,
        }

    def _process_file(self, s3_raw_file_url):
        bucket_name = s3_raw_file_url.split("/")[2]
        table_name = s3_raw_file_url.split("/")[-1].split(".")[0]
        # Read the CSV file from S3
        self.conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT DISTINCT * FROM read_csv_auto('{s3_raw_file_url}')"
        )
        cleaning_function = self._get_cleaning_function(table_name)
        cleaning_function()
        s3_processed_file_url = f"s3://{bucket_name}/processed/{table_name}.parquet"
        self._save_to_s3(table_name, s3_processed_file_url)
        return s3_processed_file_url

    def _get_cleaning_function(self, table_name):
        if table_name in self.table_cleaning_rules:
            return self.table_cleaning_rules.get(table_name)
        else:
            raise ValueError(f"No cleaning function defined for {table_name}")

    def _save_to_s3(self, table_name, s3_processed_file_url):
        self.conn.execute(
            f"""
            COPY {table_name}
            TO '{s3_processed_file_url}'
            (FORMAT 'parquet');
            """
        )

    def _process_time_off_in_lieu(self):
        pass

    def _process_timesheet(self):
        self.conn.execute(
            """
            ALTER TABLE timesheet
                RENAME COLUMN employee_code TO employee_id;
            """
        )

    def _process_roster(self):
        pass

    def _process_dim_pay_period(self):
        pass

    def _process_bonus(self):
        pass

    def _process_employee_details(self):
        pass

    def _process_allowance(self):
        pass

    def _process_employee_leave(self):
        pass

    def _process_contract_details(self):
        pass

    def _process_junior_pay_rates(self):
        self.conn.execute(
            """
            ALTER TABLE junior_pay_rates
                RENAME COLUMN "Age" TO age;
            ALTER TABLE junior_pay_rates
                RENAME COLUMN "% of Adult Pay Rate" TO percent_of_adult_pay_rate;
            """
        )

    def _process_pay_rate_adjustments(self):
        self.conn.execute(
            """
            ALTER TABLE pay_rate_adjustments
                RENAME COLUMN "Rate Type" TO rate_type;
            ALTER TABLE pay_rate_adjustments
                RENAME COLUMN "Description" TO description;
            ALTER TABLE pay_rate_adjustments
                RENAME COLUMN "Rate Calculation" TO rate_calculation;
            """
        )

    def _process_minimum_pay_rates(self):
        pass
