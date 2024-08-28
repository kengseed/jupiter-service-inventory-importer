import configparser
from mysql.connector import MySQLConnection, Error


class ServiceInventoryRepository:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("./config/config.ini")

    def __getConnection(self):
        db = MySQLConnection(
            host=self.config.get("Database", "host"),
            user=self.config.get("Database", "user"),
            password=self.config.get("Database", "password"),
            database=self.config.get("Database", "database"),
        )
        return db

    # def tryParseNumber(value):
    #     try:
    #         return value if str(value).isnumeric() else None
    #     except Error as e:
    #         return None

    def loadServiceCatalogOpen(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # **Required to load RM Static, CTG to application_profile

            # Logics
            #   [Python] Truncate table temp_jupiter_service_catalog_open
            #   [Python] Insert into temp_jupiter_service_catalog_open
            #   [Stored Procedure]
            #       - Delete jupiter_service_catalog from temp_jupiter_service_catalog_open.app_id, app_name (To add CASCADE DELETE on jupiter_interface_dependency)
            #       - Insert into jupiter_service_catalog from temp_jupiter_service_catalog_open

            # Clear temporary table and insert
            cursor.execute("TRUNCATE TABLE temp_jupiter_service_catalog_open")
            for index, row in list:
                cursor.execute(
                    "INSERT INTO temp_jupiter_service_catalog_open (service_id, app_id, app_name, service_type_name, is_internal_service, is_internal_app_service, service_code, service_name, sub_service_name, app_addtional_info, service_description, financial_transaction_related, service_action_type, interface_tech_protocol_names, interface_tech_protocol_other, service_endpoint, message_size_request, message_size_response, avg_transaction_per_sec, max_transaction_per_sec, avg_response_sec, max_response_sec, avg_transaction_per_day, max_transaction_per_day, avg_message_per_sec, max_message_per_sec, avg_streaming_per_sec, max_streaming_per_sec, interface_spec_format_names, interface_spec_format_other, message_format_names, message_format_other, service_availability_name, service_availability_other, service_available_start_time, service_available_end_time, peak_day_duration, message_spec_sharepoint_folder, message_spec_file_name, message_spec_file_name_request, message_spec_file_name_response, fill_by_email, fill_by_fullname, fill_by_date) values (%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s)",
                    (
                        row["service_id"],
                        row["app_id"],
                        row["app_name"],
                        row["service_type_name"],
                        row["is_internal_service"],
                        row["is_internal_app_service"],
                        row["service_code"],
                        row["service_name"],
                        row["sub_service_name"],
                        row["app_addtional_info"],
                        row["service_description"],
                        row["financial_transaction_related"],
                        row["service_action_type"],
                        row["interface_tech_protocol_names"],
                        row["interface_tech_protocol_other"],
                        row["service_endpoint"],
                        row["message_size_request"],
                        row["message_size_response"],
                        row["avg_transaction_per_sec"],
                        row["max_transaction_per_sec"],
                        row["avg_response_sec"],
                        row["max_response_sec"],
                        row["avg_transaction_per_day"],
                        row["max_transaction_per_day"],
                        row["avg_message_per_sec"],
                        row["max_message_per_sec"],
                        row["avg_streaming_per_sec"],
                        row["max_streaming_per_sec"],
                        row["interface_spec_format_names"],
                        row["interface_spec_format_other"],
                        row["message_format_names"],
                        row["message_format_other"],
                        row["service_availability_name"],
                        row["service_availability_other"],
                        row["service_available_start_time"],
                        row["service_available_end_time"],
                        row["peak_day_duration"],
                        row["message_spec_sharepoint_folder"],
                        row["message_spec_file_name"],
                        row["message_spec_file_name_request"],
                        row["message_spec_file_name_response"],
                        row["fill_by_email"],
                        row["fill_by_fullname"],
                        row["fill_by_date"],
                    ),
                )
                print(
                    (
                        "Added row:{index} to temporary table temp_jupiter_service_catalog_open"
                    ).format(index=index + 1)
                )

            # Transform temporary table to main table
            print(
                "Starting transform temporary table to main table jupiter_service_catalog_open..."
            )
            cursor.callproc("sp_jupiter_service_catalog_transform_open")

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def loadServiceCatalogMainframe(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Logics

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def loadInterfaceDependencyOpen(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Clear temporary table and insert
            cursor.execute("TRUNCATE TABLE temp_jupiter_interface_dependency_open")
            for index, row in list:
                cursor.execute(
                    "INSERT INTO temp_jupiter_interface_dependency_open (consumer_service_id, provider_service_id, timeout_sec, bu_expect_response_sec, fill_by_email, fill_by_fullname, fill_by_date) VALUES(%s, %s, %s, %s, %s, %s, %s)",
                    (
                        row["consumer_service_id"],
                        row["provider_service_id"],
                        row["timeout_sec"],
                        row["bu_expect_response_sec"],
                        row["fill_by_email"],
                        row["fill_by_fullname"],
                        row["fill_by_date"],
                    ),
                )
                print(
                    (
                        "Added row:{index} to temporary table temp_jupiter_interface_dependency_open"
                    ).format(index=index + 1)
                )

            # Transform temporary table to main table
            print(
                "Starting transform temporary table to main table jupiter_interface_dependency..."
            )
            cursor.callproc("sp_jupiter_interface_dependency_transform_open")

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def loadInterfaceDependencyMainframe(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Logics

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def loadBatchOpen(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Logics

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def loadBatchMainframe(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Logics

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e
