import configparser
from mysql.connector import MySQLConnection, Error
from mysql.connector.cursor import MySQLCursor


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

    def loadServiceCatalogOpen(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Clear temporary table and insert
            cursor.execute("TRUNCATE TABLE temp_jupiter_service_catalog_open")
            for index, row in list:
                cursor.execute(
                    "INSERT INTO temp_jupiter_service_catalog_open (service_id, app_id, app_name, service_type_name, is_internal_service, is_internal_app_service, service_code, service_name, sub_service_name, app_addtional_info, service_description, financial_transaction_related, service_action_type, interface_tech_protocol_names, interface_tech_protocol_other, service_endpoint, message_size_request, message_size_response, avg_transaction_per_sec, max_transaction_per_sec, avg_response_sec, max_response_sec, avg_transaction_per_day, max_transaction_per_day, avg_message_per_sec, max_message_per_sec, avg_streaming_per_sec, max_streaming_per_sec, interface_spec_format_names, interface_spec_format_other, message_format_names, message_format_other, service_availability_name, service_availability_other, service_available_start_time, service_available_end_time, peak_day_duration, message_spec_sharepoint_folder, message_spec_file_name, message_spec_file_name_request, message_spec_file_name_response, fill_by_email, fill_by_fullname, created_by, created_datetime, updated_by, updated_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp(), %s, current_timestamp())",
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
                        "SYSTEM",
                        "SYSTEM",
                    ),
                )
                print(
                    (
                        "Added row:{index} to temporary table temp_jupiter_service_catalog_open"
                    ).format(index=index + 1)
                )

            # Transform temporary table to main table
            print(
                "Starting transform temporary table to main table jupiter_service_catalog..."
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

            # Clear temporary table and insert
            cursor.execute("TRUNCATE TABLE temp_jupiter_service_catalog_mainframe")
            for index, row in list:
                cursor.execute(
                    "INSERT INTO temp_jupiter_service_catalog_mainframe (service_id, app_id, app_name, service_type_name, service_description, cicsproc_program_id, cicsproc_copybook_name, cicsproc_copybook_library_name, cicsproc_services_field_name_1, cicsproc_services_field_value_1, cicsproc_services_field_name_2, cicsproc_services_field_value_2, cicsproc_services_field_name_3, cicsproc_services_field_value_3, cicsproc_services_field_name_4, cicsproc_services_field_value_4, cicsproc_services_field_name_5, cicsproc_services_field_value_5, cicstran_cics_trans, cicstran_copybook_name, cicstran_copybook_library_name, cicstran_transaction_id, cicstran_sub_code_1, cicstran_sub_code_2, `3270_screen_id`, `3270_mapset`, `3270_program`, `3270_transaction`, tdq_queue_name, tdq_copybook_name, tdq_copybook_library_name, tsq_queue_name, tsq_copybook_name, tsq_copybook_library_name, ach_app_id, ach_copybook_name, ach_copybook_library_name, directact_cics_name, directact_copybook_name, directact_copybook_library_name, directact_file_name, directact_physical_file_name, dag_external_transaction_code, dag_tbr_class, dag_tbr_name, dag_icr_class, dag_icr_name, dag_tcr_class, dag_tcr_name, dag_dags_input, dag_dags_output, dag_tp_pre_processor, dag_application_business_module, dag_input_data_pos_1, dag_input_data_pos_8, batchproc_program_id, batchproc_copybook_name, batchproc_copybook_library_name, batchproc_services_field_name_1, batchproc_services_field_value_1, batchproc_services_field_name_2, batchproc_services_field_value_2, batchproc_services_field_name_3, batchproc_services_field_value_3, batchproc_services_field_name_4, batchproc_services_field_value_4, batchproc_services_field_name_5, batchproc_services_field_value_5, batchproc_eod_batch, batchproc_frequency, batchproc_frequency_details, created_by, created_datetime, updated_by, updated_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp(), %s, current_timestamp())",
                    (
                        row["service_id"],
                        row["app_id"],
                        row["app_name"],
                        row["service_type_name"],
                        row["service_description"],
                        row["cicsproc_program_id"],
                        row["cicsproc_copybook_name"],
                        row["cicsproc_copybook_library_name"],
                        row["cicsproc_services_field_name_1"],
                        row["cicsproc_services_field_value_1"],
                        row["cicsproc_services_field_name_2"],
                        row["cicsproc_services_field_value_2"],
                        row["cicsproc_services_field_name_3"],
                        row["cicsproc_services_field_value_3"],
                        row["cicsproc_services_field_name_4"],
                        row["cicsproc_services_field_value_4"],
                        row["cicsproc_services_field_name_5"],
                        row["cicsproc_services_field_value_5"],
                        row["cicstran_cics_trans"],
                        row["cicstran_copybook_name"],
                        row["cicstran_copybook_library_name"],
                        row["cicstran_transaction_id"],
                        row["cicstran_sub_code_1"],
                        row["cicstran_sub_code_2"],
                        row["3270_screen_id"],
                        row["3270_mapset"],
                        row["3270_program"],
                        row["3270_transaction"],
                        row["tdq_queue_name"],
                        row["tdq_copybook_name"],
                        row["tdq_copybook_library_name"],
                        row["tsq_queue_name"],
                        row["tsq_copybook_name"],
                        row["tsq_copybook_library_name"],
                        row["ach_app_id"],
                        row["ach_copybook_name"],
                        row["ach_copybook_library_name"],
                        row["directact_cics_name"],
                        row["directact_copybook_name"],
                        row["directact_copybook_library_name"],
                        row["directact_file_name"],
                        row["directact_physical_file_name"],
                        row["dag_external_transaction_code"],
                        row["dag_tbr_class"],
                        row["dag_tbr_name"],
                        row["dag_icr_class"],
                        row["dag_icr_name"],
                        row["dag_tcr_class"],
                        row["dag_tcr_name"],
                        row["dag_dags_input"],
                        row["dag_dags_output"],
                        row["dag_tp_pre_processor"],
                        row["dag_application_business_module"],
                        row["dag_input_data_pos_1"],
                        row["dag_input_data_pos_8"],
                        row["batchproc_program_id"],
                        row["batchproc_copybook_name"],
                        row["batchproc_copybook_library_name"],
                        row["batchproc_services_field_name_1"],
                        row["batchproc_services_field_value_1"],
                        row["batchproc_services_field_name_2"],
                        row["batchproc_services_field_value_2"],
                        row["batchproc_services_field_name_3"],
                        row["batchproc_services_field_value_3"],
                        row["batchproc_services_field_name_4"],
                        row["batchproc_services_field_value_4"],
                        row["batchproc_services_field_name_5"],
                        row["batchproc_services_field_value_5"],
                        row["batchproc_eod_batch"],
                        row["batchproc_frequency"],
                        row["batchproc_frequency_details"],
                        "SYSTEM",
                        "SYSTEM",
                    ),
                )
                print(
                    (
                        "Added row:{index} to temporary table temp_jupiter_service_catalog_mainframe"
                    ).format(index=index + 1)
                )

            # Transform temporary table to main table
            print(
                "Starting transform temporary table to main table jupiter_service_catalog..."
            )
            cursor.callproc("sp_jupiter_service_catalog_transform_mainframe")

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def __loadInterfaceDependencyOpen(self, cursor: MySQLCursor, list: list):
        # Clear temporary table and insert
        cursor.execute("TRUNCATE TABLE temp_jupiter_interface_dependency_open")
        for index, row in list:
            cursor.execute(
                "INSERT INTO temp_jupiter_interface_dependency_open (consumer_app_id, consumer_app_code, consumer_service_id_or_name, consumer_service_description, provider_service_id, mainframe_bulk_ach_app_id, mainframe_bulk_ach_transaction_code, mainframe_direct_access_file_name, mainframe_direct_access_cics_name, batch_eod_type, batch_frequency, batch_frequency_detail, ebs_message_type, ebs_trans_code, ebs_from_account_code, ebs_to_account_code, created_by, created_datetime, updated_by, updated_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp(), %s, current_timestamp())",
                (
                    row["consumer_app_id"],
                    row["consumer_app_code"],
                    row["consumer_service_id_or_name"],
                    row["consumer_service_description"],
                    row["provider_service_id"],
                    row["mainframe_bulk_ach_app_id"],
                    row["mainframe_bulk_ach_transaction_code"],
                    row["mainframe_direct_access_file_name"],
                    row["mainframe_direct_access_cics_name"],
                    row["batch_eod_type"],
                    row["batch_frequency"],
                    row["batch_frequency_detail"],
                    row["ebs_message_type"],
                    row["ebs_trans_code"],
                    row["ebs_from_account_code"],
                    row["ebs_to_account_code"],
                    "SYSTEM",
                    "SYSTEM",
                ),
            )
            print(
                (
                    "Added row:{index} to temporary table temp_jupiter_interface_dependency_open"
                ).format(index=index + 1)
            )

    def __loadInterfaceDependencyMainframe(self, cursor: MySQLCursor, list: list):
        # Clear temporary table and insert
        cursor.execute("TRUNCATE TABLE temp_jupiter_interface_dependency_mainframe")
        for index, row in list:
            cursor.execute(
                "INSERT INTO temp_jupiter_interface_dependency_mainframe (consumer_app_id, consumer_app_code, consumer_service_description, consumer_service_id, provider_app_code, provider_app_code_other, provider_service_type_name, provider_service_id, provider_service_description_other, mainframe_bulk_ach_app_id, mainframe_bulk_ach_transaction_code, mainframe_direct_access_file_id, batch_eod_type, batch_frequency, batch_frequency_detail, remark, created_by, created_datetime, updated_by, updated_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp(), %s, current_timestamp())",
                (
                    row["consumer_app_id"],
                    row["consumer_app_code"],
                    row["consumer_service_description"],
                    row["consumer_service_id"],
                    row["provider_app_code"],
                    row["provider_app_code_other"],
                    row["provider_service_type_name"],
                    row["provider_service_id"],
                    row["provider_service_description_other"],
                    row["mainframe_bulk_ach_app_id"],
                    row["mainframe_bulk_ach_transaction_code"],
                    row["mainframe_direct_access_file_id"],
                    row["batch_eod_type"],
                    row["batch_frequency"],
                    row["batch_frequency_detail"],
                    row["remark"],
                    "SYSTEM",
                    "SYSTEM",
                ),
            )
            print(
                (
                    "Added row:{index} to temporary table temp_jupiter_interface_dependency_mainframe"
                ).format(index=index + 1)
            )

    def loadInterfaceDependency(
        self, openDependencylist: list, mainframeDependencylist: list
    ):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Load data to temporary tables (Open, Mainframe)
            self.__loadInterfaceDependencyOpen(cursor, openDependencylist)
            self.__loadInterfaceDependencyMainframe(cursor, mainframeDependencylist)

            # Transform temporary table to main table
            print(
                "Starting transform temporary table to main table jupiter_interface_dependency..."
            )
            cursor.callproc("sp_jupiter_interface_dependency_transform")

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def __loadBatchOpen(self, cursor: MySQLCursor, list: list):
        # Clear temporary table and insert
        cursor.execute("TRUNCATE TABLE temp_jupiter_batch_dependency_open")
        for index, row in list:
            cursor.execute(
                "INSERT INTO temp_jupiter_batch_dependency_open (source_app_code, target_app_code, service_description, batch_direction, file_transfer_by, file_transfer_job_name, batch_job_name, batch_schedule_by, batch_schedule_by_other, batch_frequency_detail, batch_source_file_name, batch_soruce_control_file_name, batch_target_file_name, batch_target_control_file_name, batch_start_time, batch_spec_file_name, fill_by_email, fill_by_fullname, created_by, created_datetime, updated_by, updated_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp(), %s, current_timestamp())",
                (
                    row["source_app_code"],
                    row["target_app_code"],
                    row["service_description"],
                    row["batch_direction"],
                    row["file_transfer_by"],
                    row["file_transfer_job_name"],
                    row["batch_job_name"],
                    row["batch_schedule_by"],
                    row["batch_schedule_by_other"],
                    row["batch_frequency_detail"],
                    row["batch_source_file_name"],
                    row["batch_soruce_control_file_name"],
                    row["batch_target_file_name"],
                    row["batch_target_control_file_name"],
                    row["batch_start_time"],
                    row["batch_spec_file_name"],
                    row["fill_by_email"],
                    row["fill_by_fullname"],
                    "SYSTEM",
                    "SYSTEM",
                ),
            )
            print(
                (
                    "Added row:{index} to temporary table temp_jupiter_batch_dependency_open"
                ).format(index=index + 1)
            )

    def __loadBatchMainframe(self, cursor: MySQLCursor, list: list):
        # Clear temporary table and insert
        cursor.execute("TRUNCATE TABLE temp_jupiter_batch_dependency_mainframe")
        for index, row in list:
            cursor.execute(
                "INSERT INTO temp_jupiter_batch_dependency_mainframe (owner_app_id, owner_app_code, source_app_code, target_app_code, service_description, platform_direction, batch_direction, file_transfer_by, file_transfer_job_name, batch_job_name, batch_eod_type, batch_frequency, batch_frequency_detail, batch_mainframe_file_name, batch_mainframe_control_file_name, batch_open_file_name, batch_open_control_file_name, layout_type, layout_copybook_name, layout_copybook_library_name, layout_program_name, layout_program_library_name, layout_job_name, layout_job_library_name, layout_dag_name, fill_by_email, fill_by_fullname, created_by, created_datetime, updated_by, updated_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp(), %s, current_timestamp())",
                (
                    row["owner_app_id"],
                    row["owner_app_code"],
                    row["source_app_code"],
                    row["target_app_code"],
                    row["service_description"],
                    row["platform_direction"],
                    row["batch_direction"],
                    row["file_transfer_by"],
                    row["file_transfer_job_name"],
                    row["batch_job_name"],
                    row["batch_eod_type"],
                    row["batch_frequency"],
                    row["batch_frequency_detail"],
                    row["batch_mainframe_file_name"],
                    row["batch_mainframe_control_file_name"],
                    row["batch_open_file_name"],
                    row["batch_open_control_file_name"],
                    row["layout_type"],
                    row["layout_copybook_name"],
                    row["layout_copybook_library_name"],
                    row["layout_program_name"],
                    row["layout_program_library_name"],
                    row["layout_job_name"],
                    row["layout_job_library_name"],
                    row["layout_dag_name"],
                    row["fill_by_email"],
                    row["fill_by_fullname"],
                    "SYSTEM",
                    "SYSTEM",
                ),
            )
            print(
                (
                    "Added row:{index} to temporary table temp_jupiter_batch_dependency_mainframe"
                ).format(index=index + 1)
            )

    def loadBatchDependency(
        self, openDependencylist: list, mainframeDependencylist: list
    ):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Load data to temporary tables (Open, Mainframe)
            self.__loadBatchOpen(cursor, openDependencylist)
            self.__loadBatchMainframe(cursor, mainframeDependencylist)

            # Transform temporary table to main table
            print(
                "Starting transform temporary table to main table jupiter_service_catalog and jupiter_interface_dependency..."
            )
            cursor.callproc("sp_jupiter_batch_dependency_transform")

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e

    def loadIntegrationServiceToHubMapping(self, list: list):
        try:
            db = self.__getConnection()
            cursor = db.cursor()

            # Clear temporary table and insert
            cursor.execute("TRUNCATE TABLE jupiter_service_to_hub_mapping")
            for index, row in list:
                cursor.execute(
                    "INSERT INTO jupiter_service_to_hub_mapping (service_id, target_state_or_provider, created_by, created_datetime, updated_by, updated_datetime) VALUES(%s, %s, %s, current_timestamp(), %s, current_timestamp())",
                    (
                        row["service_id"],
                        row["target_state_or_provider"],
                        "SYSTEM",
                        "SYSTEM",
                    ),
                )
                print(
                    (
                        "Added row:{index} to temporary table jupiter_service_to_hub_mapping"
                    ).format(index=index + 1)
                )

            # Commit & Close Connection
            db.commit()
            cursor.close()
            db.close()
        except Error as e:
            print(e)
            raise e
