package validation

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.Try

object Schema {
  val schemas: Map[String, StructType] = Map()
  val rows: Map[String, Row] = Map()

  // products
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/products/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/products", "20211126093828")
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/products/2021-11-25/", "products_id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/products/2021-11-25/", "products_id")

  val products_schema = StructType(
    Array(
      StructField("products_id", IntegerType, true),
      StructField("products_quantity", IntegerType, true),
      StructField("products_model", StringType, true),
      StructField("products_image", StringType, true),
      StructField("products_price", DoubleType, true),
      StructField("products_date_added", StringType, true),
      StructField("products_last_modified", StringType, true),
      StructField("products_date_available", StringType, true),
      StructField("products_weight", DoubleType, true),
      StructField("products_status", IntegerType, true),
      StructField("products_tax_class_id", IntegerType, true),
      StructField("manufacturers_id", IntegerType, true),
      StructField("products_ordered", IntegerType, true),
      StructField("products_used", IntegerType, true),
      StructField("products_bundle", StringType, true),
      StructField("products_mature", StringType, true),
      StructField("products_wholesaleprice", DoubleType, true),
      StructField("products_promo_url", StringType, true),
      StructField("products_dev_url", StringType, true),
      StructField("products_cflfile", StringType, true),
      StructField("derivedfrom_products_id", IntegerType, true),
      StructField("denomination", IntegerType, true),
      StructField("account_product", StringType, true),
      StructField("derivation_allowed", StringType, true),
      StructField("products_custompage", StringType, true),
      StructField("products_avbundle", StringType, true),
      StructField("products_bonus", IntegerType, true)
    )
  )

  val products_row = (r: Array[String]) =>
    Row(
      Try(r(0).toInt).getOrElse(null),
      Try(r(1).toInt).getOrElse(null),
      r(2),
      r(3),
      Try(r(4).toDouble).getOrElse(null),
      r(5),
      r(6),
      r(7),
      Try(r(8).toDouble).getOrElse(null),
      Try(r(9).toInt).getOrElse(null),
      Try(r(10).toInt).getOrElse(null),
      Try(r(11).toInt).getOrElse(null),
      Try(r(12).toInt).getOrElse(null),
      Try(r(13).toInt).getOrElse(null),
      r(14),
      r(15),
      Try(r(16).toDouble).getOrElse(null),
      r(17),
      r(18),
      r(19),
      Try(r(20).toInt).getOrElse(null),
      Try(r(21).toInt).getOrElse(null),
      r(22),
      r(23),
      r(24),
      r(25),
      Try(r(26).toInt).getOrElse(null)
    )

  // products_description
  // val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/products_description/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/products_description", "20211126124235")
  // val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/products_description/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/products_description", "20211126131526")
  // compareTotals(dfduo.df1, dfduo.df2)
  // compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/products_description/2021-11-25/", "products_id")
  // compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/products_description/2021-11-25/", "products_id")

  val products_description_schema = StructType(
    Array(
      StructField("products_id", IntegerType, true),
      StructField("language_id", IntegerType, true),
      StructField("products_name", StringType, true),
      StructField("products_description", StringType, true),
      StructField("products_url", StringType, true),
      StructField("products_viewed", IntegerType, true),
      StructField("products_dev_url", StringType, true)
    )
  )

  val products_description_row = (r: Array[String]) =>
    Row(
      Try(r(0).toInt).getOrElse(null),
      Try(r(1).toInt).getOrElse(null),
      r(2),
      r(3),
      r(4),
      Try(r(5).toInt).getOrElse(null),
      r(6)
    )

  // logical_uri_mapping
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/logical_uri_mapping/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/logical_uri_mapping", "20211126094403")
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/logical_uri_mapping/2021-11-25/", "id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/logical_uri_mapping/2021-11-25/", "id")

  val logical_uri_mapping_schema = StructType(
    Array(
      StructField("id", IntegerType, true),
      StructField("logical_uri", StringType, true),
      StructField("creation", StringType, true),
      StructField("logical_uri_available_shards_id", IntegerType, true)
    )
  )

  val logical_uri_mapping_row = (r: Array[String]) =>
    Row(Try(r(0).toInt).getOrElse(null), r(1), r(2), Try(r(3).toInt).getOrElse(null))

  // orders_products
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/orders_products/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/orders_products", "20211126092732")
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/orders_products/2021-11-25/", "orders_products_id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/orders_products/2021-11-25/", "orders_products_id")
  val orders_products_schema = StructType(
    Array(
      StructField("orders_products_id", LongType, true),
      StructField("orders_id", IntegerType, true),
      StructField("products_id", IntegerType, true),
      StructField("products_model", StringType, true),
      StructField("products_name", StringType, true),
      StructField("products_price", DoubleType, true),
      StructField("final_price", DoubleType, true),
      StructField("products_tax", DoubleType, true),
      StructField("products_quantity", IntegerType, true)
    )
  )

  val orders_products_row = (r: Array[String]) =>
    Row(
      Try(r(0).toLong).getOrElse(null),
      Try(r(1).toInt).getOrElse(null),
      Try(r(2).toInt).getOrElse(null),
      r(3),
      r(4),
      Try(r(5).toDouble).getOrElse(null),
      Try(r(6).toDouble).getOrElse(null),
      Try(r(7).toDouble).getOrElse(null),
      Try(r(8).toInt).getOrElse(null)
    )

  // customers_info
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/customers_info/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/customers_info", "20211126094225")
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/customers_info/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/customers_info", "20211126091857")
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/customers_info/2021-11-25/", "customers_info_id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/customers_info/2021-11-25/", "customers_info_id")
  val customers_info_schema = StructType(
    Array(
      StructField("customers_info_id", IntegerType, true),
      StructField("customers_info_date_of_last_logon", StringType, true),
      StructField("customers_info_date_download_page", StringType, true),
      StructField("customers_info_date_installer_download", StringType, true),
      StructField("customers_info_date_installer_complete", StringType, true),
      StructField("customers_info_number_of_logons", IntegerType, true),
      StructField("customers_info_number_of_client_logons", IntegerType, true),
      StructField("customers_info_date_of_last_client_logon", StringType, true),
      StructField("customers_info_number_of_product_downloads", IntegerType, true),
      StructField("customers_info_date_of_last_product_download", StringType, true),
      StructField("customers_info_date_account_created", StringType, true),
      StructField("customers_info_date_account_last_modified", StringType, true),
      StructField("customers_info_registration_user_agent", StringType, true),
      StructField("customers_info_stores_id", IntegerType, true),
      StructField("global_product_notifications", IntegerType, true),
      StructField("last_time_sushiboat_product_viewed", StringType, true),
      StructField("number_of_sushiboat_products_viewed", IntegerType, true),
      StructField("number_of_products_tried_on", IntegerType, true),
      StructField("last_time_product_tried_on", StringType, true),
      StructField("number_of_conversations", IntegerType, true),
      StructField("date_of_last_conversation", StringType, true),
      StructField("funnel_stage", StringType, true),
      StructField("funnel_time", StringType, true),
      StructField("remider_stage_last_sent", StringType, true),
      StructField("reminder_date_sent", StringType, true)
    )
  )

  val customers_info_row = (r: Array[String]) =>
    Row(
      Try(r(0).toInt).getOrElse(null),
      r(1),
      r(2),
      r(3),
      r(4),
      Try(r(5).toInt).getOrElse(null),
      Try(r(6).toInt).getOrElse(null),
      r(7),
      Try(r(8).toInt).getOrElse(null),
      r(9),
      r(10),
      r(11),
      r(12),
      Try(r(13).toInt).getOrElse(null),
      Try(r(14).toInt).getOrElse(null),
      r(15),
      Try(r(16).toInt).getOrElse(null),
      Try(r(17).toInt).getOrElse(null),
      r(18),
      Try(r(19).toInt).getOrElse(null),
      r(20),
      r(21),
      r(22),
      r(23),
      r(24)
    )

  //royalty_escrows
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/royalty_escrows/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/royalty_escrows", "20211126130517", Schema.royalty_escrows_schema, Schema.royalty_escrows_row)
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/royalty_escrows/2021-11-25/", "royalty_escrows_id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/royalty_escrows/2021-11-25/", "royalty_escrows_id")
  val royalty_escrows_schema = StructType(
    Array(
      StructField("royalty_escrows_id", LongType, true),
      StructField("customers_id", LongType, true),
      StructField("creation_date", StringType, true),
      StructField("delivery_date", StringType, true),
      StructField("productsales_log_id", StringType, true),
      StructField("amount", DoubleType, true),
      StructField("status", IntegerType, true),
      StructField("reason", StringType, true),
      StructField("last_modified", StringType, true)
    )
  )

  val royalty_escrows_row = (r: Array[String]) =>
    Row(
      Try(r(0).toLong).getOrElse(null),
      Try(r(1).toLong).getOrElse(null),
      r(2),
      r(3),
      r(4),
      Try(r(5).toDouble).getOrElse(null),
      Try(r(6).toInt).getOrElse(null),
      r(7),
      r(8)
    )

  //customers_modifybalance_log
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/customers_modifybalance_log/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/customers_modifybalance_log", "20211126131606")
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/customers_modifybalance_log/2021-11-25/", "customers_modifybalance_log_id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/customers_modifybalance_log/2021-11-25/", "customers_modifybalance_log_id")
  val customers_modifybalance_log_schema = StructType(
    Array(
      StructField("customers_modifybalance_log_id", LongType, true),
      StructField("customers_id", IntegerType, true),
      StructField("amount", IntegerType, true),
      StructField("wealth_before", IntegerType, true),
      StructField("wealth_after", IntegerType, true),
      StructField("free_wealth_before", IntegerType, true),
      StructField("free_wealth_after", IntegerType, true),
      StructField("reason", StringType, true),
      StructField("data", StringType, true),
      StructField("date", StringType, true),
      StructField("event_type", IntegerType, true),
      StructField("platform", IntegerType, true)
    )
  )

  val customers_modifybalance_log_row = (r: Array[String]) =>
    Row(
      Try(r(0).toLong).getOrElse(null),
      Try(r(1).toInt).getOrElse(null),
      Try(r(2).toInt).getOrElse(null),
      Try(r(3).toInt).getOrElse(null),
      Try(r(4).toInt).getOrElse(null),
      Try(r(5).toInt).getOrElse(null),
      Try(r(6).toInt).getOrElse(null),
      r(7),
      r(8),
      r(9),
      Try(r(10).toInt).getOrElse(null),
      Try(r(11).toInt).getOrElse(null)
    )

  //customers
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/customers/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/customers", "20211126093928")
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/customers/2021-11-25/", "customers_id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/customers/2021-11-25/", "customers_id")
  val customers_schema = StructType(
    Array(
      StructField("customers_id", IntegerType, true),
      StructField("customers_ip_address", StringType, true),
      StructField("customers_gender", StringType, true),
      StructField("customers_firstname", StringType, true),
      StructField("customers_lastname", StringType, true),
      StructField("customers_dob", StringType, true),
      StructField("customers_country", StringType, true),
      StructField("customers_state", StringType, true),
      StructField("customers_email_address", StringType, true),
      StructField("customers_default_address_id", IntegerType, true),
      StructField("customers_telephone", StringType, true),
      StructField("customers_fax", StringType, true),
      StructField("customers_password", StringType, true),
      StructField("customers_newsletter", StringType, true),
      StructField("IS_MATURE", IntegerType, true),
      StructField("customers_avatarname", StringType, true),
      StructField("customers_wealth", IntegerType, true),
      StructField("customers_free_wealth", IntegerType, true),
      StructField("customers_nameregistered", StringType, true),
      StructField("customers_referred_by", IntegerType, true),
      StructField("customers_needreminding", StringType, true),
      StructField("customers_acquired_by", StringType, true),
      StructField("customers_start_dna", StringType, true),
      StructField("customers_end_dna", StringType, true),
      StructField("customers_securitykey", StringType, true),
      StructField("is_admin", StringType, true),
      StructField("wants_mail", StringType, true),
      StructField("show_sub_cats", IntegerType, true),
      StructField("removed_from_help_by", IntegerType, true),
      StructField("account_enabled", IntegerType, true),
      StructField("survey", IntegerType, true),
      StructField("customers_first_convo", IntegerType, true),
      StructField("faf_visible", IntegerType, true),
      StructField("customers_last_chk_msg", StringType, true),
      StructField("customers_last_rewards_mail", StringType, true),
      StructField("proxy_inventory_build_date", StringType, true),
      StructField("is_scam_artist", IntegerType, true),
      StructField("invite_emails_sent", IntegerType, true),
      StructField("has_try_pass", IntegerType, true),
      StructField("customers_hide_help", IntegerType, true)
    )
  )

  val customers_row = (r: Array[String]) =>
    Row(
      Try(r(0).toInt).getOrElse(null),
      r(1),
      r(2),
      r(3),
      r(4),
      r(5),
      r(6),
      r(7),
      r(8),
      Try(r(9).toInt).getOrElse(null),
      r(10),
      r(11),
      r(12),
      r(13),
      Try(r(14).toInt).getOrElse(null),
      r(15),
      Try(r(16).toInt).getOrElse(null),
      Try(r(17).toInt).getOrElse(null),
      r(18),
      Try(r(19).toInt).getOrElse(null),
      r(20),
      r(21),
      r(22),
      r(23),
      r(24),
      r(25),
      r(26),
      Try(r(27).toInt).getOrElse(null),
      Try(r(28).toInt).getOrElse(null),
      Try(r(29).toInt).getOrElse(null),
      Try(r(30).toInt).getOrElse(null),
      Try(r(31).toInt).getOrElse(null),
      Try(r(32).toInt).getOrElse(null),
      r(33),
      r(34),
      r(35),
      Try(r(36).toInt).getOrElse(null),
      Try(r(37).toInt).getOrElse(null),
      Try(r(38).toInt).getOrElse(null),
      Try(r(39).toInt).getOrElse(null)
    )

  //orders
  //val dfduo = readDFs("s3://production-cdc-hudi/mysql_dump/2021-11-25/orders/", "s3://production-cdc-hudi/clean/5n_dm_3/osCommerse/orders", "20211126093240")
  //compareTotals(dfduo.df1, dfduo.df2)
  //compareNumericColumns(1, 10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/orders/2021-11-25/", "orders_id")
  //compareNonNumericColumns(10)(dfduo.df1, dfduo.df2, "s3://production-cdc-hudi/validation_result/orders/2021-11-25/", "orders_id")
  val orders_schema = StructType(
    Array(
      StructField("orders_id", IntegerType, true),
      StructField("customers_id", IntegerType, true),
      StructField("customers_name", StringType, true),
      StructField("customers_company", StringType, true),
      StructField("customers_street_address", StringType, true),
      StructField("customers_suburb", StringType, true),
      StructField("customers_city", StringType, true),
      StructField("customers_postcode", StringType, true),
      StructField("customers_state", StringType, true),
      StructField("customers_country", StringType, true),
      StructField("customers_telephone", StringType, true),
      StructField("customers_email_address", StringType, true),
      StructField("customers_address_format_id", IntegerType, true),
      StructField("delivery_name", StringType, true),
      StructField("delivery_company", StringType, true),
      StructField("delivery_street_address", StringType, true),
      StructField("delivery_suburb", StringType, true),
      StructField("delivery_city", StringType, true),
      StructField("delivery_postcode", StringType, true),
      StructField("delivery_state", StringType, true),
      StructField("delivery_country", StringType, true),
      StructField("delivery_address_format_id", IntegerType, true),
      StructField("billing_name", StringType, true),
      StructField("billing_company", StringType, true),
      StructField("billing_street_address", StringType, true),
      StructField("billing_suburb", StringType, true),
      StructField("billing_city", StringType, true),
      StructField("billing_postcode", StringType, true),
      StructField("billing_state", StringType, true),
      StructField("billing_country", StringType, true),
      StructField("billing_address_format_id", IntegerType, true),
      StructField("payment_method", StringType, true),
      StructField("cc_type", StringType, true),
      StructField("cc_owner", StringType, true),
      StructField("cc_number", StringType, true),
      StructField("cc_expires", StringType, true),
      StructField("last_modified", StringType, true),
      StructField("date_purchased", StringType, true),
      StructField("orders_status", IntegerType, true),
      StructField("orders_date_finished", StringType, true),
      StructField("orders_stores_id", IntegerType, true),
      StructField("currency", StringType, true),
      StructField("currency_value", DoubleType, true),
      StructField("paypal_ipn_id", IntegerType, true),
      StructField("reminders_sent", IntegerType, true),
      StructField("previous_reminder_time", StringType, true),
      StructField("is_gift", IntegerType, true),
      StructField("recipient_name", StringType, true),
      StructField("recipient_id", IntegerType, true),
      StructField("buy_for", IntegerType, true)
    )
  )

  val orders_row = (r: Array[String]) =>
    Row(
      Try(r(0).toInt).getOrElse(null),
      Try(r(1).toInt).getOrElse(null),
      r(2),
      r(3),
      r(4),
      r(5),
      r(6),
      r(7),
      r(8),
      r(9),
      r(10),
      r(11),
      Try(r(12).toInt).getOrElse(null),
      r(13),
      r(14),
      r(15),
      r(16),
      r(17),
      r(18),
      r(19),
      r(20),
      Try(r(21).toInt).getOrElse(null),
      r(22),
      r(23),
      r(24),
      r(25),
      r(26),
      r(27),
      r(28),
      r(29),
      Try(r(30).toInt).getOrElse(null),
      r(31),
      r(32),
      r(33),
      r(34),
      r(35),
      r(36),
      r(37),
      Try(r(38).toInt).getOrElse(null),
      r(39),
      Try(r(40).toInt).getOrElse(null),
      r(41),
      Try(r(42).toDouble).getOrElse(null),
      Try(r(43).toInt).getOrElse(null),
      Try(r(44).toInt).getOrElse(null),
      r(45),
      Try(r(46).toInt).getOrElse(null),
      r(47),
      Try(r(48).toInt).getOrElse(null),
      Try(r(49).toInt).getOrElse(null)
    )
}
